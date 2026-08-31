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

//! Iceberg's complete FE and BE role-binding factories.
//!
//! The control factory is the only provider entry point allowed to construct
//! the catalog runtime.  The execution factory accepts only a frozen catalog
//! definition and builds local, lazy reader and writer factories; it does not
//! create a catalog client or contact REST, HMS, or object storage.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use novarocks_connector_binding::{
    ConnectorControlReadBinding, ConnectorControlRoleBinding, ConnectorControlRoleBindingFactory,
    ConnectorControlWriteBinding, ConnectorExecutionReadBinding, ConnectorExecutionRoleBinding,
    ConnectorExecutionRoleBindingFactory, ConnectorExecutionWriteBinding,
    ConnectorMaterializationError, ConnectorMaterializationErrorClass,
    ConnectorMaterializationRetryDisposition, MaterializationContext, NormalizedCatalogProperties,
};
use novarocks_spi::connector::read_stack::ConnectorReadRegistrationLease;
use novarocks_spi::connector::{
    CatalogProperties, CatalogProviderKind, CatalogWriteExecutionBundleFactory,
    ConnectorControlFactory, ConnectorControlFactoryRequest, ConnectorError, ConnectorErrorKind,
};

use crate::IcebergCatalogWriteExecutionFactory;
use crate::connector_factory::IcebergConnectorFactory;
use crate::file_reader::execution_installer::IcebergExecutionBindingFactory;
use crate::resources::{IcebergExecutionResources, IcebergMetadataResources};
use crate::typed_provider_factory::IcebergTypedProviderFactory;
use crate::typed_read::page_source_provider::IcebergPageSourceProviderOptions;

/// FE-only factory for one exact Iceberg control generation.
#[derive(Clone)]
pub struct IcebergControlRoleBindingFactory {
    resources: IcebergMetadataResources,
    blocking_materialization_permits: Arc<tokio::sync::Semaphore>,
}

impl IcebergControlRoleBindingFactory {
    pub fn new(
        resources: IcebergMetadataResources,
        max_blocking_materializations: NonZeroUsize,
    ) -> Self {
        Self {
            resources,
            blocking_materialization_permits: Arc::new(tokio::sync::Semaphore::new(
                max_blocking_materializations.get(),
            )),
        }
    }
}

impl ConnectorControlRoleBindingFactory for IcebergControlRoleBindingFactory {
    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::Iceberg
    }

    fn normalize_and_validate(
        &self,
        properties: CatalogProperties,
    ) -> Result<NormalizedCatalogProperties, ConnectorMaterializationError> {
        ensure_iceberg(&properties)?;
        NormalizedCatalogProperties::try_new(properties).map_err(invalid_definition)
    }

    fn materialize(
        &self,
        properties: NormalizedCatalogProperties,
        context: MaterializationContext,
    ) -> BoxFuture<'static, Result<ConnectorControlRoleBinding, ConnectorMaterializationError>>
    {
        let resources = self.resources.clone();
        let permits = Arc::clone(&self.blocking_materialization_permits);
        Box::pin(async move {
            context.check_active()?;
            // `JoinHandle` drop cannot stop `spawn_blocking`. Move this permit
            // into the closure so a timed-out attempt continues to occupy one
            // bounded slot until its actual blocking work exits; retries can
            // wait, but they cannot accumulate detached workers or sockets.
            let permit = permits.acquire_owned().await.map_err(|_| {
                ConnectorMaterializationError::new(
                    ConnectorMaterializationErrorClass::Internal,
                    ConnectorMaterializationRetryDisposition::Transient,
                    "Iceberg control materialization limiter is closed",
                )
            })?;
            context.check_active()?;
            let worker_context = context.clone();
            let binding = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                materialize_control_blocking(resources, properties, worker_context)
            })
            .await
            .map_err(|error| {
                ConnectorMaterializationError::new(
                    ConnectorMaterializationErrorClass::Internal,
                    ConnectorMaterializationRetryDisposition::Transient,
                    format!("Iceberg control materialization worker failed: {error}"),
                )
            })??;
            context.check_active()?;
            Ok(binding)
        })
    }
}

fn materialize_control_blocking(
    resources: IcebergMetadataResources,
    properties: NormalizedCatalogProperties,
    context: MaterializationContext,
) -> Result<ConnectorControlRoleBinding, ConnectorMaterializationError> {
    context.check_active()?;
    let catalog_properties = properties.as_catalog_properties().clone();
    ensure_iceberg(&catalog_properties)?;

    // The existing provider factory owns the catalog runtime and all
    // control capabilities. Its read installer is redirected into the
    // complete role group rather than a parallel FE registry.
    let captured_read = Arc::new(Mutex::new(None));
    let capture = Arc::clone(&captured_read);
    let factory = IcebergConnectorFactory::new(resources).with_read_control_installer(Arc::new(
        move |_handle, metadata, splits, encoder, request_factory| {
            let read =
                ConnectorControlReadBinding::new(metadata, splits, Some(request_factory), encoder);
            let mut slot = capture.lock().map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Iceberg role binding read capture lock was poisoned",
                )
            })?;
            if slot.replace(read).is_some() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Iceberg control generation installed more than one typed read group",
                ));
            }
            Ok(Arc::new(RoleReadRegistrationLease))
        },
    ));
    let request = ConnectorControlFactoryRequest::try_new(
        factory.provider_id().clone(),
        catalog_properties.handle().catalog_name().clone(),
        catalog_properties
            .execution_properties()
            .iter()
            .map(|property| (property.key().to_owned(), property.value().to_owned()))
            .collect(),
    )
    .and_then(|request| request.with_catalog_properties(catalog_properties.clone()))
    .map_err(ConnectorMaterializationError::from)?;
    let creation = factory
        .create_control(request)
        .map_err(ConnectorMaterializationError::from)?;
    context.check_active()?;
    let (control, _durable_properties) = creation.into_parts();
    let control = control
        .with_catalog_properties(catalog_properties)
        .map_err(ConnectorMaterializationError::from)?;
    let read = captured_read
        .lock()
        .map_err(|_| {
            ConnectorMaterializationError::new(
                ConnectorMaterializationErrorClass::Internal,
                ConnectorMaterializationRetryDisposition::Transient,
                "Iceberg role binding read capture lock was poisoned",
            )
        })?
        .take()
        .ok_or_else(|| {
            ConnectorMaterializationError::new(
                ConnectorMaterializationErrorClass::Internal,
                ConnectorMaterializationRetryDisposition::Transient,
                "Iceberg control generation did not install its typed read group",
            )
        })?;
    let write = control
        .write()
        .cloned()
        .map(ConnectorControlWriteBinding::new);
    context.check_active()?;
    ConnectorControlRoleBinding::try_new(properties, Arc::new(control), Some(read), write)
        .map_err(ConnectorMaterializationError::from)
}

/// BE-only factory for a frozen Iceberg catalog definition.
///
/// Its only inputs are the startup-composed local resources and immutable
/// properties. Reader and writer network I/O remains deferred to their
/// request-scoped operations.
#[derive(Clone)]
pub struct IcebergExecutionRoleBindingFactory {
    resources: IcebergExecutionResources,
    read_options: IcebergPageSourceProviderOptions,
}

impl IcebergExecutionRoleBindingFactory {
    pub fn new(
        resources: IcebergExecutionResources,
        read_options: IcebergPageSourceProviderOptions,
    ) -> Self {
        Self {
            resources,
            read_options,
        }
    }
}

impl ConnectorExecutionRoleBindingFactory for IcebergExecutionRoleBindingFactory {
    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::Iceberg
    }

    fn bind(
        &self,
        properties: &NormalizedCatalogProperties,
    ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
        let catalog_properties = properties.as_catalog_properties();
        ensure_iceberg(catalog_properties)?;

        // `build` only binds startup-sealed filesystem resources to the exact
        // immutable catalog definition and constructs lazy reader factories.
        let typed_read = IcebergTypedProviderFactory::new(
            self.resources.binding().clone(),
            self.read_options.clone(),
        )
        .build(catalog_properties)
        .map_err(ConnectorMaterializationError::from)?;
        let execution = IcebergExecutionBindingFactory::new(self.resources.clone())
            .bind_for_catalog_properties(catalog_properties)
            .map_err(ConnectorMaterializationError::from)?;
        let typed_write = IcebergCatalogWriteExecutionFactory::new(
            self.resources.binding().clone(),
            self.resources.runtime().clone(),
        )
        .build(catalog_properties)
        .map_err(ConnectorMaterializationError::from)?;
        let read =
            ConnectorExecutionReadBinding::new(typed_read.provider_factory(), typed_read.decoder());
        let write = ConnectorExecutionWriteBinding::new(typed_write.execution());
        ConnectorExecutionRoleBinding::try_new(
            properties.clone(),
            Some(execution),
            Some(read),
            Some(write),
        )
        .map_err(ConnectorMaterializationError::from)
    }
}

/// Marker retained by the control generation while the named read group is
/// alive. The role host, rather than this provider, owns registry retirement.
struct RoleReadRegistrationLease;

impl ConnectorReadRegistrationLease for RoleReadRegistrationLease {}

fn ensure_iceberg(properties: &CatalogProperties) -> Result<(), ConnectorMaterializationError> {
    if properties.provider_kind() == CatalogProviderKind::Iceberg {
        return Ok(());
    }
    Err(ConnectorMaterializationError::new(
        ConnectorMaterializationErrorClass::InvalidDefinition,
        ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
        "Iceberg role binding factory received another provider kind",
    ))
}

fn invalid_definition(detail: String) -> ConnectorMaterializationError {
    ConnectorMaterializationError::new(
        ConnectorMaterializationErrorClass::InvalidDefinition,
        ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
        detail,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperty, CatalogVersion, ConnectorInstanceId,
    };

    fn properties() -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("catalog.iceberg").expect("catalog"),
                CatalogVersion::from_bytes([9; 32]),
            ),
            CatalogProviderKind::Iceberg,
            1,
            Vec::new(),
            Vec::new(),
        )
        .expect("catalog properties")
    }

    #[test]
    fn execution_binding_is_complete_and_local_for_frozen_properties() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let access = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergExecutionRoleBindingFactory::new(
            IcebergExecutionResources::new(access, runtime.handle().clone()),
            IcebergPageSourceProviderOptions::with_default_budget(),
        );
        let normalized = NormalizedCatalogProperties::try_new(properties())
            .expect("normalized Iceberg properties");

        let binding = factory
            .bind(&normalized)
            .expect("local role binding without catalog I/O");

        assert_eq!(binding.properties(), &normalized);
        assert_eq!(
            binding
                .execution()
                .expect("legacy execution facets")
                .provider_id()
                .as_str(),
            CatalogProviderKind::Iceberg.provider_id()
        );
        assert_eq!(
            binding
                .execution()
                .expect("legacy execution facets")
                .key()
                .instance_id,
            *normalized.handle().catalog_name()
        );
        assert!(
            binding
                .execution()
                .expect("legacy execution facets")
                .read()
                .is_some()
        );
        assert!(
            binding
                .execution()
                .expect("legacy execution facets")
                .write()
                .is_some()
        );
        assert!(binding.read().is_some());
        assert!(binding.write().is_some());
    }

    #[tokio::test]
    async fn control_materialization_captures_one_request_scoped_read_group() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let access = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(tokio::runtime::Handle::current())),
            Arc::new(TokioFileTaskSpawner::new(tokio::runtime::Handle::current())),
        );
        let factory = IcebergControlRoleBindingFactory::new(
            IcebergMetadataResources::new(access, tokio::runtime::Handle::current()),
            NonZeroUsize::new(1).expect("nonzero blocking materialization limit"),
        );
        let catalog_properties = CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("catalog.iceberg").expect("catalog"),
                CatalogVersion::from_bytes([8; 32]),
            ),
            CatalogProviderKind::Iceberg,
            1,
            vec![
                CatalogProperty::new(
                    "iceberg.catalog.warehouse",
                    warehouse.path().display().to_string(),
                )
                .expect("warehouse property"),
            ],
            Vec::new(),
        )
        .expect("catalog properties");
        let normalized = factory
            .normalize_and_validate(catalog_properties)
            .expect("normalize Iceberg properties");

        let binding = factory
            .materialize(
                normalized.clone(),
                MaterializationContext::new(
                    std::time::Instant::now() + std::time::Duration::from_secs(5),
                ),
            )
            .await
            .expect("materialize Iceberg control role");

        assert_eq!(binding.properties(), &normalized);
        assert_eq!(
            binding.control().catalog_handle().expect("exact handle"),
            normalized.handle()
        );
        let read = binding.read().expect("one complete typed read group");
        assert!(read.request_factory().is_some());
        assert_eq!(
            read.encoder().owner(),
            normalized.handle().catalog_name().as_str()
        );
    }
}
