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

//! StarRocks role-binding factories.
//!
//! Control materialization owns only metadata generation construction. The
//! current connector has no typed worker read or write contract, so its
//! execution factory publishes an explicit all-`None` binding.

use std::sync::Arc;

use futures::future::BoxFuture;
use novarocks_connector_binding::{
    ConnectorControlRoleBinding, ConnectorControlRoleBindingFactory, ConnectorExecutionRoleBinding,
    ConnectorExecutionRoleBindingFactory, ConnectorMaterializationError,
    ConnectorMaterializationErrorClass, ConnectorMaterializationRetryDisposition,
    MaterializationContext, NormalizedCatalogProperties,
};
use novarocks_spi::connector::{CatalogProperties, CatalogProviderKind};

use crate::STARROCKS_PROVIDER_ID;
use crate::control::{StarRocksControlGeneration, StarRocksMetadataSource};
use crate::domain::StarRocksLocalBindingRef;

/// FE-only StarRocks control factory. Metadata I/O stays behind the returned
/// control generation and uses each metadata request's existing context.
#[derive(Clone)]
pub struct StarRocksControlRoleBindingFactory {
    metadata: Arc<dyn StarRocksMetadataSource>,
    local_binding: StarRocksLocalBindingRef,
}

impl StarRocksControlRoleBindingFactory {
    pub fn new(
        metadata: Arc<dyn StarRocksMetadataSource>,
        local_binding: StarRocksLocalBindingRef,
    ) -> Self {
        Self {
            metadata,
            local_binding,
        }
    }
}

impl ConnectorControlRoleBindingFactory for StarRocksControlRoleBindingFactory {
    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::StarRocks
    }

    fn normalize_and_validate(
        &self,
        properties: CatalogProperties,
    ) -> Result<NormalizedCatalogProperties, ConnectorMaterializationError> {
        ensure_starrocks(&properties)?;
        NormalizedCatalogProperties::try_new(properties).map_err(invalid_definition)
    }

    fn materialize(
        &self,
        properties: NormalizedCatalogProperties,
        context: MaterializationContext,
    ) -> BoxFuture<'static, Result<ConnectorControlRoleBinding, ConnectorMaterializationError>>
    {
        let metadata = Arc::clone(&self.metadata);
        let local_binding = self.local_binding.clone();
        Box::pin(async move {
            context.check_active()?;
            let catalog_properties = properties.as_catalog_properties().clone();
            ensure_starrocks(&catalog_properties)?;
            let control = StarRocksControlGeneration::try_new(
                crate::StarRocksConnectorConfig::new(
                    catalog_properties.handle().catalog_name().clone(),
                    local_binding,
                ),
                metadata,
            )
            .and_then(|binding| binding.with_catalog_properties(catalog_properties))
            .map_err(ConnectorMaterializationError::from)?;
            context.check_active()?;
            ConnectorControlRoleBinding::try_new(properties, Arc::new(control), None, None)
                .map_err(ConnectorMaterializationError::from)
        })
    }
}

/// BE-only StarRocks factory. It carries no execution capability until a
/// separately accepted StarRocks read execution contract exists.
#[derive(Default)]
pub struct StarRocksExecutionRoleBindingFactory;

impl StarRocksExecutionRoleBindingFactory {
    pub const fn new() -> Self {
        Self
    }
}

impl ConnectorExecutionRoleBindingFactory for StarRocksExecutionRoleBindingFactory {
    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::StarRocks
    }

    fn bind(
        &self,
        properties: &NormalizedCatalogProperties,
    ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
        ensure_starrocks(properties.as_catalog_properties())?;
        ConnectorExecutionRoleBinding::try_new(properties.clone(), None, None, None)
            .map_err(ConnectorMaterializationError::from)
    }
}

fn ensure_starrocks(properties: &CatalogProperties) -> Result<(), ConnectorMaterializationError> {
    if properties.provider_kind() == CatalogProviderKind::StarRocks {
        return Ok(());
    }
    Err(ConnectorMaterializationError::new(
        ConnectorMaterializationErrorClass::InvalidDefinition,
        ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
        format!("{STARROCKS_PROVIDER_ID} role binding factory received another provider kind"),
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
    use std::time::{Duration, Instant};

    use super::*;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, ConnectorRequestContext,
    };

    struct NoRemoteMetadata;

    impl StarRocksMetadataSource for NoRemoteMetadata {
        fn namespace_exists(
            &self,
            _namespace: &str,
            _context: &ConnectorRequestContext,
        ) -> Result<bool, novarocks_spi::connector::ConnectorError> {
            panic!("control role materialization must not call StarRocks metadata")
        }

        fn table_exists(
            &self,
            _namespace: &str,
            _table: &str,
            _context: &ConnectorRequestContext,
        ) -> Result<bool, novarocks_spi::connector::ConnectorError> {
            panic!("control role materialization must not call StarRocks metadata")
        }

        fn list_tables(
            &self,
            _namespace: &str,
            _context: &ConnectorRequestContext,
        ) -> Result<Vec<String>, novarocks_spi::connector::ConnectorError> {
            panic!("control role materialization must not call StarRocks metadata")
        }

        fn load_table(
            &self,
            _namespace: &str,
            _table: &str,
            _context: &ConnectorRequestContext,
        ) -> Result<crate::StarRocksResolvedTable, novarocks_spi::connector::ConnectorError>
        {
            panic!("control role materialization must not call StarRocks metadata")
        }
    }

    fn properties(kind: CatalogProviderKind) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("catalog.starrocks").expect("catalog"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            kind,
            1,
            Vec::new(),
            Vec::new(),
        )
        .expect("catalog properties")
    }

    #[test]
    fn control_materialization_stamps_the_exact_properties_without_metadata_io() {
        let factory = StarRocksControlRoleBindingFactory::new(
            Arc::new(NoRemoteMetadata),
            StarRocksLocalBindingRef::parse("default").expect("local binding"),
        );
        let normalized = factory
            .normalize_and_validate(properties(CatalogProviderKind::StarRocks))
            .expect("normalize StarRocks properties");
        let binding = futures::executor::block_on(factory.materialize(
            normalized.clone(),
            MaterializationContext::new(Instant::now() + Duration::from_secs(1)),
        ))
        .expect("materialize local StarRocks control generation");

        assert_eq!(binding.properties(), &normalized);
        assert_eq!(
            binding.control().catalog_handle().expect("exact handle"),
            normalized.handle()
        );
        assert!(binding.read().is_none());
        assert!(binding.write().is_none());
    }

    #[test]
    fn execution_factory_publishes_no_starrocks_capability_until_its_typed_contract_exists() {
        let normalized =
            NormalizedCatalogProperties::try_new(properties(CatalogProviderKind::StarRocks))
                .expect("normalized StarRocks properties");

        let binding = StarRocksExecutionRoleBindingFactory::new()
            .bind(&normalized)
            .expect("StarRocks has an explicit capability-free local binding");

        assert_eq!(binding.properties(), &normalized);
        assert!(binding.execution().is_none());
        assert!(binding.read().is_none());
        assert!(binding.write().is_none());
    }
}
