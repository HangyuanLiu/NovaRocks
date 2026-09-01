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

//! Server-owned StarRocks role-binding composition.
//!
//! Role-binding factories combine a provider implementation with the generic
//! binding contract and are therefore composition-root glue. Keeping this
//! module in Server lets the StarRocks provider remain below the native wire
//! layer; its current execution binding deliberately publishes no capability.

use std::sync::Arc;

use crate::app_config::StarRocksLocalBindingRegistry;

use futures::future::BoxFuture;
use novarocks_connector_binding::{
    ConnectorControlRoleBinding, ConnectorControlRoleBindingFactory, ConnectorExecutionRoleBinding,
    ConnectorExecutionRoleBindingFactory, ConnectorMaterializationError,
    ConnectorMaterializationErrorClass, ConnectorMaterializationRetryDisposition,
    MaterializationContext, NormalizedCatalogProperties,
};
use novarocks_connector_starrocks::{
    STARROCKS_PROVIDER_ID, StarRocksConnectorConfig, StarRocksControlGeneration,
    StarRocksLocalBindingRef,
};
use novarocks_spi::connector::{CatalogProperties, CatalogProviderKind};

/// FE-only StarRocks control factory. Metadata I/O stays behind the returned
/// control generation and uses each metadata request's existing context.
#[derive(Clone)]
pub struct StarRocksControlRoleBindingFactory {
    local_bindings: StarRocksLocalBindingRegistry,
}

impl StarRocksControlRoleBindingFactory {
    pub(crate) fn new(local_bindings: StarRocksLocalBindingRegistry) -> Self {
        Self { local_bindings }
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
        starrocks_local_binding(&properties)?;
        NormalizedCatalogProperties::try_new(properties).map_err(invalid_definition)
    }

    fn materialize(
        &self,
        properties: NormalizedCatalogProperties,
        context: MaterializationContext,
    ) -> BoxFuture<'static, Result<ConnectorControlRoleBinding, ConnectorMaterializationError>>
    {
        let local_bindings = self.local_bindings.clone();
        Box::pin(async move {
            context.check_active()?;
            let catalog_properties = properties.as_catalog_properties().clone();
            ensure_starrocks(&catalog_properties)?;
            let local_binding = starrocks_local_binding(&catalog_properties)?;
            let metadata = local_bindings.resolve(&local_binding).map_err(|error| {
                ConnectorMaterializationError::new(
                    ConnectorMaterializationErrorClass::InvalidDefinition,
                    ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    error.to_string(),
                )
            })?;
            let control = StarRocksControlGeneration::try_new(
                StarRocksConnectorConfig::new(
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

fn starrocks_local_binding(
    properties: &CatalogProperties,
) -> Result<StarRocksLocalBindingRef, ConnectorMaterializationError> {
    const LOCAL_BINDING_PROPERTY: &str = "local_binding";

    let mut matches = properties
        .execution_properties()
        .iter()
        .filter(|property| property.key() == LOCAL_BINDING_PROPERTY);
    let Some(property) = matches.next() else {
        return Err(invalid_definition(
            "StarRocks catalog definition requires a local_binding property",
        ));
    };
    if matches.next().is_some() {
        return Err(invalid_definition(
            "StarRocks catalog definition declares duplicate local_binding properties",
        ));
    }
    StarRocksLocalBindingRef::parse(property.value()).map_err(|error| {
        invalid_definition(format!(
            "invalid StarRocks local_binding catalog property: {error}"
        ))
    })
}

fn invalid_definition(detail: impl Into<String>) -> ConnectorMaterializationError {
    ConnectorMaterializationError::new(
        ConnectorMaterializationErrorClass::InvalidDefinition,
        ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
        detail.into(),
    )
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperty, CatalogVersion, ConnectorCancellation, ConnectorInstanceId,
        ConnectorRequestContext,
    };

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            64 * 1024,
            128 * 1024,
        )
        .expect("request context")
    }

    fn properties(
        instance_id: &str,
        kind: CatalogProviderKind,
        local_binding: Option<&str>,
    ) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse(instance_id).expect("catalog"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            kind,
            1,
            local_binding
                .map(|value| vec![CatalogProperty::new("local_binding", value).expect("property")])
                .unwrap_or_default(),
            Vec::new(),
        )
        .expect("catalog properties")
    }

    fn control_factory() -> StarRocksControlRoleBindingFactory {
        let config: crate::app_config::NovaRocksConfig = toml::from_str(
            r#"
[[connector.starrocks.local_bindings]]
local_binding = "metadata-blue"
endpoints = ["http://127.0.0.1:9"]
username = "admin"
password = "test-password"
request_timeout_ms = 1000
retry_count = 0

[[connector.starrocks.local_bindings]]
local_binding = "metadata-green"
endpoints = ["http://127.0.0.1:10"]
username = "admin"
password = "test-password"
request_timeout_ms = 1000
retry_count = 0
"#,
        )
        .expect("parse local binding config");
        let registry = config
            .connector
            .starrocks_local_binding_registry(novarocks_types::ClusterRole::Fe)
            .expect("construct local registry without metadata I/O");
        StarRocksControlRoleBindingFactory::new(registry)
    }

    #[test]
    fn control_materialization_stamps_the_exact_properties_without_metadata_io() {
        let factory = control_factory();
        let normalized = factory
            .normalize_and_validate(properties(
                "catalog.starrocks",
                CatalogProviderKind::StarRocks,
                Some("metadata-blue"),
            ))
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
    fn control_materialization_keeps_two_catalogs_on_their_exact_local_bindings() {
        let factory = control_factory();
        let blue = factory
            .normalize_and_validate(properties(
                "catalog.starrocks-blue",
                CatalogProviderKind::StarRocks,
                Some("metadata-blue"),
            ))
            .expect("normalize blue catalog");
        let green = factory
            .normalize_and_validate(properties(
                "catalog.starrocks-green",
                CatalogProviderKind::StarRocks,
                Some("metadata-green"),
            ))
            .expect("normalize green catalog");

        let blue = futures::executor::block_on(factory.materialize(
            blue,
            MaterializationContext::new(Instant::now() + Duration::from_secs(1)),
        ))
        .expect("materialize blue catalog");
        let green = futures::executor::block_on(factory.materialize(
            green,
            MaterializationContext::new(Instant::now() + Duration::from_secs(1)),
        ))
        .expect("materialize green catalog");

        assert_eq!(
            blue.control()
                .execution_distribution()
                .declaration(&request_context())
                .expect("blue declaration")
                .starrocks_local_binding(),
            Some("metadata-blue")
        );
        assert_eq!(
            green
                .control()
                .execution_distribution()
                .declaration(&request_context())
                .expect("green declaration")
                .starrocks_local_binding(),
            Some("metadata-green")
        );
    }

    #[test]
    fn control_materialization_requires_an_exact_configured_local_binding() {
        let factory = control_factory();
        let missing = factory
            .normalize_and_validate(properties(
                "catalog.starrocks",
                CatalogProviderKind::StarRocks,
                None,
            ))
            .expect_err("local binding is required");
        assert_eq!(
            missing.disposition(),
            ConnectorMaterializationRetryDisposition::UntilDefinitionChanges
        );

        let unknown = factory
            .normalize_and_validate(properties(
                "catalog.starrocks",
                CatalogProviderKind::StarRocks,
                Some("missing"),
            ))
            .expect("the definition is structurally valid");
        let error = match futures::executor::block_on(factory.materialize(
            unknown,
            MaterializationContext::new(Instant::now() + Duration::from_secs(1)),
        )) {
            Ok(_) => panic!("unconfigured local binding must fail closed"),
            Err(error) => error,
        };
        assert_eq!(
            error.disposition(),
            ConnectorMaterializationRetryDisposition::UntilDefinitionChanges
        );
        assert!(
            error
                .to_string()
                .contains("StarRocks local binding `missing` is not configured")
        );
    }

    #[test]
    fn execution_factory_publishes_no_starrocks_capability_until_its_typed_contract_exists() {
        let normalized = NormalizedCatalogProperties::try_new(properties(
            "catalog.starrocks",
            CatalogProviderKind::StarRocks,
            None,
        ))
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
