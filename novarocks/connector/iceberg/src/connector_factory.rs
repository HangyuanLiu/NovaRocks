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

//! Provider-side construction for an unpublished Iceberg control generation.
//!
//! Server composition owns the resources passed here.  The eventual SPI
//! factory uses this value to construct every capability before Frontend
//! publishes the returned binding; neither a Core catalog registry nor a
//! process-global credential/runtime lookup participates in this step.

use crate::PROVIDER_ID;
use crate::catalog_config::parse_catalog_configuration_with_object_store_binding;
use crate::catalog_control::IcebergCatalogControlState;
use crate::catalog_control::cleanup_maintenance::IcebergCleanupMaintenanceAdapter;
use crate::catalog_control::data_mutation::IcebergDataMutationAdapter;
use crate::catalog_control::metadata_maintenance::IcebergMetadataMaintenanceAdapter;
use crate::catalog_control::staged_create::IcebergStagedCreateAdapter;
use crate::catalog_control::unanchored_ctas_cleanup::IcebergUnanchoredCtasCleanupAdapter;
use crate::commit::IcebergWriteControl;
use crate::distributed_rewrite::IcebergDistributedRewriteControl;
use crate::execution_declaration::IcebergInstanceDistribution;
use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;
use crate::resources::IcebergMetadataResources;
use novarocks_spi::connector::{
    ConnectorControlBinding, ConnectorControlCreation, ConnectorControlFactory,
    ConnectorControlFactoryRequest, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorInstanceDescriptor, ConnectorInstanceIncarnation,
    ConnectorProviderId,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct IcebergConnectorFactory {
    control_resources: IcebergMetadataResources,
    provider_id: ConnectorProviderId,
}

impl IcebergConnectorFactory {
    pub fn new(control_resources: IcebergMetadataResources) -> Self {
        Self {
            control_resources,
            provider_id: ConnectorProviderId::parse(PROVIDER_ID)
                .expect("static Iceberg provider ID is valid"),
        }
    }

    pub fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    /// Build provider-private state before an attachment is durably recorded
    /// or a binding is published.  Dropping the returned value releases the
    /// catalog client and all generation-local reservations.
    pub fn prepare_unpublished(
        &self,
        request: &ConnectorControlFactoryRequest,
    ) -> Result<IcebergUnpublishedControl, ConnectorError> {
        if request.provider_id() != &self.provider_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg control factory received a request for another provider",
            ));
        }
        let configuration = parse_catalog_configuration_with_object_store_binding(
            request.instance_id().as_str(),
            request.properties(),
            self.control_resources
                .planning_binding()
                .object_store_config(),
        )
        .map_err(invalid)?;
        if configuration.object_store_config.is_some()
            && self
                .control_resources
                .planning_binding()
                .object_store_config()
                .is_none()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg object-store catalog requires a server-composed credential binding",
            ));
        }
        let durable_properties = sanitize_durable_properties(&configuration.properties);
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                self.control_resources.clone(),
            )
            .map_err(unavailable)?,
        );
        Ok(IcebergUnpublishedControl {
            runtime,
            durable_properties,
        })
    }
}

impl ConnectorControlFactory for IcebergConnectorFactory {
    fn provider_id(&self) -> &ConnectorProviderId {
        self.provider_id()
    }

    fn create_control(
        &self,
        request: ConnectorControlFactoryRequest,
    ) -> Result<ConnectorControlCreation, ConnectorError> {
        let unpublished = self.prepare_unpublished(&request)?;
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: self.provider_id.clone(),
            instance_id: request.instance_id().clone(),
        };
        let incarnation = ConnectorInstanceIncarnation::new();
        let provider = Arc::new(IcebergMetadata::new(
            descriptor.clone(),
            incarnation,
            Arc::clone(&unpublished.runtime),
        ));
        let key = ConnectorExecutionBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        let metadata_maintenance = Arc::new(IcebergMetadataMaintenanceAdapter::new(
            key.clone(),
            Arc::clone(&unpublished.runtime),
        )?);
        let write_control = Arc::new(IcebergWriteControl::new(
            descriptor.clone(),
            incarnation,
            Arc::clone(&unpublished.runtime),
        ));
        let data_mutation = Arc::new(IcebergDataMutationAdapter::try_new(Arc::clone(&provider))?);
        let distributed_rewrite = Arc::new(IcebergDistributedRewriteControl::new(
            descriptor.clone(),
            incarnation,
            Arc::clone(&unpublished.runtime),
            Arc::clone(&provider),
            Arc::clone(&write_control),
        )?);
        let cleanup_maintenance = Arc::new(IcebergCleanupMaintenanceAdapter::new(
            key.clone(),
            Arc::clone(&unpublished.runtime),
        )?);
        // Slot presence answers "does this provider implement this operation
        // family", which is a property of the Iceberg connector, not of the
        // catalog this generation happens to be pointed at. Gating on
        // `rest_catalog().is_some()` conflated the two and made an absent slot
        // stand in for an unsupported request.
        //
        // The adapters therefore attach for every generation, and whether a
        // specific request is supported is decided by the catalog owner at
        // admission, before the first side effect.
        let staged_create = Some(Arc::new(IcebergStagedCreateAdapter::try_new(
            Arc::clone(&provider),
            Arc::clone(&write_control),
        )?));
        let unanchored_ctas_cleanup = match IcebergUnanchoredCtasCleanupAdapter::try_new(
            descriptor.clone(),
            incarnation,
            Arc::clone(&unpublished.runtime),
        ) {
            Ok(capability) => Some(Arc::new(capability)),
            // Sweeping the unanchored namespace needs a warehouse this
            // generation can enumerate and delete under. A catalog without one
            // still attaches for reads and ordinary table operations; CTAS is
            // what cannot run, and it is refused at admission rather than by a
            // missing slot here.
            Err(error) if error.kind() == ConnectorErrorKind::Unsupported => None,
            Err(error) => return Err(error),
        };
        let binding = ConnectorControlBinding::try_new_with_all_maintenance_capabilities_cleanup_and_staged_create(
                descriptor.clone(),
                incarnation,
                provider.clone(),
                provider.clone(),
                Arc::new(IcebergInstanceDistribution::new(descriptor, incarnation)),
                Some(provider.clone()),
                Some(data_mutation),
                Some(metadata_maintenance),
                Some(distributed_rewrite),
                Some(cleanup_maintenance),
                staged_create.map(|capability| capability as Arc<dyn novarocks_spi::connector::ConnectorStagedCreate>),
                Some(write_control),
                Some(provider.clone()),
            )?
            .try_with_unanchored_ctas_cleanup(unanchored_ctas_cleanup.map(|capability| {
                capability as Arc<dyn novarocks_spi::connector::ConnectorUnanchoredCtasCleanup>
            }))?
            .try_with_staged_publication_recovery(Some(provider.clone()))?
            .try_with_view_metadata(Some(provider))?;
        ConnectorControlCreation::try_new(&request, binding, unpublished.durable_properties)
    }
}

#[allow(dead_code)] // Held until the provider has assembled every capability.
#[derive(Debug)]
pub struct IcebergUnpublishedControl {
    runtime: Arc<IcebergMetadataContext>,
    durable_properties: Vec<(String, String)>,
}

#[allow(dead_code)]
impl IcebergUnpublishedControl {
    pub(crate) fn runtime(&self) -> &Arc<IcebergMetadataContext> {
        &self.runtime
    }

    pub fn durable_properties(&self) -> &[(String, String)] {
        &self.durable_properties
    }
}

fn sanitize_durable_properties(properties: &[(String, String)]) -> Vec<(String, String)> {
    let mut durable = properties
        .iter()
        .filter(|(key, _)| !credential_like_property(key))
        .cloned()
        .collect::<Vec<_>>();
    durable.sort_by(|left, right| left.0.cmp(&right.0));
    durable
}

fn credential_like_property(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    [
        "password",
        "secret",
        "token",
        "credential",
        "accesskey",
        "access_key",
        "private-key",
        "private_key",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

fn invalid(error: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, error)
}

fn unavailable(error: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, error)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::ConnectorInstanceId;

    use super::*;

    fn object_store_config() -> novarocks_fs::ObjectStoreConfig {
        novarocks_fs::ObjectStoreConfig {
            endpoint: "http://minio:9000".to_string(),
            access_key_id: novarocks_fs::SecretValue::new("server-access"),
            access_key_secret: novarocks_fs::SecretValue::new("server-secret"),
            session_token: None,
            enable_path_style_access: Some(true),
            region: None,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    fn rest_factory_request(
        factory: &IcebergConnectorFactory,
        uri: String,
        extra: Vec<(String, String)>,
    ) -> ConnectorControlFactoryRequest {
        let mut properties = vec![
            ("iceberg.catalog.type".to_string(), "rest".to_string()),
            ("uri".to_string(), uri),
            (
                "iceberg.catalog.warehouse".to_string(),
                "file:///tmp/novarocks-rest-factory-warehouse".to_string(),
            ),
        ];
        properties.extend(extra);
        ConnectorControlFactoryRequest::try_new(
            factory.provider_id().clone(),
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            properties,
        )
        .expect("REST factory request")
    }

    #[test]
    fn factory_request_rejects_duplicate_properties_before_provider_construction() {
        let error = ConnectorControlFactoryRequest::try_new(
            ConnectorProviderId::parse(PROVIDER_ID).expect("provider ID"),
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            vec![
                (
                    "iceberg.catalog.warehouse".to_string(),
                    "/tmp/first".to_string(),
                ),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    "/tmp/second".to_string(),
                ),
            ],
        )
        .expect_err("duplicate properties must fail before provider construction");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains("duplicate connector catalog property")
        );
    }

    #[test]
    fn factory_rejects_invalid_catalog_properties_before_runtime_construction() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request = ConnectorControlFactoryRequest::try_new(
            factory.provider_id().clone(),
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            vec![("iceberg.catalog.type".to_string(), "unknown".to_string())],
        )
        .expect("factory request");

        let error = factory
            .prepare_unpublished(&request)
            .expect_err("invalid catalog properties must not create a runtime");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("hadoop|rest|hive"));
    }

    #[test]
    fn unpublished_generation_redacts_credentials_before_attachment_persistence() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request = ConnectorControlFactoryRequest::try_new(
            factory.provider_id().clone(),
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            vec![
                ("type".to_string(), "iceberg".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse.path().display().to_string(),
                ),
                ("aws.s3.access_key".to_string(), "not-durable".to_string()),
                ("aws.s3.secret_key".to_string(), "not-durable".to_string()),
            ],
        )
        .expect("request");
        let unpublished = factory.prepare_unpublished(&request).expect("runtime");

        assert!(
            unpublished
                .durable_properties()
                .iter()
                .all(|(key, _)| !credential_like_property(key))
        );
        let client = unpublished.runtime().novarocks_catalog().vendored_client();
        assert!(Arc::strong_count(&client) >= 1);
    }

    #[test]
    fn rejects_request_credentials_when_the_role_has_no_matching_binding() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request = ConnectorControlFactoryRequest::try_new(
            factory.provider_id().clone(),
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            vec![
                (
                    "iceberg.catalog.warehouse".to_string(),
                    "s3://warehouse/iceberg".to_string(),
                ),
                (
                    "aws.s3.endpoint".to_string(),
                    "http://minio:9000".to_string(),
                ),
                ("aws.s3.access_key".to_string(), "request-only".to_string()),
                ("aws.s3.secret_key".to_string(), "request-only".to_string()),
            ],
        )
        .expect("request");

        let error = factory
            .prepare_unpublished(&request)
            .expect_err("request-only credentials must not create a generation");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains("server-composed credential binding")
        );
    }

    #[test]
    fn restore_reuses_server_credentials_after_durable_redaction() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let config = object_store_config();
        let binding = crate::access_binding::IcebergReadBinding::new(
            Some(config.clone()),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request = ConnectorControlFactoryRequest::try_new(
            factory.provider_id().clone(),
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            vec![
                (
                    "iceberg.catalog.warehouse".to_string(),
                    "s3://warehouse/iceberg".to_string(),
                ),
                ("aws.s3.endpoint".to_string(), config.endpoint.clone()),
                (
                    "aws.s3.access_key".to_string(),
                    config.access_key_id.expose_secret().to_string(),
                ),
                (
                    "aws.s3.secret_key".to_string(),
                    config.access_key_secret.expose_secret().to_string(),
                ),
                (
                    "aws.s3.enable_path_style_access".to_string(),
                    "true".to_string(),
                ),
            ],
        )
        .expect("request");
        let first = factory.prepare_unpublished(&request).expect("create");
        let durable = first.durable_properties().to_vec();
        assert!(
            durable
                .iter()
                .all(|(key, _)| !credential_like_property(key))
        );
        drop(first);

        let restored = ConnectorControlFactoryRequest::try_new(
            factory.provider_id().clone(),
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            durable,
        )
        .expect("restore request");
        factory
            .prepare_unpublished(&restored)
            .expect("restore with server credentials");
    }

    #[test]
    fn created_binding_installs_exact_generation_control_capabilities() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request = ConnectorControlFactoryRequest::try_new(
            factory.provider_id().clone(),
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            vec![(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("request");

        let creation = factory.create_control(request).expect("control creation");
        let maintenance = creation
            .binding()
            .metadata_maintenance()
            .expect("metadata maintenance");

        assert_eq!(maintenance.descriptor(), creation.binding().descriptor());
        assert_eq!(
            maintenance.binding_key().incarnation,
            creation.binding().incarnation()
        );
        let write = creation.binding().write().expect("write control");
        assert_eq!(
            write.binding_key().instance_id,
            creation.binding().descriptor().instance_id
        );
        assert_eq!(
            write.binding_key().incarnation,
            creation.binding().incarnation()
        );
        let mutation = creation.binding().mutation().expect("catalog mutation");
        assert_eq!(mutation.descriptor(), creation.binding().descriptor());
        assert_eq!(mutation.incarnation(), creation.binding().incarnation());
        let data_mutation = creation.binding().data_mutation().expect("data mutation");
        assert_eq!(data_mutation.descriptor(), creation.binding().descriptor());
        assert_eq!(
            data_mutation.binding_key().incarnation,
            creation.binding().incarnation()
        );
        let distributed_rewrite = creation
            .binding()
            .distributed_rewrite()
            .expect("distributed rewrite");
        assert_eq!(
            distributed_rewrite.descriptor(),
            creation.binding().descriptor()
        );
        assert_eq!(
            distributed_rewrite.binding_key().incarnation,
            creation.binding().incarnation()
        );
        let cleanup = creation
            .binding()
            .cleanup_maintenance()
            .expect("cleanup maintenance");
        assert_eq!(cleanup.descriptor(), creation.binding().descriptor());
        assert_eq!(
            cleanup.binding_key().incarnation,
            creation.binding().incarnation()
        );
        // The slot says the Iceberg connector implements staged creation, not
        // that this catalog can serve any particular request. A Hadoop
        // generation installs it and refuses CTAS at admission instead.
        assert!(
            creation.binding().staged_create().is_some(),
            "every Iceberg generation installs the staged-create adapter"
        );
        // The sweeper attaches too, and deliberately does not ask whether this
        // catalog can CTAS. One that cannot has never staged anything
        // unanchored, so the sweep finds nothing; gating it here would replace
        // the catalog's own explanation of why CTAS is impossible with a
        // generic "no cleanup capability" from the lease derivation.
        assert!(
            creation.binding().unanchored_ctas_cleanup().is_some(),
            "the sweeper attaches wherever its warehouse is usable"
        );
        let recovery = creation
            .binding()
            .staged_publication_recovery()
            .expect("staged publication recovery");
        assert_eq!(
            recovery.binding_key().incarnation,
            creation.binding().incarnation()
        );
        let views = creation.binding().view_metadata().expect("view metadata");
        assert_eq!(views.descriptor(), creation.binding().descriptor());
        assert_eq!(views.incarnation(), creation.binding().incarnation());
        let statistics = creation.binding().statistics().expect("statistics");
        assert_eq!(statistics.descriptor(), creation.binding().descriptor());
        assert_eq!(statistics.incarnation(), creation.binding().incarnation());
        assert!(statistics.collection().is_some());
    }

    #[test]
    fn rest_factory_exposes_standard_staged_create() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let creation = factory
            .create_control(rest_factory_request(
                &factory,
                "http://127.0.0.1:1".to_string(),
                Vec::new(),
            ))
            .expect("standard REST control");
        assert!(
            creation.binding().staged_create().is_some(),
            "standard REST staged create must not depend on a private capability"
        );
    }

    #[test]
    fn rest_factory_without_unanchored_cleanup_still_attaches_for_non_ctas_work() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request = ConnectorControlFactoryRequest::try_new(
            factory.provider_id().clone(),
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            vec![
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), "http://127.0.0.1:1".to_string()),
            ],
        )
        .expect("REST request");

        let creation = factory
            .create_control(request)
            .expect("REST control without a GC-capable warehouse");
        assert!(creation.binding().staged_create().is_some());
        assert!(
            creation.binding().unanchored_ctas_cleanup().is_none(),
            "missing cleanup capability belongs to CTAS preflight, not catalog attachment"
        );
    }

    #[test]
    fn hive_generation_installs_the_staged_create_adapter_and_refuses_at_admission() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request = ConnectorControlFactoryRequest::try_new(
            factory.provider_id().clone(),
            ConnectorInstanceId::parse("hive").expect("instance ID"),
            vec![
                ("iceberg.catalog.type".to_string(), "hive".to_string()),
                (
                    "hive.metastore.uris".to_string(),
                    "thrift://127.0.0.1:9083".to_string(),
                ),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse.path().display().to_string(),
                ),
            ],
        )
        .expect("Hive factory request");

        let creation = factory.create_control(request).expect("Hive control");
        // Slot presence answers "does this provider implement staged creation",
        // which is a property of the connector. Whether *this* catalog can run
        // a CTAS is a separate question, and it is answered by the catalog
        // owner before the statement's source executes.
        assert!(
            creation.binding().staged_create().is_some(),
            "every Iceberg generation installs the staged-create adapter"
        );
    }
}
