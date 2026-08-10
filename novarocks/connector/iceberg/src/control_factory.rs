// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
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
use crate::control_runtime::IcebergControlRuntime;
use crate::resources::IcebergControlResources;
use novarocks_spi::connector::{
    ConnectorControlFactoryRequest, ConnectorError, ConnectorErrorKind, ConnectorProviderId,
};

#[derive(Clone)]
pub struct IcebergControlFactory {
    control_resources: IcebergControlResources,
    provider_id: ConnectorProviderId,
}

impl IcebergControlFactory {
    pub fn new(control_resources: IcebergControlResources) -> Self {
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
        let runtime = IcebergControlRuntime::try_new(
            IcebergCatalogControlState::new(configuration),
            self.control_resources.clone(),
        )
        .map_err(unavailable)?;
        Ok(IcebergUnpublishedControl {
            runtime,
            durable_properties,
        })
    }
}

#[allow(dead_code)] // Held until the provider has assembled every capability.
#[derive(Debug)]
pub struct IcebergUnpublishedControl {
    runtime: IcebergControlRuntime,
    durable_properties: Vec<(String, String)>,
}

#[allow(dead_code)]
impl IcebergUnpublishedControl {
    pub(crate) fn runtime(&self) -> &IcebergControlRuntime {
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
            access_key_id: "server-access".to_string(),
            access_key_secret: "server-secret".to_string(),
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
        let factory = IcebergControlFactory::new(IcebergControlResources::new(
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
        assert!(Arc::strong_count(unpublished.runtime().catalog()) >= 1);
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
        let factory = IcebergControlFactory::new(IcebergControlResources::new(
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
        let factory = IcebergControlFactory::new(IcebergControlResources::new(
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
                    config.access_key_id.clone(),
                ),
                (
                    "aws.s3.secret_key".to_string(),
                    config.access_key_secret.clone(),
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
}
