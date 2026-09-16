// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorDocumentManagementAdmissionRequest, ConnectorDocumentManagementOperation,
    ConnectorDocumentStorageManagement, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorPrepareDocumentsRequest, ProviderBindingEpoch,
};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct IcebergDocumentStorage {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    runtime: Arc<crate::metadata_context::IcebergMetadataContext>,
}

impl IcebergDocumentStorage {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        runtime: Arc<crate::metadata_context::IcebergMetadataContext>,
    ) -> Self {
        Self {
            descriptor,
            incarnation,
            runtime,
        }
    }

    pub(crate) fn runtime(&self) -> &Arc<crate::metadata_context::IcebergMetadataContext> {
        &self.runtime
    }

    pub(crate) fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub(crate) const fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionTokenV1 {
    version: u16,
    operation: String,
    operation_id: [u8; 16],
    namespace: String,
    table: String,
    expected_object_id: Option<Vec<u8>>,
}

impl ConnectorDocumentStorageManagement for IcebergDocumentStorage {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }

    fn admit_management(
        &self,
        request: ConnectorDocumentManagementAdmissionRequest,
    ) -> Result<Bytes, ConnectorError> {
        super::io::check_context(request.context())?;
        if self.runtime.control_state().configuration().kind
            != crate::catalog_config::IcebergCatalogKind::Rest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "application-document management requires an Iceberg REST catalog",
            ));
        }
        if request.operation() == ConnectorDocumentManagementOperation::Create {
            self.runtime
                .novarocks_catalog()
                .admit_create(crate::catalog::CatalogCreateIntent::CreateTableAsSelect)
                .map_err(|unsupported| {
                    ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        unsupported.message().to_string(),
                    )
                })?;
        }
        let operation = match request.operation() {
            ConnectorDocumentManagementOperation::Create => "create",
            ConnectorDocumentManagementOperation::SingleTargetUpdate => "single-target-update",
            ConnectorDocumentManagementOperation::Publication => "publication",
        };
        let token = AdmissionTokenV1 {
            version: 1,
            operation: operation.to_string(),
            operation_id: request.operation_id().to_bytes(),
            namespace: request.target().namespace.to_string(),
            table: request.target().table.to_string(),
            expected_object_id: request
                .expected_object_id()
                .map(|object| object.as_bytes().to_vec()),
        };
        serde_json::to_vec(&token)
            .map(Bytes::from)
            .map_err(|error| internal(format!("encode Iceberg document admission: {error}")))
    }

    fn prepare_documents(
        &self,
        request: ConnectorPrepareDocumentsRequest,
    ) -> Result<Bytes, ConnectorError> {
        super::io::check_context(request.context())?;
        let token: AdmissionTokenV1 = serde_json::from_slice(request.admission().provider_token())
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!("decode Iceberg document admission: {error}"),
                )
            })?;
        validate_admission_token(&token, request.admission())?;
        let (file_io, root) = match request.admission().operation() {
            ConnectorDocumentManagementOperation::Create => {
                let root = create_document_root(
                    &self.runtime.control_state().configuration().warehouse_uri,
                    request.admission().operation_id().to_bytes(),
                );
                let binding = self
                    .runtime
                    .resources()
                    .planning_binding()
                    .for_request(request.context().clone());
                (
                    crate::fs_io::build_file_io_for_location(&root, binding),
                    root,
                )
            }
            ConnectorDocumentManagementOperation::SingleTargetUpdate
            | ConnectorDocumentManagementOperation::Publication => {
                let target = request.admission().target();
                let physical = self
                    .runtime
                    .load_table_classified_for_request(
                        &target.namespace,
                        &target.table,
                        request.context(),
                    )
                    .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
                let table = physical.into_table();
                let expected = request.admission().expected_object_id().ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "Iceberg document update requires an exact table object",
                    )
                })?;
                super::observation::validate_expected_object(expected, table.metadata())?;
                (
                    table.file_io().clone(),
                    table.metadata().location().to_string(),
                )
            }
        };
        let manifest = super::io::prepare_document_carriers(
            self.runtime.resources().catalog_runtime(),
            &file_io,
            &root,
            request.documents(),
            request.context(),
        )?;
        super::codec::encode_document_manifest(&manifest)
    }
}

fn validate_admission_token(
    token: &AdmissionTokenV1,
    admission: &novarocks_spi::connector::ConnectorDocumentManagementAdmission,
) -> Result<(), ConnectorError> {
    let operation = match admission.operation() {
        ConnectorDocumentManagementOperation::Create => "create",
        ConnectorDocumentManagementOperation::SingleTargetUpdate => "single-target-update",
        ConnectorDocumentManagementOperation::Publication => "publication",
    };
    if token.version != 1
        || token.operation != operation
        || token.operation_id != admission.operation_id().to_bytes()
        || token.namespace != admission.target().namespace.as_ref()
        || token.table != admission.target().table.as_ref()
        || token.expected_object_id.as_deref()
            != admission
                .expected_object_id()
                .map(|object| object.as_bytes().as_ref())
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg document admission token does not match the exact operation",
        ));
    }
    Ok(())
}

fn create_document_root(warehouse: &str, operation_id: [u8; 16]) -> String {
    let operation = operation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!(
        "{}/_novarocks/document-staging/v1/{operation}",
        warehouse.trim_end_matches('/')
    )
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

#[cfg(test)]
#[path = "tests/admission_support.rs"]
mod admission_support;

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCancellation,
        ConnectorDocumentManagementAdmissionRequest, ConnectorDocumentStorageManagement,
        ConnectorInstanceId, ConnectorMutationOperationId, ConnectorProviderBindingKey,
        ConnectorProviderId, ConnectorRequestContext, ConnectorTableIdentity,
        ConnectorTableObjectId, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::admission_support::{AdmissionCatalogSpy, AdmissionFileIoSpy};
    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog::CatalogCreateIntent;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergMetadataResources;

    struct Active;
    impl ConnectorCancellation for Active {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    #[test]
    fn native_hadoop_rejects_every_document_management_operation_before_catalog_io() {
        assert_native_management_refused_before_io("hadoop");
    }

    #[test]
    fn native_hive_rejects_every_document_management_operation_before_catalog_io() {
        assert_native_management_refused_before_io("hive");
    }

    fn assert_native_management_refused_before_io(catalog_kind: &str) {
        let tokio = tokio::runtime::Runtime::new().unwrap();
        let handle = tokio.handle().clone();
        let warehouse = tempfile::tempdir().unwrap();
        // No metastore is needed: the native catalog must refuse document
        // management without contacting this reserved loopback endpoint.
        let metastore = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        metastore.set_nonblocking(true).unwrap();
        let mut properties = vec![
            ("iceberg.catalog.type".to_string(), catalog_kind.to_string()),
            (
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            ),
        ];
        if catalog_kind == "hive" {
            properties.push((
                "hive.metastore.uris".to_string(),
                format!("thrift://{}", metastore.local_addr().unwrap()),
            ));
        }
        let configuration =
            crate::catalog_config::parse_catalog_configuration("ice", &properties).unwrap();
        let file_io = Arc::new(AdmissionFileIoSpy::default());
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            file_io.clone(),
            file_io.clone(),
        );
        let state = IcebergCatalogControlState::new(configuration);
        let resources = IcebergMetadataResources::new(binding, handle);
        let native_runtime = crate::metadata_context::IcebergMetadataContext::try_new(
            state.clone(),
            resources.clone(),
        )
        .unwrap();
        // These catalogs still support ordinary empty-table creation. This
        // test narrows document management, not all catalog write semantics.
        native_runtime
            .novarocks_catalog()
            .admit_create(CatalogCreateIntent::EmptyTable)
            .expect("native catalog still admits an empty table");
        assert!(
            native_runtime
                .novarocks_catalog()
                .admit_create(CatalogCreateIntent::CreateTableAsSelect)
                .is_err(),
        );
        let catalog = Arc::new(AdmissionCatalogSpy::new(Arc::clone(
            native_runtime.novarocks_catalog(),
        )));
        let runtime = Arc::new(
            crate::metadata_context::IcebergMetadataContext::with_catalog_for_test(
                state,
                resources,
                catalog.clone(),
            ),
        );
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
        };
        let epoch = ProviderBindingEpoch::new();
        let storage = IcebergDocumentStorage::new(descriptor.clone(), epoch, runtime);
        let owner = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation: epoch,
        };
        let catalog_handle = CatalogHandle::new(
            descriptor.instance_id.clone(),
            CatalogVersion::from_bytes([3; 32]),
        );
        let target = ConnectorTableIdentity {
            instance_id: descriptor.instance_id,
            namespace: Arc::from("db"),
            table: Arc::from("mv"),
        };
        let context = || {
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                Arc::new(Active),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .unwrap()
        };
        for (operation, object_id) in [
            (ConnectorDocumentManagementOperation::Create, None),
            (
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                Some(ConnectorTableObjectId::try_new(Bytes::from_static(b"uuid")).unwrap()),
            ),
            (
                ConnectorDocumentManagementOperation::Publication,
                Some(ConnectorTableObjectId::try_new(Bytes::from_static(b"uuid")).unwrap()),
            ),
        ] {
            let request = ConnectorDocumentManagementAdmissionRequest::try_new(
                owner.clone(),
                catalog_handle.clone(),
                ConnectorMutationOperationId::new(),
                target.clone(),
                object_id,
                operation,
                context(),
            )
            .unwrap();
            let error = storage.admit_management(request).unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
            catalog.assert_no_io();
            file_io.assert_no_dispatch();
            assert!(warehouse.path().read_dir().unwrap().next().is_none());
            assert_eq!(
                metastore.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock,
                "document admission must not contact the native metastore",
            );
        }
    }
}
