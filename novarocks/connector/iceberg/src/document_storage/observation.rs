// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Mutex;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorDocument, ConnectorDocumentDiscoveryPage,
    ConnectorDocumentDiscoveryRequest, ConnectorDocumentLoadRequest,
    ConnectorDocumentManagementObservation, ConnectorDocumentObservationRequest,
    ConnectorDocumentStorageObservation, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorManagedObjectMarker, ConnectorStoredDocument,
    ConnectorTableObjectId, FrozenConnectorDocumentObservation, ProviderBindingEpoch,
};

use super::envelope::DOCUMENT_MANIFEST_PROPERTY;

pub(crate) const MANAGED_KIND_PROPERTY: &str = "novarocks.managed.kind";
pub(crate) const MANAGED_OWNER_PROPERTY: &str = "novarocks.managed.owner";
pub(crate) const MANAGED_INCARNATION_PROPERTY: &str = "novarocks.managed.incarnation";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DocumentLoadCacheKey {
    catalog_version: [u8; 32],
    object_id: Vec<u8>,
    metadata_version: [u8; 32],
    document: novarocks_spi::connector::ConnectorDocumentId,
}

#[derive(Default)]
struct DocumentLoadCache(Mutex<HashMap<DocumentLoadCacheKey, ConnectorDocument>>);

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CommittedVersionV1 {
    version: u16,
    table_uuid: String,
    metadata_location: Option<String>,
    last_updated_ms: i64,
    current_snapshot_id: Option<i64>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct OutputVersionV1 {
    version: u16,
    table_uuid: String,
    snapshot_id: i64,
}

pub(crate) fn project_documents(
    metadata: &crate::iceberg::spec::TableMetadata,
    limits: novarocks_spi::connector::ConnectorDocumentStorageLimits,
) -> Result<Vec<ConnectorStoredDocument>, ConnectorError> {
    let Some(encoded) = metadata.properties().get(DOCUMENT_MANIFEST_PROPERTY) else {
        return Ok(Vec::new());
    };
    let manifest = super::codec::decode_document_manifest_with_limits(encoded.as_bytes(), limits)?;
    manifest
        .documents
        .iter()
        .map(super::codec::stored_document)
        .collect()
}

pub(crate) fn managed_marker(
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<ConnectorManagedObjectMarker, ConnectorError> {
    let properties = metadata.properties();
    let kind = properties.get(MANAGED_KIND_PROPERTY).ok_or_else(|| {
        if properties.contains_key(MANAGED_OWNER_PROPERTY)
            || properties.contains_key(MANAGED_INCARNATION_PROPERTY)
        {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg managed-object marker is only partially present",
            )
        } else {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg table has no managed-object marker",
            )
        }
    })?;
    let owner = properties.get(MANAGED_OWNER_PROPERTY).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg managed-object marker has no owner",
        )
    })?;
    let incarnation = properties
        .get(MANAGED_INCARNATION_PROPERTY)
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg managed-object marker has no incarnation",
            )
        })?;
    ConnectorManagedObjectMarker::try_new(kind, owner, incarnation).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode Iceberg managed-object marker: {error}"),
        )
    })
}

pub(crate) fn validate_managed_rewrite_attachment(
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<(), ConnectorError> {
    match managed_marker(metadata) {
        Err(error) if error.kind() == ConnectorErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
        Ok(_) => {}
    }
    let snapshot = metadata.current_snapshot().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "managed Iceberg table has no current publication snapshot",
        )
    })?;
    let encoded = snapshot
        .summary()
        .additional_properties
        .get(DOCUMENT_MANIFEST_PROPERTY)
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "managed Iceberg publication snapshot has no document attachment manifest",
            )
        })?;
    let manifest = super::codec::decode_document_manifest(encoded.as_bytes())?;
    let expected_version = output_committed_version(metadata.uuid(), snapshot.snapshot_id())?;
    if !manifest
        .documents
        .iter()
        .any(|document| match &document.attachment {
            super::envelope::IcebergDocumentAttachmentV1::ExactOutput {
                committed_version,
                snapshot_id: Some(snapshot_id),
            } => {
                *snapshot_id == snapshot.snapshot_id()
                    && committed_version.as_slice() == expected_version.payload().as_ref()
            }
            _ => false,
        })
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "managed Iceberg publication snapshot has no exact-output document attachment bound to its current output version",
        ));
    }
    Ok(())
}

pub(crate) fn table_object_id(
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<ConnectorTableObjectId, ConnectorError> {
    ConnectorTableObjectId::try_new(Bytes::from(metadata.uuid().to_string()))
}

pub(crate) fn committed_version(
    table: &crate::iceberg::table::Table,
) -> Result<ConnectorCommittedVersion, ConnectorError> {
    let metadata = table.metadata();
    let payload = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "table_uuid": metadata.uuid().to_string(),
        "metadata_location": table.metadata_location(),
        "last_updated_ms": metadata.last_updated_ms(),
        "current_snapshot_id": metadata.current_snapshot_id(),
    }))
    .map(Bytes::from)
    .map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("encode Iceberg committed metadata version: {error}"),
        )
    })?;
    ConnectorCommittedVersion::try_new(payload, metadata.current_snapshot_id())
}

pub(crate) fn output_committed_version(
    table_uuid: uuid::Uuid,
    snapshot_id: i64,
) -> Result<ConnectorCommittedVersion, ConnectorError> {
    let payload = serde_json::to_vec(&OutputVersionV1 {
        version: 1,
        table_uuid: table_uuid.to_string(),
        snapshot_id,
    })
    .map(Bytes::from)
    .map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("encode Iceberg committed output version: {error}"),
        )
    })?;
    ConnectorCommittedVersion::try_new(payload, Some(snapshot_id))
}

pub(crate) fn validate_expected_object(
    expected: &ConnectorTableObjectId,
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<(), ConnectorError> {
    if &table_object_id(metadata)? != expected {
        return Err(ConnectorError::table_object_binding(
            novarocks_spi::connector::ConnectorTableObjectBindingFailure::Replaced,
            "Iceberg table name now resolves to a different table UUID",
        ));
    }
    Ok(())
}

impl ConnectorDocumentStorageObservation for super::create::IcebergDocumentStorage {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation()
    }

    fn observe_documents(
        &self,
        request: ConnectorDocumentObservationRequest,
    ) -> Result<FrozenConnectorDocumentObservation, ConnectorError> {
        let table = load_exact_table(self, &request)?;
        FrozenConnectorDocumentObservation::try_new(
            &request,
            committed_version(&table)?,
            project_documents(table.metadata(), request.limits())?,
        )
    }

    fn load_document(
        &self,
        request: ConnectorDocumentLoadRequest,
    ) -> Result<ConnectorDocument, ConnectorError> {
        super::io::check_context(request.context())?;
        let physical = self
            .runtime()
            .load_table_classified_for_request(
                &request.target().namespace,
                &request.target().table,
                request.context(),
            )
            .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
        let table = physical.into_table();
        validate_expected_object(request.expected_object_id(), table.metadata())?;
        let exact_metadata = resolve_observed_metadata(
            self,
            &table,
            request.metadata_version(),
            request.limits(),
            request.context(),
        )?;
        let projected = project_documents(&exact_metadata, request.limits())?;
        if !projected
            .iter()
            .any(|document| document == request.document())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg document envelope is absent from the exact observed metadata",
            ));
        }
        let key = DocumentLoadCacheKey {
            catalog_version: *request.catalog_handle().version().as_bytes(),
            object_id: request.expected_object_id().as_bytes().to_vec(),
            metadata_version: request.metadata_version().digest(),
            document: request.document().id().clone(),
        };
        let cache = request
            .context()
            .request_scope_extension_or_insert_with(DocumentLoadCache::default);
        if let Some(document) = cache
            .0
            .lock()
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Iceberg document load cache lock poisoned",
                )
            })?
            .get(&key)
            .cloned()
        {
            return Ok(document);
        }
        let document = super::io::load_deferred_document(
            self.runtime().resources().catalog_runtime(),
            table.file_io(),
            request.document(),
            request.limits(),
            request.context(),
        )?;
        cache
            .0
            .lock()
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Iceberg document load cache lock poisoned",
                )
            })?
            .insert(key, document.clone());
        Ok(document)
    }

    fn observe_current_management(
        &self,
        request: ConnectorDocumentObservationRequest,
    ) -> Result<ConnectorDocumentManagementObservation, ConnectorError> {
        let table = load_exact_table(self, &request)?;
        ConnectorDocumentManagementObservation::try_new(
            &request,
            committed_version(&table)?,
            managed_marker(table.metadata())?,
            project_documents(table.metadata(), request.limits())?,
        )
    }

    fn discover_documents(
        &self,
        request: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
        super::discovery::discover(self, &request)
    }
}

fn resolve_observed_metadata(
    storage: &super::create::IcebergDocumentStorage,
    current: &crate::iceberg::table::Table,
    observed: &ConnectorCommittedVersion,
    limits: novarocks_spi::connector::ConnectorDocumentStorageLimits,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<std::sync::Arc<crate::iceberg::spec::TableMetadata>, ConnectorError> {
    let payload = decode_committed_version_with_limits(observed, limits)?;
    if payload.table_uuid != current.metadata().uuid().to_string()
        || payload.current_snapshot_id != observed.snapshot_id()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg document load version is not bound to the exact table object and snapshot",
        ));
    }
    if &committed_version(current)? == observed {
        return Ok(current.metadata_ref());
    }
    let location = payload.metadata_location.clone().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "historical Iceberg document load version has no metadata location",
        )
    })?;
    let file_io = current.file_io().clone();
    let metadata_input = file_io.new_input(&location).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            format!("open historical Iceberg document metadata: {error}"),
        )
    })?;
    let metadata_size = storage
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move { metadata_input.metadata().await })
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                format!("inspect historical Iceberg document metadata runtime: {error}"),
            )
        })?
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                format!("inspect historical Iceberg document metadata: {error}"),
            )
        })?
        .size;
    if metadata_size > limits.max_decode_working_set_bytes() as u64 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "historical Iceberg document metadata exceeds the caller decode working-set budget",
        ));
    }
    let metadata = storage
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move { read_observed_metadata(&file_io, &location, &payload).await })
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                format!("load historical Iceberg document metadata runtime: {error}"),
            )
        })?
        .map_err(|error| {
            ConnectorError::new(
                if error.kind() == crate::iceberg::ErrorKind::DataInvalid {
                    ConnectorErrorKind::CorruptData
                } else {
                    ConnectorErrorKind::Unavailable
                },
                format!("load historical Iceberg document metadata: {error}"),
            )
        })?;
    super::io::check_context(context)?;
    Ok(std::sync::Arc::new(metadata))
}

async fn read_observed_metadata(
    file_io: &crate::iceberg::io::FileIO,
    location: &str,
    expected: &CommittedVersionV1,
) -> Result<crate::iceberg::spec::TableMetadata, crate::iceberg::Error> {
    let metadata = crate::iceberg::spec::TableMetadata::read_from(file_io, location).await?;
    if metadata.uuid().to_string() != expected.table_uuid
        || metadata.last_updated_ms() != expected.last_updated_ms
        || metadata.current_snapshot_id() != expected.current_snapshot_id
    {
        return Err(crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::DataInvalid,
            "historical Iceberg metadata does not match the exact observed version",
        ));
    }
    Ok(metadata)
}

#[cfg(test)]
fn decode_committed_version(
    version: &ConnectorCommittedVersion,
) -> Result<CommittedVersionV1, ConnectorError> {
    decode_committed_version_with_limits(
        version,
        novarocks_spi::connector::ConnectorDocumentStorageLimits::spec_default(),
    )
}

fn decode_committed_version_with_limits(
    version: &ConnectorCommittedVersion,
    limits: novarocks_spi::connector::ConnectorDocumentStorageLimits,
) -> Result<CommittedVersionV1, ConnectorError> {
    super::codec::preflight_decode(
        version.payload(),
        limits,
        "Iceberg committed metadata version",
    )?;
    let payload: CommittedVersionV1 =
        serde_json::from_slice(version.payload()).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode Iceberg committed metadata version: {error}"),
            )
        })?;
    if payload.version != 1 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg committed metadata version has an unsupported version",
        ));
    }
    Ok(payload)
}

fn load_exact_table(
    storage: &super::create::IcebergDocumentStorage,
    request: &ConnectorDocumentObservationRequest,
) -> Result<crate::iceberg::table::Table, ConnectorError> {
    super::io::check_context(request.context())?;
    let physical = storage
        .runtime()
        .load_table_classified_for_request(
            &request.target().namespace,
            &request.target().table,
            request.context(),
        )
        .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
    let table = physical.into_table();
    validate_expected_object(request.expected_object_id(), table.metadata())?;
    Ok(table)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use novarocks_spi::connector::{ConnectorTableObjectBindingFailure, ConnectorTableObjectId};

    use super::*;

    fn metadata(properties: HashMap<String, String>) -> crate::iceberg::spec::TableMetadata {
        let schema = crate::iceberg::spec::Schema::builder()
            .with_fields(vec![std::sync::Arc::new(
                crate::iceberg::spec::NestedField::required(
                    1,
                    "id",
                    crate::iceberg::spec::Type::Primitive(
                        crate::iceberg::spec::PrimitiveType::Long,
                    ),
                ),
            )])
            .build()
            .unwrap();
        crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://warehouse/table".to_string(),
            crate::iceberg::spec::FormatVersion::V2,
            properties,
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    #[test]
    fn exact_object_validation_reports_a_typed_replacement() {
        let metadata = metadata(HashMap::new());
        let wrong = ConnectorTableObjectId::try_new(Bytes::from_static(b"replaced-table")).unwrap();
        let error = validate_expected_object(&wrong, &metadata).unwrap_err();
        assert_eq!(
            error.table_object_binding_failure(),
            Some(ConnectorTableObjectBindingFailure::Replaced)
        );
    }

    #[test]
    fn partial_management_marker_is_corrupt_not_unmanaged() {
        let metadata = metadata(HashMap::from([(
            MANAGED_OWNER_PROPERTY.to_string(),
            "deployment".to_string(),
        )]));
        assert_eq!(
            managed_marker(&metadata).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn historical_observation_metadata_remains_exactly_loadable_after_current_advances() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let handle = runtime.handle().clone();
        let directory = tempfile::tempdir().expect("metadata directory");
        let old_location = directory.path().join("v1.metadata.json");
        let old_location = old_location.display().to_string();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            std::sync::Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            std::sync::Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle)),
        );
        let file_io = crate::fs_io::build_file_io_for_location(&old_location, binding);
        let old = metadata(HashMap::from([(
            "document-generation".to_string(),
            "old".to_string(),
        )]));
        runtime
            .block_on(old.write_to(&file_io, &old_location))
            .expect("write old metadata");
        let old_table = crate::iceberg::table::Table::builder()
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .file_io(file_io.clone())
            .metadata(old.clone())
            .metadata_location(old_location.clone())
            .build()
            .unwrap();
        let observed = committed_version(&old_table).expect("observed version");
        let payload = decode_committed_version(&observed).expect("decode observed version");
        let current = old
            .into_builder(Some(old_location.clone()))
            .set_properties(HashMap::from([(
                "document-generation".to_string(),
                "new".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert_eq!(
            current.properties().get("document-generation"),
            Some(&"new".to_string())
        );

        let loaded = runtime
            .block_on(read_observed_metadata(&file_io, &old_location, &payload))
            .expect("load exact historical metadata");
        assert_eq!(loaded.uuid(), current.uuid());
        assert_eq!(
            loaded.properties().get("document-generation"),
            Some(&"old".to_string())
        );
    }

    #[test]
    fn managed_rewrite_without_a_publication_attachment_fails_closed() {
        let managed_metadata = metadata(HashMap::from([
            (MANAGED_KIND_PROPERTY.to_string(), "mv".to_string()),
            (MANAGED_OWNER_PROPERTY.to_string(), "deployment".to_string()),
            (
                MANAGED_INCARNATION_PROPERTY.to_string(),
                "process".to_string(),
            ),
        ]));
        assert_eq!(
            validate_managed_rewrite_attachment(&managed_metadata)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        validate_managed_rewrite_attachment(&metadata(HashMap::new())).unwrap();
    }

    fn managed_metadata_with_attachment(
        current_snapshot_id: i64,
        attachment_snapshot_id: i64,
        attachment_version_snapshot_id: i64,
    ) -> crate::iceberg::spec::TableMetadata {
        let base = metadata(HashMap::from([
            (MANAGED_KIND_PROPERTY.to_string(), "mv".to_string()),
            (MANAGED_OWNER_PROPERTY.to_string(), "deployment".to_string()),
            (
                MANAGED_INCARNATION_PROPERTY.to_string(),
                "process".to_string(),
            ),
        ]));
        let output_version =
            output_committed_version(base.uuid(), attachment_version_snapshot_id).unwrap();
        let content = vec![1, 2, 3];
        let manifest = super::super::envelope::IcebergDocumentManifestV1 {
            version: super::super::envelope::DOCUMENT_MANIFEST_VERSION,
            documents: vec![super::super::envelope::IcebergDocumentEnvelopeV1 {
                version: super::super::envelope::DOCUMENT_ENVELOPE_VERSION,
                owner: "novarocks.mv".to_string(),
                name: "definition".to_string(),
                format_owner: "novarocks.mv".to_string(),
                format_name: "definition".to_string(),
                format_version: 1,
                revision: novarocks_spi::connector::ConnectorDocumentRevision::for_content(
                    &content,
                )
                .to_bytes(),
                encoded_len: content.len() as u64,
                references: Vec::new(),
                attachment: super::super::envelope::IcebergDocumentAttachmentV1::ExactOutput {
                    committed_version: output_version.payload().to_vec(),
                    snapshot_id: Some(attachment_snapshot_id),
                },
                carrier: super::super::envelope::IcebergDocumentCarrierV1::Available { content },
            }],
        };
        let encoded = super::super::codec::encode_document_manifest(&manifest).unwrap();
        let snapshot = crate::iceberg::spec::Snapshot::builder()
            .with_snapshot_id(current_snapshot_id)
            .with_sequence_number(1)
            .with_timestamp_ms(base.last_updated_ms() + 1)
            .with_manifest_list("memory://table/snap.avro")
            .with_summary(crate::iceberg::spec::Summary {
                operation: crate::iceberg::spec::Operation::Append,
                additional_properties: HashMap::from([(
                    DOCUMENT_MANIFEST_PROPERTY.to_string(),
                    std::str::from_utf8(&encoded).unwrap().to_string(),
                )]),
            })
            .build();
        base.into_builder(None)
            .set_branch_snapshot(snapshot, "main")
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    #[test]
    fn managed_rewrite_requires_attachment_bound_to_current_snapshot() {
        validate_managed_rewrite_attachment(&managed_metadata_with_attachment(12, 12, 12)).unwrap();
        assert_eq!(
            validate_managed_rewrite_attachment(&managed_metadata_with_attachment(12, 11, 11))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        assert_eq!(
            validate_managed_rewrite_attachment(&managed_metadata_with_attachment(12, 12, 11))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }
}
