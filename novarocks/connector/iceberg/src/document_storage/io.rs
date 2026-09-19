// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use novarocks_spi::connector::{
    ConnectorDocument, ConnectorDocumentCarrier, ConnectorDocumentSet, ConnectorError,
    ConnectorErrorKind, ConnectorRequestContext, ConnectorStoredDocument,
    DEFAULT_CONNECTOR_DOCUMENT_AVAILABLE_CONTENT_BYTES,
    DEFAULT_CONNECTOR_DOCUMENT_AVAILABLE_CONTENT_TOTAL_BYTES,
};

use super::codec::{document_attachment, document_references};
use super::envelope::{
    DOCUMENT_ENVELOPE_VERSION, DOCUMENT_MANIFEST_VERSION, DOCUMENT_SIDECAR_DIRECTORY,
    IcebergDocumentCarrierV1, IcebergDocumentEnvelopeV1, IcebergDocumentManifestV1,
};

pub(crate) fn prepare_document_carriers(
    runtime: &crate::resources::IcebergCatalogRuntime,
    file_io: &crate::iceberg::io::FileIO,
    table_location: &str,
    documents: &ConnectorDocumentSet,
    context: &ConnectorRequestContext,
) -> Result<IcebergDocumentManifestV1, ConnectorError> {
    check_context(context)?;
    let mut available_bytes = 0usize;
    let mut envelopes = Vec::with_capacity(documents.documents().len());
    let mut sidecars = Vec::new();
    for document in documents.documents() {
        check_context(context)?;
        let can_be_available = document.content().len()
            <= DEFAULT_CONNECTOR_DOCUMENT_AVAILABLE_CONTENT_BYTES
            && available_bytes
                .checked_add(document.content().len())
                .is_some_and(|total| {
                    total <= DEFAULT_CONNECTOR_DOCUMENT_AVAILABLE_CONTENT_TOTAL_BYTES
                });
        let carrier = if can_be_available {
            available_bytes += document.content().len();
            IcebergDocumentCarrierV1::Available {
                content: document.content().to_vec(),
            }
        } else {
            let location = immutable_sidecar_location(table_location, document);
            sidecars.push((location.clone(), document.content().clone()));
            IcebergDocumentCarrierV1::Deferred { location }
        };
        envelopes.push(envelope(document, carrier));
    }
    let manifest = IcebergDocumentManifestV1 {
        version: DOCUMENT_MANIFEST_VERSION,
        documents: envelopes,
    };
    super::codec::validate_manifest(&manifest)?;
    // Provider-token sizing is part of admission. Prove it before the first
    // immutable sidecar write so a local envelope failure has zero effects.
    super::codec::encode_document_manifest(&manifest)?;
    for (location, content) in sidecars {
        check_context(context)?;
        let output = file_io.new_output(&location).map_err(|error| {
            unavailable(format!(
                "open Iceberg document sidecar `{location}`: {error}"
            ))
        })?;
        runtime
            .block_on(async move { output.write(content).await })
            .map_err(|error| {
                unavailable(format!("write Iceberg document sidecar runtime: {error}"))
            })?
            .map_err(|error| {
                unavailable(format!(
                    "write Iceberg document sidecar `{location}`: {error}"
                ))
            })?;
    }
    Ok(manifest)
}

pub(crate) fn load_deferred_document(
    runtime: &crate::resources::IcebergCatalogRuntime,
    file_io: &crate::iceberg::io::FileIO,
    stored: &ConnectorStoredDocument,
    limits: novarocks_spi::connector::ConnectorDocumentStorageLimits,
    context: &ConnectorRequestContext,
) -> Result<ConnectorDocument, ConnectorError> {
    check_context(context)?;
    if stored.encoded_len() > limits.max_document_bytes()
        || stored.encoded_len() > limits.max_decode_working_set_bytes()
    {
        return Err(super::codec::exhausted(
            "Iceberg deferred document exceeds the caller decode budget",
        ));
    }
    let ConnectorDocumentCarrier::DeferredContent(handle) = stored.carrier() else {
        return Err(super::codec::invalid(
            "Iceberg explicit document load requires a deferred carrier",
        ));
    };
    let location = std::str::from_utf8(handle.as_bytes()).map_err(|_| {
        super::codec::corrupt("Iceberg deferred document location is not valid UTF-8")
    })?;
    let input = file_io.new_input(location).map_err(|error| {
        unavailable(format!(
            "open Iceberg document sidecar `{location}`: {error}"
        ))
    })?;
    let expected_len = u64::try_from(stored.encoded_len()).map_err(|_| {
        super::codec::exhausted("Iceberg deferred document length does not fit the storage API")
    })?;
    let content = runtime
        .block_on(async move {
            if !input.exists().await? {
                return Ok(None);
            }
            if input.metadata().await?.size != expected_len {
                return Err(crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::DataInvalid,
                    "Iceberg document sidecar size does not match its exact envelope",
                ));
            }
            input.read().await.map(Some)
        })
        .map_err(|error| unavailable(format!("read Iceberg document sidecar runtime: {error}")))?
        .map_err(|error| {
            let kind = match error.kind() {
                crate::iceberg::ErrorKind::DataInvalid => ConnectorErrorKind::CorruptData,
                crate::iceberg::ErrorKind::TableNotFound => ConnectorErrorKind::NotFound,
                _ => ConnectorErrorKind::Unavailable,
            };
            ConnectorError::new(
                kind,
                format!("read Iceberg document sidecar `{location}`: {error}"),
            )
        })?
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                format!("Iceberg document sidecar `{location}` is missing"),
            )
        })?;
    if content.len() != stored.encoded_len()
        || novarocks_spi::connector::ConnectorDocumentRevision::for_content(&content)
            != stored.id().revision()
    {
        return Err(super::codec::corrupt(
            "Iceberg document sidecar does not match its exact length and revision",
        ));
    }
    let attachment = match stored.attachment() {
        novarocks_spi::connector::ConnectorStoredDocumentAttachment::TableMetadata => {
            novarocks_spi::connector::ConnectorDocumentAttachment::TableMetadata
        }
        novarocks_spi::connector::ConnectorStoredDocumentAttachment::ExactOutput(version) => {
            novarocks_spi::connector::ConnectorDocumentAttachment::ExactOutput(version.clone())
        }
    };
    ConnectorDocument::try_new(
        stored.id().owner().clone(),
        stored.id().name().clone(),
        stored.format().clone(),
        content,
        stored.references().to_vec(),
        attachment,
    )
}

fn envelope(
    document: &ConnectorDocument,
    carrier: IcebergDocumentCarrierV1,
) -> IcebergDocumentEnvelopeV1 {
    IcebergDocumentEnvelopeV1 {
        version: DOCUMENT_ENVELOPE_VERSION,
        owner: document.id().owner().as_str().to_string(),
        name: document.id().name().as_str().to_string(),
        format_owner: document.format().owner().to_string(),
        format_name: document.format().name().to_string(),
        format_version: document.format().version(),
        revision: document.id().revision().to_bytes(),
        encoded_len: document.content().len() as u64,
        references: document_references(document.references()),
        attachment: document_attachment(document.attachment()),
        carrier,
    }
}

fn immutable_sidecar_location(table_location: &str, document: &ConnectorDocument) -> String {
    let revision = document
        .id()
        .revision()
        .to_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!(
        "{}/{DOCUMENT_SIDECAR_DIRECTORY}/{revision}.bin",
        table_location.trim_end_matches('/')
    )
}

pub(crate) fn check_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Iceberg document storage request was cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg document storage request deadline elapsed",
        ));
    }
    Ok(())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorDocumentAttachment, ConnectorDocumentFormat,
        ConnectorDocumentName, ConnectorDocumentOwner, ConnectorRequestContext,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::*;

    struct Active;
    impl ConnectorCancellation for Active {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct Cancelled;
    impl ConnectorCancellation for Cancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }

    fn context(cancellation: Arc<dyn ConnectorCancellation>) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            cancellation,
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("context")
    }

    fn document(content: Bytes) -> ConnectorDocument {
        ConnectorDocument::try_new(
            ConnectorDocumentOwner::parse("novarocks.mv").unwrap(),
            ConnectorDocumentName::parse("definition").unwrap(),
            ConnectorDocumentFormat::try_new("novarocks.mv", "definition", 999).unwrap(),
            content,
            Vec::new(),
            ConnectorDocumentAttachment::TableMetadata,
        )
        .unwrap()
    }

    #[test]
    fn large_opaque_document_is_explicitly_loaded_from_immutable_sidecar() {
        let tokio = tokio::runtime::Runtime::new().unwrap();
        let runtime = crate::resources::IcebergCatalogRuntime::new(tokio.handle().clone());
        let file_io = crate::iceberg::io::FileIO::new_with_memory();
        let content = Bytes::from(vec![0xA5; 9 * 1024]);
        let documents = ConnectorDocumentSet::try_new(vec![document(content.clone())]).unwrap();
        let context = context(Arc::new(Active));
        let manifest = prepare_document_carriers(
            &runtime,
            &file_io,
            "memory://warehouse/table",
            &documents,
            &context,
        )
        .unwrap();
        assert!(matches!(
            manifest.documents[0].carrier,
            IcebergDocumentCarrierV1::Deferred { .. }
        ));
        let stored = crate::document_storage::codec::stored_document(&manifest.documents[0])
            .expect("stored envelope");
        let loaded = load_deferred_document(
            &runtime,
            &file_io,
            &stored,
            novarocks_spi::connector::ConnectorDocumentStorageLimits::spec_default(),
            &context,
        )
        .unwrap();
        assert_eq!(loaded.content(), &content);
        assert_eq!(loaded.format().version(), 999);
    }

    #[test]
    fn cancellation_is_checked_before_sidecar_allocation_or_write() {
        let tokio = tokio::runtime::Runtime::new().unwrap();
        let runtime = crate::resources::IcebergCatalogRuntime::new(tokio.handle().clone());
        let file_io = crate::iceberg::io::FileIO::new_with_memory();
        let documents =
            ConnectorDocumentSet::try_new(vec![document(Bytes::from(vec![1; 9 * 1024]))]).unwrap();
        let error = prepare_document_carriers(
            &runtime,
            &file_io,
            "memory://warehouse/table",
            &documents,
            &context(Arc::new(Cancelled)),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    }

    #[test]
    fn caller_decode_limit_rejects_a_sidecar_before_reading_it() {
        let tokio = tokio::runtime::Runtime::new().unwrap();
        let runtime = crate::resources::IcebergCatalogRuntime::new(tokio.handle().clone());
        let file_io = crate::iceberg::io::FileIO::new_with_memory();
        let documents =
            ConnectorDocumentSet::try_new(vec![document(Bytes::from(vec![7; 9 * 1024]))]).unwrap();
        let context = context(Arc::new(Active));
        let manifest = prepare_document_carriers(
            &runtime,
            &file_io,
            "memory://warehouse/table",
            &documents,
            &context,
        )
        .unwrap();
        let stored = crate::document_storage::codec::stored_document(&manifest.documents[0])
            .expect("stored envelope");
        let limits = novarocks_spi::connector::ConnectorDocumentStorageLimits::try_new(
            1024, 1024, 1024, 4, 4, 64,
        )
        .unwrap();
        assert_eq!(
            load_deferred_document(&runtime, &file_io, &stored, limits, &context)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted,
        );
    }

    #[test]
    fn missing_and_tampered_sidecars_keep_distinct_error_kinds() {
        let tokio = tokio::runtime::Runtime::new().unwrap();
        let runtime = crate::resources::IcebergCatalogRuntime::new(tokio.handle().clone());
        let file_io = crate::iceberg::io::FileIO::new_with_memory();
        let content = Bytes::from(vec![0x3C; 9 * 1024]);
        let documents = ConnectorDocumentSet::try_new(vec![document(content.clone())]).unwrap();
        let context = context(Arc::new(Active));
        let manifest = prepare_document_carriers(
            &runtime,
            &file_io,
            "memory://warehouse/table",
            &documents,
            &context,
        )
        .unwrap();
        let stored = crate::document_storage::codec::stored_document(&manifest.documents[0])
            .expect("stored envelope");
        let IcebergDocumentCarrierV1::Deferred { location } = &manifest.documents[0].carrier else {
            panic!("large document must use a sidecar");
        };
        let output = file_io.new_output(location).unwrap();
        runtime
            .block_on(async move { output.delete().await })
            .unwrap()
            .unwrap();
        assert_eq!(
            load_deferred_document(
                &runtime,
                &file_io,
                &stored,
                novarocks_spi::connector::ConnectorDocumentStorageLimits::spec_default(),
                &context,
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::NotFound
        );

        let output = file_io.new_output(location).unwrap();
        let content_len = content.len();
        runtime
            .block_on(async move { output.write(Bytes::from(vec![0xC3; content_len])).await })
            .unwrap()
            .unwrap();
        assert_eq!(
            load_deferred_document(
                &runtime,
                &file_io,
                &stored,
                novarocks_spi::connector::ConnectorDocumentStorageLimits::spec_default(),
                &context,
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::CorruptData
        );
    }
}
