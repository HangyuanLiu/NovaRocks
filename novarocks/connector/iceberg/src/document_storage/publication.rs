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

//! Binding prepared application documents to Iceberg commit versions.

use std::collections::{BTreeMap, HashMap};

use novarocks_spi::connector::{
    ConnectorDocumentAttachment, ConnectorDocumentPublicationIntent, ConnectorDocumentUpdateIntent,
    ConnectorError, ConnectorErrorKind, ConnectorManagedObjectMarkerChange,
    ConnectorMutationOperationId, ConnectorPreparedDocumentSet,
};
use sha2::{Digest, Sha256};

use super::envelope::{
    DOCUMENT_MANIFEST_PROPERTY, IcebergDocumentAttachmentV1, IcebergDocumentManifestV1,
};

/// An internal action input, never a persisted Iceberg summary property.
///
/// Snapshot ids belong to the commit action, so `finish_write` cannot resolve
/// CommitOutput while it still owns only the prepared manifest. The action
/// removes this key after it mints the id and replaces it with the real
/// `DOCUMENT_MANIFEST_PROPERTY` in the same snapshot summary.
pub(crate) const PENDING_DOCUMENT_MANIFEST_PROPERTY: &str =
    "novarocks.internal.pending-documents.v1";
pub(crate) const DOCUMENT_PREPARATION_DIGEST_PROPERTY: &str =
    "novarocks.documents.preparation-digest.v1";
pub(crate) const DOCUMENT_UPDATE_OPERATION_PROPERTY: &str =
    "novarocks.documents.update.operation-id.v1";

pub(crate) fn update_properties(
    intent: &ConnectorDocumentUpdateIntent,
    operation_id: ConnectorMutationOperationId,
    current_metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<HashMap<String, String>, ConnectorError> {
    let prepared = intent.prepared_documents();
    let replacements = validated_prepared_manifest(prepared)?;
    if replacements.documents.iter().any(|document| {
        !matches!(
            document.attachment,
            IcebergDocumentAttachmentV1::TableMetadata
        )
    }) {
        return Err(corrupt(
            "Iceberg document update contains a non-metadata attachment",
        ));
    }
    let manifest = merge_table_metadata_manifest(current_metadata, replacements)?;
    let encoded = super::codec::encode_document_manifest(&manifest)?;
    let encoded = std::str::from_utf8(&encoded)
        .map_err(|_| corrupt("prepared Iceberg document manifest is not UTF-8 metadata"))?;
    let mut properties =
        HashMap::from([(DOCUMENT_MANIFEST_PROPERTY.to_string(), encoded.to_string())]);
    properties.insert(
        DOCUMENT_UPDATE_OPERATION_PROPERTY.to_string(),
        operation_marker(operation_id),
    );
    if let ConnectorManagedObjectMarkerChange::Replace { replacement, .. } = intent.marker_change()
    {
        properties.insert(
            super::observation::MANAGED_KIND_PROPERTY.to_string(),
            replacement.kind().to_string(),
        );
        properties.insert(
            super::observation::MANAGED_OWNER_PROPERTY.to_string(),
            replacement.owner().to_string(),
        );
        properties.insert(
            super::observation::MANAGED_INCARNATION_PROPERTY.to_string(),
            replacement.incarnation().to_string(),
        );
    }
    Ok(properties)
}

/// Merge a partial application-document update into the exact table-metadata
/// manifest loaded and version-checked by the caller.
///
/// An update document replaces the document with the same `(owner, name)` and
/// leaves every other envelope byte-for-byte unchanged. New names are appended
/// in the provider-prepared order. Commit-output documents live on snapshots,
/// so neither the current nor replacement table-metadata manifest may contain
/// one.
fn merge_table_metadata_manifest(
    current_metadata: &crate::iceberg::spec::TableMetadata,
    replacements: IcebergDocumentManifestV1,
) -> Result<IcebergDocumentManifestV1, ConnectorError> {
    let mut current = match current_metadata
        .properties()
        .get(DOCUMENT_MANIFEST_PROPERTY)
    {
        Some(encoded) => super::codec::decode_document_manifest(encoded.as_bytes())?,
        None => IcebergDocumentManifestV1 {
            version: super::envelope::DOCUMENT_MANIFEST_VERSION,
            documents: Vec::new(),
        },
    };
    if current.documents.iter().any(|document| {
        !matches!(
            document.attachment,
            IcebergDocumentAttachmentV1::TableMetadata
        )
    }) {
        return Err(corrupt(
            "Iceberg table-metadata document manifest contains a non-metadata attachment",
        ));
    }

    let mut replacement_indexes = HashMap::new();
    let mut replacements = replacements
        .documents
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    for (index, replacement) in replacements.iter().enumerate() {
        let replacement = replacement
            .as_ref()
            .expect("replacement document is present while indexing");
        if replacement_indexes
            .insert((replacement.owner.clone(), replacement.name.clone()), index)
            .is_some()
        {
            return Err(corrupt(
                "Iceberg document update repeats an owner/name identity",
            ));
        }
    }

    let mut current_names = std::collections::HashSet::new();
    for document in &mut current.documents {
        let key = (document.owner.clone(), document.name.clone());
        if !current_names.insert(key.clone()) {
            return Err(corrupt(
                "Iceberg table-metadata manifest repeats an owner/name identity",
            ));
        }
        if let Some(index) = replacement_indexes.get(&key) {
            *document = replacements[*index]
                .take()
                .expect("a replacement owner/name is consumed exactly once");
        }
    }
    current.documents.extend(replacements.into_iter().flatten());
    Ok(current)
}

/// Prepare the metadata-attached part of one publication against its frozen
/// target. The resulting SetProperties update must share the catalog commit
/// with the snapshot that carries the commit-output documents.
pub(crate) fn publication_metadata_properties(
    intent: &ConnectorDocumentPublicationIntent,
    current_metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<Option<HashMap<String, String>>, ConnectorError> {
    let prepared = validated_prepared_manifest(intent.prepared_documents())?;
    let replacements = prepared
        .documents
        .into_iter()
        .filter(|document| {
            matches!(
                document.attachment,
                IcebergDocumentAttachmentV1::TableMetadata
            )
        })
        .collect::<Vec<_>>();
    if replacements.is_empty() {
        return Ok(None);
    }
    let merged = merge_table_metadata_manifest(
        current_metadata,
        IcebergDocumentManifestV1 {
            version: super::envelope::DOCUMENT_MANIFEST_VERSION,
            documents: replacements,
        },
    )?;
    let encoded = super::codec::encode_document_manifest(&merged)?;
    let encoded = String::from_utf8(encoded.to_vec())
        .map_err(|_| corrupt("prepared Iceberg metadata manifest is not UTF-8"))?;
    Ok(Some(HashMap::from([(
        DOCUMENT_MANIFEST_PROPERTY.to_string(),
        encoded,
    )])))
}

pub(crate) fn operation_marker(operation_id: ConnectorMutationOperationId) -> String {
    operation_id
        .to_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn manifest_digest(encoded: &str) -> [u8; 32] {
    prepared_manifest_digest(encoded.as_bytes())
}

pub(crate) fn prepared_manifest_digest(encoded: &[u8]) -> [u8; 32] {
    Sha256::digest(encoded).into()
}

fn digest_marker(digest: [u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn add_pending_snapshot_property(
    properties: &mut BTreeMap<String, String>,
    intent: &ConnectorDocumentPublicationIntent,
) -> Result<(), ConnectorError> {
    let manifest = validated_prepared_manifest(intent.prepared_documents())?;
    if !manifest.documents.iter().any(|document| {
        matches!(
            document.attachment,
            IcebergDocumentAttachmentV1::CommitOutput
        )
    }) {
        return Err(corrupt(
            "Iceberg publication manifest has no commit-output attachment",
        ));
    }
    let encoded = super::codec::encode_document_manifest(&manifest)?;
    let encoded = std::str::from_utf8(&encoded)
        .map_err(|_| corrupt("prepared Iceberg publication manifest is not UTF-8 metadata"))?;
    if properties
        .insert(
            PENDING_DOCUMENT_MANIFEST_PROPERTY.to_string(),
            encoded.to_string(),
        )
        .is_some()
        || properties.contains_key(DOCUMENT_MANIFEST_PROPERTY)
    {
        return Err(corrupt(
            "Iceberg publication manifest conflicts with snapshot properties",
        ));
    }
    Ok(())
}

pub(crate) fn resolve_snapshot_properties(
    properties: &BTreeMap<String, String>,
    table_uuid: uuid::Uuid,
    snapshot_id: i64,
) -> Result<BTreeMap<String, String>, ConnectorError> {
    let mut resolved = properties.clone();
    let Some(encoded) = resolved.remove(PENDING_DOCUMENT_MANIFEST_PROPERTY) else {
        return Ok(resolved);
    };
    if resolved.contains_key(DOCUMENT_MANIFEST_PROPERTY)
        || resolved.contains_key(DOCUMENT_PREPARATION_DIGEST_PROPERTY)
    {
        return Err(corrupt(
            "Iceberg publication contains conflicting document properties",
        ));
    }
    resolved.insert(
        DOCUMENT_PREPARATION_DIGEST_PROPERTY.to_string(),
        digest_marker(prepared_manifest_digest(encoded.as_bytes())),
    );
    let mut manifest = super::codec::decode_document_manifest(encoded.as_bytes())?;
    // TableMetadata envelopes are committed in the same TableCommit as
    // SetProperties. A snapshot may retain only its output attachments.
    manifest.documents.retain(|document| {
        !matches!(
            document.attachment,
            IcebergDocumentAttachmentV1::TableMetadata
        )
    });
    let committed = super::observation::output_committed_version(table_uuid, snapshot_id)?;
    for document in &mut manifest.documents {
        if matches!(
            document.attachment,
            IcebergDocumentAttachmentV1::CommitOutput
        ) {
            document.attachment = IcebergDocumentAttachmentV1::ExactOutput {
                committed_version: committed.payload().to_vec(),
                snapshot_id: committed.snapshot_id(),
            };
        }
    }
    let encoded = super::codec::encode_document_manifest(&manifest)?;
    let encoded = String::from_utf8(encoded.to_vec())
        .map_err(|_| corrupt("resolved Iceberg publication manifest is not UTF-8 metadata"))?;
    resolved.insert(DOCUMENT_MANIFEST_PROPERTY.to_string(), encoded);
    Ok(resolved)
}

pub(crate) fn validate_committed_snapshot(
    metadata: &crate::iceberg::spec::TableMetadata,
    snapshot_id: i64,
) -> Result<(), ConnectorError> {
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| corrupt("Iceberg publication snapshot is absent during finalization"))?;
    if snapshot
        .summary()
        .additional_properties
        .contains_key(PENDING_DOCUMENT_MANIFEST_PROPERTY)
    {
        return Err(corrupt(
            "Iceberg publication persisted its internal pending manifest",
        ));
    }
    let encoded = snapshot
        .summary()
        .additional_properties
        .get(DOCUMENT_MANIFEST_PROPERTY)
        .ok_or_else(|| corrupt("Iceberg publication snapshot has no document manifest"))?;
    let preparation_digest = snapshot
        .summary()
        .additional_properties
        .get(DOCUMENT_PREPARATION_DIGEST_PROPERTY)
        .ok_or_else(|| corrupt("Iceberg publication snapshot has no preparation digest"))?;
    if preparation_digest.len() != 64
        || !preparation_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(corrupt(
            "Iceberg publication snapshot has an invalid preparation digest",
        ));
    }
    let manifest = super::codec::decode_document_manifest(encoded.as_bytes())?;
    let expected = super::observation::output_committed_version(metadata.uuid(), snapshot_id)?;
    let mut exact_outputs = 0usize;
    for document in &manifest.documents {
        match &document.attachment {
            IcebergDocumentAttachmentV1::ExactOutput {
                committed_version,
                snapshot_id: Some(attached_snapshot),
            } if committed_version.as_slice() == expected.payload().as_ref()
                && *attached_snapshot == snapshot_id =>
            {
                exact_outputs += 1;
            }
            IcebergDocumentAttachmentV1::CommitOutput => {
                return Err(corrupt(
                    "Iceberg publication snapshot contains an unresolved commit output",
                ));
            }
            IcebergDocumentAttachmentV1::TableMetadata => {
                return Err(corrupt(
                    "Iceberg publication snapshot contains a metadata-attached document",
                ));
            }
            _ => {}
        }
    }
    if exact_outputs == 0 {
        return Err(corrupt(
            "Iceberg publication snapshot has no exact output attachment",
        ));
    }
    Ok(())
}

pub(crate) fn validate_expected_manifest(
    metadata: &crate::iceberg::spec::TableMetadata,
    snapshot_id: i64,
    unresolved: &[u8],
) -> Result<(), ConnectorError> {
    let unresolved = std::str::from_utf8(unresolved)
        .map_err(|_| corrupt("Iceberg evidence document manifest is not UTF-8 metadata"))?;
    let pending = BTreeMap::from([(
        PENDING_DOCUMENT_MANIFEST_PROPERTY.to_string(),
        unresolved.to_string(),
    )]);
    let expected = resolve_snapshot_properties(&pending, metadata.uuid(), snapshot_id)?;
    let actual = metadata.snapshot_by_id(snapshot_id).and_then(|snapshot| {
        snapshot
            .summary()
            .additional_properties
            .get(DOCUMENT_MANIFEST_PROPERTY)
    });
    if actual != expected.get(DOCUMENT_MANIFEST_PROPERTY) {
        return Err(corrupt(
            "Iceberg publication snapshot document manifest differs from its exact write evidence",
        ));
    }
    let prepared = super::codec::decode_document_manifest(unresolved.as_bytes())?;
    let metadata_documents = prepared
        .documents
        .iter()
        .filter(|document| {
            matches!(
                document.attachment,
                IcebergDocumentAttachmentV1::TableMetadata
            )
        })
        .collect::<Vec<_>>();
    if !metadata_documents.is_empty() {
        let actual = metadata
            .properties()
            .get(DOCUMENT_MANIFEST_PROPERTY)
            .ok_or_else(|| corrupt("Iceberg publication has no metadata document manifest"))?;
        let actual = super::codec::decode_document_manifest(actual.as_bytes())?;
        if metadata_documents.iter().any(|expected| {
            actual
                .documents
                .iter()
                .filter(|candidate| {
                    candidate.owner == expected.owner && candidate.name == expected.name
                })
                .collect::<Vec<_>>()
                .as_slice()
                != [*expected]
        }) {
            return Err(corrupt(
                "Iceberg publication metadata document differs from its exact write evidence",
            ));
        }
    }
    validate_expected_manifest_digest(
        metadata,
        snapshot_id,
        prepared_manifest_digest(unresolved.as_bytes()),
    )
}

pub(crate) fn validate_expected_manifest_digest(
    metadata: &crate::iceberg::spec::TableMetadata,
    snapshot_id: i64,
    expected: [u8; 32],
) -> Result<(), ConnectorError> {
    let actual = metadata.snapshot_by_id(snapshot_id).and_then(|snapshot| {
        snapshot
            .summary()
            .additional_properties
            .get(DOCUMENT_PREPARATION_DIGEST_PROPERTY)
    });
    let expected = digest_marker(expected);
    if actual.map(String::as_str) != Some(expected.as_str()) {
        return Err(corrupt(
            "Iceberg publication snapshot preparation digest differs from its exact write evidence",
        ));
    }
    validate_committed_snapshot(metadata, snapshot_id)
}

fn validated_prepared_manifest(
    prepared: &ConnectorPreparedDocumentSet,
) -> Result<IcebergDocumentManifestV1, ConnectorError> {
    let manifest = super::codec::decode_document_manifest(prepared.provider_token())?;
    if manifest.documents.len() != prepared.documents().len()
        || manifest
            .documents
            .iter()
            .zip(prepared.documents())
            .any(|(envelope, document)| {
                envelope.owner != document.id().owner().as_str()
                    || envelope.name != document.id().name().as_str()
                    || envelope.revision != document.id().revision().to_bytes()
                    || !attachment_matches(&envelope.attachment, document.attachment())
            })
    {
        return Err(corrupt(
            "prepared Iceberg document manifest does not match its exact document set",
        ));
    }
    Ok(manifest)
}

fn attachment_matches(
    encoded: &IcebergDocumentAttachmentV1,
    expected: &ConnectorDocumentAttachment,
) -> bool {
    match (encoded, expected) {
        (
            IcebergDocumentAttachmentV1::TableMetadata,
            ConnectorDocumentAttachment::TableMetadata,
        )
        | (IcebergDocumentAttachmentV1::CommitOutput, ConnectorDocumentAttachment::CommitOutput) => {
            true
        }
        (
            IcebergDocumentAttachmentV1::ExactOutput {
                committed_version,
                snapshot_id,
            },
            ConnectorDocumentAttachment::ExactOutput(expected),
        ) => {
            committed_version.as_slice() == expected.payload().as_ref()
                && *snapshot_id == expected.snapshot_id()
        }
        _ => false,
    }
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::document_storage::envelope::{
        DOCUMENT_ENVELOPE_VERSION, DOCUMENT_MANIFEST_VERSION, IcebergDocumentCarrierV1,
        IcebergDocumentEnvelopeV1,
    };

    fn prepared_manifest(name: &str) -> Bytes {
        let content = name.as_bytes().to_vec();
        let revision =
            novarocks_spi::connector::ConnectorDocumentRevision::for_content(&content).to_bytes();
        super::super::codec::encode_document_manifest(&IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![IcebergDocumentEnvelopeV1 {
                version: DOCUMENT_ENVELOPE_VERSION,
                owner: "novarocks.mv".to_string(),
                name: name.to_string(),
                format_owner: "novarocks.mv".to_string(),
                format_name: name.to_string(),
                format_version: 1,
                revision,
                encoded_len: content.len() as u64,
                references: Vec::new(),
                attachment: IcebergDocumentAttachmentV1::CommitOutput,
                carrier: IcebergDocumentCarrierV1::Available { content },
            }],
        })
        .unwrap()
    }

    fn metadata_with_snapshot_properties(
        table_uuid: uuid::Uuid,
        snapshot_id: i64,
        properties: BTreeMap<String, String>,
    ) -> crate::iceberg::spec::TableMetadata {
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
        let base = crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://warehouse/table".to_string(),
            crate::iceberg::spec::FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .assign_uuid(table_uuid)
        .build()
        .unwrap()
        .metadata;
        let snapshot = crate::iceberg::spec::Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_sequence_number(1)
            .with_timestamp_ms(base.last_updated_ms() + 1)
            .with_manifest_list("memory://table/snap.avro")
            .with_summary(crate::iceberg::spec::Summary {
                operation: crate::iceberg::spec::Operation::Append,
                additional_properties: properties.into_iter().collect(),
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
    fn commit_output_is_bound_after_the_snapshot_id_exists() {
        let content = vec![1, 2, 3];
        let revision =
            novarocks_spi::connector::ConnectorDocumentRevision::for_content(&content).to_bytes();
        let manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![IcebergDocumentEnvelopeV1 {
                version: DOCUMENT_ENVELOPE_VERSION,
                owner: "novarocks.mv".to_string(),
                name: "publication".to_string(),
                format_owner: "novarocks.mv".to_string(),
                format_name: "publication".to_string(),
                format_version: 1,
                revision,
                encoded_len: 3,
                references: Vec::new(),
                attachment: IcebergDocumentAttachmentV1::CommitOutput,
                carrier: IcebergDocumentCarrierV1::Available { content },
            }],
        };
        let encoded = super::super::codec::encode_document_manifest(&manifest).unwrap();
        let properties = BTreeMap::from([(
            PENDING_DOCUMENT_MANIFEST_PROPERTY.to_string(),
            String::from_utf8(encoded.to_vec()).unwrap(),
        )]);
        let table_uuid = uuid::Uuid::new_v4();
        let resolved = resolve_snapshot_properties(&properties, table_uuid, 41).unwrap();
        assert!(!resolved.contains_key(PENDING_DOCUMENT_MANIFEST_PROPERTY));
        assert_eq!(
            resolved[DOCUMENT_PREPARATION_DIGEST_PROPERTY],
            digest_marker(prepared_manifest_digest(encoded.as_ref()))
        );
        let stored = super::super::codec::decode_document_manifest(
            resolved[DOCUMENT_MANIFEST_PROPERTY].as_bytes(),
        )
        .unwrap();
        let IcebergDocumentAttachmentV1::ExactOutput {
            committed_version,
            snapshot_id,
        } = &stored.documents[0].attachment
        else {
            panic!("commit output was not bound")
        };
        let expected = super::super::observation::output_committed_version(table_uuid, 41).unwrap();
        assert_eq!(committed_version.as_slice(), expected.payload().as_ref());
        assert_eq!(*snapshot_id, Some(41));
    }

    #[test]
    fn exact_validation_rejects_a_different_manifest_with_the_same_preparation_digest() {
        let table_uuid = uuid::Uuid::new_v4();
        let snapshot_id = 41;
        let expected = prepared_manifest("expected");
        let other = prepared_manifest("other");
        let mut actual = resolve_snapshot_properties(
            &BTreeMap::from([(
                PENDING_DOCUMENT_MANIFEST_PROPERTY.to_string(),
                String::from_utf8(other.to_vec()).unwrap(),
            )]),
            table_uuid,
            snapshot_id,
        )
        .unwrap();
        actual.insert(
            DOCUMENT_PREPARATION_DIGEST_PROPERTY.to_string(),
            digest_marker(prepared_manifest_digest(expected.as_ref())),
        );
        let metadata = metadata_with_snapshot_properties(table_uuid, snapshot_id, actual);

        assert_eq!(
            validate_expected_manifest(&metadata, snapshot_id, expected.as_ref())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn metadata_publication_replaces_only_the_named_layout_document() {
        let metadata_document = |name: &str, content: &[u8]| IcebergDocumentEnvelopeV1 {
            version: DOCUMENT_ENVELOPE_VERSION,
            owner: "novarocks.mv".to_string(),
            name: name.to_string(),
            format_owner: "novarocks.mv".to_string(),
            format_name: name.to_string(),
            format_version: 1,
            revision: novarocks_spi::connector::ConnectorDocumentRevision::for_content(content)
                .to_bytes(),
            encoded_len: content.len() as u64,
            references: Vec::new(),
            attachment: IcebergDocumentAttachmentV1::TableMetadata,
            carrier: IcebergDocumentCarrierV1::Available {
                content: content.to_vec(),
            },
        };
        let old_layout = metadata_document("layout", b"old-layout");
        let configuration = metadata_document("configuration", b"unchanged");
        let current_manifest =
            super::super::codec::encode_document_manifest(&IcebergDocumentManifestV1 {
                version: DOCUMENT_MANIFEST_VERSION,
                documents: vec![old_layout.clone(), configuration.clone()],
            })
            .expect("old metadata manifest");
        let current = metadata_with_snapshot_properties(uuid::Uuid::new_v4(), 41, BTreeMap::new())
            .into_builder(None)
            .set_properties(HashMap::from([(
                DOCUMENT_MANIFEST_PROPERTY.to_string(),
                String::from_utf8(current_manifest.to_vec()).expect("manifest text"),
            )]))
            .expect("set old documents")
            .build()
            .expect("current metadata")
            .metadata;
        let new_layout = metadata_document("layout", b"new-layout");
        let merged = merge_table_metadata_manifest(
            &current,
            IcebergDocumentManifestV1 {
                version: DOCUMENT_MANIFEST_VERSION,
                documents: vec![new_layout.clone()],
            },
        )
        .expect("replace layout");
        assert_eq!(merged.documents, vec![new_layout, configuration]);
    }
}
