// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use novarocks_spi::connector::{ConnectorDocumentId, ConnectorError};

use super::envelope::{IcebergDocumentCarrierV1, IcebergDocumentManifestV1};

/// Reference counts for physical document carriers across every retained
/// metadata/snapshot/ref root. A sidecar becomes collectable only at zero.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct IcebergDocumentRetentionIndex {
    reference_counts: HashMap<ConnectorDocumentId, usize>,
    sidecars: HashMap<ConnectorDocumentId, HashSet<String>>,
}

impl IcebergDocumentRetentionIndex {
    pub(crate) fn from_manifests(
        manifests: impl IntoIterator<Item = (IcebergDocumentManifestV1, Vec<ConnectorDocumentId>)>,
    ) -> Result<Self, ConnectorError> {
        let mut index = Self::default();
        let manifests = manifests.into_iter().collect::<Vec<_>>();
        let mut by_id = HashMap::new();
        // Snapshot P references D/L on table metadata. Resolve the graph over
        // all retained physical manifests, never one attachment in isolation.
        // A missing historical revision remains an error, so cleanup cannot
        // delete a carrier whose reachability it cannot prove.
        for (manifest, _) in &manifests {
            for envelope in &manifest.documents {
                let stored = super::codec::stored_document(envelope)?;
                if let Some(previous) = by_id.insert(stored.id().clone(), stored.clone())
                    && previous != stored
                {
                    return Err(ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                        "Iceberg retained manifests disagree on a document identity",
                    ));
                }
                if let IcebergDocumentCarrierV1::Deferred { location } = &envelope.carrier {
                    index
                        .sidecars
                        .entry(stored.id().clone())
                        .or_default()
                        .insert(location.clone());
                }
            }
        }
        for (_, roots) in manifests {
            let reachable = super::reference::reachable_document_ids(&by_id, roots)?;
            for id in reachable {
                let count = index.reference_counts.entry(id).or_default();
                *count = count.checked_add(1).ok_or_else(|| {
                    super::codec::exhausted(
                        "Iceberg document retention reference count exceeds its limit",
                    )
                })?;
            }
        }
        Ok(index)
    }

    pub(crate) fn retained_sidecars(&self) -> HashSet<&str> {
        self.reference_counts
            .iter()
            .filter(|(_, count)| **count > 0)
            .filter_map(|(id, _)| self.sidecars.get(id))
            .flat_map(|locations| locations.iter().map(String::as_str))
            .collect()
    }

    #[cfg(test)]
    fn is_reachable(&self, id: &ConnectorDocumentId) -> bool {
        self.reference_counts
            .get(id)
            .is_some_and(|count| *count > 0)
    }
}

pub(crate) fn retained_sidecars_for_roots(
    metadata: &crate::iceberg::spec::TableMetadata,
    roots: Option<&[novarocks_spi::connector::ConnectorDocumentRetentionRoot]>,
) -> Result<HashSet<String>, ConnectorError> {
    let include_physical_attachments = roots.is_none();
    let requested_roots = roots
        .unwrap_or_default()
        .iter()
        .map(|root| root.document().clone())
        .collect::<HashSet<_>>();
    let mut observed_requested_roots = HashSet::with_capacity(requested_roots.len());
    let mut retained_manifests = Vec::new();

    if let Some(encoded) = metadata
        .properties()
        .get(super::envelope::DOCUMENT_MANIFEST_PROPERTY)
    {
        let manifest = super::codec::decode_document_manifest(encoded.as_bytes())?;
        let manifest_roots = physical_roots(
            metadata,
            &manifest,
            PhysicalAttachmentRoot::TableMetadata,
            &requested_roots,
            &mut observed_requested_roots,
            include_physical_attachments,
        )?;
        retained_manifests.push((manifest, manifest_roots));
    }

    for snapshot in metadata.snapshots() {
        let Some(encoded) = snapshot
            .summary()
            .additional_properties
            .get(super::envelope::DOCUMENT_MANIFEST_PROPERTY)
        else {
            continue;
        };
        let manifest = super::codec::decode_document_manifest(encoded.as_bytes())?;
        let manifest_roots = physical_roots(
            metadata,
            &manifest,
            PhysicalAttachmentRoot::Snapshot,
            &requested_roots,
            &mut observed_requested_roots,
            include_physical_attachments,
        )?;
        retained_manifests.push((manifest, manifest_roots));
    }

    if observed_requested_roots != requested_roots {
        return Err(ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::CorruptData,
            "Iceberg document retention graph names a document absent from retained metadata",
        ));
    }

    let index = IcebergDocumentRetentionIndex::from_manifests(retained_manifests)?;
    Ok(index
        .retained_sidecars()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect())
}

#[derive(Clone, Copy)]
enum PhysicalAttachmentRoot {
    TableMetadata,
    Snapshot,
}

fn physical_roots(
    metadata: &crate::iceberg::spec::TableMetadata,
    manifest: &IcebergDocumentManifestV1,
    attachment_root: PhysicalAttachmentRoot,
    requested_roots: &HashSet<ConnectorDocumentId>,
    observed_requested_roots: &mut HashSet<ConnectorDocumentId>,
    include_physical_attachments: bool,
) -> Result<Vec<ConnectorDocumentId>, ConnectorError> {
    let mut roots = Vec::new();
    for envelope in &manifest.documents {
        let document = super::codec::stored_document(envelope)?;
        let id = document.id().clone();
        if requested_roots.contains(&id) {
            observed_requested_roots.insert(id.clone());
            roots.push(id.clone());
        }
        let is_physical_root = match (&attachment_root, &envelope.attachment) {
            (
                PhysicalAttachmentRoot::TableMetadata,
                super::envelope::IcebergDocumentAttachmentV1::TableMetadata,
            ) => true,
            (
                PhysicalAttachmentRoot::TableMetadata,
                super::envelope::IcebergDocumentAttachmentV1::ExactOutput {
                    snapshot_id: Some(snapshot_id),
                    ..
                },
            ) => metadata.snapshot_by_id(*snapshot_id).is_some(),
            (
                PhysicalAttachmentRoot::Snapshot,
                super::envelope::IcebergDocumentAttachmentV1::ExactOutput {
                    snapshot_id: Some(_),
                    ..
                },
            ) => true,
            _ => false,
        };
        if include_physical_attachments && is_physical_root && !roots.contains(&id) {
            roots.push(id);
        }
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document_storage::envelope::{
        DOCUMENT_ENVELOPE_VERSION, DOCUMENT_MANIFEST_VERSION, IcebergDocumentAttachmentV1,
        IcebergDocumentCarrierV1, IcebergDocumentEnvelopeV1, IcebergDocumentReferenceV1,
    };
    use novarocks_spi::connector::{
        ConnectorDocumentName, ConnectorDocumentOwner, ConnectorDocumentRevision,
    };

    fn id(name: &str, content: &[u8]) -> ConnectorDocumentId {
        ConnectorDocumentId::new(
            ConnectorDocumentOwner::parse("novarocks.mv").unwrap(),
            ConnectorDocumentName::parse(name).unwrap(),
            ConnectorDocumentRevision::for_content(content),
        )
    }

    fn envelope(
        name: &str,
        content: &[u8],
        references: Vec<IcebergDocumentReferenceV1>,
    ) -> IcebergDocumentEnvelopeV1 {
        IcebergDocumentEnvelopeV1 {
            version: DOCUMENT_ENVELOPE_VERSION,
            owner: "novarocks.mv".to_string(),
            name: name.to_string(),
            format_owner: "novarocks.mv".to_string(),
            format_name: name.to_string(),
            format_version: 1,
            revision: ConnectorDocumentRevision::for_content(content).to_bytes(),
            encoded_len: content.len() as u64,
            references,
            attachment: IcebergDocumentAttachmentV1::TableMetadata,
            carrier: IcebergDocumentCarrierV1::Deferred {
                location: format!("memory://table/{name}.bin"),
            },
        }
    }

    fn snapshot_manifest(
        snapshot_id: i64,
        publication_content: &[u8],
    ) -> IcebergDocumentManifestV1 {
        let definition = id("definition", b"definition");
        let mut publication = envelope(
            "publication",
            publication_content,
            vec![IcebergDocumentReferenceV1 {
                relationship: "definition".to_string(),
                owner: definition.owner().as_str().to_string(),
                name: definition.name().as_str().to_string(),
                revision: definition.revision().to_bytes(),
            }],
        );
        publication.attachment = IcebergDocumentAttachmentV1::ExactOutput {
            committed_version: format!("snapshot-{snapshot_id}").into_bytes(),
            snapshot_id: Some(snapshot_id),
        };
        publication.carrier = IcebergDocumentCarrierV1::Deferred {
            location: format!("memory://table/publication-{snapshot_id}.bin"),
        };
        IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![
                envelope("definition", b"definition", Vec::new()),
                publication,
            ],
        }
    }

    fn empty_metadata() -> crate::iceberg::spec::TableMetadata {
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
            std::collections::HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    #[test]
    fn shared_document_survives_until_the_last_retained_root_is_released() {
        let definition = id("definition", b"definition");
        let publication = id("publication", b"publication");
        let manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![
                envelope("definition", b"definition", Vec::new()),
                envelope(
                    "publication",
                    b"publication",
                    vec![IcebergDocumentReferenceV1 {
                        relationship: "definition".to_string(),
                        owner: definition.owner().as_str().to_string(),
                        name: definition.name().as_str().to_string(),
                        revision: definition.revision().to_bytes(),
                    }],
                ),
            ],
        };
        let two_roots = IcebergDocumentRetentionIndex::from_manifests([
            (manifest.clone(), vec![publication.clone()]),
            (manifest.clone(), vec![publication.clone()]),
        ])
        .unwrap();
        assert!(two_roots.is_reachable(&definition));
        assert!(
            two_roots
                .retained_sidecars()
                .contains("memory://table/definition.bin")
        );

        let one_root =
            IcebergDocumentRetentionIndex::from_manifests([(manifest, vec![publication])]).unwrap();
        assert!(one_root.is_reachable(&definition));

        let no_roots = IcebergDocumentRetentionIndex::from_manifests(Vec::new()).unwrap();
        assert!(!no_roots.is_reachable(&definition));
        assert!(no_roots.retained_sidecars().is_empty());
    }

    #[test]
    fn cleanup_roots_protect_only_the_reachable_sidecars() {
        let definition = id("definition", b"definition");
        let publication = id("publication", b"publication");
        let orphan = id("orphan", b"orphan");
        let manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![
                envelope("definition", b"definition", Vec::new()),
                envelope(
                    "publication",
                    b"publication",
                    vec![IcebergDocumentReferenceV1 {
                        relationship: "definition".to_string(),
                        owner: definition.owner().as_str().to_string(),
                        name: definition.name().as_str().to_string(),
                        revision: definition.revision().to_bytes(),
                    }],
                ),
                envelope("orphan", b"orphan", Vec::new()),
            ],
        };
        let encoded = crate::document_storage::codec::encode_document_manifest(&manifest).unwrap();
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
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://warehouse/table".to_string(),
            crate::iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::from([(
                crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY.to_string(),
                std::str::from_utf8(&encoded).unwrap().to_string(),
            )]),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let roots =
            vec![novarocks_spi::connector::ConnectorDocumentRetentionRoot::new(publication)];
        let retained = retained_sidecars_for_roots(&metadata, Some(&roots)).unwrap();
        assert!(retained.contains("memory://table/definition.bin"));
        assert!(retained.contains("memory://table/publication.bin"));
        assert!(!retained.contains("memory://table/orphan.bin"));
        assert_ne!(definition, orphan);
    }

    #[test]
    fn product_publication_reaches_table_documents_across_manifests() {
        // The product stores D/L/C on table metadata and only P on the
        // snapshot. A per-manifest graph cannot resolve P's D/L references.
        let table_manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![envelope("definition", b"definition", Vec::new())],
        };
        let mut publication_manifest = snapshot_manifest(11, b"publication-11");
        publication_manifest
            .documents
            .retain(|document| document.name == "publication");
        let table_encoded =
            crate::document_storage::codec::encode_document_manifest(&table_manifest).unwrap();
        let publication_encoded =
            crate::document_storage::codec::encode_document_manifest(&publication_manifest)
                .unwrap();
        let base = empty_metadata()
            .into_builder(None)
            .set_properties(std::collections::HashMap::from([(
                crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY.to_string(),
                std::str::from_utf8(&table_encoded).unwrap().to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let snapshot = crate::iceberg::spec::Snapshot::builder()
            .with_snapshot_id(11)
            .with_sequence_number(1)
            .with_timestamp_ms(base.last_updated_ms() + 1)
            .with_manifest_list("memory://table/snap-11.avro")
            .with_summary(crate::iceberg::spec::Summary {
                operation: crate::iceberg::spec::Operation::Append,
                additional_properties: std::collections::HashMap::from([(
                    crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY.to_string(),
                    std::str::from_utf8(&publication_encoded)
                        .unwrap()
                        .to_string(),
                )]),
            })
            .build();
        let metadata = base
            .into_builder(None)
            .add_snapshot(snapshot)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        let retained = retained_sidecars_for_roots(&metadata, None).unwrap();
        assert!(retained.contains("memory://table/definition.bin"));
        assert!(retained.contains("memory://table/publication-11.bin"));
    }

    #[test]
    fn historical_publication_with_missing_definition_revision_blocks_cleanup() {
        let current = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![envelope("definition", b"new-definition", Vec::new())],
        };
        let mut historical = snapshot_manifest(11, b"publication-11");
        historical
            .documents
            .retain(|document| document.name == "publication");
        let publication = id("publication", b"publication-11");
        let error = IcebergDocumentRetentionIndex::from_manifests([
            (current, Vec::new()),
            (historical, vec![publication]),
        ])
        .expect_err("an unresolved historical D revision must prevent cleanup");
        assert_eq!(
            error.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn inherited_maintenance_attachment_survives_until_the_last_snapshot_expires() {
        let manifest_1 = snapshot_manifest(11, b"publication-11");
        // A metadata maintenance snapshot preserves the prior publication
        // envelope verbatim; it does not fabricate a new application payload.
        let manifest_2 = manifest_1.clone();
        let encoded_1 =
            crate::document_storage::codec::encode_document_manifest(&manifest_1).unwrap();
        let encoded_2 =
            crate::document_storage::codec::encode_document_manifest(&manifest_2).unwrap();
        let base = empty_metadata();
        let timestamp_ms = base.last_updated_ms();
        let snapshot_1 = crate::iceberg::spec::Snapshot::builder()
            .with_snapshot_id(11)
            .with_sequence_number(1)
            .with_timestamp_ms(timestamp_ms + 1)
            .with_manifest_list("memory://table/snap-11.avro")
            .with_summary(crate::iceberg::spec::Summary {
                operation: crate::iceberg::spec::Operation::Append,
                additional_properties: std::collections::HashMap::from([(
                    crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY.to_string(),
                    std::str::from_utf8(&encoded_1).unwrap().to_string(),
                )]),
            })
            .build();
        let snapshot_2 = crate::iceberg::spec::Snapshot::builder()
            .with_snapshot_id(12)
            .with_parent_snapshot_id(Some(11))
            .with_sequence_number(2)
            .with_timestamp_ms(timestamp_ms + 2)
            .with_manifest_list("memory://table/snap-12.avro")
            .with_summary(crate::iceberg::spec::Summary {
                operation: crate::iceberg::spec::Operation::Append,
                additional_properties: std::collections::HashMap::from([(
                    crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY.to_string(),
                    std::str::from_utf8(&encoded_2).unwrap().to_string(),
                )]),
            })
            .build();
        let metadata = base
            .into_builder(None)
            .add_snapshot(snapshot_1)
            .unwrap()
            .add_snapshot(snapshot_2)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        let retained = retained_sidecars_for_roots(&metadata, None).unwrap();
        assert!(retained.contains("memory://table/definition.bin"));
        assert!(retained.contains("memory://table/publication-11.bin"));

        let after_first_expiry = metadata
            .clone()
            .into_builder(None)
            .remove_snapshots(&[11])
            .build()
            .unwrap()
            .metadata;
        let retained = retained_sidecars_for_roots(&after_first_expiry, None).unwrap();
        assert!(retained.contains("memory://table/definition.bin"));
        assert!(retained.contains("memory://table/publication-11.bin"));

        let after_last_expiry = metadata
            .into_builder(None)
            .remove_snapshots(&[11, 12])
            .build()
            .unwrap()
            .metadata;
        assert!(
            retained_sidecars_for_roots(&after_last_expiry, None)
                .unwrap()
                .is_empty()
        );
    }
}
