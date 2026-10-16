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

//! `RewriteManifestsCommit` — group manifests by (partition_spec_id,
//! content_type) and merge each group into a single manifest, emitting a
//! single `operation=replace` snapshot.
//!
//! Spec: docs/design/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §5.
//!
//! Key properties:
//! * snapshot.sequence_number = last_sequence_number + 1 (catalog invariant —
//!   iceberg-rs strictly increases snapshot seq per commit). The per-entry
//!   data_sequence_number / file_sequence_number inside merged manifests are
//!   preserved unchanged.
//! * v3 row-lineage fields (first_row_id, referenced_data_file, etc.) round-trip via
//!   ManifestEntry's public fields
//! * DELETED entries are dropped from merged manifests
//! * ADDED + EXISTING entries become EXISTING in the merged manifest

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataFile, DataFileBuilder, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestStatus, ManifestWriterBuilder, Operation, SnapshotReference, SnapshotRetention,
    Summary,
};
use crate::iceberg::{TableRequirement, TableUpdate};

fn clone_data_file_with_first_row_id(
    source: &DataFile,
    partition_spec_id: i32,
    first_row_id: Option<i64>,
) -> Result<DataFile, String> {
    let mut builder = DataFileBuilder::default();
    builder
        .content(source.content_type())
        .file_path(source.file_path().to_string())
        .file_format(source.file_format())
        .partition(source.partition().clone())
        .partition_spec_id(partition_spec_id)
        .record_count(source.record_count())
        .file_size_in_bytes(source.file_size_in_bytes())
        .column_sizes(source.column_sizes().clone())
        .value_counts(source.value_counts().clone())
        .null_value_counts(source.null_value_counts().clone())
        .nan_value_counts(source.nan_value_counts().clone())
        .lower_bounds(source.lower_bounds().clone())
        .upper_bounds(source.upper_bounds().clone())
        .key_metadata(source.key_metadata().map(|bytes| bytes.to_vec()))
        .split_offsets(source.split_offsets().map(|offsets| offsets.to_vec()))
        .equality_ids(source.equality_ids())
        .first_row_id(first_row_id)
        .referenced_data_file(source.referenced_data_file())
        .content_offset(source.content_offset())
        .content_size_in_bytes(source.content_size_in_bytes());
    if let Some(sort_order_id) = source.sort_order_id() {
        builder.sort_order_id(sort_order_id);
    }
    builder
        .build()
        .map_err(|error| format!("clone Iceberg data file for manifest rewrite: {error}"))
}

fn finalize_snapshot_summary(
    mut properties: HashMap<String, String>,
    previous: Option<&Summary>,
    table_uuid: uuid::Uuid,
    previous_snapshot_id: i64,
    new_snapshot_id: i64,
) -> Result<HashMap<String, String>, crate::iceberg::Error> {
    const TOTALS: [(&str, &str, &str); 6] = [
        ("total-data-files", "added-data-files", "deleted-data-files"),
        (
            "total-delete-files",
            "added-delete-files",
            "removed-delete-files",
        ),
        ("total-records", "added-records", "deleted-records"),
        ("total-files-size", "added-files-size", "removed-files-size"),
        (
            "total-position-deletes",
            "added-position-deletes",
            "removed-position-deletes",
        ),
        (
            "total-equality-deletes",
            "added-equality-deletes",
            "removed-equality-deletes",
        ),
    ];
    for (total, added, removed) in TOTALS {
        let base = match previous {
            None => 0,
            Some(summary) => match summary
                .additional_properties
                .get(total)
                .and_then(|value| value.parse::<u64>().ok())
            {
                Some(value) => value,
                None => continue,
            },
        };
        let added = properties
            .get(added)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let removed = properties
            .get(removed)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        properties.insert(
            total.to_string(),
            base.saturating_add(added)
                .saturating_sub(removed)
                .to_string(),
        );
    }
    properties.insert("engine-name".to_string(), "novarocks".to_string());
    properties.insert(
        "engine-version".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
    );
    if let Some(document_manifest) = previous.and_then(|summary| {
        summary
            .additional_properties
            .get(crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY)
    }) {
        let mut manifest =
            crate::document_storage::codec::decode_document_manifest(document_manifest.as_bytes())
                .map_err(document_manifest_error)?;
        let previous_output_version =
            crate::document_storage::observation::output_committed_version(
                table_uuid,
                previous_snapshot_id,
            )
            .map_err(document_manifest_error)?;
        let output_version = crate::document_storage::observation::output_committed_version(
            table_uuid,
            new_snapshot_id,
        )
        .map_err(document_manifest_error)?;
        let mut rebound = false;
        for document in &mut manifest.documents {
            if let crate::document_storage::envelope::IcebergDocumentAttachmentV1::ExactOutput {
                committed_version,
                snapshot_id,
            } = &mut document.attachment
                && *snapshot_id == Some(previous_snapshot_id)
            {
                if committed_version.as_slice() != previous_output_version.payload().as_ref() {
                    return Err(crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Iceberg rewrite document attachment does not match the rewritten table UUID and snapshot",
                    ));
                }
                *committed_version = output_version.payload().to_vec();
                *snapshot_id = Some(new_snapshot_id);
                rebound = true;
            }
        }
        if !rebound {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "Iceberg rewrite document manifest has no exact-output attachment bound to the rewritten snapshot",
            ));
        }
        let document_manifest = crate::document_storage::codec::encode_document_manifest(&manifest)
            .map_err(document_manifest_error)?;
        let document_manifest = String::from_utf8(document_manifest.to_vec()).map_err(|error| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                format!("encode Iceberg rewrite document manifest as UTF-8: {error}"),
            )
        })?;
        properties.insert(
            crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY.to_string(),
            document_manifest,
        );
    }
    Ok(properties)
}

fn document_manifest_error(
    error: novarocks_spi::connector::ConnectorError,
) -> crate::iceberg::Error {
    crate::iceberg::Error::new(
        crate::iceberg::ErrorKind::DataInvalid,
        format!("rewrite Iceberg document manifest: {error}"),
    )
}

/// Stable byte encoding for `ManifestContentType` used as `BTreeMap` key.
/// Data → 0, Deletes → 1. This ensures deterministic iteration order.
fn content_type_key(c: ManifestContentType) -> u8 {
    match c {
        ManifestContentType::Data => 0,
        ManifestContentType::Deletes => 1,
    }
}

/// Spec §5.2 Step 2: group manifest list entries by (partition_spec_id, content_type).
///
/// Uses `BTreeMap<(i32, u8), ...>` so iteration order is deterministic across
/// runs: spec_id ascending, then content_type by encoded byte (Data=0,
/// Deletes=1). This guarantees that the order of entries in the new manifest
/// list — and the order of physical-delete calls — is consistent.
pub(crate) fn group_manifests_by_spec_and_content(
    manifests: &[ManifestFile],
) -> BTreeMap<(i32, u8), Vec<ManifestFile>> {
    let mut groups: BTreeMap<(i32, u8), Vec<ManifestFile>> = BTreeMap::new();
    for m in manifests {
        let key = (m.partition_spec_id, content_type_key(m.content));
        groups.entry(key).or_default().push(m.clone());
    }
    groups
}

/// Merge all entries from a group of manifest files into one new manifest.
/// Drops DELETED entries (spec §5.2 Step 3). Sets remaining entries' status
/// to EXISTING (round-tripping snapshot_id, sequence_number, file_sequence_number
/// and all DataFile v3 row-lineage fields via ManifestWriter::add_existing_file).
async fn merge_manifest_group(
    file_io: &FileIO,
    table_metadata: &crate::iceberg::spec::TableMetadata,
    group: &[ManifestFile],
    new_manifest_path: &str,
    new_snapshot_id: i64,
    format_version: FormatVersion,
) -> Result<ManifestFile, crate::iceberg::Error> {
    let first = group
        .first()
        .ok_or_else(|| rewrite_error("Manifest merge requires a nonempty input group"))?;
    let spec_id = first.partition_spec_id;
    let content = first.content;
    if group
        .iter()
        .any(|manifest| manifest.partition_spec_id != spec_id || manifest.content != content)
    {
        return Err(rewrite_error(
            "Manifest merge input group has conflicting spec or content identities",
        ));
    }
    if format_version == FormatVersion::V1 && content == ManifestContentType::Deletes {
        return Err(crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::FeatureUnsupported,
            "V1 tables cannot contain delete manifests",
        ));
    }

    // Look up partition spec and schema from the table metadata.
    let partition_spec = table_metadata
        .partition_spec_by_id(spec_id)
        .ok_or_else(|| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                format!("partition_spec_id {spec_id} not found in table metadata"),
            )
        })?
        .as_ref()
        .clone();

    let schema = table_metadata.current_schema().clone();

    let output_file = file_io.new_output(new_manifest_path)?;
    let builder =
        ManifestWriterBuilder::new(output_file, Some(new_snapshot_id), schema, partition_spec);

    let mut writer = match (format_version, content) {
        (FormatVersion::V1, ManifestContentType::Data) => builder.build_v1(),
        (FormatVersion::V2, ManifestContentType::Data) => builder.build_v2_data(),
        (FormatVersion::V2, ManifestContentType::Deletes) => builder.build_v2_deletes(),
        (FormatVersion::V3, ManifestContentType::Data) => builder.build_v3_data(),
        (FormatVersion::V3, ManifestContentType::Deletes) => builder.build_v3_deletes(),
        (FormatVersion::V1, ManifestContentType::Deletes) => {
            unreachable!("V1 delete manifests were rejected before output construction")
        }
    };

    // Collect all live entries from all manifests in the group.
    for manifest_file in group {
        let manifest = crate::manifest::load_manifest(&manifest_file, file_io).await?;
        if manifest.metadata().partition_spec().spec_id() != spec_id
            || *manifest.metadata().content() != content
        {
            return Err(rewrite_error(
                "Manifest header contradicts its frozen list descriptor",
            ));
        }

        // For V3 row-lineage: stamp an explicit first_row_id on each data file
        // so that the value is preserved after merge. When INSERT writes data
        // files with first_row_id=None, the manifest-list writer assigns the
        // manifest-level first_row_id. After merging, the new merged manifest
        // would get a fresh manifest-level first_row_id from next_row_id(),
        // causing _row_id values to shift. By stamping at the data-file level,
        // the read path (read.rs:338: df.first_row_id().or(manifest fallback))
        // uses the explicit per-file value, preserving the §5.4 invariant.
        //
        // Reuse the IRU-5 inheritance cursor. Explicit file IDs do not
        // consume a manifest range; absent IDs use only real list assignment.
        let manifest_first_row_id: Option<i64> = manifest_file
            .first_row_id
            .map(i64::try_from)
            .transpose()
            .map_err(|_| rewrite_error("Manifest first_row_id exceeds i64"))?;
        let mut row_ids = crate::commit::staging::FirstRowIdInheritance::new(manifest_first_row_id);

        for entry_ref in manifest.entries() {
            let entry: &ManifestEntry = entry_ref.as_ref();
            if entry.status == ManifestStatus::Deleted {
                // Spec §5.2 Step 3: discard DELETED entries.
                continue;
            }

            // Round-trip the entry as EXISTING, preserving all DataFile fields
            // (including v3 row-lineage: first_row_id, referenced_data_file,
            // content_offset, content_size_in_bytes) via the data_file clone.
            // The sequence numbers and snapshot_id from the original entry are
            // preserved to maintain the causal ordering invariants.
            //
            // load_manifest applies only the protocol's explicit inheritance.
            // Carried files require those actual facts, never guessed defaults.
            let (snap_id, seq, file_seq) = carried_identity(entry)?;

            let orig_df = &entry.data_file;
            // Explicit file IDs do not consume inherited row-ID space. The
            // same staging resolver owns data/delete and overflow semantics.
            let stamped_first_row_id = row_ids.resolve(orig_df, entry.status)?;

            let data_file =
                clone_data_file_with_first_row_id(orig_df, spec_id, stamped_first_row_id).map_err(
                    |e| crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, e),
                )?;
            writer
                .add_existing_file(data_file, snap_id, seq, file_seq)
                .map_err(|e| {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        format!("ManifestWriter::add_existing_file: {e}"),
                    )
                })?;
        }
    }

    writer.write_manifest_file().await
}

fn carried_identity(
    entry: &ManifestEntry,
) -> Result<(i64, i64, Option<i64>), crate::iceberg::Error> {
    let snapshot = entry
        .snapshot_id
        .ok_or_else(|| rewrite_error("Carried manifest entry has no original snapshot identity"))?;
    let sequence = entry
        .sequence_number
        .filter(|sequence| *sequence >= 0)
        .ok_or_else(|| rewrite_error("Carried manifest entry has no actual data sequence"))?;
    if entry
        .file_sequence_number
        .is_some_and(|sequence| sequence < 0)
    {
        return Err(rewrite_error(
            "Carried manifest entry has a negative file sequence",
        ));
    }
    Ok((snapshot, sequence, entry.file_sequence_number))
}

/// One operation owns reusable merged manifests; each staging attempt owns
/// only its manifest-list. Reuse requires exact source descriptors in the
/// latest target snapshot, including sequence/row-lineage facts.
#[derive(Clone)]
struct ReusableRewrite {
    sources: Vec<ManifestFile>,
    outputs: Vec<ManifestFile>,
}

pub(crate) struct RewriteManifestsPreparer {
    reusable: Mutex<Option<ReusableRewrite>>,
}
impl RewriteManifestsPreparer {
    pub(crate) fn new() -> Self {
        Self {
            reusable: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl crate::commit::staging::Preparer for RewriteManifestsPreparer {
    async fn prepare(
        &self,
        view: &crate::commit::staging::StagedView<'_>,
        intent: &crate::commit::model::OperationIntent,
    ) -> crate::iceberg::Result<crate::commit::staging::PreparedChange> {
        use crate::commit::model::{ArtifactClass, ArtifactKind};
        use crate::commit::staging::PreparedChange;
        let metadata = view.metadata();
        view.artifacts().check_active()?;
        crate::document_storage::observation::validate_managed_rewrite_attachment(metadata)
            .map_err(|error| {
                crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, error.message())
                    .with_source(error)
            })?;
        let parent = metadata.snapshot_for_ref(intent.target_ref());
        let no_op = || PreparedChange {
            requirements: vec![TableRequirement::RefSnapshotIdMatch {
                r#ref: intent.target_ref().to_owned(),
                snapshot_id: parent.map(|snapshot| snapshot.snapshot_id()),
            }],
            updates: Vec::new(),
        };
        let Some(parent) = parent else {
            return Ok(no_op());
        };
        let latest =
            crate::manifest::load_manifest_list(parent, view.artifacts().file_io(), metadata)
                .await?;
        view.artifacts().check_active()?;
        let latest = latest.entries();
        let cached = self
            .reusable
            .lock()
            .map_err(|_| rewrite_error("Manifest reuse state is poisoned"))?
            .clone();
        let reusable =
            match cached.filter(|cache| rewrite_sources_are_current(&cache.sources, latest)) {
                Some(cache) => cache,
                None => {
                    // This is a policy operation: invalidated selections are
                    // replanned, rather than treated as an explicit-input promise.
                    let groups = group_manifests_by_spec_and_content(latest);
                    if groups.values().all(|group| group.len() <= 1) {
                        return Ok(no_op());
                    }
                    let snapshot_id = crate::commit::staging::new_snapshot_id(metadata);
                    let mut sources = Vec::new();
                    let mut outputs = Vec::new();
                    for group in groups.values().filter(|group| group.len() > 1) {
                        let object = view
                            .artifacts()
                            .allocate(ArtifactClass::Operation, ArtifactKind::Manifest)?;
                        let output = merge_manifest_group(
                            view.artifacts().file_io(),
                            metadata,
                            group,
                            object.path(),
                            snapshot_id,
                            metadata.format_version(),
                        )
                        .await?;
                        view.artifacts().check_active()?;
                        sources.extend(group.iter().cloned());
                        outputs.push(output);
                    }
                    let cache = ReusableRewrite { sources, outputs };
                    *self
                        .reusable
                        .lock()
                        .map_err(|_| rewrite_error("Manifest reuse state is poisoned"))? =
                        Some(cache.clone());
                    cache
                }
            };
        let snapshot_id = crate::commit::staging::new_snapshot_id(metadata);
        let mut manifests = latest
            .iter()
            .filter(|manifest| {
                !reusable
                    .sources
                    .iter()
                    .any(|source| source.manifest_path == manifest.manifest_path)
            })
            .cloned()
            .collect::<Vec<_>>();
        for mut manifest in reusable.outputs.clone() {
            // List-level introduction/sequence is attempt-local; immutable
            // merged entries retain their exact original file/snapshot facts.
            manifest.added_snapshot_id = snapshot_id;
            manifest.sequence_number = -1;
            manifests.push(manifest);
        }
        let properties = finalize_snapshot_summary(
            HashMap::from([
                (
                    "replaced-manifests-count".to_owned(),
                    reusable.sources.len().to_string(),
                ),
                (
                    "added-manifests-count".to_owned(),
                    reusable.outputs.len().to_string(),
                ),
            ]),
            Some(parent.summary()),
            metadata.uuid(),
            parent.snapshot_id(),
            snapshot_id,
        )?;
        crate::commit::overwrite::prepare_snapshot_change(
            view,
            intent,
            snapshot_id,
            Operation::Replace,
            manifests,
            properties,
            false,
        )
        .await
    }
}

/// Existence is insufficient: only exact membership of the latest list
/// preserves a selected manifest's inherited sequence and row-ID meaning.
fn rewrite_sources_are_current(sources: &[ManifestFile], latest: &[ManifestFile]) -> bool {
    sources
        .iter()
        .all(|source| latest.iter().any(|current| current == source))
}
fn rewrite_error(message: &str) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, message)
}

#[cfg(test)]
mod document_attachment_tests {
    use super::*;

    fn exact_manifest(table_uuid: uuid::Uuid, snapshot_id: i64) -> String {
        let content = vec![1, 2, 3];
        let committed_version =
            crate::document_storage::observation::output_committed_version(table_uuid, snapshot_id)
                .unwrap();
        let manifest = crate::document_storage::envelope::IcebergDocumentManifestV1 {
            version: crate::document_storage::envelope::DOCUMENT_MANIFEST_VERSION,
            documents: vec![
                crate::document_storage::envelope::IcebergDocumentEnvelopeV1 {
                    version: crate::document_storage::envelope::DOCUMENT_ENVELOPE_VERSION,
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
                    attachment:
                        crate::document_storage::envelope::IcebergDocumentAttachmentV1::ExactOutput {
                            committed_version: committed_version.payload().to_vec(),
                            snapshot_id: Some(snapshot_id),
                        },
                    carrier:
                        crate::document_storage::envelope::IcebergDocumentCarrierV1::Available {
                            content,
                        },
                },
            ],
        };
        String::from_utf8(
            crate::document_storage::codec::encode_document_manifest(&manifest)
                .expect("manifest")
                .to_vec(),
        )
        .expect("UTF-8")
    }

    #[test]
    fn rewrite_snapshot_rebinds_the_document_attachment_to_its_new_version() {
        let table_uuid = uuid::Uuid::new_v4();
        let previous = Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from([(
                crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY.to_string(),
                exact_manifest(table_uuid, 41),
            )]),
        };
        let rewritten =
            finalize_snapshot_summary(HashMap::new(), Some(&previous), table_uuid, 41, 99)
                .expect("rebind manifest");
        let rebound = crate::document_storage::codec::decode_document_manifest(
            rewritten
                .get(crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY)
                .expect("manifest")
                .as_bytes(),
        )
        .expect("decode");
        let expected =
            crate::document_storage::observation::output_committed_version(table_uuid, 99).unwrap();
        assert!(matches!(
            &rebound.documents[0].attachment,
            crate::document_storage::envelope::IcebergDocumentAttachmentV1::ExactOutput {
                committed_version,
                snapshot_id: Some(99),
            } if committed_version.as_slice() == expected.payload().as_ref()
        ));
    }

    #[test]
    fn rewrite_snapshot_rejects_a_stale_document_attachment() {
        let table_uuid = uuid::Uuid::new_v4();
        let previous = Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from([(
                crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY.to_string(),
                exact_manifest(table_uuid, 40),
            )]),
        };
        assert!(
            finalize_snapshot_summary(HashMap::new(), Some(&previous), table_uuid, 41, 99,)
                .is_err()
        );
    }

    #[test]
    fn rewrite_snapshot_rejects_a_foreign_committed_output_version() {
        let table_uuid = uuid::Uuid::new_v4();
        let previous = Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from([(
                crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY.to_string(),
                exact_manifest(uuid::Uuid::new_v4(), 41),
            )]),
        };
        assert!(
            finalize_snapshot_summary(HashMap::new(), Some(&previous), table_uuid, 41, 99).is_err()
        );
    }
}

#[cfg(test)]
mod row_id_rewrite_tests {
    use super::*;
    use crate::iceberg::spec::{
        NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Struct, TableMetadataBuilder,
        Type,
    };
    #[test]
    fn carried_entry_requires_actual_snapshot_and_data_sequence_and_preserves_file_sequence_none() {
        let file = DataFileBuilder::default()
            .content(crate::iceberg::spec::DataContentType::Data)
            .file_path("memory://rewrite/carried.parquet".into())
            .file_format(crate::iceberg::spec::DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(10)
            .build()
            .unwrap();
        let mut entry = ManifestEntry {
            status: ManifestStatus::Existing,
            snapshot_id: Some(77),
            sequence_number: Some(4),
            file_sequence_number: None,
            data_file: file,
        };
        assert_eq!(carried_identity(&entry).unwrap(), (77, 4, None));
        entry.file_sequence_number = Some(2);
        assert_eq!(carried_identity(&entry).unwrap(), (77, 4, Some(2)));
        entry.sequence_number = None;
        assert_eq!(
            carried_identity(&entry).unwrap_err().kind(),
            crate::iceberg::ErrorKind::DataInvalid
        );
        entry.sequence_number = Some(4);
        entry.snapshot_id = None;
        assert_eq!(
            carried_identity(&entry).unwrap_err().kind(),
            crate::iceberg::ErrorKind::DataInvalid
        );
    }
    #[tokio::test]
    async fn explicit_rows_do_not_shift_the_next_inherited_file_during_real_v3_merge() {
        let io = FileIO::new_with_memory();
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://rewrite/table".into(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let mut writer = ManifestWriterBuilder::new(
            io.new_output("memory://rewrite/source.avro").unwrap(),
            Some(11),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v3_data();
        let file = |path: &str, first: Option<i64>, count| {
            DataFileBuilder::default()
                .content(crate::iceberg::spec::DataContentType::Data)
                .file_path(path.to_owned())
                .file_format(crate::iceberg::spec::DataFileFormat::Parquet)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .record_count(count)
                .file_size_in_bytes(20)
                .first_row_id(first)
                .build()
                .unwrap()
        };
        writer
            .add_existing_file(
                file("memory://rewrite/explicit.parquet", Some(700), 5),
                11,
                5,
                Some(5),
            )
            .unwrap();
        writer
            .add_file(file("memory://rewrite/inherited.parquet", None, 3), 10)
            .unwrap();
        let mut source = writer.write_manifest_file().await.unwrap();
        source.first_row_id = Some(100);
        source.sequence_number = 10;
        let merged = merge_manifest_group(
            &io,
            &metadata,
            &[source],
            "memory://rewrite/merged.avro",
            22,
            FormatVersion::V3,
        )
        .await
        .unwrap();
        let actual = crate::manifest::load_manifest(&merged, &io).await.unwrap();
        let actual = actual
            .entries()
            .iter()
            .map(|entry| {
                (
                    entry.data_file().file_path().to_owned(),
                    entry.data_file().first_row_id(),
                )
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(actual["memory://rewrite/explicit.parquet"], Some(700));
        assert_eq!(actual["memory://rewrite/inherited.parquet"], Some(100));
    }
}
