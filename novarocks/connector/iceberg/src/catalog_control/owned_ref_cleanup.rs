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

//! Strict, read-only recognition of Catalog refs owned by NovaRocks.
//!
//! This module deliberately recognises only proven NovaRocks schemas. A
//! matching prefix alone, an unreadable snapshot, or any marker drift is not a
//! candidate; it remains live until an operator can inspect it.

use sha2::{Digest, Sha256};
use std::str::FromStr;

use crate::commit::write_fence::{WRITE_FENCE_REF_PREFIX, observe_fence};
use crate::iceberg::spec::TableMetadata;
use novarocks_spi::connector::LakePublicationId;

const CURRENT_MV_PROVENANCE_VERSION: u16 = 2;
const LEGACY_WRITE_FENCE_PROVENANCE_VERSION: u16 = 1;
const OWNED_REF_PROVENANCE_DOMAIN: &[u8] = b"novarocks.iceberg.owned-ref-gc.v1\0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OwnedRefCandidate {
    pub name: String,
    pub head_snapshot_id: i64,
    pub provenance_version: u16,
    /// Exact, table-incarnation-bound proof observed during discovery. This is
    /// not an authority token: it is only a fail-closed guard against marker
    /// drift before the catalog CAS retires the ref.
    pub provenance_digest: [u8; 32],
    pub created_at_ms: i64,
}

/// Enumerate only aged, proven NovaRocks branches. The current table metadata
/// itself binds the candidate to one table incarnation; execution repeats that
/// binding before its exact CAS drop.
pub(crate) fn collect_owned_ref_candidates(
    metadata: &TableMetadata,
    namespace: &str,
    table: &str,
    older_than_ms: i64,
) -> Vec<OwnedRefCandidate> {
    let mut candidates = Vec::new();
    for (name, reference) in metadata.refs() {
        if !reference.is_branch() || name == "main" {
            continue;
        }
        let Some(snapshot) = metadata.snapshot_by_id(reference.snapshot_id) else {
            continue;
        };
        let created_at_ms = snapshot.timestamp_ms();
        if created_at_ms <= 0 || created_at_ms >= older_than_ms {
            continue;
        }
        let provenance = if let Some(raw_publication_id) =
            name.strip_prefix(crate::commit::MV_PUBLICATION_STAGING_REF_PREFIX)
        {
            let Ok(publication_id) = LakePublicationId::from_str(raw_publication_id) else {
                continue;
            };
            crate::commit::MvPublicationProvenanceV2::from_snapshot_summary(snapshot)
                .ok()
                .flatten()
                .filter(|provenance| provenance.publication_id == publication_id)
                .and_then(|provenance| {
                    provenance.content_hash().ok().map(|provenance_hash| {
                        (
                            CURRENT_MV_PROVENANCE_VERSION,
                            vec![
                                "mv".to_string(),
                                publication_id.to_string(),
                                provenance_hash,
                            ],
                        )
                    })
                })
        } else if let Some(operation_id) = name.strip_prefix(WRITE_FENCE_REF_PREFIX) {
            observe_fence(metadata, name)
                .ok()
                .flatten()
                .filter(|observed| {
                    observed.snapshot_id == reference.snapshot_id
                        && observed.facts.write_operation_id == operation_id
                        && observed.facts.namespace == namespace
                        && observed.facts.table_name == table
                        && !observed.facts.target_ref.is_empty()
                        && !observed
                            .facts
                            .target_ref
                            .starts_with(WRITE_FENCE_REF_PREFIX)
                        && valid_legacy_fence_identity(&observed.facts)
                })
                .map(|observed| {
                    (
                        LEGACY_WRITE_FENCE_PROVENANCE_VERSION,
                        vec![
                            "fence".to_string(),
                            observed.facts.cluster_identity_digest.clone(),
                            observed.facts.control_plane_incarnation.to_string(),
                            observed.facts.resource_epoch.to_string(),
                            observed.facts.coordination_attempt.to_string(),
                            observed.facts.write_operation_id.clone(),
                            observed.facts.namespace.clone(),
                            observed.facts.table_name.clone(),
                            observed.facts.target_ref.clone(),
                            observed.facts.coordination_attempt_id.clone(),
                            observed.facts.fence_digest.clone(),
                        ],
                    )
                })
        } else {
            None
        };
        if let Some((provenance_version, operation_identity)) = provenance {
            candidates.push(OwnedRefCandidate {
                name: name.clone(),
                head_snapshot_id: reference.snapshot_id,
                provenance_version,
                provenance_digest: owned_ref_provenance_digest(
                    metadata,
                    name,
                    reference.snapshot_id,
                    provenance_version,
                    &operation_identity,
                ),
                created_at_ms,
            });
        }
    }
    candidates.sort_by(|left, right| left.name.cmp(&right.name));
    candidates
}

fn valid_owned_identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

fn valid_legacy_fence_identity(facts: &crate::commit::write_fence::IcebergWriteFenceFacts) -> bool {
    facts.control_plane_incarnation > 0
        && facts.resource_epoch > 0
        && facts.coordination_attempt > 0
        && [
            &facts.cluster_identity_digest,
            &facts.write_operation_id,
            &facts.namespace,
            &facts.table_name,
            &facts.target_ref,
            &facts.coordination_attempt_id,
            &facts.fence_digest,
        ]
        .into_iter()
        .all(|value| valid_owned_identity(value))
        && facts
            .write_operation_id
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_'))
}

fn owned_ref_provenance_digest(
    metadata: &TableMetadata,
    name: &str,
    head_snapshot_id: i64,
    provenance_version: u16,
    operation_identity: &[String],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(OWNED_REF_PROVENANCE_DOMAIN);
    for value in [
        metadata.uuid().to_string(),
        name.to_string(),
        head_snapshot_id.to_string(),
        provenance_version.to_string(),
    ]
    .into_iter()
    .chain(operation_identity.iter().cloned())
    {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.finalize().into()
}

/// Revalidate the complete provenance proof immediately before an exact CAS
/// retirement. It intentionally does not apply the age threshold again: the
/// frozen candidate already carries the observed creation time, while this
/// check answers only whether the ref/head/marker/table proof drifted.
pub(crate) fn matches_owned_ref_candidate(
    metadata: &TableMetadata,
    namespace: &str,
    table: &str,
    expected: &OwnedRefCandidate,
) -> bool {
    collect_owned_ref_candidates(
        metadata,
        namespace,
        table,
        expected.created_at_ms.saturating_add(1),
    )
    .into_iter()
    .any(|candidate| candidate == *expected)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type,
    };

    fn metadata_with_branch(
        name: &str,
        timestamp_ms: i64,
        properties: HashMap<String, String>,
    ) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let snapshot = Snapshot::builder()
            .with_snapshot_id(7)
            .with_sequence_number(1)
            .with_timestamp_ms(timestamp_ms)
            .with_manifest_list("file:///tmp/owned-ref/metadata/snap-7.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: properties,
            })
            .build();
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "file:///tmp/owned-ref".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .assign_uuid(uuid::Uuid::from_u128(0x5eed))
        .add_snapshot(snapshot)
        .expect("snapshot")
        .set_ref(
            "main",
            SnapshotReference::new(7, SnapshotRetention::branch(None, None, None)),
        )
        .expect("main")
        .set_ref(
            name,
            SnapshotReference::new(7, SnapshotRetention::branch(None, None, None)),
        )
        .expect("branch")
        .build()
        .expect("metadata")
        .metadata
    }

    fn publication_id(suffix: u16) -> LakePublicationId {
        LakePublicationId::from_str(&format!("01890f3c-4e70-7cc0-8000-{suffix:012x}"))
            .expect("publication ID")
    }

    fn mv_marker(publication_id: LakePublicationId, fingerprint: &str) -> HashMap<String, String> {
        crate::commit::MvPublicationProvenanceV2 {
            provenance_version: crate::commit::MV_PUBLICATION_PROVENANCE_VERSION,
            publication_id,
            technique: crate::commit::RefreshTechnique::Full,
            bases: vec![crate::commit::ProvenanceBase {
                table_fqn: "ice.db.base".to_string(),
                uuid: "00112233-4455-6677-8899-aabbccddeeff".to_string(),
                from_snapshot: None,
                to_snapshot: 1,
            }],
            definition_fingerprint: fingerprint.to_string(),
            descriptor_properties_digest_base64: Some("descriptor-digest".to_string()),
            rows: 1,
        }
        .to_summary_properties()
        .expect("MV publication properties")
        .into_iter()
        .collect()
    }

    #[test]
    fn only_known_prefixes_are_owned() {
        assert!(crate::commit::MV_PUBLICATION_STAGING_REF_PREFIX.starts_with("__novarocks_"));
        assert!(WRITE_FENCE_REF_PREFIX.starts_with("novarocks-"));
    }

    #[test]
    fn mv_owned_ref_requires_strict_marker_and_freezes_full_provenance() {
        let publication_id = publication_id(0x41);
        let name = format!(
            "{}{publication_id}",
            crate::commit::MV_PUBLICATION_STAGING_REF_PREFIX
        );
        let metadata = metadata_with_branch(&name, 100, mv_marker(publication_id, "definition-a"));
        let candidates = collect_owned_ref_candidates(&metadata, "db", "target", 101);
        assert_eq!(candidates.len(), 1);
        let expected = candidates.into_iter().next().expect("candidate");
        assert!(matches_owned_ref_candidate(
            &metadata, "db", "target", &expected
        ));
        assert_ne!(
            expected.provenance_digest,
            owned_ref_provenance_digest(
                &metadata,
                &name,
                7,
                CURRENT_MV_PROVENANCE_VERSION,
                &[
                    "mv".to_string(),
                    publication_id.to_string(),
                    "different-provenance-hash".to_string(),
                ],
            ),
            "the frozen proof must bind the full marker identity"
        );

        let drifted = metadata_with_branch(&name, 100, mv_marker(publication_id, "definition-b"));
        assert!(
            !matches_owned_ref_candidate(&drifted, "db", "target", &expected),
            "a marker change must invalidate the frozen owned-ref proof"
        );

        let malformed = metadata_with_branch(
            &format!(
                "{}not-a-v7-id",
                crate::commit::MV_PUBLICATION_STAGING_REF_PREFIX
            ),
            100,
            mv_marker(publication_id, "definition-a"),
        );
        assert!(collect_owned_ref_candidates(&malformed, "db", "target", 101).is_empty());
        let young = metadata_with_branch(&name, 101, mv_marker(publication_id, "definition-a"));
        assert!(collect_owned_ref_candidates(&young, "db", "target", 101).is_empty());
    }

    #[test]
    fn similar_prefix_without_a_complete_marker_is_never_owned() {
        let publication_id = publication_id(0x41);
        let name = format!(
            "{}{publication_id}",
            crate::commit::MV_PUBLICATION_STAGING_REF_PREFIX
        );
        let metadata = metadata_with_branch(
            &name,
            100,
            HashMap::from([(
                crate::commit::MV_PUBLICATION_ID_PROP.to_string(),
                publication_id.to_string(),
            )]),
        );
        assert!(collect_owned_ref_candidates(&metadata, "db", "target", 101).is_empty());
    }

    #[test]
    fn legacy_write_fence_requires_its_complete_versioned_marker() {
        let name = format!("{WRITE_FENCE_REF_PREFIX}op_41");
        let marker = HashMap::from([
            ("novarocks.write-fence.version".to_string(), "1".to_string()),
            (
                "novarocks.write-fence.operation-id".to_string(),
                "op_41".to_string(),
            ),
            (
                "novarocks.write-fence.cluster-identity-digest".to_string(),
                "cluster".to_string(),
            ),
            (
                "novarocks.write-fence.control-plane-incarnation".to_string(),
                "1".to_string(),
            ),
            (
                "novarocks.write-fence.resource-epoch".to_string(),
                "2".to_string(),
            ),
            (
                "novarocks.write-fence.coordination-attempt".to_string(),
                "3".to_string(),
            ),
            (
                "novarocks.write-fence.coordination-attempt-id".to_string(),
                "attempt".to_string(),
            ),
            (
                "novarocks.write-fence.namespace".to_string(),
                "db".to_string(),
            ),
            (
                "novarocks.write-fence.table".to_string(),
                "target".to_string(),
            ),
            (
                "novarocks.write-fence.target-ref".to_string(),
                "main".to_string(),
            ),
            (
                "novarocks.write-fence.digest".to_string(),
                "digest".to_string(),
            ),
        ]);
        let metadata = metadata_with_branch(&name, 100, marker.clone());
        assert_eq!(
            collect_owned_ref_candidates(&metadata, "db", "target", 101).len(),
            1
        );

        let mut unknown_version = marker;
        unknown_version.insert(
            "novarocks.write-fence.version".to_string(),
            "future".to_string(),
        );
        let metadata = metadata_with_branch(&name, 100, unknown_version);
        assert!(collect_owned_ref_candidates(&metadata, "db", "target", 101).is_empty());
    }
}
