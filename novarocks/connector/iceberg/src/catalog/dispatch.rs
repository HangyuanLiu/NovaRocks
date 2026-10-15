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

//! Verify the admitted ref in authoritative Hadoop publication metadata.

pub(crate) fn committed_snapshot_id(
    metadata: &crate::iceberg::spec::TableMetadata,
    target_ref: &str,
    expected_snapshot_id: Option<i64>,
) -> Result<Option<i64>, crate::iceberg::Error> {
    let Some(expected_snapshot_id) = expected_snapshot_id else {
        // Schema/property/statistics-only commits do not produce a snapshot.
        // An existing current snapshot is unrelated to their effect, and an
        // unborn main branch is valid, so neither should be reported as this
        // publication's snapshot proof.
        return Ok(None);
    };
    let observed = crate::ref_snapshot::resolve_branch_head_snapshot_id(metadata, target_ref)
        .map_err(|error| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::Unexpected,
                format!(
                    "Iceberg catalog returned committed metadata without admitted target ref \
                     '{target_ref}': {error}"
                ),
            )
        })?;
    if observed != Some(expected_snapshot_id) {
        return Err(crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::Unexpected,
            format!(
                "Iceberg catalog returned target ref '{target_ref}' at snapshot {observed:?}, \
                 expected {expected_snapshot_id}"
            ),
        ));
    }
    Ok(observed)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::committed_snapshot_id;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadata,
        TableMetadataBuilder, Type,
    };

    fn metadata_with_distinct_branch_heads() -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let snapshot = |snapshot_id| {
            Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_sequence_number(snapshot_id)
                .with_timestamp_ms(snapshot_id)
                .with_manifest_list(format!(
                    "file:///tmp/catalog-proof/metadata/snap-{snapshot_id}.avro"
                ))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build()
        };
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "file:///tmp/catalog-proof".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .add_snapshot(snapshot(7))
        .expect("main snapshot")
        .add_snapshot(snapshot(11))
        .expect("staging snapshot")
        .set_ref(
            "main",
            SnapshotReference::new(7, SnapshotRetention::branch(None, None, None)),
        )
        .expect("main ref")
        .set_ref(
            "novarocks-mv-staging",
            SnapshotReference::new(11, SnapshotRetention::branch(None, None, None)),
        )
        .expect("staging ref")
        .build()
        .expect("metadata")
        .metadata
    }

    #[test]
    fn update_proof_uses_main_head_for_main_target() {
        let metadata = metadata_with_distinct_branch_heads();
        assert_eq!(
            committed_snapshot_id(&metadata, "main", Some(7)).expect("main proof"),
            Some(7)
        );
    }

    #[test]
    fn update_proof_uses_non_main_head_for_non_main_target() {
        let metadata = metadata_with_distinct_branch_heads();
        assert_eq!(
            committed_snapshot_id(&metadata, "novarocks-mv-staging", Some(11))
                .expect("staging proof"),
            Some(11)
        );
    }

    #[test]
    fn update_proof_fails_closed_when_admitted_ref_is_missing() {
        let metadata = metadata_with_distinct_branch_heads();
        let error = committed_snapshot_id(&metadata, "missing", Some(11)).expect_err("missing ref");
        assert_eq!(error.kind(), crate::iceberg::ErrorKind::Unexpected);
    }

    #[test]
    fn metadata_only_update_on_unborn_main_has_no_snapshot_proof() {
        let mut metadata = metadata_with_distinct_branch_heads();
        metadata = metadata
            .into_builder(None)
            .remove_snapshots(&[7, 11])
            .remove_ref("main")
            .remove_ref("novarocks-mv-staging")
            .build()
            .expect("unborn metadata")
            .metadata;
        assert_eq!(
            committed_snapshot_id(&metadata, "main", None).expect("metadata-only proof"),
            None
        );
    }
}
