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

//! Read-only recognition of legacy NovaRocks write-fence metadata.
//!
//! NovaRocks no longer establishes, raises, asserts, or retires these refs.
//! Old catalog state can nevertheless contain them, and the owned-ref GC must
//! identify that state strictly before its age-gated CAS retirement. This
//! module is therefore a decoder only: it has no catalog or file-IO mutation
//! API and does not manufacture marker summaries or ref names.

use crate::iceberg::spec::{Summary, TableMetadata};

/// Prefix used by legacy provider-private write-fence refs.
pub const WRITE_FENCE_REF_PREFIX: &str = "novarocks-write-fence-";

const FENCE_PROP_VERSION: &str = "novarocks.write-fence.version";
const FENCE_PROP_OPERATION_ID: &str = "novarocks.write-fence.operation-id";
const FENCE_PROP_CLUSTER_DIGEST: &str = "novarocks.write-fence.cluster-identity-digest";
const FENCE_PROP_INCARNATION: &str = "novarocks.write-fence.control-plane-incarnation";
const FENCE_PROP_RESOURCE_EPOCH: &str = "novarocks.write-fence.resource-epoch";
const FENCE_PROP_ATTEMPT_NUMBER: &str = "novarocks.write-fence.coordination-attempt";
const FENCE_PROP_ATTEMPT_ID: &str = "novarocks.write-fence.coordination-attempt-id";
const FENCE_PROP_NAMESPACE: &str = "novarocks.write-fence.namespace";
const FENCE_PROP_TABLE: &str = "novarocks.write-fence.table";
const FENCE_PROP_TARGET_REF: &str = "novarocks.write-fence.target-ref";
const FENCE_PROP_DIGEST: &str = "novarocks.write-fence.digest";
const FENCE_MARKER_VERSION: &str = "1";

/// Immutable provenance decoded from a legacy marker snapshot.
///
/// This shape is intentionally retained only so owned-ref GC can prove that a
/// stale ref belongs to NovaRocks. It is not an authority token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergWriteFenceFacts {
    pub cluster_identity_digest: String,
    pub control_plane_incarnation: u64,
    pub resource_epoch: u64,
    pub coordination_attempt: u64,
    pub write_operation_id: String,
    pub namespace: String,
    pub table_name: String,
    pub target_ref: String,
    pub coordination_attempt_id: String,
    pub fence_digest: String,
}

/// A legacy fence marker observed on its ref head.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedFence {
    pub snapshot_id: i64,
    pub facts: IcebergWriteFenceFacts,
}

/// Decode the marker currently referenced by `fence_ref`.
///
/// Any absent ref, missing snapshot, or malformed/unknown marker is kept out
/// of the candidate set by callers through `.ok().flatten()`; this makes the
/// legacy GC fail closed.
pub fn observe_fence(
    metadata: &TableMetadata,
    fence_ref: &str,
) -> Result<Option<ObservedFence>, String> {
    let Some(reference) = metadata.refs().get(fence_ref) else {
        return Ok(None);
    };
    let snapshot_id = reference.snapshot_id;
    let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
        format!("legacy write-fence ref '{fence_ref}' points at missing snapshot {snapshot_id}")
    })?;
    let facts = parse_marker_summary(snapshot.summary(), fence_ref)?;
    Ok(Some(ObservedFence { snapshot_id, facts }))
}

fn parse_marker_summary(
    summary: &Summary,
    fence_ref: &str,
) -> Result<IcebergWriteFenceFacts, String> {
    let get = |key: &str| {
        summary
            .additional_properties
            .get(key)
            .cloned()
            .ok_or_else(|| format!("legacy write-fence marker on '{fence_ref}' is missing {key}"))
    };
    let version = get(FENCE_PROP_VERSION)?;
    if version != FENCE_MARKER_VERSION {
        return Err(format!(
            "legacy write-fence marker on '{fence_ref}' has layout version {version}; this build understands {FENCE_MARKER_VERSION}"
        ));
    }
    let parse_u64 = |key: &str, raw: String| {
        raw.parse::<u64>().map_err(|error| {
            format!("legacy write-fence marker on '{fence_ref}' has invalid {key}: {error}")
        })
    };
    Ok(IcebergWriteFenceFacts {
        cluster_identity_digest: get(FENCE_PROP_CLUSTER_DIGEST)?,
        control_plane_incarnation: parse_u64(FENCE_PROP_INCARNATION, get(FENCE_PROP_INCARNATION)?)?,
        resource_epoch: parse_u64(FENCE_PROP_RESOURCE_EPOCH, get(FENCE_PROP_RESOURCE_EPOCH)?)?,
        coordination_attempt: parse_u64(
            FENCE_PROP_ATTEMPT_NUMBER,
            get(FENCE_PROP_ATTEMPT_NUMBER)?,
        )?,
        write_operation_id: get(FENCE_PROP_OPERATION_ID)?,
        namespace: get(FENCE_PROP_NAMESPACE)?,
        table_name: get(FENCE_PROP_TABLE)?,
        target_ref: get(FENCE_PROP_TARGET_REF)?,
        coordination_attempt_id: get(FENCE_PROP_ATTEMPT_ID)?,
        fence_digest: get(FENCE_PROP_DIGEST)?,
    })
}

/// Whether a snapshot carries the legacy fence-marker layout tag.
pub fn is_fence_marker_snapshot(summary: &Summary) -> bool {
    summary
        .additional_properties
        .contains_key(FENCE_PROP_VERSION)
}

/// Whether `ref_name` belongs to the legacy NovaRocks fence namespace.
pub fn is_fence_ref(ref_name: &str) -> bool {
    ref_name.starts_with(WRITE_FENCE_REF_PREFIX)
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

    fn marker() -> HashMap<String, String> {
        HashMap::from([
            (
                FENCE_PROP_VERSION.to_string(),
                FENCE_MARKER_VERSION.to_string(),
            ),
            (FENCE_PROP_OPERATION_ID.to_string(), "op-1".to_string()),
            (FENCE_PROP_CLUSTER_DIGEST.to_string(), "cluster".to_string()),
            (FENCE_PROP_INCARNATION.to_string(), "7".to_string()),
            (FENCE_PROP_RESOURCE_EPOCH.to_string(), "3".to_string()),
            (FENCE_PROP_ATTEMPT_NUMBER.to_string(), "1".to_string()),
            (FENCE_PROP_ATTEMPT_ID.to_string(), "attempt-1".to_string()),
            (FENCE_PROP_NAMESPACE.to_string(), "db".to_string()),
            (FENCE_PROP_TABLE.to_string(), "t".to_string()),
            (FENCE_PROP_TARGET_REF.to_string(), "main".to_string()),
            (FENCE_PROP_DIGEST.to_string(), "digest".to_string()),
        ])
    }

    fn metadata_with_fence(properties: HashMap<String, String>) -> TableMetadata {
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
            .with_timestamp_ms(1)
            .with_manifest_list("file:///tmp/legacy-fence/metadata/snap-7.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: properties,
            })
            .build();
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "file:///tmp/legacy-fence".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .add_snapshot(snapshot)
        .expect("snapshot")
        .set_ref(
            "main",
            SnapshotReference::new(7, SnapshotRetention::branch(None, None, None)),
        )
        .expect("main")
        .set_ref(
            "novarocks-write-fence-op-1",
            SnapshotReference::new(7, SnapshotRetention::branch(None, None, None)),
        )
        .expect("fence")
        .build()
        .expect("metadata")
        .metadata
    }

    #[test]
    fn observes_a_complete_legacy_marker() {
        let metadata = metadata_with_fence(marker());
        let observed = observe_fence(&metadata, "novarocks-write-fence-op-1")
            .expect("decode")
            .expect("marker");
        assert_eq!(observed.snapshot_id, 7);
        assert_eq!(observed.facts.write_operation_id, "op-1");
        assert_eq!(observed.facts.coordination_attempt, 1);
    }

    #[test]
    fn malformed_legacy_marker_fails_closed() {
        let mut properties = marker();
        properties.remove(FENCE_PROP_DIGEST);
        let metadata = metadata_with_fence(properties);
        assert!(observe_fence(&metadata, "novarocks-write-fence-op-1").is_err());
    }

    #[test]
    fn prefix_and_marker_tag_are_recognized_without_writing_any_state() {
        assert!(is_fence_ref("novarocks-write-fence-op-1"));
        assert!(!is_fence_ref("main"));
        assert!(is_fence_marker_snapshot(&Summary {
            operation: Operation::Append,
            additional_properties: marker(),
        }));
    }
}
