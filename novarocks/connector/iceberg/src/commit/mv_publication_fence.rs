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

//! Strict, read-only decoding for legacy MV publication-fence remnants.
//!
//! No current publication path establishes, advances, or asserts this ref.
//! The decoder exists solely so GC and diagnostics can recognize an old
//! catalog artifact without treating malformed evidence as safe to remove.

use serde::{Deserialize, Serialize};

use crate::iceberg::spec::{Snapshot, TableMetadata};

pub const MV_PUBLICATION_FENCE_REF: &str = "__novarocks_mv_publication_fence_v1";
pub const MV_PUBLICATION_FENCE_MARKER_PROP: &str = "novarocks.mv.fence.v1";
pub const MV_PUBLICATION_FENCE_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MvPublicationFenceMarker {
    pub fence_version: u16,
    pub resource_digest: String,
    pub target_table_uuid: String,
    pub cluster_digest: String,
    pub control_plane_incarnation: u64,
    pub resource_epoch: u64,
    pub token_digest: String,
    pub operation_id: String,
}

impl MvPublicationFenceMarker {
    pub fn from_json(raw: &str) -> Result<Self, String> {
        let marker: Self = serde_json::from_str(raw)
            .map_err(|err| format!("failed to parse MV publication fence marker JSON: {err}"))?;
        if marker.fence_version != MV_PUBLICATION_FENCE_VERSION {
            return Err(format!(
                "unsupported MV publication fence version: expected {}, got {}",
                MV_PUBLICATION_FENCE_VERSION, marker.fence_version
            ));
        }
        uuid::Uuid::parse_str(&marker.target_table_uuid)
            .map_err(|_| "MV publication fence target table UUID is invalid".to_string())?;
        for (name, value, len) in [
            ("resource digest", marker.resource_digest.as_str(), 64),
            ("cluster digest", marker.cluster_digest.as_str(), 64),
            ("token digest", marker.token_digest.as_str(), 64),
            ("operation ID", marker.operation_id.as_str(), 32),
        ] {
            if value.len() != len || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(format!("MV publication fence {name} is not valid hex"));
            }
        }
        if marker.control_plane_incarnation == 0 || marker.resource_epoch == 0 {
            return Err("MV publication fence generation fields must be nonzero".to_string());
        }
        Ok(marker)
    }

    pub fn from_snapshot_summary(snapshot: &Snapshot) -> Result<Self, String> {
        let raw = snapshot
            .summary()
            .additional_properties
            .get(MV_PUBLICATION_FENCE_MARKER_PROP)
            .ok_or_else(|| {
                format!(
                    "iceberg MV fence: snapshot {} on {MV_PUBLICATION_FENCE_REF} carries no fence marker",
                    snapshot.snapshot_id()
                )
            })?;
        Self::from_json(raw)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyMvPublicationFenceObservation {
    pub snapshot_id: i64,
    pub marker: MvPublicationFenceMarker,
}

pub fn is_legacy_mv_publication_fence_ref(name: &str) -> bool {
    name == MV_PUBLICATION_FENCE_REF
}

pub fn observe_legacy_mv_publication_fence(
    metadata: &TableMetadata,
) -> Result<Option<LegacyMvPublicationFenceObservation>, String> {
    let Some(reference) = metadata.refs().get(MV_PUBLICATION_FENCE_REF) else {
        return Ok(None);
    };
    if !reference.is_branch() {
        return Err(format!(
            "iceberg MV fence: {MV_PUBLICATION_FENCE_REF} is a tag, expected branch"
        ));
    }
    let snapshot = metadata
        .snapshot_by_id(reference.snapshot_id)
        .ok_or_else(|| {
            format!(
                "iceberg MV fence: {MV_PUBLICATION_FENCE_REF} names missing snapshot {}",
                reference.snapshot_id
            )
        })?;
    Ok(Some(LegacyMvPublicationFenceObservation {
        snapshot_id: reference.snapshot_id,
        marker: MvPublicationFenceMarker::from_snapshot_summary(snapshot)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_rejects_malformed_legacy_evidence() {
        let err = MvPublicationFenceMarker::from_json(
            r#"{"fence_version":1,"resource_digest":"00","target_table_uuid":"not-a-uuid","cluster_digest":"00","control_plane_incarnation":0,"resource_epoch":0,"token_digest":"00","operation_id":"00"}"#,
        )
        .unwrap_err();
        assert!(err.contains("UUID"), "{err}");
    }

    #[test]
    fn only_the_exact_legacy_ref_is_recognized() {
        assert!(is_legacy_mv_publication_fence_ref(MV_PUBLICATION_FENCE_REF));
        assert!(!is_legacy_mv_publication_fence_ref("main"));
    }
}
