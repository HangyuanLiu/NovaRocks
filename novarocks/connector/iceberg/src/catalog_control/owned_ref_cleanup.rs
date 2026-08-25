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

use crate::commit::write_fence::{WRITE_FENCE_REF_PREFIX, observe_fence};
use crate::iceberg::spec::TableMetadata;

pub(crate) const MV_STAGING_REF_PREFIX: &str = "__novarocks_mv_refresh_";
const MV_REFRESH_ID_PROP: &str = "novarocks.mv.refresh_id";
const MV_ID_PROP: &str = "novarocks.mv.id";
const MV_REFRESH_TOKEN_PROP: &str = "novarocks.mv.refresh_token";
const CURRENT_MV_PROVENANCE_VERSION: u16 = 1;
const LEGACY_WRITE_FENCE_PROVENANCE_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OwnedRefCandidate {
    pub name: String,
    pub head_snapshot_id: i64,
    pub provenance_version: u16,
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
        let provenance_version =
            if let Some(raw_refresh_id) = name.strip_prefix(MV_STAGING_REF_PREFIX) {
                let Ok(refresh_id) = raw_refresh_id.parse::<i64>() else {
                    continue;
                };
                let props = &snapshot.summary().additional_properties;
                let marker_matches = props
                    .get(MV_REFRESH_ID_PROP)
                    .and_then(|value| value.parse::<i64>().ok())
                    == Some(refresh_id)
                    && props
                        .get(MV_ID_PROP)
                        .and_then(|value| value.parse::<i64>().ok())
                        .is_some()
                    && props
                        .get(MV_REFRESH_TOKEN_PROP)
                        .is_some_and(|value| !value.is_empty());
                marker_matches.then_some(CURRENT_MV_PROVENANCE_VERSION)
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
                    })
                    .map(|_| LEGACY_WRITE_FENCE_PROVENANCE_VERSION)
            } else {
                None
            };
        if let Some(provenance_version) = provenance_version {
            candidates.push(OwnedRefCandidate {
                name: name.clone(),
                head_snapshot_id: reference.snapshot_id,
                provenance_version,
                created_at_ms,
            });
        }
    }
    candidates.sort_by(|left, right| left.name.cmp(&right.name));
    candidates
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
    use super::*;

    #[test]
    fn only_known_prefixes_are_owned() {
        assert!(MV_STAGING_REF_PREFIX.starts_with("__novarocks_"));
        assert!(WRITE_FENCE_REF_PREFIX.starts_with("novarocks-"));
    }
}
