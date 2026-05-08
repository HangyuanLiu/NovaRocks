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

//! Shared helpers for snapshot-lifecycle maintenance commands
//! (EXPIRE SNAPSHOTS / REMOVE ORPHAN FILES / REWRITE MANIFESTS).

use std::collections::{HashMap, HashSet};

use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;

/// Set of object-store paths (data / delete / manifest / manifest-list / DV puffin).
pub type FileSet = HashSet<String>;

/// Compute the set of snapshot ids reachable from any branch / tag via the
/// parent chain. These snapshots must NOT be expired (EXPIRE) and the files
/// they reference must NOT be deleted (EXPIRE / ORPHAN).
pub fn compute_live_snapshot_set(metadata: &TableMetadata) -> HashSet<i64> {
    // Build snapshot_id -> parent_snapshot_id map.
    // parent_snapshot_id is None for root snapshots (no parent).
    let parent_of: HashMap<i64, Option<i64>> = metadata
        .snapshots()
        .map(|s| (s.snapshot_id(), s.parent_snapshot_id()))
        .collect();

    let mut live: HashSet<i64> = HashSet::new();
    for snap_ref in metadata.refs().values() {
        let mut sid = Some(snap_ref.snapshot_id);
        while let Some(id) = sid {
            if !live.insert(id) {
                break; // already visited; cycle protection
            }
            sid = parent_of.get(&id).copied().flatten();
        }
    }
    live
}

/// For each snapshot in `snapshot_ids`, collect all paths of files it directly
/// or transitively references:
///   * manifest list path
///   * each manifest path
///   * each data file / delete file / DV puffin file referenced by manifest entries
///
/// Returns the merged set across all input snapshots. Manifest reads are
/// async (FileIO), so this fn is async.
pub async fn enumerate_files_for_snapshots(
    file_io: &FileIO,
    metadata: &TableMetadata,
    snapshot_ids: &HashSet<i64>,
) -> Result<FileSet, iceberg::Error> {
    let mut out = FileSet::new();
    for sid in snapshot_ids {
        let snapshot = metadata.snapshot_by_id(*sid).ok_or_else(|| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("snapshot id {sid} not found in metadata"),
            )
        })?;
        out.insert(snapshot.manifest_list().to_string());
        let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
        for manifest_file in manifest_list.entries() {
            out.insert(manifest_file.manifest_path.clone());
            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                let data_file = entry.data_file();
                out.insert(data_file.file_path().to_string());
            }
        }
    }
    Ok(out)
}

/// For each candidate file path that points to a puffin (`.puffin`), check
/// whether any DV blob in the puffin references a data file that is still
/// in `live_data_files`. If so, remove the puffin from `candidates_to_delete`
/// (file-level conservative protection per spec §3.2 Step 7 / §4.2 Step 4).
///
/// `dv_index` maps puffin file path → set of referenced data file paths,
/// built by the caller from manifest entries (delete file with format Puffin
/// and `referenced_data_file` set).
pub fn puffin_half_reference_protection(
    candidates_to_delete: &mut FileSet,
    dv_index: &HashMap<String, HashSet<String>>,
    live_data_files: &FileSet,
) {
    candidates_to_delete.retain(|path| {
        if !is_puffin_path(path) {
            return true;
        }
        let referenced = match dv_index.get(path) {
            Some(set) => set,
            // Unknown puffin (not in dv_index) → keep as candidate (allow delete).
            None => return true,
        };
        // Keep candidate (delete) only if NO referenced data file is still live.
        !referenced.iter().any(|d| live_data_files.contains(d))
    });
}

/// Returns true if the file path looks like a puffin file.
pub fn is_puffin_path(path: &str) -> bool {
    path.ends_with(".puffin")
}

/// Build a `dv_index` (puffin path → set of referenced data file paths) from
/// a flat list of (puffin_path, referenced_data_file) pairs.
///
/// This is used by callers that iterate manifest entries and collect DV blob
/// references before calling `puffin_half_reference_protection`.
pub fn build_dv_index(pairs: &[(String, String)]) -> HashMap<String, HashSet<String>> {
    let mut idx: HashMap<String, HashSet<String>> = HashMap::new();
    for (puffin_path, ref_data) in pairs {
        idx.entry(puffin_path.clone())
            .or_default()
            .insert(ref_data.clone());
    }
    idx
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadata,
        TableMetadataBuilder, Type,
    };

    /// Build a `TableMetadata` with the given (snapshot_id, parent_snapshot_id) pairs
    /// and ref bindings.
    ///
    /// V2 chosen deliberately: this fixture is for graph-shape testing of the
    /// live-set walk algorithm, which doesn't depend on format-version-specific
    /// fields. Tests that exercise V3 row-lineage / DV / puffin should build
    /// real V3 tables via test_helpers::v3_table_with_n_data_files instead.
    ///
    /// Snapshots are added in the order given; sequence numbers are assigned
    /// monotonically. Timestamps are assigned as 1_700_000_000_000 + seq_num * 1000
    /// so they are strictly increasing.
    ///
    /// Refs are bound after all snapshots are added. Only snapshots that were
    /// added can be referenced.
    ///
    /// Marked `pub(crate)` so Tasks 4 and 5 (and future EXPIRE tests) can reuse it.
    pub(crate) fn build_test_metadata_with_snapshots(
        snapshots: Vec<(i64, Option<i64>)>,
        refs: Vec<(&str, i64)>,
    ) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("build schema");

        let mut builder = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "/tmp/test_table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("TableMetadataBuilder::new");

        let mut seq: i64 = 1;
        for (sid, parent) in snapshots {
            let ts = 1_700_000_000_000i64 + seq * 1000;
            let manifest_list = format!("/tmp/test_table/metadata/snap-{sid}-ml.avro");
            let summary = Summary {
                operation: iceberg::spec::Operation::Append,
                additional_properties: HashMap::new(),
            };
            let snapshot = Snapshot::builder()
                .with_snapshot_id(sid)
                .with_parent_snapshot_id(parent)
                .with_sequence_number(seq)
                .with_timestamp_ms(ts)
                .with_manifest_list(manifest_list)
                .with_summary(summary)
                .build();
            builder = builder.add_snapshot(snapshot).expect("add_snapshot");
            seq += 1;
        }

        for (ref_name, sid) in refs {
            let reference =
                SnapshotReference::new(sid, SnapshotRetention::branch(None, None, None));
            builder = builder.set_ref(ref_name, reference).expect("set_ref");
        }

        builder.build().expect("build").metadata
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::test_support::build_test_metadata_with_snapshots;
    use super::*;

    // ---- compute_live_snapshot_set tests ----

    #[test]
    fn live_set_linear_main_chain() {
        // s1 (root) <- s2 <- s3, main = s3
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, Some(2))],
            vec![("main", 3)],
        );
        let live = compute_live_snapshot_set(&metadata);
        let mut got: Vec<i64> = live.into_iter().collect();
        got.sort();
        assert_eq!(got, vec![1, 2, 3]);
    }

    #[test]
    fn live_set_branch_tag_protect_ancestors() {
        // s1 <- s2 <- s3, main=s3, branch dev=s2, tag v1=s1
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, Some(2))],
            vec![("main", 3), ("dev", 2), ("v1", 1)],
        );
        let live = compute_live_snapshot_set(&metadata);
        // All three are still reachable via main's ancestor chain; dev and v1
        // are just extra refs into the same chain — no extras.
        assert_eq!(live, [1i64, 2, 3].iter().copied().collect::<HashSet<_>>());
    }

    #[test]
    fn live_set_dangling_snapshot_not_live() {
        // s1 <- s2 <- s3 (main), s4 with parent=s2 but no ref pointing to it.
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, Some(2)), (4, Some(2))],
            vec![("main", 3)],
        );
        let live = compute_live_snapshot_set(&metadata);
        assert_eq!(live, [1i64, 2, 3].iter().copied().collect::<HashSet<_>>());
        assert!(!live.contains(&4), "dangling snapshot s4 must not be live");
    }

    #[test]
    fn live_set_handles_no_refs() {
        // Snapshots present but no refs at all → live set is empty.
        let metadata = build_test_metadata_with_snapshots(vec![(1, None)], vec![]);
        let live = compute_live_snapshot_set(&metadata);
        assert!(
            live.is_empty(),
            "expected empty live set when no refs exist"
        );
    }

    // ---- enumerate_files_for_snapshots tests ----

    #[tokio::test]
    async fn enumerate_files_empty_set_returns_empty() {
        // Empty snapshot_ids → no I/O, empty result.
        // Use an empty in-memory table (no snapshots at all) so metadata is valid.
        let metadata = build_test_metadata_with_snapshots(vec![], vec![]);
        let file_io = iceberg::io::FileIO::new_with_memory();
        let result = enumerate_files_for_snapshots(&file_io, &metadata, &HashSet::new())
            .await
            .expect("enumerate should succeed for empty set");
        assert!(
            result.is_empty(),
            "expected empty FileSet for empty snapshot_id set"
        );
    }

    #[tokio::test]
    async fn enumerate_files_includes_manifest_list_and_data() {
        use crate::connector::iceberg::commit::test_helpers::v3_table_with_multi_batch_appends;

        // Two separate appends → two snapshots, with non-overlapping file paths.
        let fixture = v3_table_with_multi_batch_appends(&[1, 1]).await;
        let metadata = fixture.table.metadata();

        let all_ids: HashSet<i64> = metadata.snapshots().map(|s| s.snapshot_id()).collect();
        assert_eq!(all_ids.len(), 2, "expected 2 snapshots");

        let file_io = fixture.table.file_io();
        let files = enumerate_files_for_snapshots(file_io, metadata, &all_ids)
            .await
            .expect("enumerate_files_for_snapshots");

        // Each INSERT writes: 1 data file + 1 manifest + 1 manifest list.
        // Total from 2 snapshots: >= 6 paths (2 manifest-lists + 2 manifests + 2 data files).
        assert!(
            files.len() >= 6,
            "expected >= 6 file paths, got {}: {:?}",
            files.len(),
            files
        );

        // Each snapshot's manifest list must appear in the output.
        for sid in &all_ids {
            let snap = metadata.snapshot_by_id(*sid).unwrap();
            assert!(
                files.contains(snap.manifest_list()),
                "manifest list missing for snap {sid}: {}",
                snap.manifest_list()
            );
        }
    }

    // ---- puffin_half_reference_protection tests ----

    #[test]
    fn puffin_protect_full_orphan_deletes_all() {
        // a.puffin's only referenced data is NOT live → candidate kept (will be deleted).
        let mut candidates: FileSet = ["a.puffin", "b.parquet"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let dv_index =
            build_dv_index(&[("a.puffin".to_string(), "removed_data.parquet".to_string())]);
        let live: FileSet = ["other.parquet".to_string()].into_iter().collect();
        puffin_half_reference_protection(&mut candidates, &dv_index, &live);
        assert!(
            candidates.contains("a.puffin"),
            "a.puffin should stay in candidates (all refs are dead)"
        );
        assert!(candidates.contains("b.parquet"));
    }

    #[test]
    fn puffin_protect_half_referenced_keeps_puffin() {
        // a.puffin has 2 blobs: one references live data → puffin removed from candidates.
        let mut candidates: FileSet = ["a.puffin".to_string()].into_iter().collect();
        let dv_index = build_dv_index(&[
            ("a.puffin".to_string(), "data1.parquet".to_string()),
            ("a.puffin".to_string(), "data2.parquet".to_string()),
        ]);
        let live: FileSet = ["data1.parquet".to_string()].into_iter().collect();
        puffin_half_reference_protection(&mut candidates, &dv_index, &live);
        assert!(
            !candidates.contains("a.puffin"),
            "a.puffin must be protected (one blob still references live data1.parquet)"
        );
    }

    #[test]
    fn puffin_protect_unknown_puffin_kept_as_candidate() {
        // Puffin in candidates but not in dv_index → keep as candidate (allow delete).
        // Caller is responsible for tracking orphan puffins.
        let mut candidates: FileSet = ["unknown.puffin".to_string()].into_iter().collect();
        let dv_index = build_dv_index(&[]);
        let live: FileSet = ["whatever.parquet".to_string()].into_iter().collect();
        puffin_half_reference_protection(&mut candidates, &dv_index, &live);
        assert!(
            candidates.contains("unknown.puffin"),
            "unknown puffin (not in dv_index) must remain as candidate"
        );
    }

    #[test]
    fn puffin_protect_non_puffin_files_unchanged() {
        // Non-puffin paths are never touched by the helper.
        let mut candidates: FileSet = ["x.parquet", "y.avro"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let original = candidates.clone();
        puffin_half_reference_protection(&mut candidates, &HashMap::new(), &FileSet::new());
        assert_eq!(
            candidates, original,
            "non-puffin files must not be affected"
        );
    }
}
