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

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::TableMetadata;

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
) -> Result<FileSet, crate::iceberg::Error> {
    let mut out = FileSet::new();
    for sid in snapshot_ids {
        let snapshot = metadata.snapshot_by_id(*sid).ok_or_else(|| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
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
