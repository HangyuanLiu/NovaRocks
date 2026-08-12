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

//! `TruncateCommit` — write a single `operation=delete` snapshot that marks
//! every live data + delete file as DELETED while preserving schema, partition
//! spec, properties, and other refs.
//!
//! Differences from `OverwriteCommit`:
//! * No `written` files: TRUNCATE never adds rows.
//! * No row-lineage advance: spec says `last-row-id` is NOT advanced, so the
//!   manifest list is written with `first_row_id: None` and the V3 snapshot
//!   row range carries `(next_row_id, 0)` — the validator at
//!   `iceberg-0.9.0/src/spec/table_metadata_builder.rs:419` rejects a V3
//!   snapshot with a null first-row-id, but `added_rows_count = 0` means
//!   `next_row_id` is preserved across the snapshot.
//! * Splits enumerated entries by `DataContentType` so position-delete /
//!   equality-delete / Iceberg v3 deletion-vector entries land in a separate
//!   `Deletes`-typed manifest (the existing `write_overwrite_deletes_manifest`
//!   helper is hard-wired to `build_v*_data()` and would reject delete-content
//!   entries via `ManifestWriter::check_data_file`).
//! * Summary `operation = "delete"` plus the proper `deleted-data-files` /
//!   `removed-position-delete-files` / `removed-equality-delete-files` counts.
//!
//! Even when the base table is empty we still write a `delete` snapshot with
//! `deleted-data-files = 0` so TRUNCATE leaves an audit trail entry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFile, FormatVersion, Operation, PartitionSpecRef, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use crate::iceberg::table::Table;
use crate::iceberg::transaction::{
    ActionCommit, ApplyTransactionAction, Transaction, TransactionAction,
};
use crate::iceberg::{TableRequirement, TableUpdate};
use async_trait::async_trait;
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction, merge_snapshot_summary_properties};
use super::helpers::{
    finalize_snapshot_summary, generate_snapshot_id, metadata_dir, now_ms, write_manifest_list,
};
use super::overwrite::{
    enumerate_live_all_files, write_overwrite_deletes_manifest, write_truncate_deletes_manifest,
};
use crate::commit::CommitOutcome;
use crate::commit::abort::AbortLog;

pub struct TruncateCommit;

#[async_trait]
impl IcebergCommitAction for TruncateCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = TruncateTxnAction {
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            partition_spec: ctx.collector.partition_spec.clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
            target_ref: ctx.target_ref.to_string(),
            snapshot_properties: ctx.snapshot_properties.clone(),
        };

        // TRUNCATE goes through the same fenced submission seam as the row-DML
        // families: it destroys table content, so a stale owner's late commit
        // has to be refused at the catalog rather than merely reported.
        let submitted = crate::commit::helpers::submit_fenced_action(
            ctx.catalog,
            ctx.table,
            Arc::new(action),
            ctx.fence,
            "Truncate",
        )
        .await
        .map_err(crate::commit::helpers::FencedSubmitError::into_detail)?;
        let new_snapshot_id = match submitted {
            crate::commit::helpers::FencedSubmit::Committed(table_after) => table_after
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or(0),
            // An empty TRUNCATE over an empty base changes nothing; the
            // previous behaviour reported the current snapshot, or 0.
            crate::commit::helpers::FencedSubmit::NoOp => ctx
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or(0),
        };
        let written_manifest_paths = manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .clone();
        Ok(CommitOutcome {
            new_snapshot_id,
            written_manifest_paths,
        })
    }
}

struct TruncateTxnAction {
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema_id: i32,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    target_ref: String,
    snapshot_properties: std::collections::BTreeMap<String, String>,
}

impl TruncateTxnAction {
    fn record_manifest_path(&self, path: String) {
        self.abort_handle.record_manifest(path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(path);
    }
}

#[async_trait]
impl TransactionAction for TruncateTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> crate::iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        if format_version == FormatVersion::V1 {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "TruncateCommit does not support V1 tables",
            ));
        }

        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let target_ref = &self.target_ref;
        let parent_snapshot_id = m
            .refs()
            .get(target_ref.as_str())
            .map(|r| r.snapshot_id)
            .or_else(|| {
                if target_ref == "main" {
                    m.current_snapshot().map(|s| s.snapshot_id())
                } else {
                    None
                }
            });
        let metadata_dir = metadata_dir(table);

        // 1. Enumerate every live entry — Data AND Deletes manifests alike.
        //    TRUNCATE must mark every live data / position-delete /
        //    equality-delete / DV entry as DELETED.
        let existing = enumerate_live_all_files(table, &self.file_io)
            .await
            .map_err(to_iceberg_unexpected)?;

        // 2. Split by content type so we can route entries into the correct
        //    manifest kind. `add_delete_file` insists that every entry in a
        //    Data manifest has DataContentType::Data; mixing types triggers a
        //    DataInvalid error from `check_data_file`.
        let (data_entries, delete_entries): (Vec<_>, Vec<_>) = existing
            .iter()
            .cloned()
            .partition(|(df, _, _)| df.content_type() == DataContentType::Data);

        let mut new_manifests: Vec<crate::iceberg::spec::ManifestFile> = Vec::new();

        // 3a. Write the data-content deletes manifest if any data files are live.
        if !data_entries.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-truncate-deleted-data-0.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
            let mf = write_overwrite_deletes_manifest(
                &self.file_io,
                &path,
                &data_entries,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 3b. Write the delete-content deletes manifest if any delete files
        //     (position-deletes, equality-deletes, or v3 deletion vectors —
        //     spec says DVs are encoded with content_type = PositionDeletes)
        //     are live.
        if !delete_entries.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-truncate-deleted-deletes-0.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
            let mf = write_truncate_deletes_manifest(
                &self.file_io,
                &path,
                &delete_entries,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 4. Write the manifest list (may be empty for the empty-table case).
        //    `first_row_id = None` because TRUNCATE never advances row
        //    lineage — spec: `last-row-id is NOT advanced`.
        let manifest_list_path = format!(
            "{metadata_dir}/snap-{}-{}.avro",
            new_snapshot_id, self.commit_uuid
        );
        self.record_manifest_path(manifest_list_path.clone());
        write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            new_manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            format_version,
            None,
        )
        .await
        .map_err(to_iceberg_unexpected)?;

        // 5. Build the snapshot with operation = delete + classification
        //    counts. TRUNCATE adds zero rows, so `added_rows_count = 0` —
        //    `next_row_id` is therefore NOT advanced (per spec: `last-row-id
        //    is NOT advanced`). We still must set `first_row_id` for V3
        //    because `add_snapshot` rejects a V3 snapshot with no row range
        //    (see iceberg-0.9.0/src/spec/table_metadata_builder.rs:419).
        //    Setting `(next_row_id, 0)` is the spec-faithful way to record
        //    "no new row ids consumed" while still satisfying the validator.
        let additional_properties = merge_snapshot_summary_properties(
            finalize_snapshot_summary(
                truncate_summary(&data_entries, &delete_entries),
                m.current_snapshot().map(|s| s.summary()),
                true,
            ),
            &self.snapshot_properties,
        )
        .map_err(to_iceberg_unexpected)?;
        let summary = Summary {
            operation: Operation::Delete,
            additional_properties,
        };
        let snapshot_builder = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(summary)
            .with_schema_id(self.schema_id);
        let snapshot = match format_version {
            FormatVersion::V3 => snapshot_builder.with_row_range(m.next_row_id(), 0).build(),
            _ => snapshot_builder.build(),
        };

        // 6. Build the TableUpdate / TableRequirement set.
        let updates = vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: target_ref.clone(),
                reference: SnapshotReference {
                    snapshot_id: new_snapshot_id,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            },
        ];
        let requirements = vec![
            TableRequirement::UuidMatch { uuid: m.uuid() },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: m.current_schema_id(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: m.default_partition_spec_id(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: target_ref.clone(),
                snapshot_id: parent_snapshot_id,
            },
        ];
        Ok(ActionCommit::new(updates, requirements))
    }
}

fn truncate_summary(
    data_entries: &[(DataFile, i64, Option<i64>)],
    delete_entries: &[(DataFile, i64, Option<i64>)],
) -> HashMap<String, String> {
    let removed_position_delete_files = delete_entries
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::PositionDeletes)
        .count();
    let removed_equality_delete_files = delete_entries
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::EqualityDeletes)
        .count();
    let removed_position_deletes = delete_entries
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::PositionDeletes)
        .map(|(df, _, _)| df.record_count())
        .sum::<u64>();
    let removed_equality_deletes = delete_entries
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::EqualityDeletes)
        .map(|(df, _, _)| df.record_count())
        .sum::<u64>();
    let deleted_records = data_entries
        .iter()
        .map(|(df, _, _)| df.record_count())
        .sum::<u64>();
    let removed_files_size: u64 = data_entries
        .iter()
        .chain(delete_entries.iter())
        .map(|(df, _, _)| df.file_size_in_bytes())
        .sum();

    let mut p = HashMap::new();
    // TRUNCATE never adds anything; the added-* counters are pinned to 0 so
    // downstream tooling that diffs snapshot summaries doesn't see stale
    // values from a prior snapshot.
    p.insert("added-data-files".to_string(), "0".to_string());
    p.insert("added-records".to_string(), "0".to_string());
    p.insert("added-files-size".to_string(), "0".to_string());
    p.insert("added-delete-files".to_string(), "0".to_string());

    p.insert(
        "deleted-data-files".to_string(),
        data_entries.len().to_string(),
    );
    p.insert("deleted-records".to_string(), deleted_records.to_string());
    p.insert(
        "removed-delete-files".to_string(),
        delete_entries.len().to_string(),
    );
    p.insert(
        "removed-position-delete-files".to_string(),
        removed_position_delete_files.to_string(),
    );
    p.insert(
        "removed-equality-delete-files".to_string(),
        removed_equality_delete_files.to_string(),
    );
    p.insert(
        "removed-position-deletes".to_string(),
        removed_position_deletes.to_string(),
    );
    p.insert(
        "removed-equality-deletes".to_string(),
        removed_equality_deletes.to_string(),
    );
    p.insert(
        "removed-files-size".to_string(),
        removed_files_size.to_string(),
    );
    p
}

// Local copy of the helper at `overwrite.rs::to_iceberg_unexpected` —
// the original is `pub(super)`-scoped to `commit::overwrite` and is not
// visible from sibling submodules. Keep in sync with that definition;
// promote to `commit::helpers` if a third call site shows up.
fn to_iceberg_unexpected(s: String) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, s)
}
