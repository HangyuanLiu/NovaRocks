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

//! `Transaction::fast_append` wrapper for INSERT INTO. V2 tables delegate to
//! iceberg-rust's built-in fast append action; V3 row-lineage tables use a
//! custom action so manifest-list `first_row_id` and snapshot row ranges are
//! populated for subsequent `_row_id` scans and deletion-vector commits.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, MAIN_BRANCH, ManifestFile, Operation, PartitionSpecRef, SchemaRef, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction};
use super::data_file::written_file_to_iceberg_data_file;
use super::helpers::{
    generate_snapshot_id, metadata_dir, now_ms, read_base_manifest_list, write_manifest_list,
};
use super::overwrite::write_added_data_manifest;
use super::types::{CommitOutcome, IcebergWriteMode, WrittenFile};

pub struct FastAppendCommit;

#[async_trait]
impl IcebergCommitAction for FastAppendCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;

        // Spec §4.1: empty input is a no-op — return the existing snapshot id
        // (or 0 for an empty table) and skip the catalog round-trip.
        if written.is_empty() {
            let id = ctx
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or(0);
            return Ok(CommitOutcome {
                new_snapshot_id: id,
                written_manifest_paths: vec![],
            });
        }

        // FastAppendAction::validate_added_data_files rejects any non-Data
        // content — catch the misuse here with a clearer error.
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "FastAppendCommit received {:?} content; expected Data only",
                    f.content
                ));
            }
        }

        if matches!(
            crate::connector::iceberg::commit::classify_iceberg_write_mode(ctx.table),
            IcebergWriteMode::RowLineageV3
        ) {
            return commit_v3_row_lineage_append(ctx, written).await;
        }

        let data_files: Vec<iceberg::spec::DataFile> = written
            .iter()
            .map(|f| written_file_to_iceberg_data_file(f, ctx.collector))
            .collect::<Result<Vec<_>, _>>()?;

        let tx = Transaction::new(ctx.table);
        let action = tx
            .fast_append()
            .add_data_files(data_files)
            .set_commit_uuid(ctx.commit_uuid);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("fast_append apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("fast_append commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| "fast_append committed but new snapshot not visible".to_string())?;
        Ok(CommitOutcome {
            new_snapshot_id,
            // FastAppendAction owns its manifest lifecycle; nothing for us
            // to clean up on later abort.
            written_manifest_paths: vec![],
        })
    }
}

async fn commit_v3_row_lineage_append(
    ctx: CommitCtx<'_>,
    written: Vec<WrittenFile>,
) -> Result<CommitOutcome, String> {
    let row_lineage_first_row_id = ctx.table.metadata().next_row_id();
    let row_lineage_added_rows = written.iter().try_fold(0u64, |sum, f| {
        sum.checked_add(f.record_count)
            .ok_or_else(|| "row-lineage added row count overflow".to_string())
    })?;
    let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let action = FastAppendV3TxnAction {
        written,
        commit_uuid: ctx.commit_uuid,
        file_io: ctx.file_io.clone(),
        partition_spec: ctx.collector.partition_spec.clone(),
        schema: ctx.table.metadata().current_schema().clone(),
        schema_id: ctx.table.metadata().current_schema_id(),
        abort_handle: ctx.abort_handle.clone(),
        manifest_paths_out: manifest_paths_out.clone(),
        row_lineage_first_row_id,
        row_lineage_added_rows,
    };

    let tx = Transaction::new(ctx.table);
    let tx = action
        .apply(tx)
        .map_err(|e| format!("fast_append v3 apply failed: {e}"))?;
    let table_after = tx
        .commit(ctx.catalog)
        .await
        .map_err(|e| format!("fast_append v3 commit failed: {e}"))?;
    let new_snapshot_id = table_after
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .ok_or_else(|| "fast_append v3 committed but new snapshot not visible".to_string())?;
    let written_manifest_paths = manifest_paths_out
        .lock()
        .expect("manifest_paths_out poisoned")
        .clone();
    Ok(CommitOutcome {
        new_snapshot_id,
        written_manifest_paths,
    })
}

struct FastAppendV3TxnAction {
    written: Vec<WrittenFile>,
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    schema_id: i32,
    abort_handle: Arc<super::abort::AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    row_lineage_first_row_id: u64,
    row_lineage_added_rows: u64,
}

#[async_trait]
impl TransactionAction for FastAppendV3TxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let parent_snapshot_id = m.current_snapshot().map(|s| s.snapshot_id());
        let metadata_dir = metadata_dir(table);

        let mut manifests: Vec<ManifestFile> = read_base_manifest_list(table, &self.file_io)
            .await
            .map_err(to_iceberg_unexpected)?;

        let data_manifest_path = format!("{metadata_dir}/{}-append-data-0.avro", self.commit_uuid);
        self.abort_handle
            .record_manifest(data_manifest_path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(data_manifest_path.clone());
        let data_manifest = write_added_data_manifest(
            &self.file_io,
            &data_manifest_path,
            &self.written,
            self.partition_spec.clone(),
            self.schema.clone(),
            new_seq,
            new_snapshot_id,
            m.format_version(),
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        manifests.push(data_manifest);

        let manifest_list_path = format!(
            "{metadata_dir}/snap-{}-{}.avro",
            new_snapshot_id, self.commit_uuid
        );
        self.abort_handle
            .record_manifest(manifest_list_path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(manifest_list_path.clone());
        let manifest_list_next_row_id = write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            m.format_version(),
            Some(self.row_lineage_first_row_id),
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        let expected_next_row_id = self
            .row_lineage_first_row_id
            .checked_add(self.row_lineage_added_rows)
            .ok_or_else(|| {
                to_iceberg_unexpected(format!(
                    "Row ID overflow when computing append row lineage range: first_row_id={}, added_rows={}",
                    self.row_lineage_first_row_id, self.row_lineage_added_rows
                ))
            })?;
        if manifest_list_next_row_id != Some(expected_next_row_id) {
            return Err(to_iceberg_unexpected(format!(
                "Manifest list row lineage mismatch: expected next-row-id {expected_next_row_id}, got {manifest_list_next_row_id:?}"
            )));
        }

        let summary = Summary {
            operation: Operation::Append,
            additional_properties: append_summary(&self.written),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(summary)
            .with_schema_id(self.schema_id)
            .with_row_range(self.row_lineage_first_row_id, self.row_lineage_added_rows)
            .build();
        let updates = vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
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
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: m.current_schema_id(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: m.default_partition_spec_id(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: parent_snapshot_id,
            },
        ];
        Ok(ActionCommit::new(updates, requirements))
    }
}

fn append_summary(written: &[WrittenFile]) -> std::collections::HashMap<String, String> {
    let mut p = std::collections::HashMap::new();
    p.insert("added-data-files".to_string(), written.len().to_string());
    p.insert(
        "added-records".to_string(),
        written
            .iter()
            .map(|f| f.record_count)
            .sum::<u64>()
            .to_string(),
    );
    p.insert(
        "added-files-size".to_string(),
        written
            .iter()
            .map(|f| f.file_size_in_bytes)
            .sum::<u64>()
            .to_string(),
    );
    p
}

fn to_iceberg_unexpected(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_compiles() {
        let _ = FastAppendCommit;
    }
}
