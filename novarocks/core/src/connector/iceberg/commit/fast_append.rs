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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, ManifestFile, Operation, PartitionSpecRef, SchemaRef, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction, merge_snapshot_summary_properties};
use super::data_file::written_file_to_iceberg_data_file;
use super::helpers::{
    effective_next_row_id, finalize_snapshot_summary, generate_snapshot_id, metadata_dir, now_ms,
    read_snapshot_manifest_list, required_target_ref_snapshot_id, snapshot_summary,
    snapshot_total_records, target_ref_snapshot_id, write_manifest_list,
};
use super::overwrite::write_added_data_manifest;
use super::types::{CommitOutcome, IcebergWriteMode, WrittenFile};
use crate::connector::iceberg::stats_assembler::{CommitType, FileSketchSet, StatsAssembler};

pub struct FastAppendCommit;

#[async_trait]
impl IcebergCommitAction for FastAppendCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;

        // Spec §4.1: empty input is a no-op — return the existing snapshot id
        // (or 0 for an empty table) and skip the catalog round-trip.
        if written.is_empty() {
            let id = target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref).unwrap_or(0);
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

        if ctx.target_ref != "main" {
            return Err(format!(
                "FastAppendCommit branch target_ref={} requires the custom v3 row-lineage append path",
                ctx.target_ref
            ));
        }

        let data_files: Vec<iceberg::spec::DataFile> = written
            .iter()
            .map(|f| written_file_to_iceberg_data_file(f, ctx.collector))
            .collect::<Result<Vec<_>, _>>()?;

        let sketch_sets = ctx.collector.take_sketch_sets();
        let prev_snapshot_id = target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref);

        let tx = Transaction::new(ctx.table);
        let action = tx
            .fast_append()
            .add_data_files(data_files)
            .set_commit_uuid(ctx.commit_uuid)
            .set_snapshot_properties(ctx.snapshot_properties.clone().into_iter().collect());
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
        let new_sequence_number = table_after.metadata().last_sequence_number();
        // Best-effort Puffin NDV registration; failure must not abort the
        // commit because data is already published.
        register_puffin_stats(
            &table_after,
            ctx.catalog,
            ctx.file_io,
            CommitType::Append,
            sketch_sets,
            new_snapshot_id,
            new_sequence_number,
            prev_snapshot_id,
        )
        .await;
        Ok(CommitOutcome {
            new_snapshot_id,
            // FastAppendAction owns its manifest lifecycle; nothing for us
            // to clean up on later abort.
            written_manifest_paths: vec![],
        })
    }
}

/// Run `StatsAssembler::assemble` and, on success, apply
/// `UpdateStatisticsAction` against the post-commit table. Logs and swallows
/// errors so a stats failure never reverts a successful data commit.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn register_puffin_stats(
    table_after: &Table,
    catalog: &dyn iceberg::Catalog,
    file_io: &FileIO,
    commit_type: CommitType,
    sketch_sets: Vec<FileSketchSet>,
    new_snapshot_id: i64,
    new_sequence_number: i64,
    prev_snapshot_id: Option<i64>,
) {
    match StatsAssembler::assemble(
        table_after,
        commit_type,
        sketch_sets,
        new_snapshot_id,
        new_sequence_number,
        prev_snapshot_id,
        file_io,
    )
    .await
    {
        Ok(Some(stats_file)) => {
            if let Err(err) =
                super::statistics::commit_statistics_file(table_after, catalog, stats_file).await
            {
                tracing::warn!(
                    new_snapshot_id,
                    error = %err,
                    "iceberg puffin stats commit failed; snapshot committed without stats",
                );
            }
        }
        Ok(None) => {
            // Either no new sketches available (empty input) or the
            // assembler chose to reuse the previous Puffin (DELETE /
            // REWRITE). Carry-forward registration is handled by the
            // delete / rewrite paths directly.
        }
        Err(err) => {
            tracing::warn!(
                new_snapshot_id,
                error = %err,
                "iceberg puffin stats assemble failed; snapshot committed without stats",
            );
        }
    }
}

/// Carry forward the previous snapshot's Puffin statistics entry to
/// `new_snapshot_id`. Used by DELETE / REWRITE commits where NDV remains
/// a valid upper bound — the statistics file path is identical, only the
/// snapshot_id key on the metadata entry changes.
///
/// Errors are logged and swallowed; missing previous stats is normal.
pub(crate) async fn carry_forward_puffin_stats(
    table_after: &Table,
    catalog: &dyn iceberg::Catalog,
    new_snapshot_id: i64,
    prev_snapshot_id: i64,
) {
    let Some(prev) = table_after
        .metadata()
        .statistics_for_snapshot(prev_snapshot_id)
    else {
        return;
    };
    let mut entry = prev.clone();
    entry.snapshot_id = new_snapshot_id;
    // Reseat each blob's snapshot_id so the metadata entry is self-consistent.
    for blob in entry.blob_metadata.iter_mut() {
        blob.snapshot_id = new_snapshot_id;
    }
    if let Err(err) = super::statistics::commit_statistics_file(table_after, catalog, entry).await {
        tracing::warn!(
            new_snapshot_id,
            error = %err,
            "iceberg puffin carry-forward stats commit failed",
        );
    }
}

async fn commit_v3_row_lineage_append(
    ctx: CommitCtx<'_>,
    written: Vec<WrittenFile>,
) -> Result<CommitOutcome, String> {
    let row_lineage_first_row_id = effective_next_row_id(ctx.table.metadata())?;
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
        row_lineage: Some((row_lineage_first_row_id, row_lineage_added_rows)),
        target_ref: ctx.target_ref.to_string(),
        snapshot_properties: ctx.snapshot_properties.clone(),
        #[cfg(test)]
        fail_before_manifest_list_write: false,
    };

    let sketch_sets = ctx.collector.take_sketch_sets();
    let prev_snapshot_id = ctx
        .table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());

    let tx = Transaction::new(ctx.table);
    let tx = action
        .apply(tx)
        .map_err(|e| format!("fast_append v3 apply failed: {e}"))?;
    let table_after = tx
        .commit(ctx.catalog)
        .await
        .map_err(|e| format!("fast_append v3 commit failed: {e}"))?;
    let new_snapshot_id =
        required_target_ref_snapshot_id(table_after.metadata(), ctx.target_ref, "fast_append v3")?;
    let new_sequence_number = table_after.metadata().last_sequence_number();
    register_puffin_stats(
        &table_after,
        ctx.catalog,
        ctx.file_io,
        CommitType::Append,
        sketch_sets,
        new_snapshot_id,
        new_sequence_number,
        prev_snapshot_id,
    )
    .await;
    let written_manifest_paths = manifest_paths_out
        .lock()
        .expect("manifest_paths_out poisoned")
        .clone();
    Ok(CommitOutcome {
        new_snapshot_id,
        written_manifest_paths,
    })
}

/// Snapshot changes built against an invisible staged table. The caller owns
/// catalog publication and must prepend the staged table's initialization
/// updates and submit exactly one assert-create commit.
pub(crate) struct StagedFastAppendAction {
    pub action: ActionCommit,
    pub outcome: Option<CommitOutcome>,
    pub abort_handle: Arc<super::abort::AbortLog>,
}

/// Build, but do not submit, v2 or v3 fast-append changes for atomic CTAS
/// publication. This deliberately bypasses `Transaction::commit`, whose first
/// step reloads a visible table and therefore cannot operate on a staged
/// create response.
pub(crate) async fn build_staged_fast_append_action(
    ctx: CommitCtx<'_>,
) -> Result<StagedFastAppendAction, String> {
    let written = ctx.collector.take_written_files()?;
    if written.is_empty() {
        return Ok(StagedFastAppendAction {
            action: ActionCommit::new(Vec::new(), Vec::new()),
            outcome: None,
            abort_handle: ctx.abort_handle,
        });
    }
    for file in &written {
        if file.content != DataContentType::Data {
            return Err(format!(
                "staged fast append received {:?} content; expected Data only",
                file.content
            ));
        }
    }
    if ctx.target_ref != "main" {
        return Err("atomic staged-table publication only supports the main ref".to_string());
    }

    if !matches!(
        crate::connector::iceberg::commit::classify_iceberg_write_mode(ctx.table),
        IcebergWriteMode::RowLineageV3
    ) {
        let manifest_paths_out = Arc::new(Mutex::new(Vec::new()));
        let action = FastAppendV3TxnAction {
            written,
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            partition_spec: ctx.collector.partition_spec.clone(),
            schema: ctx.table.metadata().current_schema().clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            abort_handle: Arc::clone(&ctx.abort_handle),
            manifest_paths_out: Arc::clone(&manifest_paths_out),
            row_lineage: None,
            target_ref: ctx.target_ref.to_string(),
            snapshot_properties: ctx.snapshot_properties.clone(),
            #[cfg(test)]
            fail_before_manifest_list_write: false,
        };
        let mut action = Arc::new(action)
            .commit(ctx.table)
            .await
            .map_err(|error| format!("build staged v2 fast-append changes: {error}"))?;
        let updates = action.take_updates();
        let requirements = action.take_requirements();
        let new_snapshot_id = updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot.snapshot_id()),
                _ => None,
            })
            .ok_or_else(|| {
                "staged v2 fast append did not build an add-snapshot update".to_string()
            })?;
        let written_manifest_paths = manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .clone();
        return Ok(StagedFastAppendAction {
            action: ActionCommit::new(updates, requirements),
            outcome: Some(CommitOutcome {
                new_snapshot_id,
                written_manifest_paths,
            }),
            abort_handle: ctx.abort_handle,
        });
    }

    let row_lineage_first_row_id = effective_next_row_id(ctx.table.metadata())?;
    let row_lineage_added_rows = written.iter().try_fold(0u64, |sum, file| {
        sum.checked_add(file.record_count)
            .ok_or_else(|| "row-lineage added row count overflow".to_string())
    })?;
    let manifest_paths_out = Arc::new(Mutex::new(Vec::new()));
    let action = FastAppendV3TxnAction {
        written,
        commit_uuid: ctx.commit_uuid,
        file_io: ctx.file_io.clone(),
        partition_spec: ctx.collector.partition_spec.clone(),
        schema: ctx.table.metadata().current_schema().clone(),
        schema_id: ctx.table.metadata().current_schema_id(),
        abort_handle: Arc::clone(&ctx.abort_handle),
        manifest_paths_out: Arc::clone(&manifest_paths_out),
        row_lineage: Some((row_lineage_first_row_id, row_lineage_added_rows)),
        target_ref: ctx.target_ref.to_string(),
        snapshot_properties: ctx.snapshot_properties.clone(),
        #[cfg(test)]
        fail_before_manifest_list_write: false,
    };
    let mut action = Arc::new(action)
        .commit(ctx.table)
        .await
        .map_err(|error| format!("build staged fast-append changes: {error}"))?;
    let updates = action.take_updates();
    let requirements = action.take_requirements();
    let new_snapshot_id = updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot.snapshot_id()),
            _ => None,
        })
        .ok_or_else(|| "staged fast append did not build an add-snapshot update".to_string())?;
    let written_manifest_paths = manifest_paths_out
        .lock()
        .expect("manifest_paths_out poisoned")
        .clone();
    Ok(StagedFastAppendAction {
        action: ActionCommit::new(updates, requirements),
        outcome: Some(CommitOutcome {
            new_snapshot_id,
            written_manifest_paths,
        }),
        abort_handle: ctx.abort_handle,
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
    row_lineage: Option<(u64, u64)>,
    target_ref: String,
    snapshot_properties: BTreeMap<String, String>,
    #[cfg(test)]
    fail_before_manifest_list_write: bool,
}

#[async_trait]
impl TransactionAction for FastAppendV3TxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let target_ref = &self.target_ref;
        let parent_snapshot_id = target_ref_snapshot_id(m, target_ref);
        let total_records = append_total_records(
            &self.written,
            snapshot_total_records(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?,
            parent_snapshot_id.is_some(),
        )
        .map_err(to_iceberg_unexpected)?;
        let parent_summary =
            snapshot_summary(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?;
        let additional_properties = merge_snapshot_summary_properties(
            finalize_snapshot_summary(
                append_summary(&self.written, total_records),
                parent_summary,
                false,
            ),
            &self.snapshot_properties,
        )
        .map_err(to_iceberg_unexpected)?;
        let summary = Summary {
            operation: Operation::Append,
            additional_properties,
        };
        let metadata_dir = metadata_dir(table);

        let mut manifests: Vec<ManifestFile> =
            read_snapshot_manifest_list(m, &self.file_io, parent_snapshot_id)
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
        #[cfg(test)]
        if self.fail_before_manifest_list_write {
            return Err(to_iceberg_unexpected(
                "injected staged append manifest-list write failure".to_string(),
            ));
        }
        let manifest_list_next_row_id = write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            m.format_version(),
            self.row_lineage.map(|(first_row_id, _)| first_row_id),
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        if let Some((first_row_id, added_rows)) = self.row_lineage {
            let expected_next_row_id = first_row_id.checked_add(added_rows).ok_or_else(|| {
                to_iceberg_unexpected(format!(
                    "Row ID overflow when computing append row lineage range: first_row_id={first_row_id}, added_rows={added_rows}"
                ))
            })?;
            if manifest_list_next_row_id != Some(expected_next_row_id) {
                return Err(to_iceberg_unexpected(format!(
                    "Manifest list row lineage mismatch: expected next-row-id {expected_next_row_id}, got {manifest_list_next_row_id:?}"
                )));
            }
        }

        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(summary)
            .with_schema_id(self.schema_id);
        let snapshot = match self.row_lineage {
            Some((first_row_id, added_rows)) => {
                snapshot.with_row_range(first_row_id, added_rows).build()
            }
            None => snapshot.build(),
        };
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

fn append_total_records(
    written: &[WrittenFile],
    parent_total_records: Option<u64>,
    has_parent_snapshot: bool,
) -> Result<Option<u64>, String> {
    let added_records = written.iter().try_fold(0u64, |sum, f| {
        sum.checked_add(f.record_count)
            .ok_or_else(|| "append added row count overflow".to_string())
    })?;
    match (parent_total_records, has_parent_snapshot) {
        (Some(parent), _) => parent
            .checked_add(added_records)
            .map(Some)
            .ok_or_else(|| "append total-records overflow".to_string()),
        (None, false) => Ok(Some(added_records)),
        (None, true) => Ok(None),
    }
}

fn append_summary(
    written: &[WrittenFile],
    total_records: Option<u64>,
) -> std::collections::HashMap<String, String> {
    let mut p = std::collections::HashMap::new();
    let added_records = written.iter().map(|f| f.record_count).sum::<u64>();
    p.insert("added-data-files".to_string(), written.len().to_string());
    p.insert("added-records".to_string(), added_records.to_string());
    if let Some(total_records) = total_records {
        p.insert("total-records".to_string(), total_records.to_string());
    }
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
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use iceberg::spec::{
        DataContentType, DataFileFormat, FormatVersion, NestedField, PrimitiveType, Schema, Struct,
        Type,
    };
    use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

    use super::*;
    use crate::connector::iceberg::commit::{CommitOpKind, IcebergCommitCollector};

    #[test]
    fn type_compiles() {
        let _ = FastAppendCommit;
    }

    #[test]
    fn append_summary_sets_initial_total_records() {
        let written = vec![test_written_data_file(7), test_written_data_file(11)];
        let total_records = append_total_records(&written, None, false).unwrap();
        let summary = append_summary(&written, total_records);

        assert_eq!(summary["added-records"], "18");
        assert_eq!(summary["total-records"], "18");

        // finalize_snapshot_summary carries total-* from a first snapshot (no previous).
        // super::finalize_snapshot_summary is imported in the parent module via use super::helpers.
        let finalized = super::finalize_snapshot_summary(summary, None, false);
        // 2 files, 18 records, 2 * 1024 = 2048 bytes.
        assert_eq!(
            finalized.get("total-data-files").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            finalized.get("total-records").map(String::as_str),
            Some("18")
        );
        assert_eq!(
            finalized.get("total-files-size").map(String::as_str),
            Some("2048")
        );
        assert_eq!(
            finalized.get("engine-name").map(String::as_str),
            Some("novarocks")
        );
    }

    #[test]
    fn append_summary_adds_to_parent_total_records() {
        let written = vec![test_written_data_file(7), test_written_data_file(11)];
        let total_records = append_total_records(&written, Some(5), true).unwrap();
        let summary = append_summary(&written, total_records);

        assert_eq!(summary["added-records"], "18");
        assert_eq!(summary["total-records"], "23");
    }

    #[test]
    fn append_summary_omits_total_records_when_parent_is_legacy() {
        let written = vec![test_written_data_file(7)];
        let total_records = append_total_records(&written, None, true).unwrap();
        let summary = append_summary(&written, total_records);

        assert!(!summary.contains_key("total-records"));
    }

    #[tokio::test]
    async fn v2_fast_append_rejects_branch_target_ref() {
        let warehouse_dir = tempfile::TempDir::new().expect("warehouse tempdir");
        let warehouse_uri = format!(
            "file://{}",
            warehouse_dir.path().join("warehouse").display()
        );
        let entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "fast_append_test",
            &[
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), warehouse_uri),
            ],
        )
        .expect("build hadoop catalog entry");
        let catalog: Arc<dyn Catalog> = Arc::new(
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)
                .expect("build hadoop catalog"),
        );
        let namespace = NamespaceIdent::new("db".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create_namespace");
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("build schema");
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(FormatVersion::V2)
                    .build(),
            )
            .await
            .expect("create_table");
        let table_ident = TableIdent::new(namespace, "t".to_string());
        let metadata = table.metadata();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::FastAppend,
                table_ident,
                metadata.current_snapshot().map(|s| s.snapshot_id()),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!("{}/staging", metadata.location()),
                novarocks_types::UniqueId::new(0, 0),
            )
            .with_table_metadata(metadata.clone()),
        );
        collector.inject_written_file(test_written_data_file(1));
        let file_io = table.file_io().clone();
        let abort_handle = collector.abort_log.clone();
        let snapshot_properties = BTreeMap::new();
        let ctx = CommitCtx {
            collector: &collector,
            table: &table,
            catalog: catalog.as_ref(),
            file_io: &file_io,
            commit_uuid: Uuid::new_v4(),
            abort_handle,
            target_ref: "branch_a",
            snapshot_properties: &snapshot_properties,
        };

        let err = FastAppendCommit.commit(ctx).await.unwrap_err();
        assert_eq!(
            err,
            "FastAppendCommit branch target_ref=branch_a requires the custom v3 row-lineage append path"
        );
    }

    #[tokio::test]
    async fn staged_v2_append_registers_and_aborts_manifest_and_manifest_list() {
        let warehouse_dir = tempfile::TempDir::new().expect("warehouse tempdir");
        let warehouse_uri = format!(
            "file://{}",
            warehouse_dir.path().join("warehouse").display()
        );
        let entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "staged_fast_append_test",
            &[
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), warehouse_uri),
            ],
        )
        .expect("build hadoop catalog entry");
        let catalog: Arc<dyn Catalog> = Arc::new(
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)
                .expect("build hadoop catalog"),
        );
        let namespace = NamespaceIdent::new("db".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create_namespace");
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("build schema");
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(FormatVersion::V2)
                    .build(),
            )
            .await
            .expect("create_table");
        let metadata = table.metadata();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::FastAppend,
                TableIdent::new(namespace, "t".to_string()),
                None,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!("{}/staging", metadata.location()),
                novarocks_types::UniqueId::new(0, 0),
            )
            .with_table_metadata(metadata.clone()),
        );
        collector.inject_written_file(test_written_data_file(1));
        let file_io = table.file_io().clone();
        let abort_handle = Arc::new(super::super::abort::AbortLog::new());
        let snapshot_properties = BTreeMap::new();
        let action = build_staged_fast_append_action(CommitCtx {
            collector: &collector,
            table: &table,
            catalog: catalog.as_ref(),
            file_io: &file_io,
            commit_uuid: Uuid::new_v4(),
            abort_handle: Arc::clone(&abort_handle),
            target_ref: "main",
            snapshot_properties: &snapshot_properties,
        })
        .await
        .expect("build staged v2 append");
        let paths = action
            .outcome
            .expect("non-empty staged append outcome")
            .written_manifest_paths;
        assert!(
            paths.len() >= 2,
            "manifest file and list must both be tracked"
        );

        let builder =
            opendal::services::Fs::default().root(warehouse_dir.path().to_string_lossy().as_ref());
        let fs = opendal::Operator::new(builder).unwrap().finish();
        let root_prefix = format!("file://{}/", warehouse_dir.path().display());
        let errors = abort_handle
            .cleanup_with_path_mapper(&fs, |path| {
                path.strip_prefix(&root_prefix).unwrap_or(path).to_string()
            })
            .await;
        assert!(errors.is_empty(), "manifest cleanup errors: {errors:?}");
        for path in paths {
            let relative = path.strip_prefix(&root_prefix).unwrap_or(&path);
            assert!(
                fs.stat(relative).await.is_err(),
                "staged artifact leaked: {path}"
            );
        }
    }

    #[tokio::test]
    async fn staged_v2_partial_manifest_failure_retains_write_time_abort_paths() {
        let warehouse_dir = tempfile::TempDir::new().expect("warehouse tempdir");
        let warehouse_uri = format!(
            "file://{}",
            warehouse_dir.path().join("warehouse").display()
        );
        let entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "staged_fast_append_fault_test",
            &[
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), warehouse_uri),
            ],
        )
        .expect("build hadoop catalog entry");
        let catalog: Arc<dyn Catalog> = Arc::new(
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)
                .expect("build hadoop catalog"),
        );
        let namespace = NamespaceIdent::new("db".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create_namespace");
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("build schema");
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(FormatVersion::V2)
                    .build(),
            )
            .await
            .expect("create_table");
        let metadata = table.metadata();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::FastAppend,
                TableIdent::new(namespace, "t".to_string()),
                None,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!("{}/staging", metadata.location()),
                novarocks_types::UniqueId::new(0, 0),
            )
            .with_table_metadata(metadata.clone()),
        );
        let abort_handle = Arc::new(super::super::abort::AbortLog::new());
        let manifest_paths_out = Arc::new(Mutex::new(Vec::new()));
        let action = FastAppendV3TxnAction {
            written: vec![test_written_data_file(1)],
            commit_uuid: Uuid::new_v4(),
            file_io: table.file_io().clone(),
            partition_spec: collector.partition_spec.clone(),
            schema: metadata.current_schema().clone(),
            schema_id: metadata.current_schema_id(),
            abort_handle: Arc::clone(&abort_handle),
            manifest_paths_out: Arc::clone(&manifest_paths_out),
            row_lineage: None,
            target_ref: "main".to_string(),
            snapshot_properties: BTreeMap::new(),
            fail_before_manifest_list_write: true,
        };
        let error = match Arc::new(action).commit(&table).await {
            Ok(_) => panic!("expected injected manifest-list write failure"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("injected staged append"));
        let paths = manifest_paths_out.lock().unwrap().clone();
        assert_eq!(
            paths.len(),
            2,
            "both attempted artifacts must be registered"
        );

        let builder =
            opendal::services::Fs::default().root(warehouse_dir.path().to_string_lossy().as_ref());
        let fs = opendal::Operator::new(builder).unwrap().finish();
        let root_prefix = format!("file://{}/", warehouse_dir.path().display());
        let first_relative = paths[0]
            .strip_prefix(&root_prefix)
            .expect("manifest path under warehouse root");
        assert!(
            fs.stat(first_relative).await.is_ok(),
            "data manifest was written"
        );
        let errors = abort_handle
            .cleanup_with_path_mapper(&fs, |path| {
                path.strip_prefix(&root_prefix).unwrap_or(path).to_string()
            })
            .await;
        assert!(
            errors.is_empty(),
            "partial manifest cleanup errors: {errors:?}"
        );
        assert!(
            fs.stat(first_relative).await.is_err(),
            "data manifest leaked"
        );
    }

    fn test_written_data_file(record_count: u64) -> WrittenFile {
        WrittenFile {
            path: format!("file:///x/data-{record_count}.parquet"),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count,
            file_size_in_bytes: 1024,
            split_offsets: vec![4],
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            nan_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        }
    }
}
