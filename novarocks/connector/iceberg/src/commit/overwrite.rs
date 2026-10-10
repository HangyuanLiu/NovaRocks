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

//! Whole-table overwrite preparation and shared snapshot assembly.
//!
//! Live data and delete entries retain their original identity and source facts
//! when removed. The current attempt owns new manifests and the exact list's
//! actual row-ID allocation; frozen data files remain operation-owned inputs.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestFile, ManifestList,
    ManifestStatus, ManifestWriterBuilder, Operation, PartitionSpecRef, SchemaRef, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use crate::iceberg::table::Table;
use crate::iceberg::transaction::Transaction;
use crate::iceberg::transaction::{ActionCommit, TransactionAction};
use crate::iceberg::{TableRequirement, TableUpdate};
use async_trait::async_trait;
use uuid::Uuid;

use super::action::{CommitCtx, merge_snapshot_summary_properties};
use super::helpers::{
    effective_next_row_id, finalize_snapshot_summary, generate_snapshot_id, metadata_dir, now_ms,
    snapshot_summary, target_ref_snapshot_id, write_manifest_list,
};
use crate::commit::abort::AbortLog;
use crate::commit::{CommitOutcome, IcebergWriteMode, WrittenFile};

/// Prepare a whole-table overwrite against the current target-ref state.
/// The intent owns added files; this attempt owns only new metadata artifacts.
pub(crate) struct OverwritePreparer;

#[async_trait]
impl super::staging::Preparer for OverwritePreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        validate_added_data(intent)?;
        let parent = view
            .metadata()
            .snapshot_for_ref(intent.target_ref())
            .map(|s| s.snapshot_id());
        let mut inputs =
            super::dependency::ValidationInputs::new(view.metadata(), parent, view.artifacts());
        let removed: Vec<_> = inputs.live_set().await?.values().cloned().collect();
        if removed.is_empty() && intent.changes().added.is_empty() && intent.summary().is_empty() {
            return Ok(super::staging::PreparedChange::default());
        }
        let snapshot_id = super::staging::new_snapshot_id(view.metadata());
        let mut manifests = write_live_entry_groups(
            view,
            snapshot_id,
            removed.iter().cloned().map(|e| (e, true)).collect(),
        )
        .await?;
        manifests.extend(write_added_intent_data(view, intent, snapshot_id).await?);
        let operation = match (intent.changes().added.is_empty(), removed.is_empty()) {
            (false, true) => Operation::Append,
            (true, false) => Operation::Delete,
            _ => Operation::Overwrite,
        };
        prepare_snapshot_change(
            view,
            intent,
            snapshot_id,
            operation,
            manifests,
            snapshot_file_summary(&intent.changes().added, &removed)?,
            false,
        )
        .await
    }
}

pub(crate) fn validate_added_data(
    intent: &super::model::OperationIntent,
) -> crate::iceberg::Result<()> {
    for added in &intent.changes().added {
        if added.file().content_type() != DataContentType::Data {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "Data-only preparer received a delete entry",
            ));
        }
        if added.data_sequence() != super::model::SeqField::Inherit {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "New logical data must inherit its publication sequence",
            ));
        }
    }
    Ok(())
}

/// Added files retain their explicitly frozen spec, never inferred from a tuple.
pub(crate) async fn write_added_intent_data(
    view: &super::staging::StagedView<'_>,
    intent: &super::model::OperationIntent,
    snapshot_id: i64,
) -> crate::iceberg::Result<Vec<ManifestFile>> {
    if intent.changes().added.is_empty() {
        return Ok(Vec::new());
    }
    let metadata = view.metadata();
    let mut groups: BTreeMap<i32, Vec<_>> = BTreeMap::new();
    for added in &intent.changes().added {
        groups
            .entry(added.partition_spec_id())
            .or_default()
            .push(added.clone());
    }
    let mut manifests = Vec::new();
    for (spec_id, files) in groups {
        let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                format!("Added data references missing partition spec {spec_id}"),
            )
        })?;
        let partition_type = spec.partition_type(metadata.current_schema().as_ref())?;
        for added in &files {
            if added.file().partition().fields().len() != partition_type.fields().len() {
                return Err(crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::DataInvalid,
                    "Added data partition tuple does not match its frozen spec",
                ));
            }
            for (value, field) in added.file().partition().iter().zip(partition_type.fields()) {
                if let Some(value) = value {
                    value.clone().try_into_json(field.field_type.as_ref())?;
                }
            }
        }
        manifests.push(
            super::staging::write_manifest(
                view.artifacts(),
                metadata.format_version(),
                snapshot_id,
                metadata.current_schema().clone(),
                spec.as_ref().clone(),
                ManifestContentType::Data,
                files
                    .into_iter()
                    .map(super::staging::ManifestEntryWrite::Added),
            )
            .await?,
        );
    }
    Ok(manifests)
}

/// Carry true entry facts under their original partition spec. Assigned and
/// unassigned data groups stay separate so historic first assignment cannot
/// consume a second range for entries whose row IDs already exist.
pub(crate) async fn write_live_entry_groups(
    view: &super::staging::StagedView<'_>,
    snapshot_id: i64,
    entries: Vec<(super::dependency::LiveEntry, bool)>,
) -> crate::iceberg::Result<Vec<ManifestFile>> {
    let mut groups: BTreeMap<(i32, bool, bool, bool), Vec<super::dependency::LiveEntry>> =
        BTreeMap::new();
    for (entry, deleted) in entries {
        let is_data = entry.file.content_type() == DataContentType::Data;
        let assigned = !deleted && is_data && entry.frozen.facts().first_row_id.is_some();
        groups
            .entry((
                entry.frozen.facts().partition_spec_id,
                is_data,
                deleted,
                assigned,
            ))
            .or_default()
            .push(entry);
    }
    let mut manifests = Vec::new();
    let metadata = view.metadata();
    for ((spec_id, is_data, deleted, assigned), entries) in groups {
        let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                format!("Live entry references missing partition spec {spec_id}"),
            )
        })?;
        let first = if assigned {
            entries
                .iter()
                .filter_map(|e| e.frozen.facts().first_row_id)
                .min()
                .map(u64::try_from)
                .transpose()
                .map_err(|_| {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Assigned row ID is negative",
                    )
                })?
        } else {
            None
        };
        let mut manifest = super::staging::write_manifest(
            view.artifacts(),
            metadata.format_version(),
            snapshot_id,
            metadata.current_schema().clone(),
            spec.as_ref().clone(),
            if is_data {
                ManifestContentType::Data
            } else {
                ManifestContentType::Deletes
            },
            entries.into_iter().map(|entry| {
                if deleted {
                    super::staging::ManifestEntryWrite::Deleted {
                        file: entry.file,
                        frozen: entry.frozen,
                    }
                } else {
                    super::staging::ManifestEntryWrite::Existing {
                        file: entry.file,
                        frozen: entry.frozen,
                    }
                }
            }),
        )
        .await?;
        if metadata.format_version() == FormatVersion::V3 && assigned {
            manifest.first_row_id = first;
        }
        manifests.push(manifest);
    }
    Ok(manifests)
}

/// Shared snapshot assembly uses the actual successful manifest-list allocation.
pub(crate) async fn prepare_snapshot_change(
    view: &super::staging::StagedView<'_>,
    intent: &super::model::OperationIntent,
    snapshot_id: i64,
    operation: Operation,
    manifests: Vec<ManifestFile>,
    properties: HashMap<String, String>,
    truncate_full_table: bool,
) -> crate::iceberg::Result<super::staging::PreparedChange> {
    let metadata = view.metadata();
    let parent = metadata
        .snapshot_for_ref(intent.target_ref())
        .map(|s| s.snapshot_id());
    let parent_summary = parent
        .and_then(|id| metadata.snapshot_by_id(id))
        .map(|s| s.summary());
    let properties = merge_snapshot_summary_properties(
        finalize_snapshot_summary(properties, parent_summary, truncate_full_table),
        intent.summary(),
        metadata.uuid(),
        snapshot_id,
    )
    .map_err(to_iceberg_unexpected)?;
    let list = super::staging::write_manifest_list(
        view.artifacts(),
        metadata,
        snapshot_id,
        parent,
        manifests,
    )
    .await?;
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent)
        .with_sequence_number(metadata.next_sequence_number())
        .with_timestamp_ms(now_ms().max(metadata.last_updated_ms()))
        .with_manifest_list(list.object.path().to_string())
        .with_schema_id(metadata.current_schema_id())
        .with_summary(Summary {
            operation,
            additional_properties: properties,
        });
    let snapshot = match list.row_range {
        Some((first, count)) => snapshot.with_row_range(first, count).build(),
        None => snapshot.build(),
    };
    let retention = metadata
        .refs()
        .get(intent.target_ref())
        .map(|r| r.retention.clone())
        .unwrap_or(SnapshotRetention::Branch {
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        });
    Ok(super::staging::PreparedChange {
        updates: vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: intent.target_ref().to_string(),
                reference: SnapshotReference {
                    snapshot_id,
                    retention,
                },
            },
        ],
        requirements: vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: metadata.current_schema_id(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: metadata.default_partition_spec_id(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: intent.target_ref().to_string(),
                snapshot_id: parent,
            },
        ],
    })
}

/// Snapshot sizes count logical delete blobs rather than shared Puffin containers.
pub(crate) fn snapshot_file_summary(
    added: &[super::model::AddedContent],
    removed: &[super::dependency::LiveEntry],
) -> crate::iceberg::Result<HashMap<String, String>> {
    let mut counters = BTreeMap::<&str, u64>::new();
    for (file, is_added) in added
        .iter()
        .map(|a| (a.file(), true))
        .chain(removed.iter().map(|e| (&e.file, false)))
    {
        let size = if file.content_type() == DataContentType::PositionDeletes
            && file.file_format() == crate::iceberg::spec::DataFileFormat::Puffin
        {
            file.content_size_in_bytes()
                .ok_or_else(|| {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Deletion vector has no blob size",
                    )
                })?
                .try_into()
                .map_err(|_| {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Deletion vector blob size is negative",
                    )
                })?
        } else {
            file.file_size_in_bytes()
        };
        let mut bump = |key, value| -> crate::iceberg::Result<()> {
            let total = counters.entry(key).or_default();
            *total = total.checked_add(value).ok_or_else(|| {
                crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::DataInvalid,
                    "Snapshot summary counter overflow",
                )
            })?;
            Ok(())
        };
        bump(
            if is_added {
                "added-files-size"
            } else {
                "removed-files-size"
            },
            size,
        )?;
        match file.content_type() {
            DataContentType::Data => {
                bump(
                    if is_added {
                        "added-data-files"
                    } else {
                        "deleted-data-files"
                    },
                    1,
                )?;
                bump(
                    if is_added {
                        "added-records"
                    } else {
                        "deleted-records"
                    },
                    file.record_count(),
                )?;
            }
            DataContentType::PositionDeletes => {
                bump(
                    if is_added {
                        "added-delete-files"
                    } else {
                        "removed-delete-files"
                    },
                    1,
                )?;
                bump(
                    if is_added {
                        "added-position-delete-files"
                    } else {
                        "removed-position-delete-files"
                    },
                    1,
                )?;
                bump(
                    if is_added {
                        "added-position-deletes"
                    } else {
                        "removed-position-deletes"
                    },
                    file.record_count(),
                )?;
            }
            DataContentType::EqualityDeletes => {
                bump(
                    if is_added {
                        "added-delete-files"
                    } else {
                        "removed-delete-files"
                    },
                    1,
                )?;
                bump(
                    if is_added {
                        "added-equality-delete-files"
                    } else {
                        "removed-equality-delete-files"
                    },
                    1,
                )?;
                bump(
                    if is_added {
                        "added-equality-deletes"
                    } else {
                        "removed-equality-deletes"
                    },
                    file.record_count(),
                )?;
            }
        }
    }
    for key in [
        "added-data-files",
        "added-records",
        "added-files-size",
        "added-delete-files",
        "deleted-data-files",
        "deleted-records",
        "removed-files-size",
        "removed-delete-files",
        "added-position-delete-files",
        "removed-position-delete-files",
        "added-position-deletes",
        "removed-position-deletes",
        "added-equality-delete-files",
        "removed-equality-delete-files",
        "added-equality-deletes",
        "removed-equality-deletes",
    ] {
        counters.entry(key).or_default();
    }
    Ok(counters
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect())
}

/// One overwrite snapshot staged against `ctx`, not yet submitted.
struct PreparedOverwriteAction {
    action: Arc<OverwriteTxnAction>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
}

impl PreparedOverwriteAction {
    fn written_manifest_paths(&self) -> Vec<String> {
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .clone()
    }
}

/// Prepare the overwrite action for the eager publication path.
fn prepare_overwrite_action(ctx: &CommitCtx<'_>) -> Result<PreparedOverwriteAction, String> {
    let written = ctx.collector.take_written_files()?;
    for f in &written {
        if f.content != DataContentType::Data {
            return Err(format!(
                "OverwriteCommit received {:?} content; expected Data only",
                f.content
            ));
        }
    }
    let row_lineage_first_row_id = match crate::commit::classify_iceberg_write_mode(ctx.table) {
        IcebergWriteMode::RowLineageV3 => Some(effective_next_row_id(ctx.table.metadata())?),
        IcebergWriteMode::LegacyPositionDeletes => None,
    };
    let row_lineage_added_rows = written.iter().try_fold(0u64, |sum, f| {
        sum.checked_add(f.record_count)
            .ok_or_else(|| "row-lineage added row count overflow".to_string())
    })?;
    let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let action = Arc::new(OverwriteTxnAction {
        written,
        commit_uuid: ctx.commit_uuid,
        file_io: ctx.file_io.clone(),
        partition_spec: ctx.collector.partition_spec.clone(),
        schema_id: ctx.table.metadata().current_schema_id(),
        abort_handle: ctx.abort_handle.clone(),
        manifest_paths_out: manifest_paths_out.clone(),
        row_lineage_first_row_id,
        row_lineage_added_rows,
        target_ref: ctx.target_ref.to_string(),
        snapshot_properties: ctx.snapshot_properties.clone(),
    });
    Ok(PreparedOverwriteAction {
        action,
        manifest_paths_out,
    })
}

/// Eagerly stage a full-table overwrite without catalog I/O so statistics can
/// be bound to the transaction-local snapshot before one combined dispatch.
pub(crate) async fn stage_eager_overwrite(
    ctx: CommitCtx<'_>,
    initial_updates: Vec<TableUpdate>,
) -> Result<(Transaction, CommitOutcome), String> {
    let prepared = prepare_overwrite_action(&ctx)?;
    let action: Arc<dyn TransactionAction> = prepared.action.clone();
    let tx = Transaction::new(ctx.table)
        .stage_action_commit(ActionCommit::new(initial_updates, Vec::new()))
        .map_err(|error| format!("Overwrite initial eager stage failed: {error}"))?
        .stage_action(action)
        .await
        .map_err(|error| format!("Overwrite eager stage failed: {error}"))?;
    let snapshot_id = target_ref_snapshot_id(tx.staged_table().metadata(), ctx.target_ref)
        .ok_or_else(|| "staged overwrite did not produce a target snapshot".to_string())?;
    Ok((
        tx,
        CommitOutcome {
            new_snapshot_id: snapshot_id,
            written_manifest_paths: prepared.written_manifest_paths(),
        },
    ))
}

struct OverwriteTxnAction {
    written: Vec<WrittenFile>,
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema_id: i32,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    row_lineage_first_row_id: Option<u64>,
    row_lineage_added_rows: u64,
    target_ref: String,
    snapshot_properties: BTreeMap<String, String>,
}

#[async_trait]
impl TransactionAction for OverwriteTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> crate::iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let target_ref = &self.target_ref;
        let parent_snapshot_id = target_ref_snapshot_id(m, target_ref);
        let metadata_dir = metadata_dir(table);

        // 1. Enumerate live data files in the base snapshot.
        let existing_entries =
            enumerate_live_data_file_entries_at_snapshot(table, &self.file_io, parent_snapshot_id)
                .await
                .map_err(to_iceberg_unexpected)?;
        let existing = live_data_entries_as_delete_entries(&existing_entries);

        // Ordinary empty input over an empty base is a no-op. Managed
        // publication and operation-recovery properties require a real empty
        // overwrite snapshot, so a non-empty provider property set must flow
        // through the normal snapshot construction below.
        if self.written.is_empty() && existing.is_empty() && self.snapshot_properties.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let parent_summary =
            snapshot_summary(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?;
        let additional_properties = merge_snapshot_summary_properties(
            finalize_snapshot_summary(
                overwrite_summary(&self.written, &existing),
                parent_summary,
                false,
            ),
            &self.snapshot_properties,
            m.uuid(),
            new_snapshot_id,
        )
        .map_err(to_iceberg_unexpected)?;
        let summary = Summary {
            operation: Operation::Overwrite,
            additional_properties,
        };

        let delete_groups = group_live_data_entries_by_partition_spec(&existing_entries);
        let mut new_manifests: Vec<ManifestFile> =
            Vec::with_capacity(delete_groups.len() + usize::from(!self.written.is_empty()));

        // 2. Write deleted-data manifests grouped by their original partition
        // spec. Manifest entries carry partition tuples encoded against the
        // manifest-level spec, which can differ from the current default spec
        // after ALTER MATERIALIZED VIEW ... REPARTITION.
        for (idx, (spec_id, entries)) in delete_groups.into_iter().enumerate() {
            let path = format!(
                "{metadata_dir}/{}-overwrite-deletes-{idx}-spec-{spec_id}.avro",
                self.commit_uuid,
            );
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(path.clone());
            let partition_spec = m.partition_spec_by_id(spec_id).cloned().ok_or_else(|| {
                to_iceberg_unexpected(format!(
                    "Overwrite delete file references unknown partition spec id {spec_id}"
                ))
            })?;
            let existing = live_data_entries_as_delete_entries(&entries);
            let mf = write_overwrite_deletes_manifest(
                &self.file_io,
                &path,
                &existing,
                partition_spec,
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 3. Write the added-data manifest, if any rows were written.
        if !self.written.is_empty() {
            let path = format!("{metadata_dir}/{}-overwrite-data-0.avro", self.commit_uuid);
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(path.clone());
            let mf = write_added_data_manifest(
                &self.file_io,
                &path,
                &self.written,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_seq,
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 4. Write the manifest list (does NOT inherit base manifest entries
        //    per spec §4.3 step 4).
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
            new_manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            format_version,
            self.row_lineage_first_row_id,
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        if let Some(first_row_id) = self.row_lineage_first_row_id {
            let expected_next_row_id = first_row_id
                .checked_add(self.row_lineage_added_rows)
                .ok_or_else(|| {
                    to_iceberg_unexpected(format!(
                        "Row ID overflow when computing overwrite row lineage range: first_row_id={first_row_id}, added_rows={}",
                        self.row_lineage_added_rows
                    ))
                })?;
            if manifest_list_next_row_id != Some(expected_next_row_id) {
                return Err(to_iceberg_unexpected(format!(
                    "Manifest list row lineage mismatch: expected next-row-id {expected_next_row_id}, got {manifest_list_next_row_id:?}"
                )));
            }
        }

        // 5. Construct the Snapshot.
        let snapshot = if let Some(first_row_id) = self.row_lineage_first_row_id {
            Snapshot::builder()
                .with_snapshot_id(new_snapshot_id)
                .with_parent_snapshot_id(parent_snapshot_id)
                .with_sequence_number(new_seq)
                .with_timestamp_ms(now_ms())
                .with_manifest_list(manifest_list_path)
                .with_summary(summary)
                .with_schema_id(self.schema_id)
                .with_row_range(first_row_id, self.row_lineage_added_rows)
                .build()
        } else {
            Snapshot::builder()
                .with_snapshot_id(new_snapshot_id)
                .with_parent_snapshot_id(parent_snapshot_id)
                .with_sequence_number(new_seq)
                .with_timestamp_ms(now_ms())
                .with_manifest_list(manifest_list_path)
                .with_summary(summary)
                .with_schema_id(self.schema_id)
                .build()
        };

        // 6. Build TableUpdate / TableRequirement set.
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

#[derive(Clone)]
struct LiveDataFileEntry {
    data_file: DataFile,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
    partition_spec_id: i32,
}

/// Walk every data manifest in the base snapshot's manifest list and collect
/// each live entry's `(DataFile, sequence_number, file_sequence_number)`. The
/// sequence numbers are needed verbatim by `add_delete_file` to faithfully
/// preserve the original commit identity.
///
/// INSERT OVERWRITE intentionally preserves delete manifests (they keep
/// applying against any rows preserved from the base table), so this walker
/// skips manifests with content type `Deletes`.
#[allow(dead_code)]
pub(super) async fn enumerate_live_data_files(
    table: &Table,
    file_io: &FileIO,
) -> Result<Vec<(DataFile, i64, Option<i64>)>, String> {
    let snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let entries = enumerate_live_data_file_entries_at_snapshot(table, file_io, snapshot_id).await?;
    Ok(live_data_entries_as_delete_entries(&entries))
}

async fn enumerate_live_data_file_entries_at_snapshot(
    table: &Table,
    file_io: &FileIO,
    snapshot_id: Option<i64>,
) -> Result<Vec<LiveDataFileEntry>, String> {
    enumerate_live_files_filtered_at_snapshot(table, file_io, snapshot_id, |entry| {
        entry.content == ManifestContentType::Data
    })
    .await
}

/// Walk every manifest in the base snapshot's manifest list (Data and
/// Deletes alike) and collect every live entry's
/// `(DataFile, sequence_number, file_sequence_number)`.
///
/// Distinct from `enumerate_live_data_files` which skips delete manifests:
/// `TRUNCATE TABLE` must mark every live entry — data files,
/// position-delete files, equality-delete files, and Iceberg v3 deletion
/// vectors — as DELETED in the new snapshot, so this walker accepts both
/// `ManifestContentType::Data` and `ManifestContentType::Deletes`.
pub(super) async fn enumerate_live_all_files(
    table: &Table,
    file_io: &FileIO,
) -> Result<Vec<(DataFile, i64, Option<i64>)>, String> {
    let snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let entries =
        enumerate_live_files_filtered_at_snapshot(table, file_io, snapshot_id, |_entry| true)
            .await?;
    Ok(live_data_entries_as_delete_entries(&entries))
}

/// Shared body for `enumerate_live_data_files` and `enumerate_live_all_files`.
/// Walks the base snapshot's manifest list, applying `manifest_filter` to
/// each manifest entry — only manifests for which the filter returns `true`
/// are loaded and inspected.
async fn enumerate_live_files_filtered_at_snapshot<F>(
    table: &Table,
    file_io: &FileIO,
    snapshot_id: Option<i64>,
    manifest_filter: F,
) -> Result<Vec<LiveDataFileEntry>, String>
where
    F: Fn(&ManifestFile) -> bool,
{
    let m = table.metadata();
    let Some(snapshot_id) = snapshot_id else {
        return Ok(Vec::new());
    };
    let snap = m
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("snapshot {snapshot_id} not found in table metadata"))?;
    let bytes = file_io
        .new_input(snap.manifest_list())
        .map_err(|e| format!("FileIO::new_input({}) failed: {e}", snap.manifest_list()))?
        .read()
        .await
        .map_err(|e| format!("read manifest_list failed: {e}"))?;
    let list = ManifestList::parse_with_version(&bytes, m.format_version())
        .map_err(|e| format!("parse manifest_list failed: {e}"))?;

    let mut out = Vec::new();
    for entry in list.entries() {
        if !manifest_filter(entry) {
            continue;
        }
        let partition_spec_id = entry.partition_spec_id;
        let manifest = entry
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load_manifest({}) failed: {e}", entry.manifest_path))?;
        for me in manifest.entries() {
            if me.is_alive() {
                let data_file = me.data_file().clone();
                // For inherited entries, sequence_number / file_sequence_number
                // may be None — fall back to the manifest's sequence.
                let seq = me.sequence_number().unwrap_or(entry.sequence_number);
                let file_seq = me.file_sequence_number;
                out.push(LiveDataFileEntry {
                    data_file,
                    sequence_number: seq,
                    file_sequence_number: file_seq,
                    partition_spec_id,
                });
            }
        }
    }
    Ok(out)
}

fn group_live_data_entries_by_partition_spec(
    entries: &[LiveDataFileEntry],
) -> BTreeMap<i32, Vec<LiveDataFileEntry>> {
    let mut grouped = BTreeMap::new();
    for entry in entries {
        grouped
            .entry(entry.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(entry.clone());
    }
    grouped
}

fn live_data_entries_as_delete_entries(
    entries: &[LiveDataFileEntry],
) -> Vec<(DataFile, i64, Option<i64>)> {
    entries
        .iter()
        .map(|entry| {
            (
                entry.data_file.clone(),
                entry.sequence_number,
                entry.file_sequence_number,
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_overwrite_deletes_manifest(
    file_io: &FileIO,
    out_path: &str,
    existing: &[(DataFile, i64, Option<i64>)],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_snapshot_id: i64,
    format_version: FormatVersion,
) -> Result<ManifestFile, String> {
    let output_file = file_io
        .new_output(out_path)
        .map_err(|e| format!("FileIO::new_output({out_path}) failed: {e}"))?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(new_snapshot_id),
        None,
        schema,
        (*partition_spec).clone(),
    );
    let mut writer = match format_version {
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
        FormatVersion::V1 => {
            return Err("phase 1 does not support V1 tables".to_string());
        }
    };
    for (df, seq, file_seq) in existing {
        writer
            .add_delete_file(df.clone(), *seq, *file_seq)
            .map_err(|e| format!("ManifestWriter::add_delete_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, ManifestContentType::Data);
    Ok(manifest_file)
}

/// Sibling of `write_overwrite_deletes_manifest` used by `TruncateCommit` for
/// the delete-content (position-delete / equality-delete / Iceberg v3 deletion
/// vector) entries. The existing helper above is hard-wired to
/// `build_v*_data()` so adding a `DataFile` whose `content_type()` is
/// `PositionDeletes` or `EqualityDeletes` would be rejected by
/// `ManifestWriter::check_data_file` (which insists every entry in a Data
/// manifest has `DataContentType::Data`). A separate helper that picks
/// `build_v*_deletes()` is the cleanest fix; mirroring the existing function
/// otherwise keeps the diff minimal and the behaviour parallel.
#[allow(clippy::too_many_arguments)]
pub(super) async fn write_truncate_deletes_manifest(
    file_io: &FileIO,
    out_path: &str,
    existing: &[(DataFile, i64, Option<i64>)],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_snapshot_id: i64,
    format_version: FormatVersion,
) -> Result<ManifestFile, String> {
    let output_file = file_io
        .new_output(out_path)
        .map_err(|e| format!("FileIO::new_output({out_path}) failed: {e}"))?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(new_snapshot_id),
        None,
        schema,
        (*partition_spec).clone(),
    );
    let mut writer = match format_version {
        FormatVersion::V2 => builder.build_v2_deletes(),
        FormatVersion::V3 => builder.build_v3_deletes(),
        FormatVersion::V1 => {
            return Err("phase 1 does not support V1 tables".to_string());
        }
    };
    for (df, seq, file_seq) in existing {
        writer
            .add_delete_file(df.clone(), *seq, *file_seq)
            .map_err(|e| format!("ManifestWriter::add_delete_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, ManifestContentType::Deletes);
    Ok(manifest_file)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_added_data_manifest(
    file_io: &FileIO,
    out_path: &str,
    written: &[WrittenFile],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_seq: i64,
    new_snapshot_id: i64,
    format_version: FormatVersion,
) -> Result<ManifestFile, String> {
    let output_file = file_io
        .new_output(out_path)
        .map_err(|e| format!("FileIO::new_output({out_path}) failed: {e}"))?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(new_snapshot_id),
        None,
        schema,
        (*partition_spec).clone(),
    );
    let mut writer = match format_version {
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
        FormatVersion::V1 => return Err("phase 1 does not support V1 tables".to_string()),
    };
    for f in written {
        let df = build_minimal_data_file(f)?;
        writer
            .add_file(df, new_seq)
            .map_err(|e| format!("ManifestWriter::add_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, ManifestContentType::Data);
    Ok(manifest_file)
}

pub(super) fn build_minimal_data_file(f: &WrittenFile) -> Result<DataFile, String> {
    super::data_file::from_written_file(f).map_err(|error| error.to_string())
}

fn overwrite_summary(
    added: &[WrittenFile],
    deleted: &[(DataFile, i64, Option<i64>)],
) -> HashMap<String, String> {
    let mut p = HashMap::new();
    p.insert("added-data-files".to_string(), added.len().to_string());
    p.insert(
        "added-records".to_string(),
        added
            .iter()
            .map(|f| f.record_count)
            .sum::<u64>()
            .to_string(),
    );
    p.insert(
        "added-files-size".to_string(),
        added
            .iter()
            .map(|f| f.file_size_in_bytes)
            .sum::<u64>()
            .to_string(),
    );
    p.insert("deleted-data-files".to_string(), deleted.len().to_string());
    p.insert(
        "deleted-records".to_string(),
        deleted
            .iter()
            .map(|(df, _, _)| df.record_count())
            .sum::<u64>()
            .to_string(),
    );
    p.insert(
        "removed-files-size".to_string(),
        deleted
            .iter()
            .map(|(df, _, _)| df.file_size_in_bytes())
            .sum::<u64>()
            .to_string(),
    );
    p
}

fn to_iceberg_unexpected(s: String) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, s)
}

#[allow(dead_code)]
fn _check_status_variant_referenced() {
    let _ = ManifestStatus::Deleted;
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::sync::Arc;

    use super::*;
    use crate::commit::CommitOpKind;
    use crate::commit::action::IcebergCommitAction;
    use crate::commit::collector::IcebergCommitCollector;
    use crate::iceberg::spec::{
        DataContentType, DataFileFormat, FormatVersion, NestedField, PrimitiveType, Schema, Struct,
        Type as IcebergType,
    };
    use crate::iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    fn local_test_binding() -> crate::access_binding::IcebergReadBinding {
        let runtime = tokio::runtime::Handle::current();
        crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        )
    }

    struct LocalTableFixture {
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        _warehouse: tempfile::TempDir,
    }

    async fn empty_local_table(format_version: FormatVersion) -> LocalTableFixture {
        let warehouse = tempfile::tempdir().expect("warehouse tempdir");
        let warehouse_uri = format!("file://{}", warehouse.path().join("warehouse").display());
        let binding = local_test_binding();
        let catalog: Arc<dyn Catalog> = Arc::new(
            crate::hadoop_catalog::HadoopFileSystemCatalog::new_with_binding(
                crate::fs_io::build_file_io_for_location(&warehouse_uri, binding.clone()),
                warehouse_uri,
                binding,
            ),
        );
        let namespace = NamespaceIdent::new("db".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                IcebergType::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("build schema");
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(format_version)
                    .build(),
            )
            .await
            .expect("create table");
        LocalTableFixture {
            catalog,
            table_ident: TableIdent::new(namespace, "t".to_string()),
            _warehouse: warehouse,
        }
    }

    fn collector_for(
        fixture: &LocalTableFixture,
        table: &Table,
        op_kind: CommitOpKind,
    ) -> IcebergCommitCollector {
        let metadata = table.metadata();
        IcebergCommitCollector::new(
            op_kind,
            fixture.table_ident.clone(),
            metadata.current_snapshot_id(),
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            format!("{}/data/_staging/test", metadata.location()),
        )
    }

    fn synthetic_data_file(path: String) -> WrittenFile {
        WrittenFile {
            path,
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count: 1,
            file_size_in_bytes: 1,
            split_offsets: Vec::new(),
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        }
    }

    async fn append_synthetic_data(
        fixture: &LocalTableFixture,
        table: &Table,
        target_ref: &str,
        path: String,
    ) -> crate::commit::CommitOutcome {
        let collector = collector_for(fixture, table, CommitOpKind::FastAppend);
        collector.inject_written_file(synthetic_data_file(path));
        let snapshot_properties = BTreeMap::new();
        let abort_handle = collector.abort_log.clone();
        super::super::fast_append::FastAppendCommit
            .commit(CommitCtx {
                collector: &collector,
                table,
                catalog: fixture.catalog.as_ref(),
                file_io: table.file_io(),
                commit_uuid: Uuid::now_v7(),
                abort_handle,
                target_ref,
                snapshot_properties: &snapshot_properties,
            })
            .await
            .expect("append synthetic data file")
    }

    fn pending_document_properties() -> (BTreeMap<String, String>, Vec<u8>) {
        let content = b"publication".to_vec();
        let manifest =
            crate::document_storage::envelope::IcebergDocumentManifestV1 {
                version: crate::document_storage::envelope::DOCUMENT_MANIFEST_VERSION,
                documents: vec![crate::document_storage::envelope::IcebergDocumentEnvelopeV1 {
                version: crate::document_storage::envelope::DOCUMENT_ENVELOPE_VERSION,
                owner: "novarocks.mv".to_string(),
                name: "publication".to_string(),
                format_owner: "novarocks.mv".to_string(),
                format_name: "publication".to_string(),
                format_version: 1,
                revision: novarocks_spi::connector::ConnectorDocumentRevision::for_content(
                    &content,
                )
                .to_bytes(),
                encoded_len: content.len() as u64,
                references: Vec::new(),
                attachment:
                    crate::document_storage::envelope::IcebergDocumentAttachmentV1::CommitOutput,
                carrier: crate::document_storage::envelope::IcebergDocumentCarrierV1::Available {
                    content,
                },
            }],
            };
        let unresolved = crate::document_storage::codec::encode_document_manifest(&manifest)
            .expect("encode prepared publication")
            .to_vec();
        let properties = BTreeMap::from([(
            crate::document_storage::publication::PENDING_DOCUMENT_MANIFEST_PROPERTY.to_string(),
            String::from_utf8(unresolved.clone()).expect("manifest is utf8"),
        )]);
        (properties, unresolved)
    }

    async fn live_data_paths(table: &Table) -> BTreeSet<String> {
        enumerate_live_data_files(table, table.file_io())
            .await
            .expect("enumerate live data files")
            .into_iter()
            .map(|(file, _, _)| file.file_path().to_string())
            .collect()
    }

    #[tokio::test]
    async fn metadata_only_document_publication_creates_one_exact_snapshot() {
        let fixture = empty_local_table(FormatVersion::V2).await;
        let table = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("load table");
        let metadata = table.metadata().clone();
        let collector = IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            fixture.table_ident.clone(),
            None,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            format!("{}/data/_staging/test", metadata.location()),
        );
        let (snapshot_properties, unresolved) = pending_document_properties();
        let abort_handle = collector.abort_log.clone();

        let outcome = super::super::fast_append::commit_empty_iceberg_mv_snapshot(CommitCtx {
            collector: &collector,
            table: &table,
            catalog: fixture.catalog.as_ref(),
            file_io: table.file_io(),
            commit_uuid: Uuid::now_v7(),
            abort_handle,
            target_ref: "main",
            snapshot_properties: &snapshot_properties,
        })
        .await
        .expect("publish metadata-only application documents");

        let reloaded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload metadata-only publication");
        assert_eq!(
            reloaded.metadata().current_snapshot_id(),
            Some(outcome.new_snapshot_id)
        );
        crate::document_storage::publication::validate_expected_manifest(
            reloaded.metadata(),
            outcome.new_snapshot_id,
            &unresolved,
        )
        .expect("validate exact committed output attachment");
        let current = reloaded.metadata().current_snapshot().unwrap();
        assert_eq!(
            current.summary().additional_properties["added-data-files"],
            "0"
        );
        assert_eq!(
            current.summary().additional_properties["added-records"],
            "0"
        );
        assert!(!current.summary().additional_properties.contains_key(
            crate::document_storage::publication::PENDING_DOCUMENT_MANIFEST_PROPERTY
        ));
    }

    #[tokio::test]
    async fn v06_metadata_only_document_publication_preserves_populated_live_data_files() {
        let fixture = empty_local_table(FormatVersion::V2).await;
        let table = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("load table");
        let existing_path = format!("{}/data/existing.parquet", table.metadata().location());
        append_synthetic_data(&fixture, &table, "main", existing_path.clone()).await;

        let populated = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload populated table");
        let base_snapshot_id = populated
            .metadata()
            .current_snapshot_id()
            .expect("populated base snapshot");
        let before_paths = live_data_paths(&populated).await;
        assert_eq!(before_paths, BTreeSet::from([existing_path]));

        let collector = collector_for(&fixture, &populated, CommitOpKind::FastAppend);
        let (snapshot_properties, unresolved) = pending_document_properties();
        let abort_handle = collector.abort_log.clone();
        let outcome = super::super::fast_append::commit_empty_iceberg_mv_snapshot(CommitCtx {
            collector: &collector,
            table: &populated,
            catalog: fixture.catalog.as_ref(),
            file_io: populated.file_io(),
            commit_uuid: Uuid::now_v7(),
            abort_handle,
            target_ref: "main",
            snapshot_properties: &snapshot_properties,
        })
        .await
        .expect("publish metadata-only documents over populated base");

        let published = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload document publication");
        assert_ne!(outcome.new_snapshot_id, base_snapshot_id);
        assert_eq!(
            published.metadata().current_snapshot_id(),
            Some(outcome.new_snapshot_id)
        );
        assert_eq!(live_data_paths(&published).await, before_paths);
        crate::document_storage::publication::validate_expected_manifest(
            published.metadata(),
            outcome.new_snapshot_id,
            &unresolved,
        )
        .expect("validate documents on the advanced snapshot");
    }

    #[tokio::test]
    async fn v06_ordinary_non_main_branch_append_remains_supported() {
        let fixture = empty_local_table(FormatVersion::V3).await;
        let table = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("load table");
        let first_path = format!("{}/data/main.parquet", table.metadata().location());
        let seed = append_synthetic_data(&fixture, &table, "main", first_path).await;
        let seeded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload seeded table");
        let plan = super::super::ref_action::RefActionPlan {
            catalog: "iceberg".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            action: super::super::ref_action::RefAction::CreateBranch {
                name: "dev".to_string(),
                snapshot_id: seed.new_snapshot_id,
                replace: false,
                if_not_exists: false,
                expected_table_uuid: Some(seeded.metadata().uuid()),
            },
        };
        super::super::ref_action::execute_ref_action(fixture.catalog.as_ref(), &seeded, &plan)
            .await
            .expect("create dev branch");

        let branched = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload branched table");
        let main_before = branched.metadata().current_snapshot_id();
        let dev_before = branched.metadata().refs()["dev"].snapshot_id;
        let branch_path = format!("{}/data/dev.parquet", branched.metadata().location());
        let outcome = append_synthetic_data(&fixture, &branched, "dev", branch_path).await;

        let reloaded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload after branch append");
        assert_eq!(reloaded.metadata().current_snapshot_id(), main_before);
        assert_ne!(outcome.new_snapshot_id, dev_before);
        assert_eq!(
            reloaded.metadata().refs()["dev"].snapshot_id,
            outcome.new_snapshot_id
        );
    }
}

#[cfg(test)]
pub(crate) mod preparer_tests {
    use super::*;
    use crate::commit::model::{
        AddedContent, Dependency, FileChanges, FrozenRequest, IsolationLevel, OperationIntent,
        OperationIntentParts, OperationToken, RequestShape, TableTarget,
    };
    use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation, OperationLimits};
    use crate::commit::staging::{PreparedChange, Preparer, StagingBase, StagingEngine};
    use crate::iceberg::spec::{
        DataFileBuilder, DataFileFormat, Manifest, NestedField, PartitionSpec, PrimitiveType,
        Schema, SortOrder, Struct, TableMetadata, TableMetadataBuilder, Type,
    };
    use crate::iceberg::{NamespaceIdent, TableIdent};
    use novarocks_spi::connector::{
        ConnectorRequestContext, ConnectorStopOwner, ConnectorWriteOperationId,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };
    use std::time::{Duration, Instant};

    pub(crate) struct Fixture {
        pub directory: tempfile::TempDir,
        pub operation: IcebergCommitOperation,
        _stop: ConnectorStopOwner,
    }
    impl Fixture {
        pub(crate) fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let location = format!("file://{}", directory.path().display());
            let runtime = tokio::runtime::Handle::current();
            let binding = crate::access_binding::IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
                Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime.clone())),
            );
            let stop = ConnectorStopOwner::new();
            let operation = IcebergCommitOperation::new(
                OperationToken::from_write(ConnectorWriteOperationId::from_bytes([7; 16])),
                location,
                binding,
                ConnectorRequestContext::try_new(
                    Instant::now() + Duration::from_secs(60),
                    stop.view(),
                    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
                )
                .unwrap(),
                crate::resources::IcebergCatalogRuntime::new(runtime),
                OperationLimits::default(),
            )
            .unwrap();
            Self {
                directory,
                operation,
                _stop: stop,
            }
        }
        pub(crate) fn cancel(&self) {
            self._stop.request_stop();
        }

        pub(crate) fn metadata(&self, version: FormatVersion) -> TableMetadata {
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap();
            TableMetadataBuilder::new(
                schema,
                PartitionSpec::unpartition_spec(),
                SortOrder::unsorted_order(),
                format!("file://{}", self.directory.path().display()),
                version,
                HashMap::new(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata
        }
        pub(crate) fn intent(
            &self,
            metadata: &TableMetadata,
            target_ref: &str,
            added: Vec<AddedContent>,
        ) -> OperationIntent {
            OperationIntent::new(OperationIntentParts {
                target: TableTarget {
                    ident: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
                    uuid: Some(metadata.uuid()),
                },
                target_ref: target_ref.into(),
                start: metadata.snapshot_for_ref(target_ref).map(|s| {
                    crate::commit::model::StartSnapshot {
                        snapshot_id: s.snapshot_id(),
                        sequence_number: s.sequence_number(),
                    }
                }),
                changes: FileChanges {
                    added,
                    removed: Vec::new(),
                },
                dependencies: vec![Dependency::NoReadDependency],
                isolation: IsolationLevel::Snapshot,
                shape: RequestShape::SnapshotProducing,
                summary: BTreeMap::new(),
                token: self.operation.token(),
            })
            .unwrap()
        }
        pub(crate) async fn stage(
            &self,
            metadata: TableMetadata,
            intent: &OperationIntent,
            preparer: &dyn Preparer,
        ) -> (TableMetadata, FrozenRequest) {
            let attempt = self.operation.begin_attempt().unwrap();
            let mut engine = StagingEngine::begin(
                StagingBase::Existing {
                    metadata,
                    metadata_location: format!(
                        "file://{}/base.metadata.json",
                        self.directory.path().display()
                    ),
                },
                intent,
                &attempt,
            )
            .unwrap();
            engine.stage(preparer).await.unwrap();
            let after = engine.metadata().clone();
            (after, engine.freeze(&[]).unwrap())
        }
    }
    pub(crate) fn data(path: &str, count: u64, partition: Struct) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.into())
            .file_format(DataFileFormat::Parquet)
            .record_count(count)
            .file_size_in_bytes(count * 10)
            .partition(partition)
            .build()
            .unwrap()
    }
    fn dv(path: &str, data: &str, offset: i64, length: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.into())
            .file_format(DataFileFormat::Puffin)
            .record_count(1)
            .file_size_in_bytes(1000)
            .partition(Struct::empty())
            .referenced_data_file(Some(data.into()))
            .content_offset(Some(offset))
            .content_size_in_bytes(Some(length))
            .build()
            .unwrap()
    }
    pub(crate) async fn raw_entries(
        metadata: &TableMetadata,
        target_ref: &str,
        attempt: &IcebergCommitAttempt,
    ) -> Vec<crate::iceberg::spec::ManifestEntry> {
        use crate::commit::model::ArtifactWriter;
        let snapshot = metadata.snapshot_for_ref(target_ref).unwrap();
        let list = snapshot
            .load_manifest_list(attempt.file_io(), metadata)
            .await
            .unwrap();
        let mut entries = Vec::new();
        for manifest in list.entries() {
            let bytes = attempt
                .file_io()
                .new_input(&manifest.manifest_path)
                .unwrap()
                .read()
                .await
                .unwrap();
            entries.extend(
                Manifest::parse_avro(&bytes)
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|e| e.as_ref().clone()),
            );
        }
        entries
    }
    struct Seed(Vec<AddedContent>);
    #[async_trait]
    impl Preparer for Seed {
        async fn prepare(
            &self,
            view: &super::super::staging::StagedView<'_>,
            intent: &OperationIntent,
        ) -> crate::iceberg::Result<PreparedChange> {
            let id = super::super::staging::new_snapshot_id(view.metadata());
            let mut manifests = Vec::new();
            for content in [ManifestContentType::Data, ManifestContentType::Deletes] {
                let entries: Vec<_> = self
                    .0
                    .iter()
                    .filter(|a| {
                        (a.file().content_type() == DataContentType::Data)
                            == (content == ManifestContentType::Data)
                    })
                    .cloned()
                    .map(super::super::staging::ManifestEntryWrite::Added)
                    .collect();
                if entries.is_empty() {
                    continue;
                }
                manifests.push(
                    super::super::staging::write_manifest(
                        view.artifacts(),
                        view.metadata().format_version(),
                        id,
                        view.metadata().current_schema().clone(),
                        view.metadata().default_partition_spec().as_ref().clone(),
                        content,
                        entries,
                    )
                    .await?,
                );
            }
            prepare_snapshot_change(
                view,
                intent,
                id,
                Operation::Append,
                manifests,
                snapshot_file_summary(&self.0, &[])?,
                false,
            )
            .await
        }
    }

    #[tokio::test]
    async fn append_preparer_inherits_sequences_and_assigns_historical_v2_rows() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V2);
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/old.parquet", 3, Struct::empty()), 0)
                    .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let base = base
            .into_builder(None)
            .upgrade_format_version(FormatVersion::V3)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/new.parquet", 2, Struct::empty()), 0)
                    .unwrap(),
            ],
        );
        let (after, request) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let snapshot = after.current_snapshot().unwrap();
        assert_eq!(snapshot.row_range(), Some((0, 5)));
        assert_eq!(after.next_row_id(), 5);
        assert_eq!(snapshot.summary().operation, Operation::Append);
        let attempt = fixture.operation.begin_attempt().unwrap();
        let entries = raw_entries(&after, "main", &attempt).await;
        let new = entries
            .iter()
            .find(|e| e.data_file().file_path() == "s3://b/new.parquet")
            .unwrap();
        assert_eq!(new.sequence_number, None);
        assert_eq!(new.file_sequence_number, None);
        assert_eq!(
            request
                .requirements()
                .iter()
                .filter(|r| matches!(r, TableRequirement::RefSnapshotIdMatch { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn overwrite_preparer_deletes_every_logical_delete_blob_and_uses_actual_sizes() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let seed = vec![
            AddedContent::new_logical_data(data("s3://b/a.parquet", 3, Struct::empty()), 0)
                .unwrap(),
            AddedContent::new_logical_data(data("s3://b/b.parquet", 2, Struct::empty()), 0)
                .unwrap(),
            AddedContent::new_logical_data(dv("s3://b/shared.puffin", "s3://b/a.parquet", 4, 7), 0)
                .unwrap(),
            AddedContent::new_logical_data(
                dv("s3://b/shared.puffin", "s3://b/b.parquet", 11, 11),
                0,
            )
            .unwrap(),
        ];
        let intent = fixture.intent(&base, "main", Vec::new());
        let (base, _) = fixture.stage(base, &intent, &Seed(seed)).await;
        let start = base.current_snapshot_id().unwrap();
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/new.parquet", 2, Struct::empty()), 0)
                    .unwrap(),
            ],
        );
        let (after, _) = fixture.stage(base, &intent, &OverwritePreparer).await;
        let snapshot = after.current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        assert_eq!(snapshot.row_range(), Some((5, 2)));
        assert_eq!(
            snapshot.summary().additional_properties["removed-files-size"],
            "68"
        );
        assert_eq!(
            snapshot.summary().additional_properties["removed-delete-files"],
            "2"
        );
        let entries =
            raw_entries(&after, "main", &fixture.operation.begin_attempt().unwrap()).await;
        let deleted: Vec<_> = entries
            .iter()
            .filter(|e| e.status == ManifestStatus::Deleted)
            .collect();
        assert_eq!(deleted.len(), 4);
        assert!(
            deleted
                .iter()
                .all(|e| e.snapshot_id == Some(snapshot.snapshot_id())
                    && e.sequence_number == Some(1)
                    && e.file_sequence_number == Some(1))
        );
        let data = deleted
            .iter()
            .find(|e| e.data_file().file_path() == "s3://b/a.parquet")
            .unwrap();
        assert_eq!(data.data_file().first_row_id(), Some(0));
        assert_ne!(start, snapshot.snapshot_id());
    }

    #[tokio::test]
    async fn overwrite_labels_distinguish_append_and_delete() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/a.parquet", 3, Struct::empty()), 0)
                    .unwrap(),
            ],
        );
        let (base, _) = fixture.stage(base, &intent, &OverwritePreparer).await;
        assert_eq!(
            base.current_snapshot().unwrap().summary().operation,
            Operation::Append
        );
        let intent = fixture.intent(&base, "main", Vec::new());
        let (after, _) = fixture.stage(base, &intent, &OverwritePreparer).await;
        assert_eq!(
            after.current_snapshot().unwrap().summary().operation,
            Operation::Delete
        );
        assert_eq!(after.current_snapshot().unwrap().row_range(), Some((3, 0)));
    }

    #[tokio::test]
    async fn truncate_preparer_reads_the_target_ref_and_preserves_main() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/a.parquet", 3, Struct::empty()), 0)
                    .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let branch_id = base.current_snapshot_id().unwrap();
        let base = base
            .into_builder(None)
            .set_ref(
                "audit",
                SnapshotReference::new(
                    branch_id,
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("s3://b/main-only.parquet", 5, Struct::empty()),
                    0,
                )
                .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let main = base.current_snapshot_id();
        let intent = fixture.intent(&base, "audit", Vec::new());
        let (after, request) = fixture
            .stage(base, &intent, &super::super::truncate::TruncatePreparer)
            .await;
        assert_eq!(after.current_snapshot_id(), main);
        let branch = after.snapshot_for_ref("audit").unwrap();
        assert_eq!(branch.parent_snapshot_id(), Some(branch_id));
        assert_eq!(
            branch.summary().additional_properties["deleted-records"],
            "3"
        );
        assert_eq!(branch.summary().additional_properties["total-records"], "0");
        assert_eq!(branch.row_range(), Some((8, 0)));
        assert!(
            request
                .requirements()
                .contains(&TableRequirement::RefSnapshotIdMatch {
                    r#ref: "audit".into(),
                    snapshot_id: Some(branch_id)
                })
        );
        let entries =
            raw_entries(&after, "audit", &fixture.operation.begin_attempt().unwrap()).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, ManifestStatus::Deleted);
        assert_eq!(entries[0].data_file().file_path(), "s3://b/a.parquet");
    }

    #[tokio::test]
    async fn dynamic_overwrite_assigns_only_unassigned_survivors_and_new_rows() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V2);
        let spec = crate::iceberg::spec::UnboundPartitionSpecBuilder::new()
            .add_partition_field(1, "id", crate::iceberg::spec::Transform::Identity)
            .unwrap()
            .build();
        let base = base
            .into_builder(None)
            .add_default_partition_spec(spec)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let spec_id = base.default_partition_spec_id();
        let partition = |id| {
            [Some(crate::iceberg::spec::Literal::long(id))]
                .into_iter()
                .collect::<Struct>()
        };
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/p1.parquet", 3, partition(1)), spec_id)
                    .unwrap(),
                AddedContent::new_logical_data(data("s3://b/p2.parquet", 5, partition(2)), spec_id)
                    .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let base = base
            .into_builder(None)
            .upgrade_format_version(FormatVersion::V3)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("s3://b/new-p1.parquet", 2, partition(1)),
                    spec_id,
                )
                .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::overwrite_partitions::OverwritePartitionsPreparer,
            )
            .await;
        assert_eq!(
            base.current_snapshot().unwrap().summary().operation,
            Operation::Overwrite
        );
        assert_eq!(base.current_snapshot().unwrap().row_range(), Some((0, 7)));
        assert_eq!(base.next_row_id(), 7);
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("s3://b/newer-p1.parquet", 1, partition(1)),
                    spec_id,
                )
                .unwrap(),
            ],
        );
        let (after, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::overwrite_partitions::OverwritePartitionsPreparer,
            )
            .await;
        assert_eq!(after.current_snapshot().unwrap().row_range(), Some((7, 1)));
        let entries =
            raw_entries(&after, "main", &fixture.operation.begin_attempt().unwrap()).await;
        let survivor = entries
            .iter()
            .find(|e| e.data_file().file_path() == "s3://b/p2.parquet")
            .unwrap();
        assert_eq!(survivor.status, ManifestStatus::Existing);
        assert_eq!(survivor.data_file().first_row_id(), Some(0));
        assert_eq!(survivor.sequence_number, Some(1));
        assert_eq!(survivor.file_sequence_number, Some(1));
    }
    #[tokio::test]
    async fn overwrite_preparer_can_replace_historical_unassigned_data() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V2);
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("s3://b/historic.parquet", 3, Struct::empty()),
                    0,
                )
                .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let base = base
            .into_builder(None)
            .upgrade_format_version(FormatVersion::V3)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("s3://b/replacement.parquet", 2, Struct::empty()),
                    0,
                )
                .unwrap(),
            ],
        );
        let (after, _) = fixture.stage(base, &intent, &OverwritePreparer).await;
        assert_eq!(after.current_snapshot().unwrap().row_range(), Some((0, 2)));
        assert_eq!(after.next_row_id(), 2);
        let entries =
            raw_entries(&after, "main", &fixture.operation.begin_attempt().unwrap()).await;
        let old = entries
            .iter()
            .find(|e| e.data_file().file_path() == "s3://b/historic.parquet")
            .unwrap();
        assert_eq!(old.status, ManifestStatus::Deleted);
        assert_eq!(old.data_file().first_row_id(), None);
        assert_eq!(old.sequence_number, Some(1));
        assert_eq!(old.file_sequence_number, Some(1));
    }

    #[tokio::test]
    async fn append_preparer_refuses_partition_values_that_disagree_with_frozen_spec() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let tuple = [Some(crate::iceberg::spec::Literal::long(1))]
            .into_iter()
            .collect();
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/wrong-partition.parquet", 1, tuple), 0)
                    .unwrap(),
            ],
        );
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: base,
                metadata_location: "s3://b/base.metadata.json".into(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        let error = engine
            .stage(&super::super::fast_append::FastAppendPreparer)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), crate::iceberg::ErrorKind::DataInvalid);
        assert!(engine.updates().is_empty());
    }
}
