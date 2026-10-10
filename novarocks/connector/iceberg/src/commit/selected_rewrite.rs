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

//! One-snapshot replacement of a provider-frozen Iceberg file set.
//!
//! This is intentionally separate from the legacy whole-table compaction
//! action. The E2 provider freezes these paths before any C1 writer staging;
//! the action must reject a base that no longer contains exactly that set.

use crate::commit::model::EntryIdentity;
use std::collections::{BTreeSet, HashSet};

use crate::iceberg::spec::{
    FormatVersion, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
};
use crate::iceberg::transaction::ActionCommit;
use crate::iceberg::{TableRequirement, TableUpdate};
use async_trait::async_trait;

use super::action::{CommitCtx, IcebergCommitAction, merge_snapshot_summary_properties};
use super::helpers::{
    effective_next_row_id, finalize_snapshot_summary, generate_snapshot_id, metadata_dir, now_ms,
    snapshot_summary, submit_action_commit, target_ref_snapshot_id, write_manifest_list,
};
use super::row_delta_dv_from_files::dv_descriptor_from_written;
use super::row_delta_dv_metadata::{
    WrittenDvFile, build_snapshot_index_metadata_only, group_live_files_by_partition_spec,
    group_written_dvs_by_partition_spec, partition_spec_by_id, write_added_dv_manifest,
    write_existing_delete_manifest,
};
use crate::commit::CommitOutcome;

#[derive(Clone, Debug, Default)]
pub(crate) struct SelectedRewriteFiles {
    pub(crate) kind: SelectedRewriteKind,
    pub(crate) data_paths: BTreeSet<EntryIdentity>,
    pub(crate) delete_paths: BTreeSet<EntryIdentity>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum SelectedRewriteKind {
    #[default]
    Data,
    PositionDeletes,
}

impl SelectedRewriteFiles {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if (self.kind == SelectedRewriteKind::Data && self.data_paths.is_empty())
            || (self.kind == SelectedRewriteKind::PositionDeletes && self.delete_paths.is_empty())
        {
            return Err("selected rewrite file set is empty".to_string());
        }
        if self.data_paths.iter().any(|path| path.validate().is_err())
            || self
                .delete_paths
                .iter()
                .any(|path| path.validate().is_err())
            || !self.data_paths.is_disjoint(&self.delete_paths)
        {
            return Err("selected rewrite file set is invalid".to_string());
        }
        Ok(())
    }
}

pub(crate) struct SelectedRewriteCommit {
    pub(crate) files: SelectedRewriteFiles,
}

#[async_trait]
impl IcebergCommitAction for SelectedRewriteCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        self.files.validate()?;
        let live = super::overwrite_partitions::enumerate_live_all_files_with_spec_at_snapshot(
            ctx.table,
            ctx.file_io,
            super::helpers::target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref),
        )
        .await?;
        let live_data = live
            .iter()
            .filter(|entry| {
                entry.data_file.content_type() == crate::iceberg::spec::DataContentType::Data
            })
            .map(|entry| {
                EntryIdentity::try_from(&entry.data_file).map_err(|error| error.to_string())
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        let live_deletes = live
            .iter()
            .filter(|entry| {
                entry.data_file.content_type() != crate::iceberg::spec::DataContentType::Data
            })
            .map(|entry| {
                EntryIdentity::try_from(&entry.data_file).map_err(|error| error.to_string())
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        if !self.files.data_paths.is_subset(&live_data)
            || !self.files.delete_paths.is_subset(&live_deletes)
        {
            return Err(
                "selected rewrite frozen file set is no longer live at the target ref".to_string(),
            );
        }
        match self.files.kind {
            SelectedRewriteKind::Data => {
                if self.files.data_paths != live_data || self.files.delete_paths != live_deletes {
                    return Err(
                        "selected data rewrite does not own every live data and delete file"
                            .to_string(),
                    );
                }
                // E2 plans every live data file and assigns each delete
                // dependency one canonical cohort owner.  The equality above
                // proves the legacy transaction replaces precisely the frozen
                // aggregate set; it is not a partial-rewrite fallback.
                super::rewrite_data_files::RewriteDataFilesCommit
                    .commit(ctx)
                    .await
            }
            SelectedRewriteKind::PositionDeletes => self.commit_position_delete_rewrite(ctx).await,
        }
    }
}

impl SelectedRewriteCommit {
    /// Replace exactly the frozen old Puffin DVs with BE-staged Puffin DVs.
    ///
    /// This is deliberately not `RowDeltaDvFromFilesCommit`: that action
    /// represents a logical DELETE and expands by referenced data file.  A
    /// rewrite must preserve the old logical delete set, keep all untouched
    /// manifests, and surface the one snapshot as `operation=replace`.
    async fn commit_position_delete_rewrite(
        &self,
        ctx: CommitCtx<'_>,
    ) -> Result<CommitOutcome, String> {
        if ctx.table.metadata().format_version() != FormatVersion::V3 {
            return Err(
                "selected position-delete rewrite requires an Iceberg v3 table".to_string(),
            );
        }
        let written = ctx.collector.take_written_files()?;
        if written.is_empty() {
            return Err(
                "selected position-delete rewrite produced no replacement Puffin files".to_string(),
            );
        }
        let written_dvs = written
            .iter()
            .map(dv_descriptor_from_written)
            .collect::<Result<Vec<_>, _>>()?;
        let output_paths = written_dvs
            .iter()
            .map(|file| file.path.clone())
            .collect::<HashSet<_>>();
        if output_paths.len() != written_dvs.len() {
            return Err(
                "selected position-delete rewrite has duplicate replacement paths".to_string(),
            );
        }
        let expected_data_paths = self
            .files
            .data_paths
            .iter()
            .map(|identity| identity.path().to_string())
            .collect::<HashSet<_>>();
        let output_data_paths = written_dvs
            .iter()
            .map(|file| file.referenced_data_file.clone())
            .collect::<HashSet<_>>();
        if expected_data_paths.is_empty() || output_data_paths != expected_data_paths {
            return Err(
                "selected position-delete rewrite replacements do not exactly cover frozen data-file groups"
                    .to_string(),
            );
        }

        let target_ref = ctx.target_ref;
        let base_snapshot_id = target_ref_snapshot_id(ctx.table.metadata(), target_ref);
        let index = build_snapshot_index_metadata_only(
            ctx.table,
            ctx.file_io,
            &expected_data_paths,
            target_ref,
        )
        .await?;
        let expected_delete_paths = self
            .files
            .delete_paths
            .iter()
            .map(|identity| identity.path().to_string())
            .collect::<HashSet<_>>();
        if index.replaced_delete_paths != expected_delete_paths {
            return Err(
                "selected position-delete rewrite frozen Puffin files are no longer exactly live"
                    .to_string(),
            );
        }

        let metadata = ctx.table.metadata();
        let new_sequence_number =
            metadata
                .last_sequence_number()
                .checked_add(1)
                .ok_or_else(|| {
                    "selected position-delete rewrite sequence number overflow".to_string()
                })?;
        let new_snapshot_id = generate_snapshot_id();
        let metadata_dir = metadata_dir(ctx.table);
        let summary =
            selected_position_rewrite_summary(&written_dvs, &index, metadata, base_snapshot_id)?;
        let mut manifests = index.untouched_manifests;
        let mut manifest_paths = Vec::new();
        for (index, (spec_id, files)) in
            group_live_files_by_partition_spec(index.touched_delete_existing)
                .into_iter()
                .enumerate()
        {
            let path = format!(
                "{metadata_dir}/{}-selected-rewrite-position-existing-{index}.avro",
                ctx.commit_uuid
            );
            record_manifest(&ctx, &mut manifest_paths, &path);
            manifests.push(
                write_existing_delete_manifest(
                    ctx.file_io,
                    &path,
                    &files,
                    partition_spec_by_id(metadata, spec_id).map_err(|error| error.to_string())?,
                    metadata.current_schema().clone(),
                    new_snapshot_id,
                )
                .await?,
            );
        }
        let data_files = index.data_files;
        for (index, (spec_id, dvs)) in
            group_written_dvs_by_partition_spec(&written_dvs, &data_files)?
                .into_iter()
                .enumerate()
        {
            let path = format!(
                "{metadata_dir}/{}-selected-rewrite-position-added-{index}.avro",
                ctx.commit_uuid
            );
            record_manifest(&ctx, &mut manifest_paths, &path);
            manifests.push(
                write_added_dv_manifest(
                    ctx.file_io,
                    &path,
                    &dvs,
                    &data_files,
                    partition_spec_by_id(metadata, spec_id).map_err(|error| error.to_string())?,
                    metadata.current_schema().clone(),
                    new_sequence_number,
                    new_snapshot_id,
                )
                .await?,
            );
        }
        let manifest_list_path = format!(
            "{metadata_dir}/snap-{new_snapshot_id}-{}.avro",
            ctx.commit_uuid
        );
        record_manifest(&ctx, &mut manifest_paths, &manifest_list_path);
        let row_id = effective_next_row_id(metadata)?;
        let next_row_id = write_manifest_list(
            ctx.file_io,
            &manifest_list_path,
            manifests,
            new_snapshot_id,
            base_snapshot_id,
            new_sequence_number,
            metadata.format_version(),
            Some(row_id),
        )
        .await?;
        if next_row_id != Some(row_id) {
            return Err(format!(
                "selected position-delete rewrite changed Iceberg next-row-id: expected {row_id}, got {next_row_id:?}"
            ));
        }

        let summary = merge_snapshot_summary_properties(
            summary,
            ctx.snapshot_properties,
            metadata.uuid(),
            new_snapshot_id,
        )?;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(base_snapshot_id)
            .with_sequence_number(new_sequence_number)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Replace,
                additional_properties: summary,
            })
            .with_schema_id(metadata.current_schema_id())
            .with_row_range(row_id, 0)
            .build();
        let staged = ActionCommit::new(
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: target_ref.to_string(),
                    reference: SnapshotReference {
                        snapshot_id: new_snapshot_id,
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                },
            ],
            vec![
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: metadata.current_schema_id(),
                },
                TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: metadata.default_partition_spec_id(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: target_ref.to_string(),
                    snapshot_id: base_snapshot_id,
                },
            ],
        );
        // A rewrite replaces a frozen file set and must therefore be rejected —
        // not re-staged — when the base moves, so this action deliberately keeps
        // its single conditional submission instead of taking
        // the generic re-stage loop. The staged action keeps the exact
        // target-ref requirement computed against its frozen base.
        let submitted = submit_action_commit(
            ctx.catalog,
            ctx.collector.table_ident.clone(),
            staged,
            Vec::new(),
        )
        .await
        .map_err(|error| format!("selected position-delete rewrite commit failed: {error}"))?;
        if submitted.is_none() {
            return Err(
                "selected position-delete rewrite built no table updates to submit".to_string(),
            );
        }
        Ok(CommitOutcome {
            new_snapshot_id,
            written_manifest_paths: manifest_paths,
        })
    }
}

fn record_manifest(ctx: &CommitCtx<'_>, manifest_paths: &mut Vec<String>, path: &str) {
    ctx.abort_handle.record_manifest(path.to_string());
    manifest_paths.push(path.to_string());
}

fn selected_position_rewrite_summary(
    written: &[WrittenDvFile],
    index: &super::row_delta_dv_metadata::SnapshotIndex,
    metadata: &crate::iceberg::spec::TableMetadata,
    base_snapshot_id: Option<i64>,
) -> Result<std::collections::HashMap<String, String>, String> {
    let added_position_deletes = written.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.cardinality)
            .ok_or_else(|| "selected position-delete rewrite cardinality overflow".to_string())
    })?;
    let added_bytes = written.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.file_size_in_bytes)
            .ok_or_else(|| "selected position-delete rewrite added bytes overflow".to_string())
    })?;
    let removed_bytes = index.replaced_delete_files_size;
    let mut properties = std::collections::HashMap::from([
        ("added-data-files".to_string(), "0".to_string()),
        ("deleted-data-files".to_string(), "0".to_string()),
        ("added-delete-files".to_string(), written.len().to_string()),
        (
            "removed-delete-files".to_string(),
            index.replaced_delete_files.to_string(),
        ),
        (
            "added-position-deletes".to_string(),
            added_position_deletes.to_string(),
        ),
        (
            "removed-position-deletes".to_string(),
            index.replaced_delete_records.to_string(),
        ),
        ("added-files-size".to_string(), added_bytes.to_string()),
        ("removed-files-size".to_string(), removed_bytes.to_string()),
        ("added-records".to_string(), "0".to_string()),
        ("deleted-records".to_string(), "0".to_string()),
        (
            "rewritten-delete-files".to_string(),
            index.replaced_delete_files.to_string(),
        ),
        (
            "added-delete-files-count".to_string(),
            written.len().to_string(),
        ),
    ]);
    let parent = snapshot_summary(metadata, base_snapshot_id)?;
    properties = finalize_snapshot_summary(properties, parent, false);
    Ok(properties)
}

/// Replacement of the exact provider-frozen logical set. Puffin object paths
/// never substitute for individual DV range identities.
pub(crate) struct SelectedRewritePreparer {
    pub(crate) files: SelectedRewriteFiles,
}

#[async_trait]
impl super::staging::Preparer for SelectedRewritePreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        self.files.validate().map_err(selected_invalid)?;
        let parent = view
            .metadata()
            .snapshot_for_ref(view.target_ref())
            .map(|s| s.snapshot_id());
        let mut inputs =
            super::dependency::ValidationInputs::new(view.metadata(), parent, view.artifacts());
        let live = inputs.live_set().await?;
        let live_data = live
            .keys()
            .filter(|id| matches!(id, EntryIdentity::DataFile { .. }))
            .cloned()
            .collect::<BTreeSet<_>>();
        let live_deletes = live
            .keys()
            .filter(|id| !matches!(id, EntryIdentity::DataFile { .. }))
            .cloned()
            .collect::<BTreeSet<_>>();
        if !self.files.data_paths.is_subset(&live_data)
            || !self.files.delete_paths.is_subset(&live_deletes)
        {
            return Err(selected_invalid(
                "Selected rewrite frozen logical set is no longer live at the target ref",
            ));
        }
        let data_rewrite = self.files.kind == SelectedRewriteKind::Data;
        let removed = if data_rewrite {
            if self.files.data_paths != live_data || self.files.delete_paths != live_deletes {
                return Err(selected_invalid(
                    "Selected data rewrite does not own every live data and delete entry",
                ));
            }
            self.files
                .data_paths
                .union(&self.files.delete_paths)
                .cloned()
                .collect()
        } else {
            if view.metadata().format_version() != FormatVersion::V3 {
                return Err(selected_invalid(
                    "Selected position-delete rewrite requires a V3 table",
                ));
            }
            let expected_data = self
                .files
                .data_paths
                .iter()
                .map(|entry| entry.path().to_owned())
                .collect::<BTreeSet<_>>();
            let mut output_identities = BTreeSet::new();
            let mut output_data = BTreeSet::new();
            for added in &intent.changes().added {
                let identity = EntryIdentity::try_from(added.file())?;
                let EntryIdentity::DeletionVector {
                    referenced_data_file,
                    ..
                } = &identity
                else {
                    return Err(selected_invalid(
                        "Selected delete rewrite output is not a deletion vector",
                    ));
                };
                if !output_identities.insert(identity.clone())
                    || !output_data.insert(referenced_data_file.clone())
                {
                    return Err(selected_invalid(
                        "Selected delete rewrite has duplicate logical output or referenced data ownership",
                    ));
                }
            }
            if output_data != expected_data || output_data.is_empty() {
                return Err(selected_invalid(
                    "Selected delete rewrite outputs do not exactly cover frozen data groups",
                ));
            }
            let owned_live = live_deletes
                .iter()
                .filter(|identity| match identity {
                    EntryIdentity::DeletionVector {
                        referenced_data_file,
                        ..
                    } => expected_data.contains(referenced_data_file),
                    _ => false,
                })
                .cloned()
                .collect::<BTreeSet<_>>();
            if owned_live != self.files.delete_paths {
                return Err(selected_invalid(
                    "Selected delete rewrite does not own the exact live DV entries of its data groups",
                ));
            }
            self.files.delete_paths.clone()
        };
        let intent_removed = intent
            .changes()
            .removed
            .iter()
            .map(|entry| entry.identity().clone())
            .collect::<BTreeSet<_>>();
        if intent_removed != removed {
            return Err(selected_invalid(
                "Selected rewrite intent differs from its frozen removal identity set",
            ));
        }
        super::rewrite_data_files::prepare_physical_rewrite(
            view,
            intent,
            live,
            &removed,
            data_rewrite,
        )
        .await
    }
}

fn selected_invalid(message: impl Into<String>) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, message)
}

#[cfg(test)]
mod preparer_tests {
    use super::*;
    use crate::commit::{dependency::ValidationInputs, model::*, staging::*};
    use crate::iceberg::io::FileIO;
    use crate::iceberg::spec::*;
    use crate::iceberg::{NamespaceIdent, TableIdent};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex};

    struct Writer {
        directory: tempfile::TempDir,
        io: FileIO,
        attempt: AttemptToken,
        ledger: Mutex<ArtifactLedger>,
    }
    impl Writer {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let token = OperationToken::from_write(
                novarocks_spi::connector::ConnectorWriteOperationId::from_bytes([19; 16]),
            );
            let handle = tokio::runtime::Handle::current();
            let binding = crate::access_binding::IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
                Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle)),
            );
            let io = crate::fs_io::build_file_io_for_location(
                &format!("file://{}", directory.path().display()),
                binding,
            );
            Self {
                directory,
                io,
                attempt: AttemptToken::new(token, 0),
                ledger: Mutex::new(ArtifactLedger::new(token)),
            }
        }
    }
    impl ArtifactWriter for Writer {
        fn file_io(&self) -> &FileIO {
            &self.io
        }
        fn allocate(
            &self,
            class: ArtifactClass,
            kind: ArtifactKind,
        ) -> crate::iceberg::Result<ObjectIdentity> {
            let mut ledger = self.ledger.lock().unwrap();
            let object = ObjectIdentity::new(format!(
                "file://{}/object-{}.{}",
                self.directory.path().display(),
                ledger.records().count(),
                kind.extension()
            ))?;
            ledger.register(ArtifactRecord {
                object: object.clone(),
                class,
                attempt: (class == ArtifactClass::Attempt).then_some(self.attempt),
                write_state: ArtifactWriteState::Allocated,
            })?;
            Ok(object)
        }
        fn attempt_token(&self) -> AttemptToken {
            self.attempt
        }
        fn check_active(&self) -> crate::iceberg::Result<()> {
            Ok(())
        }
        fn snapshot_artifacts(
            &self,
            refs: &[ObjectIdentity],
        ) -> crate::iceberg::Result<AttemptArtifacts> {
            self.ledger
                .lock()
                .unwrap()
                .snapshot_for_attempt(self.attempt, refs.iter().cloned())
        }
    }
    fn data(path: &str, first: Option<i64>) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.into())
            .file_format(DataFileFormat::Parquet)
            .record_count(3)
            .file_size_in_bytes(100)
            .partition_spec_id(0)
            .first_row_id(first)
            .build()
            .unwrap()
    }
    fn metadata(writer: &Writer) -> TableMetadata {
        TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            format!("file://{}", writer.directory.path().display()),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }
    async fn append_fixture(
        writer: &Writer,
        metadata: TableMetadata,
        files: Vec<DataFile>,
        snapshot_id: i64,
        target: &str,
    ) -> TableMetadata {
        let mut manifests = Vec::new();
        for content in [DataContentType::Data, DataContentType::PositionDeletes] {
            let group = files
                .iter()
                .filter(|f| f.content_type() == content)
                .cloned()
                .collect::<Vec<_>>();
            if group.is_empty() {
                continue;
            }
            manifests.push(
                write_manifest(
                    writer,
                    FormatVersion::V3,
                    snapshot_id,
                    metadata.current_schema().clone(),
                    metadata.default_partition_spec().as_ref().clone(),
                    if content == DataContentType::Data {
                        ManifestContentType::Data
                    } else {
                        ManifestContentType::Deletes
                    },
                    group.into_iter().map(|f| {
                        ManifestEntryWrite::Added(AddedContent::new_logical_data(f, 0).unwrap())
                    }),
                )
                .await
                .unwrap(),
            );
        }
        let list = crate::commit::staging::write_manifest_list(
            writer,
            &metadata,
            snapshot_id,
            None,
            manifests,
        )
        .await
        .unwrap();
        let builder = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_sequence_number(metadata.next_sequence_number())
            .with_timestamp_ms(metadata.last_updated_ms() + 1)
            .with_manifest_list(list.object.path().to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(metadata.current_schema_id());
        let (first, count) = list.row_range.unwrap();
        let snapshot = builder.with_row_range(first, count).build();
        metadata
            .into_builder(None)
            .set_branch_snapshot(snapshot, target)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }
    async fn fixture(writer: &Writer, dvs: bool) -> TableMetadata {
        let mut files = vec![
            data("file:///source-a.parquet", None),
            data("file:///source-b.parquet", None),
        ];
        if dvs {
            let mut dv = super::super::puffin_dv::DeletionVector::new();
            dv.insert(1).unwrap();
            let inputs = ["file:///source-a.parquet", "file:///source-b.parquet"].map(|path| {
                super::super::puffin_dv::DeletionVectorBlobInput {
                    referenced_data_file: path.into(),
                    deletion_vector: dv.clone(),
                }
            });
            let written = super::super::puffin_dv::write_multi_deletion_vector_puffin_allocated(
                writer,
                ArtifactClass::Operation,
                &inputs,
            )
            .await
            .unwrap();
            for blob in written {
                files.push(
                    DataFileBuilder::default()
                        .content(DataContentType::PositionDeletes)
                        .file_path(blob.path)
                        .file_format(DataFileFormat::Puffin)
                        .record_count(blob.cardinality)
                        .file_size_in_bytes(blob.file_size_in_bytes)
                        .partition_spec_id(0)
                        .content_offset(Some(blob.content_offset))
                        .content_size_in_bytes(Some(blob.content_size_in_bytes))
                        .referenced_data_file(Some(blob.referenced_data_file))
                        .build()
                        .unwrap(),
                );
            }
        }
        let metadata = append_fixture(writer, metadata(writer), files, 10, "dev").await;
        append_fixture(
            writer,
            metadata,
            vec![data("file:///only-main.parquet", None)],
            20,
            "main",
        )
        .await
    }
    fn intent(
        writer: &Writer,
        metadata: &TableMetadata,
        added: Vec<AddedContent>,
        removed: Vec<FrozenEntry>,
        shape: RequestShape,
    ) -> OperationIntent {
        OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
                uuid: Some(metadata.uuid()),
            },
            target_ref: "dev".into(),
            start: Some(StartSnapshot {
                snapshot_id: 10,
                sequence_number: 1,
            }),
            changes: FileChanges { added, removed },
            dependencies: vec![Dependency::RefUnchanged],
            isolation: IsolationLevel::Snapshot,
            shape,
            summary: BTreeMap::new(),
            token: writer.attempt.operation(),
        })
        .unwrap()
    }
    fn base(metadata: TableMetadata) -> StagingBase {
        StagingBase::Existing {
            metadata,
            metadata_location: "file:///metadata.json".into(),
        }
    }
    async fn live(
        writer: &Writer,
        metadata: &TableMetadata,
        parent: i64,
    ) -> crate::commit::dependency::LiveSet {
        ValidationInputs::new(metadata, Some(parent), writer)
            .live_set()
            .await
            .unwrap()
            .clone()
    }

    #[tokio::test]
    async fn physical_rewrite_uses_frozen_data_age_exact_ref_and_actual_row_allocation() {
        let writer = Writer::new();
        let metadata = fixture(&writer, false).await;
        let original = live(&writer, &metadata, 10).await;
        for preserve in [true, false] {
            let file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(
                    if preserve {
                        "file:///preserved.parquet"
                    } else {
                        "file:///historical-output.parquet"
                    }
                    .into(),
                )
                .file_format(DataFileFormat::Parquet)
                .record_count(6)
                .file_size_in_bytes(200)
                .partition_spec_id(0)
                .first_row_id(preserve.then_some(0))
                .build()
                .unwrap();
            let intent = intent(
                &writer,
                &metadata,
                vec![AddedContent::rewritten_data(file, 0, 1).unwrap()],
                original.values().map(|e| e.frozen.clone()).collect(),
                RequestShape::SnapshotProducing,
            );
            let mut staging =
                StagingEngine::begin(base(metadata.clone()), &intent, &writer).unwrap();
            staging
                .stage(&super::super::rewrite_data_files::RewriteDataFilesPreparer)
                .await
                .unwrap();
            let snapshot = staging.metadata().snapshot_for_ref("dev").unwrap();
            assert_eq!(snapshot.parent_snapshot_id(), Some(10));
            assert_eq!(snapshot.sequence_number(), 3);
            assert_eq!(snapshot.summary().operation, Operation::Replace);
            assert_eq!(
                snapshot.added_rows_count(),
                Some(if preserve { 0 } else { 6 })
            );
            assert_eq!(
                staging
                    .metadata()
                    .snapshot_for_ref("main")
                    .unwrap()
                    .snapshot_id(),
                20
            );
            let output = live(&writer, staging.metadata(), snapshot.snapshot_id()).await;
            assert_eq!(output.len(), 1);
            let entry = output.values().next().unwrap();
            assert_eq!(entry.frozen.facts().data_sequence, Some(1));
            assert_eq!(entry.frozen.facts().file_sequence, Some(3));
            assert_eq!(
                entry.frozen.facts().first_row_id,
                Some(if preserve { 0 } else { 9 })
            );
        }
    }

    #[tokio::test]
    async fn physical_rewrite_mixed_assigned_and_historical_outputs_allocates_only_unassigned_rows()
    {
        let writer = Writer::new();
        let metadata = fixture(&writer, false).await;
        let original = live(&writer, &metadata, 10).await;
        let added = vec![
            AddedContent::rewritten_data(data("file:///already-assigned.parquet", Some(0)), 0, 1)
                .unwrap(),
            AddedContent::rewritten_data(data("file:///historical-null.parquet", None), 0, 1)
                .unwrap(),
        ];
        let intent = intent(
            &writer,
            &metadata,
            added,
            original.values().map(|e| e.frozen.clone()).collect(),
            RequestShape::SnapshotProducing,
        );
        let mut staging = StagingEngine::begin(base(metadata), &intent, &writer).unwrap();
        staging
            .stage(&super::super::rewrite_data_files::RewriteDataFilesPreparer)
            .await
            .unwrap();
        let snapshot = staging.metadata().snapshot_for_ref("dev").unwrap();
        assert_eq!(snapshot.row_range(), Some((9, 3)));
        assert_eq!(staging.metadata().next_row_id(), 12);
        let output = live(&writer, staging.metadata(), snapshot.snapshot_id()).await;
        assert_eq!(output.len(), 2);
        assert_eq!(
            output[&EntryIdentity::DataFile {
                path: "file:///already-assigned.parquet".into()
            }]
                .frozen
                .facts()
                .first_row_id,
            Some(0)
        );
        assert_eq!(
            output[&EntryIdentity::DataFile {
                path: "file:///historical-null.parquet".into()
            }]
                .frozen
                .facts()
                .first_row_id,
            Some(9)
        );
        for entry in output.values() {
            assert_eq!(entry.frozen.facts().data_sequence, Some(1));
            assert_eq!(entry.frozen.facts().file_sequence, Some(3));
        }
    }

    #[tokio::test]
    async fn selected_delete_rewrite_replaces_one_blob_and_retains_other_shared_puffin_blob() {
        let writer = Writer::new();
        let metadata = fixture(&writer, true).await;
        let original = live(&writer, &metadata, 10).await;
        let old=original.keys().find(|id|matches!(id,EntryIdentity::DeletionVector{referenced_data_file,..} if referenced_data_file=="file:///source-a.parquet")).unwrap().clone();
        let mut dv = super::super::puffin_dv::DeletionVector::new();
        dv.insert(1).unwrap();
        let blob = super::super::puffin_dv::write_single_deletion_vector_puffin_allocated(
            &writer,
            ArtifactClass::Operation,
            "file:///source-a.parquet",
            &dv,
        )
        .await
        .unwrap();
        let replacement = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(blob.path)
            .file_format(DataFileFormat::Puffin)
            .record_count(blob.cardinality)
            .file_size_in_bytes(blob.file_size_in_bytes)
            .partition_spec_id(0)
            .content_offset(Some(blob.content_offset))
            .content_size_in_bytes(Some(blob.content_size_in_bytes))
            .referenced_data_file(Some(blob.referenced_data_file))
            .build()
            .unwrap();
        let intent = intent(
            &writer,
            &metadata,
            vec![AddedContent::rewritten_data(replacement, 0, 1).unwrap()],
            vec![original[&old].frozen.clone()],
            RequestShape::SnapshotProducing,
        );
        let files = SelectedRewriteFiles {
            kind: SelectedRewriteKind::PositionDeletes,
            data_paths: BTreeSet::from([EntryIdentity::DataFile {
                path: "file:///source-a.parquet".into(),
            }]),
            delete_paths: BTreeSet::from([old.clone()]),
        };
        let mut staging = StagingEngine::begin(base(metadata.clone()), &intent, &writer).unwrap();
        staging
            .stage(&SelectedRewritePreparer { files })
            .await
            .unwrap();
        let snapshot = staging.metadata().snapshot_for_ref("dev").unwrap();
        assert_eq!(snapshot.added_rows_count(), Some(0));
        let output = live(&writer, staging.metadata(), snapshot.snapshot_id()).await;
        assert_eq!(output.len(), 4);
        assert!(!output.contains_key(&old));
        let retained=original.keys().find(|id|matches!(id,EntryIdentity::DeletionVector{referenced_data_file,..} if referenced_data_file=="file:///source-b.parquet")).unwrap();
        assert_eq!(output[retained].frozen, original[retained].frozen);
        assert!(
            writer
                .io
                .new_input(old.path())
                .unwrap()
                .exists()
                .await
                .unwrap()
        );
        assert_eq!(
            snapshot.summary().additional_properties["removed-files-size"],
            match old {
                EntryIdentity::DeletionVector { length, .. } => length.to_string(),
                _ => unreachable!(),
            }
        );
    }

    #[tokio::test]
    async fn selected_delete_rewrite_accepts_two_replacement_entries_in_one_puffin() {
        let writer = Writer::new();
        let metadata = fixture(&writer, true).await;
        let original = live(&writer, &metadata, 10).await;
        let mut dv = super::super::puffin_dv::DeletionVector::new();
        dv.insert(1).unwrap();
        let inputs = ["file:///source-a.parquet", "file:///source-b.parquet"].map(|path| {
            super::super::puffin_dv::DeletionVectorBlobInput {
                referenced_data_file: path.into(),
                deletion_vector: dv.clone(),
            }
        });
        let blobs = super::super::puffin_dv::write_multi_deletion_vector_puffin_allocated(
            &writer,
            ArtifactClass::Attempt,
            &inputs,
        )
        .await
        .unwrap();
        assert_eq!(blobs[0].path, blobs[1].path);
        let added = blobs
            .into_iter()
            .map(|blob| {
                AddedContent::rewritten_data(
                    DataFileBuilder::default()
                        .content(DataContentType::PositionDeletes)
                        .file_path(blob.path)
                        .file_format(DataFileFormat::Puffin)
                        .record_count(blob.cardinality)
                        .file_size_in_bytes(blob.file_size_in_bytes)
                        .partition_spec_id(0)
                        .content_offset(Some(blob.content_offset))
                        .content_size_in_bytes(Some(blob.content_size_in_bytes))
                        .referenced_data_file(Some(blob.referenced_data_file))
                        .build()
                        .unwrap(),
                    0,
                    1,
                )
                .unwrap()
            })
            .collect();
        let delete_paths = original
            .keys()
            .filter(|id| matches!(id, EntryIdentity::DeletionVector { .. }))
            .cloned()
            .collect::<BTreeSet<_>>();
        let intent = intent(
            &writer,
            &metadata,
            added,
            delete_paths
                .iter()
                .map(|id| original[id].frozen.clone())
                .collect(),
            RequestShape::SnapshotProducing,
        );
        let mut staging = StagingEngine::begin(base(metadata), &intent, &writer).unwrap();
        let files = SelectedRewriteFiles {
            kind: SelectedRewriteKind::PositionDeletes,
            data_paths: original
                .keys()
                .filter(|id| matches!(id, EntryIdentity::DataFile { .. }))
                .cloned()
                .collect(),
            delete_paths: delete_paths.clone(),
        };
        staging
            .stage(&SelectedRewritePreparer { files })
            .await
            .unwrap();
        let output = live(
            &writer,
            staging.metadata(),
            staging
                .metadata()
                .snapshot_for_ref("dev")
                .unwrap()
                .snapshot_id(),
        )
        .await;
        let dvs = output
            .iter()
            .filter(|(id, _)| matches!(id, EntryIdentity::DeletionVector { .. }))
            .collect::<Vec<_>>();
        assert_eq!(dvs.len(), 2);
        assert_eq!(dvs[0].0.path(), dvs[1].0.path());
        for (id, entry) in dvs {
            assert!(!delete_paths.contains(id));
            assert_eq!(entry.frozen.facts().data_sequence, Some(1));
            assert_eq!(entry.frozen.facts().file_sequence, Some(3));
        }
        let before = writer.ledger.lock().unwrap().records().count();
        assert!(
            super::super::puffin_dv::write_multi_deletion_vector_puffin_allocated(
                &writer,
                ArtifactClass::ExternalRegistered,
                &inputs
            )
            .await
            .is_err()
        );
        assert_eq!(writer.ledger.lock().unwrap().records().count(), before);
    }

    #[tokio::test]
    async fn selected_data_rewrite_rejects_partial_ownership_without_allocating() {
        let writer = Writer::new();
        let metadata = fixture(&writer, false).await;
        let original = live(&writer, &metadata, 10).await;
        let first = original.keys().next().unwrap().clone();
        let intent = intent(
            &writer,
            &metadata,
            vec![],
            vec![original[&first].frozen.clone()],
            RequestShape::SnapshotProducing,
        );
        let staging = StagingEngine::begin(base(metadata), &intent, &writer).unwrap();
        let before = writer.ledger.lock().unwrap().records().count();
        let error = SelectedRewritePreparer {
            files: SelectedRewriteFiles {
                kind: SelectedRewriteKind::Data,
                data_paths: BTreeSet::from([first]),
                delete_paths: BTreeSet::new(),
            },
        }
        .prepare(&staging.view(), &intent)
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("every live"));
        assert_eq!(writer.ledger.lock().unwrap().records().count(), before);
    }

    #[tokio::test]
    async fn statistics_metadata_change_has_no_ref_assertion_and_matches_standard_update() {
        let writer = Writer::new();
        let metadata = fixture(&writer, false).await;
        let intent = intent(
            &writer,
            &metadata,
            vec![],
            vec![],
            RequestShape::MetadataOnly,
        );
        let draft = novarocks_spi::connector::StatisticsArtifactDraft::try_new(
            vec![1],
            "test-statistics",
            bytes::Bytes::from_static(b"body"),
            BTreeMap::new(),
        )
        .unwrap();
        let statistics =
            crate::stats_assembler::write_puffin_artifacts_allocated(&writer, 10, 1, &[draft])
                .await
                .unwrap()
                .unwrap();
        // Temporary P9 migration comparison; remove with the vendor action in T10.
        let sdk_table = crate::iceberg::table::Table::builder()
            .identifier(intent.target().ident.clone())
            .metadata(metadata.clone())
            .file_io(writer.io.clone())
            .build()
            .unwrap();
        let mut sdk =
            super::super::statistics::stage_statistics_file(&sdk_table, statistics.clone())
                .await
                .unwrap();
        let sdk_updates = sdk.take_updates();
        assert!(sdk.take_requirements().is_empty());
        let mut staging = StagingEngine::begin(base(metadata), &intent, &writer).unwrap();
        let preparer = super::super::statistics::StatisticsPreparer {
            statistics: statistics.clone(),
        };
        let change = preparer.prepare(&staging.view(), &intent).await.unwrap();
        assert!(change.requirements.is_empty());
        assert_eq!(change.updates, sdk_updates);
        assert_eq!(
            serde_json::to_value(&change.updates).unwrap(),
            serde_json::json!([{"action":"set-statistics","snapshot-id":10,"statistics":statistics}])
        );
        staging.stage(&preparer).await.unwrap();
        let frozen = staging.freeze(&[]).unwrap();
        assert_eq!(frozen.requirements().len(), 1);
        assert!(matches!(
            frozen.requirements()[0],
            crate::iceberg::TableRequirement::UuidMatch { .. }
        ));
        assert!(
            writer
                .io
                .new_input(&statistics.statistics_path)
                .unwrap()
                .exists()
                .await
                .unwrap()
        );
        assert_eq!(statistics.blob_metadata[0].snapshot_id, 10);
        assert_eq!(statistics.blob_metadata[0].sequence_number, 1);
        assert!(statistics.file_size_in_bytes > statistics.file_footer_size_in_bytes);
    }
}
