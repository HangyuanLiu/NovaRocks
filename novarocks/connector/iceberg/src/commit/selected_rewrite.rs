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
use super::write_fence::IcebergFenceAssertion;
use crate::commit::CommitOutcome;

#[derive(Clone, Debug, Default)]
pub(crate) struct SelectedRewriteFiles {
    pub(crate) kind: SelectedRewriteKind,
    pub(crate) data_paths: BTreeSet<String>,
    pub(crate) delete_paths: BTreeSet<String>,
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
        if self.data_paths.iter().any(|path| path.is_empty())
            || self.delete_paths.iter().any(|path| path.is_empty())
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
            .map(|entry| entry.data_file.file_path().to_string())
            .collect::<BTreeSet<_>>();
        let live_deletes = live
            .iter()
            .filter(|entry| {
                entry.data_file.content_type() != crate::iceberg::spec::DataContentType::Data
            })
            .map(|entry| entry.data_file.file_path().to_string())
            .collect::<BTreeSet<_>>();
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
            .cloned()
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
            .cloned()
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

        let summary = merge_snapshot_summary_properties(summary, ctx.snapshot_properties)?;
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
        // `submit_fenced_action`'s re-stage loop. The fence assertion still
        // travels in that same atomic update: `submit_action_commit` appends it
        // to the requirements computed against the base this action observed.
        let fence_requirements = ctx
            .fence
            .map(IcebergFenceAssertion::requirements)
            .unwrap_or_default();
        let submitted = submit_action_commit(
            ctx.catalog,
            ctx.collector.table_ident.clone(),
            staged,
            fence_requirements,
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
