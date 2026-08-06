// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! One-snapshot replacement of a provider-frozen Iceberg file set.
//!
//! This is intentionally separate from the legacy whole-table compaction
//! action. The E2 provider freezes these paths before any C1 writer staging;
//! the action must reject a base that no longer contains exactly that set.

use std::collections::{BTreeSet, HashSet};

use async_trait::async_trait;
use novarocks_connector_iceberg::iceberg::spec::{
    FormatVersion, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
};
use novarocks_connector_iceberg::iceberg::{TableCommit, TableRequirement, TableUpdate};

use super::action::{CommitCtx, IcebergCommitAction, merge_snapshot_summary_properties};
use super::helpers::{
    effective_next_row_id, finalize_snapshot_summary, generate_snapshot_id, metadata_dir, now_ms,
    snapshot_summary, target_ref_snapshot_id, write_manifest_list,
};
use super::row_delta_dv_from_files::dv_descriptor_from_written;
use super::row_delta_dv_metadata::{
    WrittenDvFile, build_snapshot_index_metadata_only, group_live_files_by_partition_spec,
    group_written_dvs_by_partition_spec, partition_spec_by_id, write_added_dv_manifest,
    write_existing_delete_manifest,
};
use super::types::CommitOutcome;

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
                entry.data_file.content_type()
                    == novarocks_connector_iceberg::iceberg::spec::DataContentType::Data
            })
            .map(|entry| entry.data_file.file_path().to_string())
            .collect::<BTreeSet<_>>();
        let live_deletes = live
            .iter()
            .filter(|entry| {
                entry.data_file.content_type()
                    != novarocks_connector_iceberg::iceberg::spec::DataContentType::Data
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
        let commit = TableCommit::builder()
            .ident(ctx.collector.table_ident.clone())
            .updates(vec![
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
            ])
            .requirements(vec![
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
            ])
            .build();
        ctx.catalog
            .update_table(commit)
            .await
            .map_err(|error| format!("selected position-delete rewrite commit failed: {error}"))?;
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
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_file_set_is_nonempty_disjoint_and_path_complete() {
        assert!(SelectedRewriteFiles::default().validate().is_err());
        let mut overlap = SelectedRewriteFiles {
            kind: SelectedRewriteKind::Data,
            data_paths: BTreeSet::from(["s3://warehouse/file.parquet".to_string()]),
            delete_paths: BTreeSet::from(["s3://warehouse/file.parquet".to_string()]),
        };
        assert!(overlap.validate().is_err());
        overlap.delete_paths = BTreeSet::from(["s3://warehouse/file.puffin".to_string()]);
        assert!(overlap.validate().is_ok());

        let position = SelectedRewriteFiles {
            kind: SelectedRewriteKind::PositionDeletes,
            data_paths: BTreeSet::new(),
            delete_paths: BTreeSet::from(["s3://warehouse/file.puffin".to_string()]),
        };
        assert!(position.validate().is_ok());
    }
}
