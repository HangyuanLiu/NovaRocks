// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Provider-private Iceberg writer report conversion and catalog commit state.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ExternalMutationFinalization,
};
use opendal::Operator;

use super::catalog::registry::block_on_iceberg;
use super::change_stream_routing::{
    ChangeStreamWriterCommitPlan, ChangeStreamWriterReports, route_change_stream_staged_reports,
};
use super::commit::{
    AbortLog, CleanupAttempt, CleanupPathMapper, CommitOutcome, CommitServiceError,
    CowUpdateRewriteSet, IcebergCommitCollector, RunInput, WrittenFile, run_iceberg_commit,
};
use super::report::IcebergWriterReport;

/// Provider-private commit context for one coordinated Iceberg writer.
///
/// Application finalization, including query-cache invalidation, intentionally
/// remains outside this type. Callers supply only reports accepted by the
/// sealed generic write operation.
pub(crate) struct IcebergWriteCommitExecutor {
    pub(crate) catalog: Arc<dyn iceberg::Catalog>,
    pub(crate) table: iceberg::table::Table,
    pub(crate) collector: Arc<IcebergCommitCollector>,
    pub(crate) fs: Operator,
    pub(crate) cleanup_path_mapper: Option<CleanupPathMapper>,
    pub(crate) cow_update_rewrite: Option<CowUpdateRewriteSet>,
    pub(crate) target_ref: String,
    pub(crate) snapshot_properties: BTreeMap<String, String>,
}

impl IcebergWriteCommitExecutor {
    pub(crate) fn build_staged_create_action(
        &self,
        completion: &novarocks_spi::connector::ConnectorWriteOperationCompletion,
        abort_handle: &Arc<AbortLog>,
    ) -> Result<
        super::commit::StagedFastAppendAction,
        super::write_service::StagedCreateActionBuildFailure,
    > {
        use super::write_service::StagedCreateActionBuildFailure;

        let reports = super::write_service::decode_primary_write_completion(
            completion,
            self.table.metadata(),
        )
        .map_err(|error| StagedCreateActionBuildFailure {
            error,
            abort_handle: Arc::clone(abort_handle),
        })?;
        let mut writer_files = Vec::new();
        self.convert_iceberg_writer_reports(reports, &mut writer_files)
            .map_err(|error| StagedCreateActionBuildFailure {
                error,
                abort_handle: Arc::clone(abort_handle),
            })?;
        self.collector.inject_written_files(writer_files);
        let file_io = self.table.file_io().clone();
        let context = super::commit::CommitCtx {
            collector: &self.collector,
            table: &self.table,
            catalog: self.catalog.as_ref(),
            file_io: &file_io,
            commit_uuid: uuid::Uuid::new_v4(),
            abort_handle: Arc::clone(abort_handle),
            target_ref: &self.target_ref,
            snapshot_properties: &self.snapshot_properties,
        };
        block_on_iceberg(super::commit::build_staged_fast_append_action(context))
            .map_err(|error| StagedCreateActionBuildFailure {
                error: CommitServiceError::known_uncommitted(
                    error,
                    CleanupAttempt::not_attempted(),
                ),
                abort_handle: Arc::clone(abort_handle),
            })?
            .map_err(|error| StagedCreateActionBuildFailure {
                error: CommitServiceError::known_uncommitted(
                    error,
                    CleanupAttempt::not_attempted(),
                ),
                abort_handle: Arc::clone(abort_handle),
            })
    }

    pub(crate) fn abort_staged_create_action(
        &self,
        completion: &novarocks_spi::connector::ConnectorWriteOperationCompletion,
        abort_handle: &Arc<AbortLog>,
    ) -> Result<ExternalMutationFinalization, ConnectorError> {
        let reports = super::write_service::decode_primary_write_completion(
            completion,
            self.table.metadata(),
        )
        .map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::InvalidRequest, format!("{error:?}"))
                .with_retryable_before_progress()
        })?;
        let data_cleanup = self
            .abort_iceberg_writer_reports(reports)
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))?;
        let fs = self.fs.clone();
        let mapper = self.cleanup_path_mapper.clone();
        let manifest_errors = block_on_iceberg(async {
            if let Some(mapper) = mapper {
                abort_handle
                    .cleanup_with_path_mapper(&fs, |path| mapper(path))
                    .await
            } else {
                abort_handle.cleanup(&fs).await
            }
        })
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))?;
        let error_count = data_cleanup.error_count + manifest_errors.len();
        Ok(if error_count == 0 {
            ExternalMutationFinalization::Complete
        } else {
            ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Internal,
                format!("staged table cleanup completed with {error_count} deletion error(s)"),
            ))
        })
    }

    pub(crate) fn commit_iceberg_writer_reports(
        &self,
        reports: impl IntoIterator<Item = IcebergWriterReport>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_iceberg_writer_reports_with_snapshot_properties(
            reports,
            self.snapshot_properties.clone(),
        )
    }

    pub(crate) fn commit_iceberg_writer_reports_with_snapshot_properties(
        &self,
        reports: impl IntoIterator<Item = IcebergWriterReport>,
        snapshot_properties: BTreeMap<String, String>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let mut writer_files = Vec::new();
        self.convert_iceberg_writer_reports(reports, &mut writer_files)?;
        self.collector.inject_written_files(writer_files);
        self.run_commit_after_collector_injection_with_properties(snapshot_properties)
    }

    pub(crate) fn abort_iceberg_writer_reports(
        &self,
        reports: impl IntoIterator<Item = IcebergWriterReport>,
    ) -> Result<CleanupAttempt, String> {
        let mut writer_files = Vec::new();
        for report in reports {
            let file = self.collector.convert_writer_report(report).map_err(|message| {
                let cleanup = self.cleanup_converted_writer_files(&writer_files);
                format!(
                    "convert Iceberg staged report during abort failed: {message}; cleanup attempted={}, errors={}",
                    cleanup.attempted, cleanup.error_count
                )
            })?;
            writer_files.push(file);
        }
        Ok(self.cleanup_converted_writer_files(&writer_files))
    }

    pub(crate) fn commit_change_stream_staged_reports(
        &self,
        staged_reports: Vec<novarocks_spi::connector::ConnectorStagedReport>,
        plan: &ChangeStreamWriterCommitPlan,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let mut by_writer = Vec::with_capacity(staged_reports.len());
        for staged in staged_reports {
            staged.validate().map_err(|error| {
                CommitServiceError::invalid_input(format!(
                    "validate change-stream connector staged report: {error}"
                ))
            })?;
            let reports = super::write_contract::decode_writer_reports(
                staged.payload(),
                self.table.metadata(),
            )
            .map_err(CommitServiceError::invalid_input)?;
            by_writer.push(ChangeStreamWriterReports {
                fragment_id: staged.writer().fragment_id(),
                reports,
            });
        }
        let routed = route_change_stream_staged_reports(&self.collector, by_writer, plan).map_err(
            |error| {
                let (message, converted_files) = error.into_parts();
                CommitServiceError::known_uncommitted(
                    message,
                    self.cleanup_converted_writer_files(&converted_files),
                )
            },
        )?;
        routed.inject(&self.collector);
        self.run_commit_after_collector_injection()
    }

    fn run_commit_after_collector_injection(&self) -> Result<CommitOutcome, CommitServiceError> {
        self.run_commit_after_collector_injection_with_properties(self.snapshot_properties.clone())
    }

    fn run_commit_after_collector_injection_with_properties(
        &self,
        snapshot_properties: BTreeMap<String, String>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let file_io = self.table.file_io().clone();
        let input = RunInput {
            collector: Arc::clone(&self.collector),
            catalog: Arc::clone(&self.catalog),
            table: self.table.clone(),
            fs: self.fs.clone(),
            file_io,
            cleanup_path_mapper: self.cleanup_path_mapper.clone(),
            cow_update_rewrite: self.cow_update_rewrite.clone(),
            selected_rewrite: None,
            target_ref: self.target_ref.clone(),
            snapshot_properties,
        };
        match block_on_iceberg(async { run_iceberg_commit(input).await }) {
            Ok(result) => result,
            Err(message) => Err(CommitServiceError::known_uncommitted(
                message,
                CleanupAttempt::not_attempted(),
            )),
        }
    }

    fn convert_iceberg_writer_reports(
        &self,
        reports: impl IntoIterator<Item = IcebergWriterReport>,
        writer_files: &mut Vec<WrittenFile>,
    ) -> Result<(), CommitServiceError> {
        for report in reports {
            match self.collector.convert_writer_report(report) {
                Ok(file) => writer_files.push(file),
                Err(message) => {
                    let cleanup = self.cleanup_converted_writer_files(writer_files);
                    return Err(CommitServiceError::known_uncommitted(message, cleanup));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn cleanup_converted_writer_files(&self, files: &[WrittenFile]) -> CleanupAttempt {
        let abort_log = AbortLog::new();
        for file in files {
            abort_log.record_data_file(file.path.clone());
        }
        let fs = self.fs.clone();
        let cleanup_path_mapper = self.cleanup_path_mapper.clone();
        match block_on_iceberg(async move {
            if let Some(mapper) = cleanup_path_mapper {
                abort_log
                    .cleanup_with_path_mapper(&fs, |path| mapper(path))
                    .await
            } else {
                abort_log.cleanup(&fs).await
            }
        }) {
            Ok(cleanup_errors) => CleanupAttempt::from_cleanup_errors(&cleanup_errors),
            Err(message) => {
                CleanupAttempt::completed(vec![format!("abort cleanup runtime failed: {message}")])
            }
        }
    }
}
