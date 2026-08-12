// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Provider-private Iceberg writer report conversion and catalog commit state.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_connector_iceberg::opendal::Operator;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorWriteAbortOutcome, ExternalMutationFinalization, ExternalMutationOutcome,
};

use super::catalog::registry::block_on_iceberg;
use super::change_stream_routing::{
    ChangeStreamWriterCommitPlan, ChangeStreamWriterReports, route_change_stream_staged_reports,
};
use super::commit::{
    CleanupAttempt, CleanupPathMapper, CommitServiceError, CowUpdateRewriteSet,
    IcebergCommitCollector, RunInput, run_iceberg_commit,
};
use novarocks_connector_iceberg::commit::AbortLog;
use novarocks_connector_iceberg::commit::CommitOpKind;
use novarocks_connector_iceberg::commit::report::IcebergWriterReport;
use novarocks_connector_iceberg::commit::{CommitOutcome, WrittenFile};
use novarocks_connector_iceberg::write_codec::decode_write_receipt;

/// Build the physical commit driver for an admitted data write.
///
/// This used to run in `engine::iceberg_writer`, which meant the SQL
/// application layer loaded a concrete Iceberg table, chose the commit
/// operation, sized the staging directory and constructed the abort-cleanup
/// operator. All of it is Iceberg write machinery: it belongs with the legacy
/// implementation that serves the neutral write contract, and it disappears
/// together with this subtree once the provider factory is the sole authority.
///
/// The commit operation is derived from the neutral intent the provider already
/// signed into the preparation, so no caller names `CommitOpKind`.
pub(crate) fn build_admitted_data_write_commit_executor(
    entry: &super::catalog::IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    target_ref: &str,
    intent: novarocks_spi::connector::ConnectorWriteIntent,
    snapshot_properties: BTreeMap<String, String>,
) -> Result<(Arc<IcebergWriteCommitExecutor>, Option<i64>), String> {
    use novarocks_spi::connector::ConnectorWriteIntent;

    let catalog: Arc<dyn novarocks_connector_iceberg::iceberg::Catalog> =
        super::catalog::registry::build_iceberg_catalog(entry)?;
    let table_ident = novarocks_connector_iceberg::iceberg::TableIdent::new(
        novarocks_connector_iceberg::iceberg::NamespaceIdent::new(namespace.to_string()),
        table_name.to_string(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|error| format!("load iceberg table {namespace}.{table_name}: {error}"))?;

    let commit_op_kind = match intent {
        ConnectorWriteIntent::Append => CommitOpKind::FastAppend,
        ConnectorWriteIntent::Overwrite => CommitOpKind::Overwrite,
        ConnectorWriteIntent::PartitionOverwrite => CommitOpKind::OverwritePartitions,
        ConnectorWriteIntent::RowDelta => {
            return Err(
                "row-delta writes use the row-mutation commit driver, not the data write driver"
                    .to_string(),
            );
        }
    };

    let metadata = table.metadata();
    let base_snapshot_id = write_base_snapshot_id(metadata, target_ref)?;
    let collector = Arc::new(
        IcebergCommitCollector::new(
            commit_op_kind,
            table_ident,
            base_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            format!(
                "{}/data/_staging/{}",
                metadata.location(),
                uuid::Uuid::new_v4()
            ),
            novarocks_types::UniqueId::new(0, 0),
        )
        .with_table_metadata(metadata.clone()),
    );

    let abort_cleanup = super::commit::build_abort_cleanup_for_catalog_entry(entry)?;
    let executor = Arc::new(IcebergWriteCommitExecutor {
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties,
    });
    // The base snapshot travels back out because the application layer records
    // it in its own durable operation journal. It is resolved here so the table
    // is loaded exactly once.
    Ok((executor, base_snapshot_id))
}

/// Where a row mutation's commit base comes from.
///
/// The two row-DML entry points differ here, and the difference is preserved
/// verbatim by this relocation rather than harmonised:
///
/// - UPDATE and MERGE pass [`RowMutationCommitBase::Signed`] -- the base version
///   the provider signed at admission, which the SQL layer also records in its
///   durable journal.
/// - ADD EQUALITY DELETE and DELETE pass [`RowMutationCommitBase::RefHead`],
///   which resolves the target ref's head off the freshly loaded table.
///
/// The second form re-derives from a table that may have moved since admission,
/// the same shape of inconsistency that was fixed on the MERGE copy-on-write
/// path. Harmonising it is a behaviour change and is deliberately NOT part of
/// this relocation; see the plan's execution findings.
pub(crate) enum RowMutationCommitBase {
    Signed(Option<i64>),
    RefHead,
}

/// Build the physical commit driver for an admitted row mutation.
///
/// The row-mutation counterpart of
/// [`build_admitted_data_write_commit_executor`], and relocated here for the
/// same reason: loading a table, sizing the staging directory, choosing the
/// commit action and building the abort-cleanup operator is Iceberg write
/// machinery, not SQL application logic.
///
/// The strategy arrives already signed by the provider (SPI-5H), so the caller
/// never chooses the physical route; this function only maps that signed
/// decision onto the commit action that assembles the snapshot.
///
/// Returns the loaded table alongside the driver because the row-delete
/// registration path still needs it to derive its writer handle.
pub(crate) fn build_admitted_row_mutation_commit_executor(
    entry: &super::catalog::IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    target_ref: &str,
    strategy: novarocks_spi::connector::ConnectorRowMutationStrategy,
    base: RowMutationCommitBase,
) -> Result<
    (
        Arc<IcebergWriteCommitExecutor>,
        novarocks_connector_iceberg::iceberg::table::Table,
    ),
    String,
> {
    let catalog = super::catalog::registry::build_iceberg_catalog(entry)?;
    let table_ident = novarocks_connector_iceberg::iceberg::TableIdent::new(
        novarocks_connector_iceberg::iceberg::NamespaceIdent::new(namespace.to_string()),
        table_name.to_string(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|error| format!("load iceberg table {table_ident}: {error}"))?;
    let metadata = table.metadata();
    let base_snapshot_id = match base {
        RowMutationCommitBase::Signed(signed) => signed,
        RowMutationCommitBase::RefHead => {
            if target_ref == "main" {
                metadata.current_snapshot_id()
            } else {
                novarocks_connector_iceberg::ref_snapshot::resolve_branch_head_snapshot_id(
                    metadata, target_ref,
                )?
            }
        }
    };
    let collector = Arc::new(
        IcebergCommitCollector::new(
            row_mutation_commit_op(strategy),
            table_ident,
            base_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            format!(
                "{}/data/_staging/{}",
                metadata.location(),
                uuid::Uuid::new_v4()
            ),
            novarocks_types::UniqueId::new(0, 0),
        )
        .with_table_metadata(metadata.clone()),
    );
    let abort_cleanup = super::commit::build_abort_cleanup_for_catalog_entry(entry)?;
    let executor = Arc::new(IcebergWriteCommitExecutor {
        catalog,
        table: table.clone(),
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    });
    Ok((executor, table))
}

/// Which commit action assembles the snapshot for a signed row-mutation route.
fn row_mutation_commit_op(
    strategy: novarocks_spi::connector::ConnectorRowMutationStrategy,
) -> CommitOpKind {
    use novarocks_spi::connector::ConnectorRowMutationStrategy;

    match strategy {
        // Deletion vectors and merge-on-read both land as a row delta assembled
        // from the touched data files.
        ConnectorRowMutationStrategy::DeletionVector
        | ConnectorRowMutationStrategy::MergeOnRead => CommitOpKind::RowDeltaDvFromFiles,
        // Position deletes and equality deletes differ in the delete file each
        // writer produces, not in how the snapshot is assembled.
        ConnectorRowMutationStrategy::PositionDelete
        | ConnectorRowMutationStrategy::EqualityDelete => CommitOpKind::RowDelta,
        ConnectorRowMutationStrategy::CopyOnWrite => CommitOpKind::CowUpdate,
    }
}

/// The snapshot a write commits against: the current snapshot for `main`, or
/// the named ref's tip. A missing ref is a fault rather than an empty base.
fn write_base_snapshot_id(
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
    target_ref: &str,
) -> Result<Option<i64>, String> {
    if target_ref == "main" {
        return Ok(metadata.current_snapshot().map(|s| s.snapshot_id()));
    }
    metadata
        .refs()
        .get(target_ref)
        .map(|snapshot_ref| Some(snapshot_ref.snapshot_id))
        .ok_or_else(|| format!("iceberg ref: branch '{target_ref}' not found in table metadata"))
}

/// Convert a sealed provider commit decision into the application-neutral
/// durable outcome consumed by the frontend lifecycle runner.
pub(crate) fn commit_iceberg_connector_write(
    commit_executor: &IcebergWriteCommitExecutor,
    completion: &crate::query_execution::ConnectorWriteCompletion,
) -> Result<CommitOutcome, CommitServiceError> {
    match crate::query_execution::connector_write_transaction::commit(completion) {
        Ok(ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization: ExternalMutationFinalization::Complete,
            ..
        }) => decode_write_receipt(receipt.payload())
            .map(|new_snapshot_id| CommitOutcome {
                new_snapshot_id,
                written_manifest_paths: Vec::new(),
            })
            .map_err(CommitServiceError::invalid_input),
        Ok(ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization: ExternalMutationFinalization::Failed(failure),
            ..
        }) => match decode_write_receipt(receipt.payload()) {
            Ok(new_snapshot_id) => Err(CommitServiceError::finalize_failed_known_committed(
                Some(CommitOutcome {
                    new_snapshot_id,
                    written_manifest_paths: Vec::new(),
                }),
                failure.message().to_string(),
                super::commit::RecoveryEvidence::from_collector(&commit_executor.collector),
            )),
            Err(error) => Err(CommitServiceError::invalid_input(error)),
        },
        Ok(ExternalMutationOutcome::KnownUncommitted { failure }) => {
            Err(CommitServiceError::known_uncommitted(
                failure.message().to_string(),
                super::commit::CleanupAttempt::not_attempted(),
            ))
        }
        Ok(ExternalMutationOutcome::CommitUnknown { failure, .. }) => {
            Err(CommitServiceError::unknown(
                failure.message().to_string(),
                super::commit::RecoveryEvidence::from_collector(&commit_executor.collector),
            ))
        }
        Err(error) => Err(CommitServiceError::invalid_input(error.to_string())),
    }
}

/// Make the terminal abort decision for an exact sealed connector operation.
///
/// A staging failure is not proof that the external mutation did not commit:
/// preserve the provider's three-way truth for the frontend-owned lifecycle.
pub(crate) fn abort_iceberg_connector_write(
    commit_executor: &IcebergWriteCommitExecutor,
    session: &crate::query_execution::write_operation::ConnectorWriteOperationSession,
    context: novarocks_spi::connector::ConnectorRequestContext,
    stage_reason: String,
) -> Result<CommitOutcome, CommitServiceError> {
    match session.abort(context) {
        Ok(ConnectorWriteAbortOutcome::KnownUncommitted { cleanup }) => {
            let (cleanup, suffix) = match cleanup {
                ExternalMutationFinalization::Complete => (
                    super::commit::CleanupAttempt::completed(Vec::new()),
                    String::new(),
                ),
                ExternalMutationFinalization::Failed(failure) => (
                    super::commit::CleanupAttempt {
                        attempted: true,
                        error_count: 1,
                        error_paths: Vec::new(),
                    },
                    format!("; connector cleanup failed: {}", failure.message()),
                ),
            };
            Err(CommitServiceError::known_uncommitted(
                format!("{stage_reason}{suffix}"),
                cleanup,
            ))
        }
        Ok(ConnectorWriteAbortOutcome::KnownCommitted {
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        }) => decode_write_receipt(receipt.payload())
            .map(|new_snapshot_id| CommitOutcome {
                new_snapshot_id,
                written_manifest_paths: Vec::new(),
            })
            .map_err(CommitServiceError::invalid_input),
        Ok(ConnectorWriteAbortOutcome::KnownCommitted {
            receipt,
            finalization: ExternalMutationFinalization::Failed(failure),
        }) => match decode_write_receipt(receipt.payload()) {
            Ok(new_snapshot_id) => Err(CommitServiceError::finalize_failed_known_committed(
                Some(CommitOutcome {
                    new_snapshot_id,
                    written_manifest_paths: Vec::new(),
                }),
                failure.message().to_string(),
                super::commit::RecoveryEvidence::from_collector(&commit_executor.collector),
            )),
            Err(error) => Err(CommitServiceError::invalid_input(error)),
        },
        Ok(ConnectorWriteAbortOutcome::CommitUnknown { failure, .. }) => {
            Err(CommitServiceError::unknown(
                format!(
                    "{stage_reason}; connector abort outcome is unknown: {}",
                    failure.message()
                ),
                super::commit::RecoveryEvidence::from_collector(&commit_executor.collector),
            ))
        }
        Err(error) => Err(CommitServiceError::unknown(
            format!("{stage_reason}; connector abort RPC failed: {error}"),
            super::commit::RecoveryEvidence::from_collector(&commit_executor.collector),
        )),
    }
}

/// Provider-private commit context for one coordinated Iceberg writer.
///
/// Application finalization, including query-cache invalidation, intentionally
/// remains outside this type. Callers supply only reports accepted by the
/// sealed generic write operation.
pub(crate) struct IcebergWriteCommitExecutor {
    pub(crate) catalog: Arc<dyn novarocks_connector_iceberg::iceberg::Catalog>,
    pub(crate) table: novarocks_connector_iceberg::iceberg::table::Table,
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
            let reports = novarocks_connector_iceberg::write_codec::decode_writer_reports(
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

#[cfg(test)]
mod tests {
    use super::write_base_snapshot_id;
    use novarocks_connector_iceberg::iceberg::spec::{SnapshotReference, SnapshotRetention};

    /// Moved here with `write_base_snapshot_id` itself: the SQL write path used
    /// to own this helper and this assertion, but the base a write commits
    /// against is decided by the commit driver, so the test follows the code it
    /// pins rather than staying next to a copy nothing calls.
    #[test]
    fn branch_write_uses_the_branch_head_as_its_base_snapshot() {
        let metadata = crate::connector::iceberg::test_metadata::metadata_with_two_snapshots()
            .into_builder(None)
            .set_ref(
                "dev",
                SnapshotReference::new(1, SnapshotRetention::branch(None, None, None)),
            )
            .expect("add dev branch")
            .build()
            .expect("build metadata with dev branch")
            .metadata;

        assert_eq!(write_base_snapshot_id(&metadata, "main").unwrap(), Some(2));
        assert_eq!(write_base_snapshot_id(&metadata, "dev").unwrap(), Some(1));
    }
}
