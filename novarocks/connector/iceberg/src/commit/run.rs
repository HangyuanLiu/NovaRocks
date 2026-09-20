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

//! Engine-layer orchestrator that owns the IcebergCommitCollector lifecycle:
//! pick the right commit-action based on `CommitOpKind`, dispatch it, and on
//! failure decide whether to clean staged files or leave them for human
//! review (spec §5.4 — "commit unknown").
//!
//! Commit failure classification is delegated to `service.rs`, which exposes
//! typed lifecycle errors while preserving cleanup behavior for known
//! uncommitted failures.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::iceberg::Catalog;
use crate::iceberg::io::FileIO;
use crate::iceberg::table::Table;
use crate::iceberg::{TableCommit, TableUpdate};
use crate::opendal::Operator;
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction};
use super::collector::IcebergCommitCollector;
use super::fast_append::FastAppendCommit;
use super::overwrite::OverwriteCommit;
use super::rewrite_data_files::RewriteDataFilesCommit;
use super::row_delta::RowDeltaCommit;
use super::row_delta_dv::RowDeltaDvCommit;
use super::row_delta_dv_from_files::RowDeltaDvFromFilesCommit;
use super::selected_rewrite::SelectedRewriteCommit;
use super::service::{
    CleanupAttempt, CommitFailureKind, CommitServiceError, RecoveryEvidence, classify_commit_error,
};
use super::truncate::TruncateCommit;
use super::update_cow::CowUpdateCommit;
use super::update_cow::CowUpdateRewriteSet;
use crate::commit::{CommitOpKind, CommitOutcome};

pub type CleanupPathMapper = Arc<dyn Fn(&str) -> String + Send + Sync>;

pub struct RunInput {
    pub collector: Arc<IcebergCommitCollector>,
    pub catalog: Arc<dyn Catalog>,
    pub table: Table,
    pub fs: Operator,
    pub file_io: FileIO,
    pub cleanup_path_mapper: Option<CleanupPathMapper>,
    pub cow_update_rewrite: Option<CowUpdateRewriteSet>,
    pub selected_rewrite: Option<super::selected_rewrite::SelectedRewriteFiles>,
    /// Iceberg ref to commit to. `"main"` is the default; branch-qualified
    /// DML (`INSERT INTO t.branch_dev`) supplies the branch name here.
    pub target_ref: String,
    pub snapshot_properties: BTreeMap<String, String>,
    /// Provider-assigned partition-spec updates that must share the exact
    /// external commit with one managed overwrite snapshot on `main`.
    pub atomic_partition_replacement: Option<AtomicPartitionReplacement>,
}

pub(crate) struct AtomicPartitionReplacement {
    updates: Vec<TableUpdate>,
}

impl AtomicPartitionReplacement {
    pub(super) fn try_new(updates: Vec<TableUpdate>) -> Result<Self, String> {
        if !(updates.len() == 2 || updates.len() == 3)
            || !matches!(updates[0], TableUpdate::AddSpec { .. })
            || !matches!(updates[1], TableUpdate::SetDefaultSpec { .. })
            || updates
                .get(2)
                .is_some_and(|update| !matches!(update, TableUpdate::SetProperties { .. }))
        {
            return Err(
                "atomic Iceberg partition replacement requires AddSpec, SetDefaultSpec, and at most one SetProperties"
                    .to_string(),
            );
        }
        Ok(Self { updates })
    }
}

/// Dispatch a commit-action and return typed commit outcome/error.
///
/// On definite commit failure this function runs best-effort abort cleanup and
/// returns `KnownUncommitted`. On commit-unknown failure it leaves staged files
/// untouched and returns `Unknown` with recovery evidence.
pub async fn run_iceberg_commit(input: RunInput) -> Result<CommitOutcome, CommitServiceError> {
    let RunInput {
        collector,
        catalog,
        table,
        fs,
        file_io,
        cleanup_path_mapper,
        cow_update_rewrite,
        selected_rewrite,
        target_ref,
        snapshot_properties,
        atomic_partition_replacement,
    } = input;

    if let Some(replacement) = atomic_partition_replacement {
        if collector.op_kind != CommitOpKind::Overwrite || target_ref != "main" {
            return Err(CommitServiceError::invalid_input(
                "atomic Iceberg partition replacement requires one managed overwrite on main"
                    .to_string(),
            ));
        }
        let commit_uuid = Uuid::new_v4();
        collector.set_manifest_cleanup_token(commit_uuid.to_string());
        let ctx = CommitCtx {
            collector: &collector,
            table: &table,
            catalog: catalog.as_ref(),
            file_io: &file_io,
            commit_uuid,
            abort_handle: collector.abort_log.clone(),
            target_ref: &target_ref,
            snapshot_properties: &snapshot_properties,
        };
        let result = run_atomic_partition_replacement(ctx, replacement).await;
        return match result {
            Ok(outcome) => {
                collector.mark_committed();
                Ok(outcome)
            }
            Err(error) => {
                Err(handle_commit_error(error, &collector, &fs, cleanup_path_mapper.as_ref()).await)
            }
        };
    }

    let action: Box<dyn IcebergCommitAction> = match collector.op_kind {
        CommitOpKind::FastAppend => Box::new(FastAppendCommit),
        CommitOpKind::Overwrite => Box::new(OverwriteCommit),
        CommitOpKind::RowDelta => Box::new(RowDeltaCommit),
        CommitOpKind::RowDeltaDv => Box::new(RowDeltaDvCommit),
        CommitOpKind::RowDeltaDvFromFiles => Box::new(RowDeltaDvFromFilesCommit),
        CommitOpKind::RewriteDataFiles => Box::new(RewriteDataFilesCommit),
        CommitOpKind::SelectedRewrite => Box::new(SelectedRewriteCommit {
            files: selected_rewrite.ok_or_else(|| {
                CommitServiceError::invalid_input(
                    "selected rewrite commit requires its frozen file set".to_string(),
                )
            })?,
        }),
        CommitOpKind::CowUpdate => Box::new(CowUpdateCommit {
            rewrite: cow_update_rewrite.ok_or_else(|| {
                CommitServiceError::invalid_input(
                    "CowUpdate commit requires a rewrite set".to_string(),
                )
            })?,
        }),
        CommitOpKind::Truncate => Box::new(TruncateCommit),
        CommitOpKind::OverwritePartitions => {
            Box::new(super::overwrite_partitions::OverwritePartitionsCommit)
        }
        CommitOpKind::RewriteManifests => {
            return Err(CommitServiceError::invalid_input(
                "CommitOpKind::RewriteManifests must be invoked via run_rewrite_manifests directly, not the collector dispatcher".to_string(),
            ));
        }
    };

    let commit_uuid = Uuid::new_v4();
    collector.set_manifest_cleanup_token(commit_uuid.to_string());
    let ctx = CommitCtx {
        collector: &collector,
        table: &table,
        catalog: catalog.as_ref(),
        file_io: &file_io,
        commit_uuid,
        abort_handle: collector.abort_log.clone(),
        target_ref: &target_ref,
        snapshot_properties: &snapshot_properties,
    };

    match action.commit(ctx).await {
        Ok(outcome) => {
            collector.mark_committed();
            Ok(outcome)
        }
        Err(commit_err) => {
            Err(
                handle_commit_error(commit_err, &collector, &fs, cleanup_path_mapper.as_ref())
                    .await,
            )
        }
    }
}

async fn run_atomic_partition_replacement(
    ctx: CommitCtx<'_>,
    replacement: AtomicPartitionReplacement,
) -> Result<CommitOutcome, String> {
    // The atomic repartition path assembles its own `TableCommit` because the
    // partition-spec updates must precede the snapshot updates in one commit.
    // The staged action retains the exact target-ref requirement for its base.
    let mut staged = super::overwrite::build_staged_overwrite_action(ctx).await?;
    let snapshot_updates = staged.action.take_updates();
    if snapshot_updates.len() != 2
        || !matches!(snapshot_updates[0], TableUpdate::AddSnapshot { .. })
        || !matches!(snapshot_updates[1], TableUpdate::SetSnapshotRef { .. })
    {
        return Err(
            "atomic Iceberg repartition overwrite did not produce AddSnapshot then SetSnapshotRef"
                .to_string(),
        );
    }
    let mut updates = replacement.updates;
    updates.extend(snapshot_updates);
    let requirements = staged.action.take_requirements();
    let commit = TableCommit::builder()
        .ident(staged.table_ident.clone())
        .requirements(requirements)
        .updates(updates)
        .build();
    staged
        .catalog
        .update_table(commit)
        .await
        .map_err(|error| format!("atomic Iceberg repartition commit failed: {error}"))?;
    Ok(staged.outcome)
}

async fn handle_commit_error(
    commit_err: String,
    collector: &Arc<IcebergCommitCollector>,
    fs: &Operator,
    cleanup_path_mapper: Option<&CleanupPathMapper>,
) -> CommitServiceError {
    // The marker is an internal signal between the code that knows the verdict
    // and the classifier. It has no business in a message a user reads.
    let kind = classify_commit_error(&commit_err);
    let commit_err = commit_err
        .replacen(
            &format!("{} ", crate::commit::service::PROVEN_UNCOMMITTED_MARKER),
            "",
            1,
        )
        .replacen(crate::commit::service::PROVEN_UNCOMMITTED_MARKER, "", 1);
    match kind {
        CommitFailureKind::Unknown => {
            let evidence = RecoveryEvidence::from_collector(collector);
            tracing::warn!(
                op_kind = ?collector.op_kind,
                table = %collector.table_ident,
                base_snapshot_id = ?collector.base_snapshot_id,
                staging_dir = collector.staging_dir,
                "iceberg commit unknown — leaving all staged files for manual review: {commit_err}"
            );
            CommitServiceError::unknown(commit_err, evidence)
        }
        CommitFailureKind::FinalizeFailedKnownCommitted => {
            collector.mark_committed();
            CommitServiceError::finalize_failed_known_committed(
                None,
                commit_err,
                RecoveryEvidence::from_collector(collector),
            )
        }
        CommitFailureKind::KnownUncommitted => {
            let cleanup_errors = if let Some(mapper) = cleanup_path_mapper {
                collector
                    .abort_log
                    .cleanup_with_path_mapper(fs, |path| mapper(path))
                    .await
            } else {
                collector.abort_log.cleanup(fs).await
            };
            for e in &cleanup_errors {
                tracing::warn!(path = %e.path, source = ?e.source, "abort cleanup error");
            }
            CommitServiceError::known_uncommitted(
                commit_err,
                CleanupAttempt::from_cleanup_errors(&cleanup_errors),
            )
        }
    }
}

#[cfg(test)]
mod application_document_publication_trace_tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field};
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use novarocks_spi::connector::write_stack::session::{
        ConnectorWriteBeginRequest, ConnectorWriteControl, ConnectorWriteFinishPublication,
        ConnectorWriteFinishRequest, ConnectorWriteSessionFlavor,
        ConnectorWriteSessionReconcileRequest,
    };
    use novarocks_spi::connector::write_stack::{
        ConnectorManagedPublicationShape, ConnectorPreparedWriteSet,
    };
    use novarocks_spi::connector::{
        CatalogProperties, ConnectorCancellation, ConnectorControlBinding,
        ConnectorControlPlanningLease, ConnectorDocument, ConnectorDocumentAttachment,
        ConnectorDocumentFormat, ConnectorDocumentId, ConnectorDocumentManagementAdmissionRequest,
        ConnectorDocumentManagementOperation, ConnectorDocumentName, ConnectorDocumentOwner,
        ConnectorDocumentPublicationDeclaration, ConnectorDocumentPublicationIntent,
        ConnectorDocumentReference, ConnectorDocumentRevision, ConnectorDocumentSet,
        ConnectorDocumentStorageBinding, ConnectorDocumentStorageManagement,
        ConnectorManagedPartitionField, ConnectorManagedPartitionSpecObservation,
        ConnectorManagedPartitionSpecReplacement, ConnectorManagedPartitionTransform,
        ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationTechnique,
        ConnectorMutationOperationId, ConnectorPrepareDocumentsRequest,
        ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorTableIdentity,
        ConnectorTableObjectId, ConnectorWriteAdmissionPurpose, ConnectorWriteBaseVersion,
        ConnectorWriteFieldRequest, ConnectorWriteInputRequest, ConnectorWriteIntent,
        ConnectorWriteOperationId, ConnectorWriteTargetRef, ExternalMutationFinalization,
        ExternalMutationOutcome, LakePublicationId, MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
    };

    use super::*;
    use crate::catalog::CatalogTableName;
    use crate::catalog::error::{CatalogCommitEvidence, CatalogOutcome};
    use crate::catalog::transaction::{
        CatalogCommitDispatch, CommitProof, TransactionIdentity, TransactionShape,
    };
    use crate::commit::action::IcebergCommitAction;
    use crate::commit::collector::IcebergCommitCollector;
    use crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_MARKER_PROPERTY;
    use crate::document_storage::envelope::{
        DOCUMENT_ENVELOPE_VERSION, DOCUMENT_MANIFEST_VERSION, IcebergDocumentAttachmentV1,
        IcebergDocumentCarrierV1, IcebergDocumentEnvelopeV1, IcebergDocumentManifestV1,
    };
    use crate::document_storage::publication::PENDING_DOCUMENT_MANIFEST_PROPERTY;
    use crate::iceberg::spec::{
        DataContentType, DataFileFormat, FormatVersion, NestedField, PartitionSpec, PrimitiveType,
        Schema, Struct, Type,
    };
    use crate::iceberg::{
        Catalog, Namespace, NamespaceIdent, TableCreation, TableIdent, TableRequirement,
        TableUpdate,
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedCommit {
        requirement_kinds: Vec<&'static str>,
        uuid_requirements: Vec<Uuid>,
        ref_requirements: Vec<(String, Option<i64>)>,
        update_kinds: Vec<&'static str>,
        ref_updates: Vec<(String, i64)>,
    }

    #[derive(Debug)]
    struct RecordingCatalog {
        inner: Arc<dyn Catalog>,
        commits: Mutex<Vec<RecordedCommit>>,
    }

    impl RecordingCatalog {
        fn new(inner: Arc<dyn Catalog>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                commits: Mutex::new(Vec::new()),
            })
        }

        fn commits(&self) -> Vec<RecordedCommit> {
            self.commits.lock().expect("recording catalog lock").clone()
        }

        fn clear(&self) {
            self.commits.lock().expect("recording catalog lock").clear();
        }
    }

    #[async_trait]
    impl Catalog for RecordingCatalog {
        async fn list_namespaces(
            &self,
            parent: Option<&NamespaceIdent>,
        ) -> crate::iceberg::Result<Vec<NamespaceIdent>> {
            self.inner.list_namespaces(parent).await
        }

        async fn create_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> crate::iceberg::Result<Namespace> {
            self.inner.create_namespace(namespace, properties).await
        }

        async fn get_namespace(
            &self,
            namespace: &NamespaceIdent,
        ) -> crate::iceberg::Result<Namespace> {
            self.inner.get_namespace(namespace).await
        }

        async fn namespace_exists(
            &self,
            namespace: &NamespaceIdent,
        ) -> crate::iceberg::Result<bool> {
            self.inner.namespace_exists(namespace).await
        }

        async fn update_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> crate::iceberg::Result<()> {
            self.inner.update_namespace(namespace, properties).await
        }

        async fn drop_namespace(&self, namespace: &NamespaceIdent) -> crate::iceberg::Result<()> {
            self.inner.drop_namespace(namespace).await
        }

        async fn list_tables(
            &self,
            namespace: &NamespaceIdent,
        ) -> crate::iceberg::Result<Vec<TableIdent>> {
            self.inner.list_tables(namespace).await
        }

        async fn create_table(
            &self,
            namespace: &NamespaceIdent,
            creation: TableCreation,
        ) -> crate::iceberg::Result<Table> {
            self.inner.create_table(namespace, creation).await
        }

        async fn load_table(&self, table: &TableIdent) -> crate::iceberg::Result<Table> {
            self.inner.load_table(table).await
        }

        async fn drop_table(&self, table: &TableIdent) -> crate::iceberg::Result<()> {
            self.inner.drop_table(table).await
        }

        async fn table_exists(&self, table: &TableIdent) -> crate::iceberg::Result<bool> {
            self.inner.table_exists(table).await
        }

        async fn rename_table(
            &self,
            src: &TableIdent,
            dest: &TableIdent,
        ) -> crate::iceberg::Result<()> {
            self.inner.rename_table(src, dest).await
        }

        async fn register_table(
            &self,
            table: &TableIdent,
            metadata_location: String,
        ) -> crate::iceberg::Result<Table> {
            self.inner.register_table(table, metadata_location).await
        }

        async fn update_table(
            &self,
            mut commit: crate::iceberg::TableCommit,
        ) -> crate::iceberg::Result<Table> {
            let ident = commit.identifier().clone();
            let requirements = commit.take_requirements();
            let updates = commit.take_updates();
            let recorded = RecordedCommit {
                requirement_kinds: requirements.iter().map(requirement_kind).collect(),
                uuid_requirements: requirements
                    .iter()
                    .filter_map(|requirement| match requirement {
                        TableRequirement::UuidMatch { uuid } => Some(*uuid),
                        _ => None,
                    })
                    .collect(),
                ref_requirements: requirements
                    .iter()
                    .filter_map(|requirement| match requirement {
                        TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } => {
                            Some((r#ref.clone(), *snapshot_id))
                        }
                        _ => None,
                    })
                    .collect(),
                update_kinds: updates.iter().map(update_kind).collect(),
                ref_updates: updates
                    .iter()
                    .filter_map(|update| match update {
                        TableUpdate::SetSnapshotRef {
                            ref_name,
                            reference,
                        } => Some((ref_name.clone(), reference.snapshot_id)),
                        _ => None,
                    })
                    .collect(),
            };
            self.commits
                .lock()
                .expect("recording catalog lock")
                .push(recorded);
            self.inner
                .update_table(
                    crate::iceberg::TableCommit::builder()
                        .ident(ident)
                        .requirements(requirements)
                        .updates(updates)
                        .build(),
                )
                .await
        }

        async fn create_view(
            &self,
            namespace: &NamespaceIdent,
            creation: crate::iceberg::ViewCreation,
        ) -> crate::iceberg::Result<crate::iceberg::spec::ViewMetadata> {
            self.inner.create_view(namespace, creation).await
        }

        async fn load_view(
            &self,
            view: &TableIdent,
        ) -> crate::iceberg::Result<crate::iceberg::spec::ViewMetadata> {
            self.inner.load_view(view).await
        }

        async fn update_view(
            &self,
            commit: crate::iceberg::ViewCommit,
        ) -> crate::iceberg::Result<crate::iceberg::spec::ViewMetadata> {
            self.inner.update_view(commit).await
        }

        async fn drop_view(&self, view: &TableIdent) -> crate::iceberg::Result<()> {
            self.inner.drop_view(view).await
        }

        async fn view_exists(&self, view: &TableIdent) -> crate::iceberg::Result<bool> {
            self.inner.view_exists(view).await
        }

        async fn list_views(
            &self,
            namespace: &NamespaceIdent,
        ) -> crate::iceberg::Result<Vec<TableIdent>> {
            self.inner.list_views(namespace).await
        }
    }

    fn requirement_kind(requirement: &TableRequirement) -> &'static str {
        match requirement {
            TableRequirement::NotExist => "assert-create",
            TableRequirement::UuidMatch { .. } => "assert-table-uuid",
            TableRequirement::RefSnapshotIdMatch { .. } => "assert-ref-snapshot-id",
            TableRequirement::LastAssignedFieldIdMatch { .. } => "assert-last-field-id",
            TableRequirement::CurrentSchemaIdMatch { .. } => "assert-current-schema-id",
            TableRequirement::LastAssignedPartitionIdMatch { .. } => "assert-last-partition-id",
            TableRequirement::DefaultSpecIdMatch { .. } => "assert-default-spec-id",
            TableRequirement::DefaultSortOrderIdMatch { .. } => "assert-default-sort-order-id",
        }
    }

    fn update_kind(update: &TableUpdate) -> &'static str {
        match update {
            TableUpdate::AddSpec { .. } => "add-spec",
            TableUpdate::SetDefaultSpec { .. } => "set-default-spec",
            TableUpdate::AddSnapshot { .. } => "add-snapshot",
            TableUpdate::SetSnapshotRef { .. } => "set-snapshot-ref",
            TableUpdate::SetProperties { .. } => "set-properties",
            TableUpdate::SetStatistics { .. } => "set-statistics",
            _ => "other",
        }
    }

    struct Fixture {
        catalog: Arc<RecordingCatalog>,
        table: Table,
        provider: crate::metadata::IcebergMetadata,
        catalog_handle: novarocks_spi::connector::CatalogHandle,
        _warehouse: tempfile::TempDir,
    }

    /// Hadoop intentionally does not grant document-management admission in
    /// production. This test capability replaces only the admission decision;
    /// the real Iceberg document preparation still produces the carrier that
    /// the real write session binds and commits.
    #[derive(Clone)]
    struct HadoopDocumentTestCapability {
        storage: Arc<crate::document_storage::IcebergDocumentStorage>,
    }

    impl ConnectorDocumentStorageManagement for HadoopDocumentTestCapability {
        fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
            self.storage.descriptor()
        }

        fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
            self.storage.incarnation()
        }

        fn admit_management(
            &self,
            request: ConnectorDocumentManagementAdmissionRequest,
        ) -> Result<Bytes, novarocks_spi::connector::ConnectorError> {
            let operation = match request.operation() {
                ConnectorDocumentManagementOperation::Create => "create",
                ConnectorDocumentManagementOperation::SingleTargetUpdate => "single-target-update",
                ConnectorDocumentManagementOperation::Publication => "publication",
            };
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "operation": operation,
                "operation_id": request.operation_id().to_bytes(),
                "namespace": request.target().namespace.as_ref(),
                "table": request.target().table.as_ref(),
                "expected_object_id": request
                    .expected_object_id()
                    .map(|object| object.as_bytes().to_vec()),
            }))
            .map(Bytes::from)
            .map_err(|error| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    format!("encode test document admission: {error}"),
                )
            })
        }

        fn prepare_documents(
            &self,
            request: ConnectorPrepareDocumentsRequest,
        ) -> Result<Bytes, novarocks_spi::connector::ConnectorError> {
            ConnectorDocumentStorageManagement::prepare_documents(self.storage.as_ref(), request)
        }
    }

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            2 * 1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("request context")
    }

    async fn fixture() -> Fixture {
        fixture_with_format(FormatVersion::V2, false).await
    }

    async fn row_lineage_fixture() -> Fixture {
        fixture_with_format(FormatVersion::V3, true).await
    }

    async fn fixture_with_format(format_version: FormatVersion, row_lineage: bool) -> Fixture {
        let warehouse = tempfile::tempdir().expect("warehouse tempdir");
        let warehouse_uri = format!("file://{}", warehouse.path().join("warehouse").display());
        let binding = {
            let runtime = tokio::runtime::Handle::current();
            crate::access_binding::IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
                Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime)),
            )
        };
        let concrete = Arc::new(
            crate::hadoop_catalog::HadoopFileSystemCatalog::new_with_binding(
                crate::fs_io::build_file_io_for_location(&warehouse_uri, binding.clone()),
                warehouse_uri.clone(),
                binding.clone(),
            ),
        );
        let inner: Arc<dyn Catalog> = concrete.clone();
        let namespace = NamespaceIdent::new("db".to_string());
        inner
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let table = inner
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .properties(HashMap::from([
                        (
                            crate::document_storage::observation::MANAGED_KIND_PROPERTY.to_string(),
                            "mv".to_string(),
                        ),
                        (
                            crate::document_storage::observation::MANAGED_OWNER_PROPERTY
                                .to_string(),
                            "deployment".to_string(),
                        ),
                        (
                            crate::document_storage::observation::MANAGED_INCARNATION_PROPERTY
                                .to_string(),
                            "writer".to_string(),
                        ),
                        (
                            crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY.to_string(),
                            "false".to_string(),
                        ),
                        ("write.row-lineage".to_string(), row_lineage.to_string()),
                    ]))
                    .format_version(format_version)
                    .build(),
            )
            .await
            .expect("create table");
        let catalog = RecordingCatalog::new(inner);
        let descriptor = novarocks_spi::connector::ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("provider id"),
            instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice")
                .expect("instance id"),
        };
        let incarnation = novarocks_spi::connector::ProviderBindingEpoch::from_bytes([6; 16]);
        let catalog_handle = novarocks_spi::connector::CatalogHandle::new(
            descriptor.instance_id.clone(),
            novarocks_spi::connector::CatalogVersion::from_bytes([17; 32]),
        );
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("catalog configuration");
        let resources = crate::resources::IcebergMetadataResources::new(
            binding,
            tokio::runtime::Handle::current(),
        );
        // Eager finish obtains a fresh provider-private transaction. Its
        // dispatch must share this same recording vendored client; wrapping
        // only the legacy `vendored_client()` getter would miss that mutation.
        let owner =
            crate::catalog::factory::NovaRocksCatalogFactory::adopt_recording_hadoop_for_test(
                concrete,
                catalog.clone(),
            );
        let runtime = Arc::new(
            crate::metadata_context::IcebergMetadataContext::with_catalog_for_test(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                resources,
                owner,
            ),
        );
        let provider = crate::metadata::IcebergMetadata::new(descriptor, incarnation, runtime);
        Fixture {
            catalog,
            table,
            provider,
            catalog_handle,
            _warehouse: warehouse,
        }
    }

    fn document_storage_lease(
        fixture: &Fixture,
    ) -> novarocks_spi::connector::ConnectorDocumentStorageLease {
        let descriptor = fixture.provider.descriptor().clone();
        let incarnation = fixture.provider.incarnation();
        let storage = Arc::new(crate::document_storage::IcebergDocumentStorage::new(
            descriptor.clone(),
            incarnation,
            Arc::clone(fixture.provider.runtime()),
        ));
        let document_storage = ConnectorDocumentStorageBinding::try_new(
            descriptor.clone(),
            incarnation,
            Some(storage.clone()),
            Some(Arc::new(HadoopDocumentTestCapability { storage })),
        )
        .expect("document storage binding");
        let capability = Arc::new(fixture.provider.clone());
        let distribution = Arc::new(crate::provider_binding::IcebergInstanceDistribution::new(
            descriptor.clone(),
            incarnation,
        ));
        let binding = ConnectorControlBinding::try_new(
            descriptor.clone(),
            incarnation,
            capability.clone(),
            capability.clone(),
            distribution,
            Some(capability),
        )
        .and_then(|binding| {
            binding.with_catalog_properties(
                CatalogProperties::new(
                    fixture.catalog_handle.clone(),
                    descriptor.provider_id.clone(),
                    1,
                    Vec::new(),
                    Vec::new(),
                )
                .expect("catalog properties"),
            )
        })
        .and_then(|binding| binding.try_with_document_storage(Some(document_storage)))
        .expect("control binding");
        ConnectorControlPlanningLease::new(Arc::new(binding), || {})
            .derive_document_storage_lease()
            .expect("document storage lease")
    }

    struct PreparedPublication {
        declaration: ConnectorDocumentPublicationDeclaration,
        intent: ConnectorDocumentPublicationIntent,
        expected_manifest: Vec<u8>,
        base: ConnectorWriteBaseVersion,
        base_snapshot_id: Option<i64>,
    }

    fn prepare_publication(
        fixture: &Fixture,
        technique: ConnectorManagedPublicationTechnique,
        shape: ConnectorManagedPublicationShape,
        repartition: bool,
        content: Bytes,
        document_count: usize,
        reference_count: usize,
    ) -> PreparedPublication {
        let loaded = fixture
            .provider
            .runtime()
            .load_table("db", "t")
            .expect("load publication target");
        let metadata = loaded.table.metadata();
        let base_snapshot_id = metadata.current_snapshot_id();
        let table_uuid = metadata.uuid().to_string();
        let object_id =
            ConnectorTableObjectId::try_new(Bytes::copy_from_slice(table_uuid.as_bytes()))
                .expect("table object identity");
        let target = ConnectorTableIdentity {
            instance_id: fixture.provider.descriptor().instance_id.clone(),
            namespace: "db".into(),
            table: "t".into(),
        };
        let owner = ConnectorProviderBindingKey {
            instance_id: fixture.provider.descriptor().instance_id.clone(),
            incarnation: fixture.provider.incarnation(),
        };
        let publication_id = LakePublicationId::new_v7();
        let operation_id = ConnectorMutationOperationId::from_bytes(publication_id.to_bytes());
        let (partition_spec_replacement, expected_committed_partitioning) = if repartition {
            let prior = ConnectorManagedPartitionSpecObservation::try_from_fields(
                metadata.default_partition_spec_id(),
                &[],
            )
            .expect("prior unpartitioned observation");
            let replacement = ConnectorManagedPartitionSpecReplacement::try_new(
                ConnectorWriteOperationId::from_bytes(publication_id.to_bytes()),
                prior,
                vec![
                    ConnectorManagedPartitionField::try_new(
                        1,
                        0,
                        ConnectorManagedPartitionTransform::Identity,
                    )
                    .expect("identity partition field"),
                ],
            )
            .expect("partition replacement");
            let expected = crate::commit::write_stack::repartition::preview_managed_repartition(
                metadata,
                &replacement,
            )
            .expect("preview managed repartition")
            .committed()
            .clone();
            (Some(replacement), Some(expected))
        } else {
            (None, None)
        };
        let lease = document_storage_lease(fixture);
        let admission = lease
            .admit_management(
                ConnectorDocumentManagementAdmissionRequest::try_new(
                    owner,
                    fixture.catalog_handle.clone(),
                    operation_id,
                    target,
                    Some(object_id.clone()),
                    ConnectorDocumentManagementOperation::Publication,
                    context(),
                )
                .expect("publication admission request"),
            )
            .expect("publication admission");
        let documents = ConnectorDocumentSet::try_new(
            (0..document_count)
                .map(|index| {
                    let references = if index == 0 {
                        (0..reference_count)
                            .map(|reference| {
                                ConnectorDocumentReference::try_new(
                                    format!("uses-{}-{reference:04}", "r".repeat(96)),
                                    ConnectorDocumentId::new(
                                        ConnectorDocumentOwner::parse("novarocks.dependency")
                                            .expect("reference owner"),
                                        ConnectorDocumentName::parse(format!(
                                            "dependency-{}-{reference:04}",
                                            "n".repeat(90)
                                        ))
                                        .expect("reference name"),
                                        ConnectorDocumentRevision::from_bytes(
                                            [reference as u8; 32],
                                        ),
                                    ),
                                )
                                .expect("document reference")
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                    ConnectorDocument::try_new(
                        ConnectorDocumentOwner::parse("novarocks.mv").expect("document owner"),
                        ConnectorDocumentName::parse(format!("publication-{index}"))
                            .expect("document name"),
                        ConnectorDocumentFormat::try_new("novarocks.mv", "publication", 1)
                            .expect("document format"),
                        content.clone(),
                        references,
                        ConnectorDocumentAttachment::CommitOutput,
                    )
                    .expect("publication document")
                })
                .collect(),
        )
        .expect("publication document set");
        let prepared = lease
            .prepare_documents(
                ConnectorPrepareDocumentsRequest::try_new(admission.clone(), documents, context())
                    .expect("document preparation request"),
            )
            .expect("prepare publication documents");
        let expected_manifest = prepared.provider_token().to_vec();
        let base = ConnectorWriteBaseVersion::try_new(Bytes::from(format!(
            "iceberg/write-base/v1/{table_uuid}/main/{}",
            crate::commit::write_shared::snapshot_token(base_snapshot_id)
        )))
        .expect("publication base");
        let declaration = ConnectorDocumentPublicationDeclaration::try_new(
            publication_id,
            admission,
            object_id,
            base.clone(),
            technique,
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            partition_spec_replacement,
            expected_committed_partitioning,
        )
        .expect("publication declaration");
        let intent = ConnectorDocumentPublicationIntent::try_new(&declaration, prepared)
            .expect("publication intent");
        PreparedPublication {
            declaration,
            intent,
            expected_manifest,
            base,
            base_snapshot_id,
        }
    }

    fn data_input() -> ConnectorWriteInputRequest {
        ConnectorWriteInputRequest::Data {
            fields: vec![ConnectorWriteFieldRequest::new(Field::new(
                "id",
                DataType::Int64,
                false,
            ))],
        }
    }

    fn row_mutation_input() -> ConnectorWriteInputRequest {
        ConnectorWriteInputRequest::RowLineage {
            data_fields: vec![ConnectorWriteFieldRequest::new(Field::new(
                "id",
                DataType::Int64,
                false,
            ))],
            row_identity_fields: vec![
                ConnectorWriteFieldRequest::new(Field::new("_file", DataType::Utf8, false)),
                ConnectorWriteFieldRequest::new(Field::new("_pos", DataType::Int64, false)),
            ],
        }
    }

    fn begin_request(
        prepared: &PreparedPublication,
        shape: ConnectorManagedPublicationShape,
    ) -> ConnectorWriteBeginRequest {
        ConnectorWriteBeginRequest {
            table: Arc::from("db.t"),
            target_ref: ConnectorWriteTargetRef::main(),
            intent: match (prepared.declaration.technique(), shape) {
                (ConnectorManagedPublicationTechnique::Full, _) => ConnectorWriteIntent::Overwrite,
                (
                    ConnectorManagedPublicationTechnique::Incremental,
                    ConnectorManagedPublicationShape::RowMutation,
                ) => ConnectorWriteIntent::RowDelta,
                (ConnectorManagedPublicationTechnique::Incremental, _)
                | (ConnectorManagedPublicationTechnique::MetadataOnly, _) => {
                    ConnectorWriteIntent::Append
                }
            },
            purpose: ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            input: match shape {
                ConnectorManagedPublicationShape::RowMutation => row_mutation_input(),
                ConnectorManagedPublicationShape::Data
                | ConnectorManagedPublicationShape::InsertOnlyChangeStream => data_input(),
            },
            base: Some(prepared.base.clone()),
            flavor: ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
                declaration: prepared.declaration.clone(),
                shape,
            },
            context: context(),
        }
    }

    async fn seed_empty_snapshot(fixture: &Fixture) {
        let collector = collector(
            &fixture.table,
            CommitOpKind::FastAppend,
            fixture.table.metadata().default_partition_spec().clone(),
            Vec::new(),
        );
        FastAppendCommit
            .commit(CommitCtx {
                collector: &collector,
                table: &fixture.table,
                catalog: fixture.catalog.as_ref(),
                file_io: fixture.table.file_io(),
                commit_uuid: Uuid::now_v7(),
                abort_handle: Arc::clone(&collector.abort_log),
                target_ref: "main",
                snapshot_properties: &BTreeMap::from([(
                    ICEBERG_WRITE_SESSION_MARKER_PROPERTY.to_string(),
                    "row-mutation-base".to_string(),
                )]),
            })
            .await
            .expect("seed row-mutation base snapshot");
        fixture.catalog.clear();
        fixture
            .provider
            .runtime()
            .control_state()
            .invalidate_table_cache("db", "t");
    }

    fn prepared_snapshot_properties(label: &str) -> (BTreeMap<String, String>, Vec<u8>) {
        let content = label.as_bytes().to_vec();
        let manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![IcebergDocumentEnvelopeV1 {
                version: DOCUMENT_ENVELOPE_VERSION,
                owner: "novarocks.mv".to_string(),
                name: "publication".to_string(),
                format_owner: "novarocks.mv".to_string(),
                format_name: "publication".to_string(),
                format_version: 1,
                revision: ConnectorDocumentRevision::for_content(&content).to_bytes(),
                encoded_len: content.len() as u64,
                references: Vec::new(),
                attachment: IcebergDocumentAttachmentV1::CommitOutput,
                carrier: IcebergDocumentCarrierV1::Available { content },
            }],
        };
        let encoded = crate::document_storage::codec::encode_document_manifest(&manifest)
            .expect("encode prepared document manifest")
            .to_vec();
        (
            BTreeMap::from([
                (
                    ICEBERG_WRITE_SESSION_MARKER_PROPERTY.to_string(),
                    format!("session-{label}"),
                ),
                (
                    PENDING_DOCUMENT_MANIFEST_PROPERTY.to_string(),
                    String::from_utf8(encoded.clone()).expect("UTF-8 document manifest"),
                ),
            ]),
            encoded,
        )
    }

    fn written_file(table: &Table, label: &str) -> crate::commit::WrittenFile {
        crate::commit::WrittenFile {
            path: format!("{}/data/{label}.parquet", table.metadata().location()),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: table.metadata().default_partition_spec_id(),
            record_count: 3,
            file_size_in_bytes: 128,
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

    fn collector(
        table: &Table,
        op_kind: CommitOpKind,
        spec: Arc<PartitionSpec>,
        files: Vec<crate::commit::WrittenFile>,
    ) -> Arc<IcebergCommitCollector> {
        let collector = Arc::new(IcebergCommitCollector::new(
            op_kind,
            table.identifier().clone(),
            table.metadata().current_snapshot_id(),
            table.metadata().last_sequence_number(),
            table.metadata().current_schema().clone(),
            spec,
            format!("{}/data/_staging/test", table.metadata().location()),
        ));
        collector.inject_written_files(files);
        collector
    }

    fn assert_one_snapshot_commit(
        catalog: &RecordingCatalog,
        expected_updates: &[&'static str],
        snapshot_id: i64,
    ) {
        let commits = catalog.commits();
        assert_eq!(commits.len(), 1, "publication must mutate its target once");
        assert_eq!(commits[0].update_kinds, expected_updates);
        assert_eq!(
            commits[0].ref_updates,
            vec![("main".to_string(), snapshot_id)]
        );
        assert!(commits[0].requirement_kinds.contains(&"assert-table-uuid"));
        assert!(
            commits[0]
                .requirement_kinds
                .contains(&"assert-ref-snapshot-id")
        );
    }

    async fn live_data_paths(table: &Table) -> Vec<String> {
        let mut paths = table
            .scan()
            .build()
            .expect("build table scan")
            .plan_files()
            .await
            .expect("plan live files")
            .map_ok(|task| task.data_file_path.to_string())
            .try_collect::<Vec<_>>()
            .await
            .expect("collect live files");
        paths.sort();
        paths
    }

    fn write_control(
        fixture: &Fixture,
    ) -> crate::commit::write_stack::control::IcebergWriteSessionControl {
        crate::commit::write_stack::control::IcebergWriteSessionControl::new(
            fixture.provider.descriptor().clone(),
            fixture.provider.incarnation(),
            fixture.catalog_handle.clone(),
            Arc::clone(fixture.provider.runtime()),
        )
    }

    fn iceberg_handle<'a>(
        fixture: &Fixture,
        plan: &'a novarocks_spi::connector::write_stack::session::ConnectorWriteSessionPlan,
    ) -> &'a crate::commit::write_stack::domain::IcebergCommitHandle {
        crate::commit::write_stack::runtime::build_write_adapter(
            fixture.provider.descriptor().clone(),
            fixture.catalog_handle.clone(),
        )
        .commit_handle(plan.commit_handle())
        .expect("Iceberg commit handle")
    }

    fn recovery_evidence(
        fixture: &Fixture,
        plan: &novarocks_spi::connector::write_stack::session::ConnectorWriteSessionPlan,
        expected_manifest: &[u8],
    ) -> novarocks_spi::connector::ExternalMutationEvidence {
        let handle = iceberg_handle(fixture, plan);
        handle
            .bind_document_manifest(expected_manifest)
            .expect("bind prepared document manifest");
        crate::commit::write_stack::control::encode_session_evidence(
            fixture.provider.descriptor(),
            fixture.provider.incarnation(),
            handle,
            &RecoveryEvidence {
                table_ident: "db.t".to_string(),
                op_kind: handle.commit_op_kind(),
                base_snapshot_id: handle.table().base_snapshot_id(),
                base_sequence_number: handle.table().base_sequence_number(),
                staging_dir: handle.staging_dir(),
                manifest_cleanup_token: None,
            },
        )
        .expect("encode write recovery evidence")
    }

    fn assert_exact_target_mutation(
        fixture: &Fixture,
        base_snapshot_id: Option<i64>,
        expected_updates: &[&'static str],
        committed_snapshot_id: i64,
    ) {
        let commits = fixture.catalog.commits();
        assert_eq!(commits.len(), 1, "publication must update_table once");
        assert_eq!(
            commits[0].uuid_requirements,
            vec![fixture.table.metadata().uuid()]
        );
        assert_eq!(
            commits[0].ref_requirements,
            vec![("main".to_string(), base_snapshot_id)]
        );
        assert_eq!(commits[0].update_kinds, expected_updates);
        assert_eq!(
            commits[0].ref_updates,
            vec![("main".to_string(), committed_snapshot_id)]
        );
    }

    async fn assert_exact_manifest(fixture: &Fixture, snapshot_id: i64, expected_manifest: &[u8]) {
        let table = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload document publication");
        crate::document_storage::publication::validate_expected_manifest(
            table.metadata(),
            snapshot_id,
            expected_manifest,
        )
        .expect("exact committed document manifest");
    }

    #[tokio::test]
    async fn real_full_overwrite_prepares_begins_and_finishes_one_exact_publication() {
        let fixture = fixture().await;
        // Long, unique references exercise a near-limit manifest without
        // inflating any one application document or relying on a sidecar.
        let prepared = prepare_publication(
            &fixture,
            ConnectorManagedPublicationTechnique::Full,
            ConnectorManagedPublicationShape::Data,
            false,
            Bytes::from_static(b"near-limit-publication"),
            1,
            2_500,
        );
        assert!(
            prepared.expected_manifest.len() > 900 * 1024,
            "prepared manifest was {} bytes",
            prepared.expected_manifest.len()
        );
        assert!(prepared.expected_manifest.len() < 1024 * 1024);
        let control = write_control(&fixture);
        let plan = control
            .begin_write(begin_request(
                &prepared,
                ConnectorManagedPublicationShape::Data,
            ))
            .expect("begin full document publication");
        let evidence = recovery_evidence(&fixture, &plan, &prepared.expected_manifest);
        assert!(evidence.provider_payload().len() < MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES);
        let evidence_payload: serde_json::Value =
            serde_json::from_slice(evidence.provider_payload()).expect("decode evidence payload");
        assert_eq!(
            evidence_payload["document_manifest_digest"]
                .as_array()
                .expect("document manifest digest")
                .len(),
            32
        );
        let outcome = control
            .finish_write(ConnectorWriteFinishRequest {
                commit: plan.commit_handle(),
                prepared: ConnectorPreparedWriteSet::try_new(
                    0,
                    Vec::new(),
                    &plan.expected_targets(),
                )
                .expect("empty prepared write set"),
                statistics: Vec::new(),
                publication: ConnectorWriteFinishPublication::ApplicationDocuments(
                    prepared.intent.clone(),
                ),
                context: context(),
            })
            .expect("finish full document publication");
        let ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization,
            ..
        } = outcome
        else {
            panic!("full publication must be known committed");
        };
        assert_eq!(finalization, ExternalMutationFinalization::Complete);
        assert_eq!(receipt.resulting_row_count(), Some(0));
        let snapshot_id = receipt
            .committed_version()
            .and_then(|version| version.snapshot_id())
            .expect("committed snapshot id");
        assert_exact_target_mutation(
            &fixture,
            prepared.base_snapshot_id,
            &["add-snapshot", "set-snapshot-ref"],
            snapshot_id,
        );
        assert_exact_manifest(&fixture, snapshot_id, &prepared.expected_manifest).await;

        let reconciled = control
            .reconcile_write(ConnectorWriteSessionReconcileRequest {
                commit: plan.commit_handle(),
                evidence,
                context: context(),
            })
            .expect("reconcile known full publication");
        let ExternalMutationOutcome::KnownCommitted {
            receipt: reconciled,
            finalization,
            ..
        } = reconciled
        else {
            panic!("known publication must reconcile idempotently");
        };
        assert_eq!(finalization, ExternalMutationFinalization::Complete);
        assert_eq!(reconciled.resulting_row_count(), Some(0));
        assert_eq!(
            reconciled
                .committed_version()
                .and_then(|version| version.snapshot_id()),
            Some(snapshot_id)
        );
        assert_eq!(fixture.catalog.commits().len(), 1);
    }

    #[tokio::test]
    async fn real_incremental_row_mutation_prepares_begins_and_finishes_one_exact_publication() {
        let fixture = row_lineage_fixture().await;
        seed_empty_snapshot(&fixture).await;
        let prepared = prepare_publication(
            &fixture,
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationShape::RowMutation,
            false,
            Bytes::from_static(b"incremental-row-mutation"),
            1,
            0,
        );
        // This is the same combined ManagedPublication /
        // ApplicationDocumentPublication match arm locked for the legacy
        // flavor by `a_change_stream_publication_freezes_the_old_deletes_it_supersedes`.
        // The existing old-delete tests additionally prove that the frozen
        // references must be merged exactly before a delete artifact commits.
        assert!(
            crate::commit::write_stack::control::session_freezes_old_deletes(
                &ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
                    declaration: prepared.declaration.clone(),
                    shape: ConnectorManagedPublicationShape::RowMutation,
                },
                &crate::commit::write_stack::test_support::merge_on_read_input_shape(),
            )
        );
        let control = write_control(&fixture);
        let plan = control
            .begin_write(begin_request(
                &prepared,
                ConnectorManagedPublicationShape::RowMutation,
            ))
            .expect("begin incremental row-mutation publication");
        let outcome = control
            .finish_write(ConnectorWriteFinishRequest {
                commit: plan.commit_handle(),
                prepared: ConnectorPreparedWriteSet::try_new(
                    0,
                    Vec::new(),
                    &plan.expected_targets(),
                )
                .expect("empty prepared row-mutation set"),
                statistics: Vec::new(),
                publication: ConnectorWriteFinishPublication::ApplicationDocuments(
                    prepared.intent.clone(),
                ),
                context: context(),
            })
            .expect("finish incremental row-mutation publication");
        let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
            panic!("incremental row-mutation publication must be known committed");
        };
        assert_eq!(receipt.resulting_row_count(), Some(0));
        let snapshot_id = receipt
            .committed_version()
            .and_then(|version| version.snapshot_id())
            .expect("committed snapshot id");
        assert_exact_target_mutation(
            &fixture,
            prepared.base_snapshot_id,
            &["add-snapshot", "set-snapshot-ref"],
            snapshot_id,
        );
        assert_exact_manifest(&fixture, snapshot_id, &prepared.expected_manifest).await;
    }

    #[tokio::test]
    async fn real_eager_repartition_prepares_begins_and_finishes_one_exact_publication() {
        let fixture = fixture().await;
        let prepared = prepare_publication(
            &fixture,
            ConnectorManagedPublicationTechnique::Full,
            ConnectorManagedPublicationShape::Data,
            true,
            Bytes::from_static(b"eager-repartition"),
            1,
            0,
        );
        let expected_partitioning = prepared
            .declaration
            .expected_committed_partitioning()
            .expect("expected committed partitioning")
            .clone();
        let control = write_control(&fixture);
        let plan = control
            .begin_write(begin_request(
                &prepared,
                ConnectorManagedPublicationShape::Data,
            ))
            .expect("begin eager repartition publication");
        let evidence = recovery_evidence(&fixture, &plan, &prepared.expected_manifest);
        let outcome = control
            .finish_write(ConnectorWriteFinishRequest {
                commit: plan.commit_handle(),
                prepared: ConnectorPreparedWriteSet::try_new(
                    0,
                    Vec::new(),
                    &plan.expected_targets(),
                )
                .expect("empty prepared repartition set"),
                statistics: Vec::new(),
                publication: ConnectorWriteFinishPublication::ApplicationDocuments(
                    prepared.intent.clone(),
                ),
                context: context(),
            })
            .expect("finish eager repartition publication");
        let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
            panic!("eager repartition publication must be known committed");
        };
        assert_eq!(receipt.resulting_row_count(), Some(0));
        assert_eq!(
            receipt.committed_partitioning(),
            Some(&expected_partitioning)
        );
        let snapshot_id = receipt
            .committed_version()
            .and_then(|version| version.snapshot_id())
            .expect("committed snapshot id");
        assert_exact_target_mutation(
            &fixture,
            prepared.base_snapshot_id,
            &[
                "add-spec",
                "set-default-spec",
                "add-snapshot",
                "set-snapshot-ref",
            ],
            snapshot_id,
        );
        assert_exact_manifest(&fixture, snapshot_id, &prepared.expected_manifest).await;

        let reconciled = control
            .reconcile_write(ConnectorWriteSessionReconcileRequest {
                commit: plan.commit_handle(),
                evidence,
                context: context(),
            })
            .expect("reconcile known repartition publication");
        let ExternalMutationOutcome::KnownCommitted {
            receipt: reconciled,
            finalization,
            ..
        } = reconciled
        else {
            panic!("known repartition must reconcile idempotently");
        };
        assert_eq!(finalization, ExternalMutationFinalization::Complete);
        assert_eq!(reconciled.resulting_row_count(), Some(0));
        assert_eq!(
            reconciled.committed_partitioning(),
            Some(&expected_partitioning)
        );
        assert_eq!(fixture.catalog.commits().len(), 1);
    }

    #[tokio::test]
    async fn normal_document_publication_is_one_exact_main_commit() {
        let fixture = fixture().await;
        let file = written_file(&fixture.table, "normal");
        let collector = collector(
            &fixture.table,
            CommitOpKind::FastAppend,
            fixture.table.metadata().default_partition_spec().clone(),
            vec![file],
        );
        let (properties, unresolved) = prepared_snapshot_properties("normal");
        let outcome = FastAppendCommit
            .commit(CommitCtx {
                collector: &collector,
                table: &fixture.table,
                catalog: fixture.catalog.as_ref(),
                file_io: fixture.table.file_io(),
                commit_uuid: Uuid::now_v7(),
                abort_handle: Arc::clone(&collector.abort_log),
                target_ref: "main",
                snapshot_properties: &properties,
            })
            .await
            .expect("normal document publication");

        assert_one_snapshot_commit(
            &fixture.catalog,
            &["add-snapshot", "set-snapshot-ref"],
            outcome.new_snapshot_id,
        );
        let table = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload normal publication");
        crate::document_storage::publication::validate_expected_manifest(
            table.metadata(),
            outcome.new_snapshot_id,
            &unresolved,
        )
        .expect("exact normal document attachment");
    }

    #[derive(Debug)]
    struct RecordingUpdateDispatch {
        catalog: Arc<RecordingCatalog>,
    }

    #[async_trait]
    impl CatalogCommitDispatch for RecordingUpdateDispatch {
        async fn dispatch_once(
            &self,
            staged: Option<crate::iceberg::TableCommit>,
        ) -> crate::iceberg::Result<CommitProof> {
            let staged = staged.expect("eager publication must stage a commit");
            let expected_snapshot = staged.updated_ref_snapshot_id("main");
            let table = self.catalog.update_table(staged).await?;
            Ok(CommitProof::applied(expected_snapshot)
                .with_table_uuid(table.metadata().uuid().to_string()))
        }

        async fn adjudicate(
            &self,
        ) -> Result<Option<CommitProof>, novarocks_spi::connector::ConnectorError> {
            Ok(None)
        }

        async fn abort_before_dispatch(
            &self,
        ) -> Result<(), novarocks_spi::connector::ConnectorError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn eager_document_publication_is_one_exact_main_commit() {
        let fixture = fixture().await;
        let file = written_file(&fixture.table, "eager");
        let collector = collector(
            &fixture.table,
            CommitOpKind::FastAppend,
            fixture.table.metadata().default_partition_spec().clone(),
            vec![file],
        );
        let (properties, unresolved) = prepared_snapshot_properties("eager");
        let (transaction, outcome) =
            crate::commit::fast_append::stage_eager_fast_append(CommitCtx {
                collector: &collector,
                table: &fixture.table,
                catalog: fixture.catalog.as_ref(),
                file_io: fixture.table.file_io(),
                commit_uuid: Uuid::now_v7(),
                abort_handle: Arc::clone(&collector.abort_log),
                target_ref: "main",
                snapshot_properties: &properties,
            })
            .await
            .expect("stage eager document publication");
        let mut staged = transaction.into_table_commit();
        staged.add_requirement(TableRequirement::UuidMatch {
            uuid: fixture.table.metadata().uuid(),
        });
        let mut frontier = crate::catalog::transaction::Transaction::new(
            TransactionIdentity::new("document-publication-test", [7; 16]),
            CatalogTableName::new("db", "t"),
            TransactionShape::Existing,
            CatalogCommitEvidence::for_target("db.t"),
            Arc::new(RecordingUpdateDispatch {
                catalog: Arc::clone(&fixture.catalog),
            }),
        );
        frontier
            .stage(staged)
            .expect("stage eager catalog frontier");
        assert!(matches!(
            frontier.commit().await,
            CatalogOutcome::KnownCommitted { .. }
        ));

        assert_one_snapshot_commit(
            &fixture.catalog,
            &["add-snapshot", "set-snapshot-ref"],
            outcome.new_snapshot_id,
        );
        let table = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload eager publication");
        crate::document_storage::publication::validate_expected_manifest(
            table.metadata(),
            outcome.new_snapshot_id,
            &unresolved,
        )
        .expect("exact eager document attachment");
    }

    #[tokio::test]
    async fn empty_document_publication_keeps_data_files_and_commits_one_new_snapshot() {
        let fixture = fixture().await;
        let seed_file = written_file(&fixture.table, "seed");
        let seed_collector = collector(
            &fixture.table,
            CommitOpKind::FastAppend,
            fixture.table.metadata().default_partition_spec().clone(),
            vec![seed_file],
        );
        let seed_properties = BTreeMap::from([(
            ICEBERG_WRITE_SESSION_MARKER_PROPERTY.to_string(),
            "seed".to_string(),
        )]);
        FastAppendCommit
            .commit(CommitCtx {
                collector: &seed_collector,
                table: &fixture.table,
                catalog: fixture.catalog.as_ref(),
                file_io: fixture.table.file_io(),
                commit_uuid: Uuid::now_v7(),
                abort_handle: Arc::clone(&seed_collector.abort_log),
                target_ref: "main",
                snapshot_properties: &seed_properties,
            })
            .await
            .expect("seed populated table");
        let populated = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload populated table");
        let before_paths = live_data_paths(&populated).await;
        fixture
            .catalog
            .commits
            .lock()
            .expect("recording catalog lock")
            .clear();

        let collector = collector(
            &populated,
            CommitOpKind::FastAppend,
            populated.metadata().default_partition_spec().clone(),
            Vec::new(),
        );
        let (properties, unresolved) = prepared_snapshot_properties("empty");
        let outcome = FastAppendCommit
            .commit(CommitCtx {
                collector: &collector,
                table: &populated,
                catalog: fixture.catalog.as_ref(),
                file_io: populated.file_io(),
                commit_uuid: Uuid::now_v7(),
                abort_handle: Arc::clone(&collector.abort_log),
                target_ref: "main",
                snapshot_properties: &properties,
            })
            .await
            .expect("empty document publication");

        assert_one_snapshot_commit(
            &fixture.catalog,
            &["add-snapshot", "set-snapshot-ref"],
            outcome.new_snapshot_id,
        );
        let table = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload empty publication");
        assert_eq!(live_data_paths(&table).await, before_paths);
        crate::document_storage::publication::validate_expected_manifest(
            table.metadata(),
            outcome.new_snapshot_id,
            &unresolved,
        )
        .expect("exact empty document attachment");
    }

    #[tokio::test]
    async fn repartition_document_publication_orders_all_updates_in_one_commit() {
        let fixture = fixture().await;
        let prior = ConnectorManagedPartitionSpecObservation::try_from_fields(
            fixture.table.metadata().default_partition_spec_id(),
            &[],
        )
        .expect("prior unpartitioned observation");
        let replacement = ConnectorManagedPartitionSpecReplacement::try_new(
            ConnectorWriteOperationId::new(),
            prior,
            vec![
                ConnectorManagedPartitionField::try_new(
                    1,
                    0,
                    ConnectorManagedPartitionTransform::Identity,
                )
                .expect("identity partition field"),
            ],
        )
        .expect("partition replacement");
        let prepared = crate::commit::write_stack::repartition::preview_managed_repartition(
            fixture.table.metadata(),
            &replacement,
        )
        .expect("prepare repartition");
        let replacement = AtomicPartitionReplacement::try_new(prepared.metadata_updates().to_vec())
            .expect("atomic repartition updates");
        let collector = collector(
            &fixture.table,
            CommitOpKind::Overwrite,
            prepared
                .prospective_metadata()
                .default_partition_spec()
                .clone(),
            Vec::new(),
        );
        let (properties, unresolved) = prepared_snapshot_properties("repartition");
        let outcome = run_atomic_partition_replacement(
            CommitCtx {
                collector: &collector,
                table: &fixture.table,
                catalog: fixture.catalog.as_ref(),
                file_io: fixture.table.file_io(),
                commit_uuid: Uuid::now_v7(),
                abort_handle: Arc::clone(&collector.abort_log),
                target_ref: "main",
                snapshot_properties: &properties,
            },
            replacement,
        )
        .await
        .expect("repartition document publication");

        assert_one_snapshot_commit(
            &fixture.catalog,
            &[
                "add-spec",
                "set-default-spec",
                "add-snapshot",
                "set-snapshot-ref",
            ],
            outcome.new_snapshot_id,
        );
        let table = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload repartition publication");
        assert_ne!(
            table.metadata().default_partition_spec_id(),
            fixture.table.metadata().default_partition_spec_id()
        );
        crate::document_storage::publication::validate_expected_manifest(
            table.metadata(),
            outcome.new_snapshot_id,
            &unresolved,
        )
        .expect("exact repartition document attachment");
    }
}
