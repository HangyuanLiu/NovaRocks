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

//! Provider-private REST staged-table preparation.
//!
//! One control generation retains the exact concrete REST client used for
//! ordinary metadata. Staging therefore neither rebuilds a client nor
//! downcasts the generic catalog surface.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorInstanceDescriptor, ConnectorInstanceIncarnation, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorPartitionTransform, ConnectorRequestContext,
    ConnectorStagedCreate, ConnectorStagedCreateAbortOutcome, ConnectorStagedCreateAbortRequest,
    ConnectorStagedCreateOperationId, ConnectorStagedCreatePrepareOutcome,
    ConnectorStagedCreatePrepareRequest, ConnectorStagedCreatePublishOutcome,
    ConnectorStagedCreatePublishRequest, ConnectorStagedCreateReceipt,
    ConnectorStagedCreateReceiptPhase, ConnectorStagedCreateReconcileOutcome,
    ConnectorStagedCreateReconcilePhase, ConnectorStagedCreateReconcileRequest,
    ConnectorStagedTableHandle, ConnectorStagedWritePlanningBinding,
    ConnectorStagedWritePlanningRequest, ConnectorWriteControl, ConnectorWriteOperationCompletion,
    CreatePolicy, ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
};
use novarocks_types::naming::normalize_identifier;

use crate::commit::{
    AbortLog, CommitCtx, CommitOpKind, IcebergCommitCollector, IcebergWriteControl,
    build_staged_fast_append_action,
};
use crate::control_provider::IcebergControlProvider;
use crate::control_runtime::IcebergControlRuntime;
use crate::iceberg::{
    Catalog, ErrorKind, NamespaceIdent, TableCommit, TableCreation, TableRequirement, TableUpdate,
};

const EVIDENCE_VERSION: u16 = 1;
const CTAS_OPERATION_MARKER: &str = "novarocks.ctas.operation-id";
const CTAS_PROVENANCE_VERSION: &str = "novarocks.ctas.provenance-version";
const CTAS_PROVENANCE_TARGET: &str = "novarocks.ctas.target";
const CTAS_PROVENANCE_EXPECTED_ABSENT: &str = "novarocks.ctas.expected-absent";
const CTAS_PROVENANCE_TABLE_UUID: &str = "novarocks.ctas.table-uuid";
const CTAS_STAGING_NAMESPACE: &str = "_novarocks/ctas-staging/v1";

#[derive(Clone)]
pub(crate) struct RestStagedTableCreate {
    pub(crate) catalog: Arc<crate::iceberg_catalog_rest::RestCatalog>,
    pub(crate) table: crate::iceberg::table::Table,
    pub(crate) initialization_updates: Vec<TableUpdate>,
}

#[derive(Debug)]
pub(crate) enum RestStagedPrepareFailure {
    Conflict(String),
    KnownUncommitted(String),
    CommitUnknown(String),
}

impl From<String> for RestStagedPrepareFailure {
    fn from(message: String) -> Self {
        Self::KnownUncommitted(message)
    }
}

impl From<ConnectorError> for RestStagedPrepareFailure {
    fn from(error: ConnectorError) -> Self {
        Self::KnownUncommitted(error.to_string())
    }
}

pub(crate) fn prepare_rest_staged_table(
    runtime: &IcebergControlRuntime,
    operation_id: ConnectorStagedCreateOperationId,
    namespace_name: &str,
    table_name: &str,
    columns: &[ConnectorColumnDefinition],
    partitioning: &[ConnectorPartitionTransform],
    properties: &[(Arc<str>, Arc<str>)],
) -> Result<RestStagedTableCreate, RestStagedPrepareFailure> {
    let catalog = runtime.rest_catalog().cloned().ok_or_else(|| {
        RestStagedPrepareFailure::KnownUncommitted(
            "atomic staged table publication is unsupported by this Iceberg catalog".to_string(),
        )
    })?;
    let namespace_name = normalize_identifier(namespace_name)?;
    let table_name = normalize_identifier(table_name)?;
    let namespace = NamespaceIdent::new(namespace_name.clone());
    let location = ctas_staging_location(
        &runtime.control_state().configuration().warehouse_uri,
        operation_id,
    )?;
    let namespace_catalog = Arc::clone(&catalog);
    let namespace_for_check = namespace.clone();
    let exists = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move {
            namespace_catalog
                .namespace_exists(&namespace_for_check)
                .await
        })
        .map_err(|error| {
            RestStagedPrepareFailure::KnownUncommitted(format!(
                "check REST namespace runtime: {error}"
            ))
        })?
        .map_err(|error| {
            RestStagedPrepareFailure::KnownUncommitted(format!("check REST namespace: {error}"))
        })?;
    if !exists {
        return Err(RestStagedPrepareFailure::KnownUncommitted(format!(
            "prepare staged Iceberg table failed: namespace {namespace_name} does not exist"
        )));
    }
    let (format_version, mut properties) =
        super::catalog_mutation::table_properties(columns, None, properties)?;
    if format_version != crate::iceberg::spec::FormatVersion::V3
        && columns.iter().any(|column| {
            column.default.as_ref().is_some_and(|value| {
                !matches!(value, novarocks_spi::connector::ConnectorDefaultValue::Null)
            })
        })
    {
        return Err(RestStagedPrepareFailure::KnownUncommitted(
            "Iceberg column defaults require format-version 3".to_string(),
        ));
    }
    let schema = crate::iceberg::spec::Schema::builder()
        .with_fields(super::type_mapping::schema_fields(columns)?)
        .build()
        .map_err(|error| format!("build staged Iceberg schema: {error}"))?;
    let partition_spec = super::catalog_mutation::initial_partition_spec(&schema, partitioning)?;
    properties.insert(
        "format-version".to_string(),
        (format_version as u8).to_string(),
    );
    properties.insert(
        CTAS_OPERATION_MARKER.to_string(),
        operation_marker(operation_id),
    );
    properties.insert(
        CTAS_PROVENANCE_VERSION.to_string(),
        EVIDENCE_VERSION.to_string(),
    );
    properties.insert(
        CTAS_PROVENANCE_TARGET.to_string(),
        format!("{namespace_name}.{table_name}"),
    );
    properties.insert(
        CTAS_PROVENANCE_EXPECTED_ABSENT.to_string(),
        "true".to_string(),
    );
    let publication_properties = properties
        .iter()
        .filter(|(key, _)| !key.eq_ignore_ascii_case("format-version"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    let creation = TableCreation::builder()
        .name(table_name)
        .schema(schema)
        .location(location.clone())
        .properties(properties)
        .format_version(format_version);
    let creation = if let Some(spec) = partition_spec {
        creation.partition_spec(spec).build()
    } else {
        creation.build()
    };
    let staging_catalog = Arc::clone(&catalog);
    let staged = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move {
            staging_catalog
                .stage_create_table_typed(&namespace, creation)
                .await
        })
        .map_err(|error| {
            RestStagedPrepareFailure::KnownUncommitted(format!(
                "prepare staged REST table runtime: {error}"
            ))
        })?
        .map_err(|error| match error {
            crate::iceberg_catalog_rest::StagedCreateError::Conflict(error) => {
                RestStagedPrepareFailure::Conflict(format!("prepare staged REST table: {error}"))
            }
            crate::iceberg_catalog_rest::StagedCreateError::KnownNotDispatched(error) => {
                RestStagedPrepareFailure::KnownUncommitted(format!(
                    "prepare staged REST table: {error}"
                ))
            }
            crate::iceberg_catalog_rest::StagedCreateError::PossiblyDispatched(error) => {
                RestStagedPrepareFailure::CommitUnknown(format!(
                    "prepare staged REST table: {error}"
                ))
            }
        })?;
    let (table, mut initialization_updates) = staged.into_parts();
    if table.metadata().location() != location {
        return Err(RestStagedPrepareFailure::CommitUnknown(
            "REST stage-create returned a table at a location other than the requested CTAS staging location"
                .to_string(),
        ));
    }
    let mut response_provenance = HashMap::new();
    response_provenance.insert(
        CTAS_PROVENANCE_TABLE_UUID.to_string(),
        table.metadata().uuid().to_string(),
    );
    initialization_updates.push(TableUpdate::SetProperties {
        updates: response_provenance,
    });
    if !publication_properties.is_empty() {
        initialization_updates.push(TableUpdate::SetProperties {
            updates: publication_properties,
        });
    }
    Ok(RestStagedTableCreate {
        catalog,
        table,
        initialization_updates,
    })
}

/// Exact-generation REST staged-create capability.
///
/// The application receives an ordinary opaque table handle from
/// [`ConnectorStagedCreate::plan_write`] and continues through the normal
/// prepare/activate/write lifecycle. This capability retains only the
/// invisible target and the sealed writer aggregate required for one atomic
/// assert-create publication.
#[derive(Clone)]
pub struct IcebergStagedCreateAdapter {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    provider: Arc<IcebergControlProvider>,
    write_control: Arc<IcebergWriteControl>,
    runtime: Arc<IcebergControlRuntime>,
    operations: Arc<Mutex<HashMap<ConnectorStagedCreateOperationId, OperationState>>>,
}

#[derive(Clone)]
enum OperationState {
    Preparing,
    Prepared(PreparedOperation),
    Published,
    Aborted,
    Unknown(UnknownOperation),
}

#[derive(Clone)]
struct PreparedOperation {
    handle_digest: [u8; 32],
    staged: RestStagedTableCreate,
    policy: CreatePolicy,
    planning: Option<ConnectorStagedWritePlanningBinding>,
    write: Option<StagedWrite>,
}

#[derive(Clone)]
struct StagedWrite {
    completion: ConnectorWriteOperationCompletion,
    updates: Vec<TableUpdate>,
    expected_snapshot_id: Option<i64>,
    abort_handle: Arc<AbortLog>,
    action_built: bool,
}

#[derive(Clone)]
struct UnknownOperation {
    phase: ConnectorStagedCreateReconcilePhase,
    evidence_digest: [u8; 32],
    prepared: Option<PreparedOperation>,
}

type StagedCreateAction = (Vec<TableUpdate>, Option<i64>, Arc<AbortLog>);

pub(crate) struct IcebergFencedCleanupAction {
    pub(crate) data_prefixes: Vec<String>,
    pub(crate) objects: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PublishEvidenceV1 {
    version: u16,
    operation_marker: String,
    table_uuid: String,
    expected_snapshot_id: Option<i64>,
    handle_digest: [u8; 32],
    namespace: String,
    table: String,
}

impl IcebergStagedCreateAdapter {
    pub fn try_new(
        provider: Arc<IcebergControlProvider>,
        write_control: Arc<IcebergWriteControl>,
    ) -> Result<Self, ConnectorError> {
        if provider.runtime().rest_catalog().is_none() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Iceberg catalog has no atomic staged-create publication capability",
            ));
        }
        let owner = ConnectorExecutionBindingKey {
            instance_id: provider.descriptor().instance_id.clone(),
            incarnation: provider.incarnation(),
        };
        if write_control.binding_key() != &owner {
            return Err(invalid(
                "Iceberg staged-create and write control generations do not match",
            ));
        }
        Ok(Self {
            descriptor: provider.descriptor().clone(),
            incarnation: provider.incarnation(),
            runtime: Arc::clone(provider.runtime()),
            provider,
            write_control,
            operations: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn owner(&self) -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        }
    }

    fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
        if context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "Iceberg staged-create request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "Iceberg staged-create request deadline elapsed",
            ));
        }
        Ok(())
    }

    fn receipt(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        phase: ConnectorStagedCreateReceiptPhase,
        effect: ExternalMutationEffect,
        payload: Bytes,
    ) -> Result<ConnectorStagedCreateReceipt, ConnectorError> {
        ConnectorStagedCreateReceipt::try_new(self.owner(), operation_id, phase, effect, payload)
    }

    fn evidence(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        phase: ConnectorStagedCreateReconcilePhase,
        payload: Bytes,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        ExternalMutationEvidence::try_new(
            EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.incarnation,
            operation_id,
            operation_kind(phase),
            payload,
        )
    }

    fn publish_evidence(
        &self,
        dispatch_operation_id: ConnectorStagedCreateOperationId,
        target_operation_id: ConnectorStagedCreateOperationId,
        prepared: &PreparedOperation,
        expected_snapshot_id: Option<i64>,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let ident = prepared.staged.table.identifier();
        let payload = serde_json::to_vec(&PublishEvidenceV1 {
            version: EVIDENCE_VERSION,
            operation_marker: operation_marker(target_operation_id),
            table_uuid: prepared.staged.table.metadata().uuid().to_string(),
            expected_snapshot_id,
            handle_digest: prepared.handle_digest,
            namespace: ident.namespace.to_url_string(),
            table: ident.name.clone(),
        })
        .map(Bytes::from)
        .map_err(|error| internal(format!("encode staged-create publish evidence: {error}")))?;
        self.evidence(
            dispatch_operation_id,
            ConnectorStagedCreateReconcilePhase::Publish,
            payload,
        )
    }

    fn record_terminal(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        state: OperationState,
    ) {
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(operation_id, state);
    }

    fn set_unknown(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        phase: ConnectorStagedCreateReconcilePhase,
        evidence: &ExternalMutationEvidence,
        prepared: Option<PreparedOperation>,
    ) {
        self.record_terminal(
            operation_id,
            OperationState::Unknown(UnknownOperation {
                phase,
                evidence_digest: evidence.digest(),
                prepared,
            }),
        );
    }

    fn build_action(
        &self,
        prepared: &PreparedOperation,
        completion: &ConnectorWriteOperationCompletion,
    ) -> Result<StagedCreateAction, ConnectorError> {
        if completion.owner() != &self.owner() {
            return Err(invalid(
                "staged-create writer completion has a foreign owner",
            ));
        }
        let metadata = prepared.staged.table.metadata().clone();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::FastAppend,
                prepared.staged.table.identifier().clone(),
                None,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!(
                    "{}/data/_staging/{}",
                    metadata.location().trim_end_matches('/'),
                    completion.sealed().operation_id()
                ),
            )
            .with_table_metadata(metadata.clone()),
        );
        for cohort in completion.cohorts() {
            if let Some(accepted) = cohort.accepted() {
                for report in accepted.reports() {
                    report.validate()?;
                    if report.state()
                        != novarocks_spi::connector::ConnectorWriterTerminalState::Staged
                    {
                        return Err(invalid(
                            "accepted staged-create writer report is not in the staged state",
                        ));
                    }
                    let reports =
                        crate::write_codec::decode_writer_reports(report.payload(), &metadata)
                            .map_err(corrupt)?;
                    collector.inject_writer_reports(reports).map_err(corrupt)?;
                }
            }
            for attempt in cohort.superseded() {
                for report in attempt.reports() {
                    report.validate()?;
                    for decoded in
                        crate::write_codec::decode_writer_reports(report.payload(), &metadata)
                            .map_err(corrupt)?
                    {
                        let file = collector.convert_writer_report(decoded).map_err(corrupt)?;
                        collector.abort_log.record_data_file(file.path);
                    }
                }
            }
        }

        let abort_handle = prepared
            .write
            .as_ref()
            .map(|write| Arc::clone(&write.abort_handle))
            .ok_or_else(|| invalid("staged-create action requires a bound writer aggregate"))?;
        let table = prepared.staged.table.clone();
        let catalog: Arc<dyn Catalog> = prepared.staged.catalog.clone();
        let file_io = table.file_io().clone();
        let action_abort = Arc::clone(&abort_handle);
        let action_collector = Arc::clone(&collector);
        let built = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                let snapshot_properties = BTreeMap::new();
                build_staged_fast_append_action(CommitCtx {
                    collector: action_collector.as_ref(),
                    table: &table,
                    catalog: catalog.as_ref(),
                    file_io: &file_io,
                    commit_uuid: uuid::Uuid::now_v7(),
                    abort_handle: action_abort,
                    target_ref: "main",
                    snapshot_properties: &snapshot_properties,
                    fence: None,
                })
                .await
            })
            .map_err(|error| internal(format!("build staged-create action runtime: {error}")))?
            .map_err(|error| internal(format!("build staged-create action: {error}")))?;
        let mut action = built.action;
        let updates = action.take_updates();
        let expected_snapshot_id = built.outcome.map(|outcome| outcome.new_snapshot_id);
        Ok((updates, expected_snapshot_id, built.abort_handle))
    }

    fn completion_abort_log(
        &self,
        prepared: &PreparedOperation,
        completion: &ConnectorWriteOperationCompletion,
    ) -> Result<Arc<AbortLog>, ConnectorError> {
        let metadata = prepared.staged.table.metadata().clone();
        let collector = IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            prepared.staged.table.identifier().clone(),
            None,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            format!(
                "{}/data/_staging/{}",
                metadata.location().trim_end_matches('/'),
                completion.sealed().operation_id()
            ),
        )
        .with_table_metadata(metadata.clone());
        for cohort in completion.cohorts() {
            if let Some(accepted) = cohort.accepted() {
                for report in accepted.reports() {
                    report.validate()?;
                    if report.state()
                        != novarocks_spi::connector::ConnectorWriterTerminalState::Staged
                    {
                        return Err(invalid(
                            "accepted staged-create writer report is not in the staged state",
                        ));
                    }
                    for decoded in
                        crate::write_codec::decode_writer_reports(report.payload(), &metadata)
                            .map_err(corrupt)?
                    {
                        let file = collector.convert_writer_report(decoded).map_err(corrupt)?;
                        collector.abort_log.record_data_file(file.path);
                    }
                }
            }
            for attempt in cohort.superseded() {
                for report in attempt.reports() {
                    report.validate()?;
                    for decoded in
                        crate::write_codec::decode_writer_reports(report.payload(), &metadata)
                            .map_err(corrupt)?
                    {
                        let file = collector.convert_writer_report(decoded).map_err(corrupt)?;
                        collector.abort_log.record_data_file(file.path);
                    }
                }
            }
        }
        Ok(collector.abort_log)
    }

    fn abort_prepared(&self, prepared: &PreparedOperation) -> ExternalMutationFinalization {
        let Some(write) = &prepared.write else {
            return ExternalMutationFinalization::Complete;
        };
        let access = match self
            .runtime
            .resources()
            .planning_binding()
            .resolve_access(prepared.staged.table.metadata().location())
        {
            Ok(access) => access,
            Err(error) => {
                return cleanup_failed(format!("resolve staged-create cleanup access: {error}"));
            }
        };
        let operator = access.operator();
        let cleanup_access = access.clone();
        let abort = Arc::clone(&write.abort_handle);
        let cleanup = match self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                abort
                    .cleanup_with_path_mapper(&operator, move |path| {
                        cleanup_access
                            .bind_location(path, novarocks_fs::FileIdentity::new(path, 0, None))
                            .map(|file| file.operator_relative_path().to_string())
                            .unwrap_or_else(|_| path.to_string())
                    })
                    .await
            }) {
            Ok(cleanup) => cleanup,
            Err(error) => {
                return cleanup_failed(format!("run staged-create cleanup: {error}"));
            }
        };
        if cleanup.is_empty() {
            ExternalMutationFinalization::Complete
        } else {
            let paths = cleanup
                .iter()
                .take(8)
                .map(|error| error.path.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            cleanup_failed(format!(
                "staged-create cleanup failed for {} artifact(s): {paths}",
                cleanup.len()
            ))
        }
    }

    fn finish_write_terminal(
        &self,
        prepared: &PreparedOperation,
        finalization: ExternalMutationFinalization,
    ) -> ExternalMutationFinalization {
        let Some(write) = &prepared.write else {
            return finalization;
        };
        match self
            .write_control
            .finish_staged_terminal(write.completion.sealed().operation_id())
        {
            Ok(()) => finalization,
            Err(error) => match finalization {
                ExternalMutationFinalization::Complete => {
                    cleanup_failed(format!("release staged-create write reservation: {error}"))
                }
                ExternalMutationFinalization::Failed(existing) => cleanup_failed(format!(
                    "{}; release staged-create write reservation: {error}",
                    existing.message()
                )),
            },
        }
    }

    /// Register a catalog-authoritative fenced stage in the exact-generation
    /// foreground cache. The cache only retains writer objects; the external
    /// catalog locator remains the durable recovery authority.
    pub(crate) fn register_fenced_stage(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        staged: RestStagedTableCreate,
        policy: CreatePolicy,
        handle_payload: Bytes,
    ) -> Result<ConnectorStagedTableHandle, ConnectorError> {
        let handle =
            ConnectorStagedTableHandle::try_new(self.owner(), operation_id, handle_payload)?;
        let mut operations = self
            .operations
            .lock()
            .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
        if let Some(existing) = operations.get(&operation_id) {
            return match existing {
                OperationState::Prepared(prepared)
                    if prepared.handle_digest == handle.digest() && prepared.policy == policy =>
                {
                    Ok(handle)
                }
                OperationState::Prepared(_) => Err(invalid(
                    "Iceberg fenced CTAS exact stage replay drifted from the cached target",
                )),
                _ => Err(invalid(
                    "Iceberg fenced CTAS stage action is no longer an unpublished target",
                )),
            };
        }
        operations.insert(
            operation_id,
            OperationState::Prepared(PreparedOperation {
                handle_digest: handle.digest(),
                staged,
                policy,
                planning: None,
                write: None,
            }),
        );
        Ok(handle)
    }

    pub(crate) fn fenced_cleanup_action(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
    ) -> Result<IcebergFencedCleanupAction, ConnectorError> {
        const MAX_CLEANUP_OBJECTS: usize = 256;
        const MAX_CLEANUP_PATH_BYTES: usize = 2 * 1024;

        let prepared = take_prepared(&self.operations, operation_id)?;
        let result = (|| {
            let table_location = prepared
                .staged
                .table
                .metadata()
                .location()
                .trim_end_matches('/');
            let data_prefix = format!(
                "{}/",
                fenced_staging_data_prefix(table_location, operation_id)
            );
            let Some(write) = &prepared.write else {
                return Ok(IcebergFencedCleanupAction {
                    data_prefixes: vec![data_prefix],
                    objects: Vec::new(),
                });
            };
            let (data_files, mut objects) = write.abort_handle.snapshot_paths();
            if data_files
                .iter()
                .any(|path| !path.starts_with(&data_prefix))
            {
                return Err(corrupt(
                    "Iceberg fenced CTAS cleanup data file escaped its operation staging prefix",
                ));
            }
            let table_prefix = format!("{table_location}/");
            if objects.iter().any(|path| !path.starts_with(&table_prefix)) {
                return Err(corrupt(
                    "Iceberg fenced CTAS cleanup manifest escaped its staged table location",
                ));
            }
            objects.sort();
            objects.dedup();
            if objects.len() > MAX_CLEANUP_OBJECTS {
                return Err(invalid(format!(
                    "Iceberg fenced CTAS cleanup exceeds the {MAX_CLEANUP_OBJECTS}-object limit"
                )));
            }
            if std::iter::once(data_prefix.as_str())
                .chain(objects.iter().map(String::as_str))
                .any(|path| {
                    path.len() > MAX_CLEANUP_PATH_BYTES || path.contains('?') || path.contains('#')
                })
            {
                return Err(invalid(
                    "Iceberg fenced CTAS cleanup contains an oversized or credential-bearing path",
                ));
            }
            Ok(IcebergFencedCleanupAction {
                data_prefixes: vec![data_prefix],
                objects,
            })
        })();
        self.record_terminal(operation_id, OperationState::Prepared(prepared));
        result
    }

    pub(crate) fn plan_fenced_write(
        &self,
        request: ConnectorStagedWritePlanningRequest,
    ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
        <Self as ConnectorStagedCreate>::plan_write(self, request)
    }

    pub(crate) fn bind_fenced_write(
        &self,
        handle: ConnectorStagedTableHandle,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), ConnectorError> {
        <Self as ConnectorStagedCreate>::bind_write(self, handle, completion)
    }

    /// Build the standard assert-create request without dispatching it. The
    /// fence-aware catalog executes this payload inside its serialized publish
    /// action, so `TableRequirement::NotExist` is not a client-side fallback.
    pub(crate) fn fenced_publish_commit(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        completion_digest: [u8; 32],
    ) -> Result<TableCommit, ConnectorError> {
        let mut prepared = take_prepared(&self.operations, operation_id)?;
        let Some(write) = prepared.write.as_ref() else {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg fenced CTAS publish requires a bound writer aggregate",
            ));
        };
        if write.completion.aggregate_digest() != completion_digest {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg fenced CTAS publish completion digest is not bound to this target",
            ));
        }
        if !write.action_built {
            let completion = write.completion.clone();
            match self.build_action(&prepared, &completion) {
                Ok((updates, expected_snapshot_id, abort_handle)) => {
                    let write = prepared.write.as_mut().expect("validated staged write");
                    write.updates = updates;
                    write.expected_snapshot_id = expected_snapshot_id;
                    write.abort_handle = abort_handle;
                    write.action_built = true;
                }
                Err(error) => {
                    self.record_terminal(operation_id, OperationState::Prepared(prepared));
                    return Err(error);
                }
            }
        }
        let write = prepared.write.as_ref().expect("built staged write");
        let mut updates = prepared.staged.initialization_updates.clone();
        updates.extend(write.updates.clone());
        let commit = TableCommit::builder()
            .ident(prepared.staged.table.identifier().clone())
            .requirements(vec![TableRequirement::NotExist])
            .updates(updates)
            .build();
        self.record_terminal(operation_id, OperationState::Prepared(prepared));
        Ok(commit)
    }

    pub(crate) fn finish_fenced_published(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
    ) -> Result<(), ConnectorError> {
        let prepared = {
            let mut operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            match operations.remove(&operation_id) {
                Some(OperationState::Published) => {
                    operations.insert(operation_id, OperationState::Published);
                    return Ok(());
                }
                Some(OperationState::Prepared(prepared)) => prepared,
                Some(state) => {
                    operations.insert(operation_id, state);
                    return Err(invalid(
                        "Iceberg fenced CTAS publish cannot finalize this local operation state",
                    ));
                }
                None => return Err(invalid("unknown Iceberg staged-create operation")),
            }
        };
        invalidate_prepared(&self.runtime, &prepared);
        let finalization =
            self.finish_write_terminal(&prepared, ExternalMutationFinalization::Complete);
        self.record_terminal(operation_id, OperationState::Published);
        match finalization {
            ExternalMutationFinalization::Complete => Ok(()),
            ExternalMutationFinalization::Failed(failure) => Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                failure.message(),
            )),
        }
    }

    pub(crate) fn finish_fenced_aborted(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
    ) -> Result<(), ConnectorError> {
        let prepared = {
            let mut operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            match operations.remove(&operation_id) {
                Some(OperationState::Aborted) => {
                    operations.insert(operation_id, OperationState::Aborted);
                    return Ok(());
                }
                Some(OperationState::Prepared(prepared)) => prepared,
                Some(state) => {
                    operations.insert(operation_id, state);
                    return Err(invalid(
                        "Iceberg fenced CTAS abort cannot finalize this local operation state",
                    ));
                }
                None => return Err(invalid("unknown Iceberg staged-create operation")),
            }
        };
        let finalization =
            self.finish_write_terminal(&prepared, ExternalMutationFinalization::Complete);
        self.record_terminal(operation_id, OperationState::Aborted);
        match finalization {
            ExternalMutationFinalization::Complete => Ok(()),
            ExternalMutationFinalization::Failed(failure) => Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                failure.message(),
            )),
        }
    }
}

impl ConnectorStagedCreate for IcebergStagedCreateAdapter {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    fn prepare(
        &self,
        request: ConnectorStagedCreatePrepareRequest,
    ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
        if request.owner != self.owner() || request.table.instance_id != self.descriptor.instance_id
        {
            return Err(invalid("Iceberg staged-create prepare has a foreign owner"));
        }
        if let Err(error) = Self::validate_context(&request.context) {
            return Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                failure: failure_from_connector(error),
            });
        }
        {
            let mut operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            if operations.contains_key(&request.operation_id) {
                return Err(invalid(
                    "Iceberg staged-create operation ID is already reserved",
                ));
            }
            operations.insert(request.operation_id, OperationState::Preparing);
        }

        let mut properties = request.properties;
        properties.retain(|key, _| !key.eq_ignore_ascii_case(CTAS_OPERATION_MARKER));
        properties.insert(
            Arc::from(CTAS_OPERATION_MARKER),
            Arc::from(operation_marker(request.operation_id)),
        );
        let properties = properties.into_iter().collect::<Vec<_>>();
        let result = prepare_rest_staged_table(
            &self.runtime,
            request.operation_id,
            &request.table.namespace,
            &request.table.table,
            &request.columns,
            &request.partitioning,
            &properties,
        );
        match result {
            Ok(staged) => {
                let payload = Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes());
                let handle = ConnectorStagedTableHandle::try_new(
                    self.owner(),
                    request.operation_id,
                    payload.clone(),
                )?;
                self.record_terminal(
                    request.operation_id,
                    OperationState::Prepared(PreparedOperation {
                        handle_digest: handle.digest(),
                        staged,
                        policy: request.policy,
                        planning: None,
                        write: None,
                    }),
                );
                Ok(ConnectorStagedCreatePrepareOutcome::Prepared {
                    handle,
                    receipt: self.receipt(
                        request.operation_id,
                        ConnectorStagedCreateReceiptPhase::Prepared,
                        ExternalMutationEffect::Applied,
                        payload,
                    )?,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            Err(RestStagedPrepareFailure::Conflict(message)) => {
                self.operations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.operation_id);
                Ok(ConnectorStagedCreatePrepareOutcome::Conflict {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::AlreadyExists,
                        message,
                    ),
                })
            }
            Err(RestStagedPrepareFailure::KnownUncommitted(message)) => {
                self.operations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.operation_id);
                Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Internal,
                        message,
                    ),
                })
            }
            Err(RestStagedPrepareFailure::CommitUnknown(message)) => {
                let evidence = self.evidence(
                    request.operation_id,
                    ConnectorStagedCreateReconcilePhase::Prepare,
                    Bytes::copy_from_slice(&request.operation_id.to_bytes()),
                )?;
                self.set_unknown(
                    request.operation_id,
                    ConnectorStagedCreateReconcilePhase::Prepare,
                    &evidence,
                    None,
                );
                Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        message,
                    ),
                    evidence,
                })
            }
        }
    }

    fn plan_write(
        &self,
        request: ConnectorStagedWritePlanningRequest,
    ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
        Self::validate_context(&request.context)?;
        if request.handle.owner() != &self.owner() {
            return Err(invalid(
                "Iceberg staged writer planning has a foreign owner",
            ));
        }
        let target_operation_id = request.handle.operation_id();
        let mut prepared = take_prepared(&self.operations, target_operation_id)?;
        if prepared.handle_digest != request.handle.digest() || prepared.write.is_some() {
            self.record_terminal(target_operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged writer planning handle is stale or already bound",
            ));
        }
        if let Some(existing) = &prepared.planning {
            if existing.operation_id() == request.operation_id
                && existing.intent() == request.intent
                && existing.input_schema().as_ref() == request.input_schema.as_ref()
            {
                let existing = existing.clone();
                self.record_terminal(target_operation_id, OperationState::Prepared(prepared));
                return Ok(existing);
            }
            self.record_terminal(target_operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged target already has a different writer planning binding",
            ));
        }
        let result = self
            .provider
            .staged_write_table_handle(
                &prepared.staged.table,
                target_operation_id,
                &request.context,
            )
            .and_then(|table| {
                ConnectorStagedWritePlanningBinding::try_new(
                    &request.handle,
                    request.operation_id,
                    request.intent,
                    Arc::clone(&request.input_schema),
                    table,
                    Bytes::new(),
                    request.context.clone(),
                )
            });
        match result {
            Ok(binding) => {
                prepared.planning = Some(binding.clone());
                self.record_terminal(target_operation_id, OperationState::Prepared(prepared));
                Ok(binding)
            }
            Err(error) => {
                self.record_terminal(target_operation_id, OperationState::Prepared(prepared));
                Err(error)
            }
        }
    }

    fn bind_write(
        &self,
        handle: ConnectorStagedTableHandle,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), ConnectorError> {
        if handle.owner() != &self.owner() || completion.owner() != &self.owner() {
            return Err(invalid(
                "Iceberg staged-create write binding has a foreign owner",
            ));
        }
        let operation_id = handle.operation_id();
        let mut prepared = take_prepared(&self.operations, operation_id)?;
        if prepared.handle_digest != handle.digest()
            || prepared.write.is_some()
            || prepared.planning.as_ref().is_none_or(|planning| {
                planning.operation_id() != completion.sealed().operation_id()
            })
        {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create write binding is stale, unplanned, or already bound",
            ));
        }
        if let Err(error) = self.write_control.validate_staged_completion(&completion) {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(error);
        }
        let abort_handle = match self.completion_abort_log(&prepared, &completion) {
            Ok(abort_handle) => abort_handle,
            Err(error) => {
                self.record_terminal(operation_id, OperationState::Prepared(prepared));
                return Err(error);
            }
        };
        prepared.write = Some(StagedWrite {
            completion,
            updates: Vec::new(),
            expected_snapshot_id: None,
            abort_handle,
            action_built: false,
        });
        self.record_terminal(operation_id, OperationState::Prepared(prepared));
        Ok(())
    }

    fn publish(
        &self,
        request: ConnectorStagedCreatePublishRequest,
    ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
        if request.handle.owner() != &self.owner() || request.completion.owner() != &self.owner() {
            return Err(invalid("Iceberg staged-create publish has a foreign owner"));
        }
        let operation_id = request.handle.operation_id();
        let mut prepared = take_prepared(&self.operations, operation_id)?;
        if prepared.handle_digest != request.handle.digest() {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create publish handle digest mismatch",
            ));
        }
        let Some(write) = prepared.write.as_ref() else {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create publish requires a bound writer aggregate",
            ));
        };
        if write.completion.aggregate_digest() != request.completion.aggregate_digest()
            || write.completion.sealed().operation_id()
                != request.completion.sealed().operation_id()
        {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create publish completion is not bound to this target",
            ));
        }
        if let Err(error) = Self::validate_context(&request.context) {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                failure: failure_from_connector(error),
            });
        }
        if !write.action_built {
            match self.build_action(&prepared, &request.completion) {
                Ok((updates, expected_snapshot_id, abort_handle)) => {
                    let write = prepared.write.as_mut().expect("validated staged write");
                    write.updates = updates;
                    write.expected_snapshot_id = expected_snapshot_id;
                    write.abort_handle = abort_handle;
                    write.action_built = true;
                }
                Err(error) => {
                    self.record_terminal(operation_id, OperationState::Prepared(prepared));
                    return Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                        failure: failure_from_connector(error),
                    });
                }
            }
        }
        if let Err(error) = Self::validate_context(&request.context) {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                failure: failure_from_connector(error),
            });
        }
        let write = prepared.write.as_ref().expect("built staged write");
        let mut updates = prepared.staged.initialization_updates.clone();
        updates.extend(write.updates.clone());
        let expected_snapshot_id = write.expected_snapshot_id;
        let commit = TableCommit::builder()
            .ident(prepared.staged.table.identifier().clone())
            .requirements(vec![TableRequirement::NotExist])
            .updates(updates)
            .build();
        let catalog = Arc::clone(&prepared.staged.catalog);
        let result = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { catalog.commit_staged_table_typed(commit).await });
        match result {
            Ok(Ok(table))
                if publication_matches(&table, operation_id, &prepared, expected_snapshot_id) =>
            {
                let receipt =
                    publication_receipt(self, request.operation_id, &table, expected_snapshot_id)?;
                invalidate_prepared(&self.runtime, &prepared);
                let finalization =
                    self.finish_write_terminal(&prepared, ExternalMutationFinalization::Complete);
                self.record_terminal(operation_id, OperationState::Published);
                Ok(ConnectorStagedCreatePublishOutcome::Applied {
                    receipt,
                    finalization,
                })
            }
            Ok(Ok(_)) => {
                let evidence = self.publish_evidence(
                    request.operation_id,
                    operation_id,
                    &prepared,
                    expected_snapshot_id,
                )?;
                self.set_unknown(
                    operation_id,
                    ConnectorStagedCreateReconcilePhase::Publish,
                    &evidence,
                    Some(prepared),
                );
                Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "REST response did not prove the exact staged-create publication",
                    ),
                    evidence,
                })
            }
            Ok(Err(crate::iceberg_catalog_rest::StagedCommitError::Conflict(error))) => {
                if prepared.policy == CreatePolicy::NoOpIfExists {
                    let finalization = self.abort_prepared(&prepared);
                    let finalization = self.finish_write_terminal(&prepared, finalization);
                    self.record_terminal(operation_id, OperationState::Published);
                    Ok(ConnectorStagedCreatePublishOutcome::NoOp {
                        receipt: self.receipt(
                            request.operation_id,
                            ConnectorStagedCreateReceiptPhase::Published,
                            ExternalMutationEffect::NoOp,
                            Bytes::new(),
                        )?,
                        finalization,
                    })
                } else {
                    self.record_terminal(operation_id, OperationState::Prepared(prepared));
                    Ok(ConnectorStagedCreatePublishOutcome::Conflict {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Conflict,
                            error.to_string(),
                        ),
                    })
                }
            }
            Ok(Err(crate::iceberg_catalog_rest::StagedCommitError::KnownNotDispatched(error))) => {
                self.record_terminal(operation_id, OperationState::Prepared(prepared));
                Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        error.to_string(),
                    ),
                })
            }
            Ok(Err(crate::iceberg_catalog_rest::StagedCommitError::PossiblyDispatched(error))) => {
                let evidence = self.publish_evidence(
                    request.operation_id,
                    operation_id,
                    &prepared,
                    expected_snapshot_id,
                )?;
                self.set_unknown(
                    operation_id,
                    ConnectorStagedCreateReconcilePhase::Publish,
                    &evidence,
                    Some(prepared),
                );
                Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        error.to_string(),
                    ),
                    evidence,
                })
            }
            Ok(Err(crate::iceberg_catalog_rest::StagedCommitError::CommittedResponseInvalid(
                error,
            ))) => {
                let receipt = self.receipt(
                    request.operation_id,
                    ConnectorStagedCreateReceiptPhase::Published,
                    ExternalMutationEffect::Applied,
                    Bytes::copy_from_slice(&operation_id.to_bytes()),
                )?;
                invalidate_prepared(&self.runtime, &prepared);
                let finalization = self.finish_write_terminal(
                    &prepared,
                    ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        format!(
                            "REST staged-create publication committed but response finalization failed: {error}"
                        ),
                    )),
                );
                self.record_terminal(operation_id, OperationState::Published);
                Ok(ConnectorStagedCreatePublishOutcome::Applied {
                    receipt,
                    finalization,
                })
            }
            Err(error) => {
                self.record_terminal(operation_id, OperationState::Prepared(prepared));
                Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Internal,
                        error,
                    ),
                })
            }
        }
    }

    fn abort(
        &self,
        request: ConnectorStagedCreateAbortRequest,
    ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
        if request.handle.owner() != &self.owner() {
            return Err(invalid("Iceberg staged-create abort has a foreign owner"));
        }
        let operation_id = request.handle.operation_id();
        let prepared = take_prepared(&self.operations, operation_id)?;
        if prepared.handle_digest != request.handle.digest() {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create abort handle digest mismatch",
            ));
        }
        if request.completion.as_ref().is_some_and(|completion| {
            prepared.write.as_ref().is_none_or(|write| {
                write.completion.aggregate_digest() != completion.aggregate_digest()
                    || write.completion.sealed().operation_id()
                        != completion.sealed().operation_id()
            })
        }) {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid("Iceberg staged-create abort completion mismatch"));
        }
        let finalization = self.abort_prepared(&prepared);
        let finalization = self.finish_write_terminal(&prepared, finalization);
        self.record_terminal(operation_id, OperationState::Aborted);
        Ok(ConnectorStagedCreateAbortOutcome::Aborted {
            receipt: self.receipt(
                request.operation_id,
                ConnectorStagedCreateReceiptPhase::Aborted,
                ExternalMutationEffect::Applied,
                Bytes::new(),
            )?,
            finalization,
        })
    }

    fn reconcile(
        &self,
        request: ConnectorStagedCreateReconcileRequest,
    ) -> Result<ConnectorStagedCreateReconcileOutcome, ConnectorError> {
        Self::validate_context(&request.context)?;
        if request.evidence.descriptor() != &self.descriptor
            || request.evidence.incarnation() != self.incarnation
            || request.evidence.operation_kind() != operation_kind(request.phase)
        {
            return Err(invalid(
                "Iceberg staged-create reconcile evidence is foreign",
            ));
        }
        let operation_id = request.target_operation_id;
        let dispatch_operation_id = request.evidence.operation_id();
        let unknown = {
            let operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            let Some(OperationState::Unknown(unknown)) = operations.get(&operation_id) else {
                return Err(invalid(
                    "Iceberg staged-create reconcile requires the exact unresolved operation",
                ));
            };
            unknown.clone()
        };
        if unknown.phase != request.phase || unknown.evidence_digest != request.evidence.digest() {
            return Err(invalid(
                "Iceberg staged-create reconcile evidence digest or phase mismatch",
            ));
        }
        if request.phase == ConnectorStagedCreateReconcilePhase::Prepare {
            return Ok(ConnectorStagedCreateReconcileOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "Iceberg staged-create prepare remains unresolved",
                ),
                evidence: request.evidence,
            });
        }
        let Some(prepared) = unknown.prepared else {
            return Err(invalid(
                "Iceberg staged-create reconcile lost its exact prepared operation",
            ));
        };
        if request.phase == ConnectorStagedCreateReconcilePhase::Abort {
            let finalization = self.abort_prepared(&prepared);
            let finalization = self.finish_write_terminal(&prepared, finalization);
            self.record_terminal(operation_id, OperationState::Aborted);
            return Ok(ConnectorStagedCreateReconcileOutcome::Aborted {
                receipt: self.receipt(
                    dispatch_operation_id,
                    ConnectorStagedCreateReceiptPhase::Aborted,
                    ExternalMutationEffect::Applied,
                    Bytes::new(),
                )?,
                finalization,
            });
        }
        let evidence: PublishEvidenceV1 =
            serde_json::from_slice(request.evidence.provider_payload()).map_err(|error| {
                invalid(format!(
                    "Iceberg staged-create publish evidence is invalid: {error}"
                ))
            })?;
        let ident = prepared.staged.table.identifier();
        if evidence.version != EVIDENCE_VERSION
            || evidence.operation_marker != operation_marker(operation_id)
            || evidence.handle_digest != prepared.handle_digest
            || evidence.table_uuid != prepared.staged.table.metadata().uuid().to_string()
            || evidence.namespace != ident.namespace.to_url_string()
            || evidence.table != ident.name
        {
            return Err(invalid(
                "Iceberg staged-create publish evidence does not match the exact operation",
            ));
        }
        let catalog: Arc<dyn Catalog> = prepared.staged.catalog.clone();
        let ident = ident.clone();
        let load = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { catalog.load_table(&ident).await });
        match load {
            Ok(Ok(table))
                if publication_matches(
                    &table,
                    operation_id,
                    &prepared,
                    evidence.expected_snapshot_id,
                ) =>
            {
                let receipt = publication_receipt(
                    self,
                    dispatch_operation_id,
                    &table,
                    evidence.expected_snapshot_id,
                )?;
                invalidate_prepared(&self.runtime, &prepared);
                let finalization =
                    self.finish_write_terminal(&prepared, ExternalMutationFinalization::Complete);
                self.record_terminal(operation_id, OperationState::Published);
                Ok(ConnectorStagedCreateReconcileOutcome::Published {
                    receipt,
                    finalization,
                })
            }
            Ok(Ok(table)) if table.metadata().uuid().to_string() != evidence.table_uuid => {
                self.record_terminal(operation_id, OperationState::Prepared(prepared));
                Ok(ConnectorStagedCreateReconcileOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        "a different table is authoritative at the staged-create target",
                    ),
                })
            }
            Ok(Ok(_)) => Ok(ConnectorStagedCreateReconcileOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "the target does not yet prove the exact staged-create publication",
                ),
                evidence: request.evidence,
            }),
            Ok(Err(error)) if error.kind() == ErrorKind::TableNotFound => {
                self.record_terminal(operation_id, OperationState::Prepared(prepared));
                Ok(ConnectorStagedCreateReconcileOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "the staged-create target is authoritatively absent",
                    ),
                })
            }
            Ok(Err(error)) => Ok(ConnectorStagedCreateReconcileOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    format!("authoritative staged-create reload failed: {error}"),
                ),
                evidence: request.evidence,
            }),
            Err(error) => Ok(ConnectorStagedCreateReconcileOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    format!("authoritative staged-create reload runtime failed: {error}"),
                ),
                evidence: request.evidence,
            }),
        }
    }
}

fn take_prepared(
    operations: &Mutex<HashMap<ConnectorStagedCreateOperationId, OperationState>>,
    operation_id: ConnectorStagedCreateOperationId,
) -> Result<PreparedOperation, ConnectorError> {
    let mut operations = operations
        .lock()
        .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
    match operations.remove(&operation_id) {
        Some(OperationState::Prepared(prepared)) => Ok(prepared),
        Some(state) => {
            operations.insert(operation_id, state);
            Err(invalid(
                "Iceberg staged-create operation is not an unpublished prepared target",
            ))
        }
        None => Err(invalid("unknown Iceberg staged-create operation")),
    }
}

fn publication_matches(
    table: &crate::iceberg::table::Table,
    operation_id: ConnectorStagedCreateOperationId,
    prepared: &PreparedOperation,
    expected_snapshot_id: Option<i64>,
) -> bool {
    let metadata = table.metadata();
    metadata.uuid() == prepared.staged.table.metadata().uuid()
        && metadata
            .properties()
            .get(CTAS_OPERATION_MARKER)
            .is_some_and(|marker| marker == &operation_marker(operation_id))
        && expected_snapshot_id
            .is_none_or(|snapshot_id| metadata.snapshot_by_id(snapshot_id).is_some())
}

fn publication_receipt(
    adapter: &IcebergStagedCreateAdapter,
    operation_id: ConnectorStagedCreateOperationId,
    table: &crate::iceberg::table::Table,
    expected_snapshot_id: Option<i64>,
) -> Result<ConnectorStagedCreateReceipt, ConnectorError> {
    let mut payload = Vec::with_capacity(24);
    payload.extend_from_slice(table.metadata().uuid().as_bytes());
    payload.extend_from_slice(&expected_snapshot_id.unwrap_or(0).to_be_bytes());
    adapter.receipt(
        operation_id,
        ConnectorStagedCreateReceiptPhase::Published,
        ExternalMutationEffect::Applied,
        Bytes::from(payload),
    )
}

fn invalidate_prepared(runtime: &IcebergControlRuntime, prepared: &PreparedOperation) {
    let ident = prepared.staged.table.identifier();
    runtime
        .control_state()
        .invalidate_table_cache(&ident.namespace.to_url_string(), &ident.name);
}

fn cleanup_failed(message: impl Into<Arc<str>>) -> ExternalMutationFinalization {
    ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
        ConnectorMutationFailureKind::Unavailable,
        message,
    ))
}

fn failure_from_connector(error: ConnectorError) -> ConnectorMutationFailure {
    let kind = match error.kind() {
        ConnectorErrorKind::InvalidRequest => ConnectorMutationFailureKind::InvalidRequest,
        ConnectorErrorKind::NotFound => ConnectorMutationFailureKind::NotFound,
        ConnectorErrorKind::PermissionDenied => ConnectorMutationFailureKind::PermissionDenied,
        ConnectorErrorKind::Unsupported => ConnectorMutationFailureKind::Unsupported,
        ConnectorErrorKind::Cancelled => ConnectorMutationFailureKind::Cancelled,
        ConnectorErrorKind::DeadlineExceeded => ConnectorMutationFailureKind::DeadlineExceeded,
        ConnectorErrorKind::ResourceExhausted => ConnectorMutationFailureKind::ResourceExhausted,
        ConnectorErrorKind::Unavailable => ConnectorMutationFailureKind::Unavailable,
        ConnectorErrorKind::CorruptData => ConnectorMutationFailureKind::CorruptData,
        ConnectorErrorKind::Internal => ConnectorMutationFailureKind::Internal,
    };
    ConnectorMutationFailure::new(kind, error.to_string())
}

fn operation_kind(phase: ConnectorStagedCreateReconcilePhase) -> &'static str {
    match phase {
        ConnectorStagedCreateReconcilePhase::Prepare => "staged-create-prepare",
        ConnectorStagedCreateReconcilePhase::Publish => "staged-create-publish",
        ConnectorStagedCreateReconcilePhase::Abort => "staged-create-abort",
    }
}

fn operation_marker(operation_id: ConnectorStagedCreateOperationId) -> String {
    uuid::Uuid::from_bytes(operation_id.to_bytes()).to_string()
}

/// The CTAS root must be independent of the target table name: before the
/// single `NotExist` commit succeeds there is no table location that cleanup
/// can safely derive from catalog state.  A publication ID gives the staging
/// root a stable, enumerable owner instead.
pub(crate) fn ctas_staging_location(
    warehouse_uri: &str,
    operation_id: ConnectorStagedCreateOperationId,
) -> Result<String, RestStagedPrepareFailure> {
    let warehouse_uri = warehouse_uri.trim_end_matches('/');
    if warehouse_uri.is_empty() {
        return Err(RestStagedPrepareFailure::KnownUncommitted(
            "standard REST CTAS requires an explicit warehouse URI for its staging namespace"
                .to_string(),
        ));
    }
    Ok(format!(
        "{warehouse_uri}/{CTAS_STAGING_NAMESPACE}/{}/table",
        operation_marker(operation_id)
    ))
}

pub(crate) fn fenced_staging_data_prefix(
    table_location: &str,
    operation_id: ConnectorStagedCreateOperationId,
) -> String {
    format!(
        "{}/data/_staging/{}",
        table_location.trim_end_matches('/'),
        operation_marker(operation_id)
    )
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergControlResources;
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorMutationOperationId, ConnectorProviderId,
    };

    fn hadoop_runtime() -> IcebergControlRuntime {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let handle = executor.handle().clone();
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle.clone())),
        );
        IcebergControlRuntime::try_new(
            IcebergCatalogControlState::new(configuration),
            IcebergControlResources::new(binding, handle),
        )
        .expect("control runtime")
    }

    #[test]
    fn hadoop_generation_fails_closed_without_constructing_a_rest_client() {
        let runtime = hadoop_runtime();
        assert!(runtime.rest_catalog().is_none());
        let failure = match prepare_rest_staged_table(
            &runtime,
            ConnectorMutationOperationId::new(),
            "db",
            "t",
            &[],
            &[],
            &[],
        ) {
            Ok(_) => panic!("Hadoop must not expose a REST staged-create surface"),
            Err(failure) => failure,
        };
        assert!(matches!(
            failure,
            RestStagedPrepareFailure::KnownUncommitted(message)
                if message.contains("unsupported")
        ));
    }

    #[test]
    fn staged_capability_is_rest_only_for_the_exact_generation() {
        let runtime = Arc::new(hadoop_runtime());
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let provider = Arc::new(IcebergControlProvider::new(
            descriptor.clone(),
            ConnectorInstanceIncarnation::new(),
            Arc::clone(&runtime),
        ));
        let write = Arc::new(IcebergWriteControl::new(
            descriptor,
            provider.incarnation(),
            runtime,
        ));
        let error = match IcebergStagedCreateAdapter::try_new(provider, write) {
            Ok(_) => panic!("Hadoop must not expose staged create"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    }

    #[test]
    fn operation_marker_is_a_canonical_operation_uuid() {
        let operation_id = ConnectorMutationOperationId::new();
        assert_eq!(
            operation_marker(operation_id),
            uuid::Uuid::from_bytes(operation_id.to_bytes()).to_string()
        );
    }

    #[test]
    fn ctas_staging_location_is_warehouse_rooted_and_operation_bound() {
        let operation_id = ConnectorMutationOperationId::new();
        assert_eq!(
            ctas_staging_location("s3://warehouse/root/", operation_id).unwrap(),
            format!(
                "s3://warehouse/root/_novarocks/ctas-staging/v1/{}/table",
                operation_marker(operation_id)
            )
        );
    }

    #[test]
    fn ctas_staging_location_rejects_an_implicit_warehouse() {
        let error = ctas_staging_location("", ConnectorMutationOperationId::new()).unwrap_err();
        assert!(matches!(
            error,
            RestStagedPrepareFailure::KnownUncommitted(message)
                if message.contains("explicit warehouse URI")
        ));
    }

    #[test]
    fn fenced_staging_prefix_is_operation_bound_and_canonical() {
        let operation_id = ConnectorMutationOperationId::new();
        assert_eq!(
            fenced_staging_data_prefix("s3://warehouse/db/table/", operation_id),
            format!(
                "s3://warehouse/db/table/data/_staging/{}",
                operation_marker(operation_id)
            )
        );
    }

    #[test]
    fn cleanup_failure_is_not_overwritten_as_complete() {
        let ExternalMutationFinalization::Failed(failure) =
            cleanup_failed("delete staged manifest failed")
        else {
            panic!("cleanup failure must remain visible")
        };
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Unavailable);
        assert!(failure.message().contains("staged manifest"));
    }
}
