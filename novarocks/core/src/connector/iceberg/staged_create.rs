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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use iceberg::{Catalog, ErrorKind, TableCommit, TableRequirement, TableUpdate};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
    ConnectorInstanceIncarnation, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorStagedCreate, ConnectorStagedCreateAbortOutcome, ConnectorStagedCreateAbortRequest,
    ConnectorStagedCreatePrepareOutcome, ConnectorStagedCreatePrepareRequest,
    ConnectorStagedCreatePublishOutcome, ConnectorStagedCreatePublishRequest,
    ConnectorStagedCreateReceipt, ConnectorStagedCreateReceiptPhase,
    ConnectorStagedCreateReconcileOutcome, ConnectorStagedCreateReconcilePhase,
    ConnectorStagedCreateReconcileRequest, ConnectorStagedTableHandle,
    ConnectorStagedWritePlanningBinding, ConnectorStagedWritePlanningRequest,
    ConnectorWriteOperationCompletion, CreatePolicy, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization,
};

use super::catalog::registry::{
    IcebergCatalogEntry, RestStagedPrepareFailure, RestStagedTableCreate, block_on_iceberg,
    prepare_rest_staged_table,
};
use super::write_service::IcebergWriteServiceRegistry;

const EVIDENCE_VERSION: u16 = 1;
const CTAS_OPERATION_MARKER: &str = "novarocks.ctas.operation-id";

#[derive(Clone)]
pub(crate) struct IcebergStagedCreateAdapter {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    entry: IcebergCatalogEntry,
    write_services: IcebergWriteServiceRegistry,
    operations: Arc<
        Mutex<HashMap<novarocks_spi::connector::ConnectorStagedCreateOperationId, OperationState>>,
    >,
}

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
    abort_handle: Arc<super::commit::AbortLog>,
    action_built: bool,
}

#[derive(Clone)]
struct UnknownOperation {
    phase: ConnectorStagedCreateReconcilePhase,
    evidence_digest: [u8; 32],
    prepared: Option<PreparedOperation>,
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

#[derive(serde::Serialize, serde::Deserialize)]
struct AbortEvidenceV1 {
    version: u16,
    target_operation_id: [u8; 16],
    handle_digest: [u8; 32],
    write_operation_id: Option<[u8; 16]>,
    write_aggregate_digest: Option<[u8; 32]>,
}

impl IcebergStagedCreateAdapter {
    pub(crate) fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        entry: IcebergCatalogEntry,
        write_services: IcebergWriteServiceRegistry,
    ) -> Self {
        Self {
            descriptor,
            incarnation,
            entry,
            write_services,
            operations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn owner(&self) -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        }
    }

    fn receipt(
        &self,
        operation_id: novarocks_spi::connector::ConnectorStagedCreateOperationId,
        phase: ConnectorStagedCreateReceiptPhase,
        effect: ExternalMutationEffect,
        payload: Bytes,
    ) -> Result<ConnectorStagedCreateReceipt, ConnectorError> {
        ConnectorStagedCreateReceipt::try_new(self.owner(), operation_id, phase, effect, payload)
    }

    fn bounded_receipt(
        &self,
        operation_id: novarocks_spi::connector::ConnectorStagedCreateOperationId,
        phase: ConnectorStagedCreateReceiptPhase,
        effect: ExternalMutationEffect,
        payload: Bytes,
    ) -> ConnectorStagedCreateReceipt {
        self.receipt(operation_id, phase, effect, payload)
            .expect("provider-generated staged-create receipt is bounded and owner-exact")
    }

    fn record_terminal(
        &self,
        operation_id: novarocks_spi::connector::ConnectorStagedCreateOperationId,
        state: OperationState,
    ) {
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(operation_id, state);
    }

    fn evidence(
        &self,
        operation_id: novarocks_spi::connector::ConnectorStagedCreateOperationId,
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
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        target_operation_id: novarocks_spi::connector::ConnectorStagedCreateOperationId,
        prepared: &PreparedOperation,
        expected_snapshot_id: Option<i64>,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let payload = serde_json::to_vec(&PublishEvidenceV1 {
            version: EVIDENCE_VERSION,
            operation_marker: operation_marker(target_operation_id),
            table_uuid: prepared.staged.table.metadata().uuid().to_string(),
            expected_snapshot_id,
            handle_digest: prepared.handle_digest,
            namespace: prepared.staged.table.identifier().namespace.to_url_string(),
            table: prepared.staged.table.identifier().name.clone(),
        })
        .map(Bytes::from)
        .map_err(|error| internal(format!("encode staged publish evidence: {error}")))?;
        self.evidence(
            operation_id,
            ConnectorStagedCreateReconcilePhase::Publish,
            payload,
        )
    }

    fn abort_evidence(
        &self,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        target_operation_id: novarocks_spi::connector::ConnectorStagedCreateOperationId,
        prepared: &PreparedOperation,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let payload = serde_json::to_vec(&AbortEvidenceV1 {
            version: EVIDENCE_VERSION,
            target_operation_id: target_operation_id.to_bytes(),
            handle_digest: prepared.handle_digest,
            write_operation_id: prepared
                .write
                .as_ref()
                .map(|write| write.completion.sealed().operation_id().to_bytes()),
            write_aggregate_digest: prepared
                .write
                .as_ref()
                .map(|write| write.completion.aggregate_digest()),
        })
        .map(Bytes::from)
        .map_err(|error| internal(format!("encode staged abort evidence: {error}")))?;
        self.evidence(
            operation_id,
            ConnectorStagedCreateReconcilePhase::Abort,
            payload,
        )
    }

    fn validate_abort_evidence(
        &self,
        operation_id: novarocks_spi::connector::ConnectorStagedCreateOperationId,
        prepared: &PreparedOperation,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), ConnectorError> {
        let decoded: AbortEvidenceV1 = serde_json::from_slice(evidence.provider_payload())
            .map_err(|error| {
                invalid(format!("Iceberg staged abort evidence is invalid: {error}"))
            })?;
        let write_operation_id = prepared
            .write
            .as_ref()
            .map(|write| write.completion.sealed().operation_id().to_bytes());
        let write_aggregate_digest = prepared
            .write
            .as_ref()
            .map(|write| write.completion.aggregate_digest());
        if decoded.version != EVIDENCE_VERSION
            || decoded.target_operation_id != operation_id.to_bytes()
            || decoded.handle_digest != prepared.handle_digest
            || decoded.write_operation_id != write_operation_id
            || decoded.write_aggregate_digest != write_aggregate_digest
        {
            return Err(invalid(
                "Iceberg staged abort evidence does not match the exact prepared operation",
            ));
        }
        Ok(())
    }

    fn set_unknown(
        &self,
        operation_id: novarocks_spi::connector::ConnectorStagedCreateOperationId,
        phase: ConnectorStagedCreateReconcilePhase,
        evidence: &ExternalMutationEvidence,
        prepared: Option<PreparedOperation>,
    ) {
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                operation_id,
                OperationState::Unknown(UnknownOperation {
                    phase,
                    evidence_digest: evidence.digest(),
                    prepared,
                }),
            );
    }

    fn abort_prepared(
        &self,
        prepared: &PreparedOperation,
    ) -> Result<ExternalMutationFinalization, ConnectorError> {
        let Some(write) = &prepared.write else {
            // REST staged-create has no server-side abort token. Before a
            // writer aggregate is attached, discarding this opaque provider
            // state is a provider-confirmed no-op and never drops by name.
            return Ok(ExternalMutationFinalization::Complete);
        };
        self.write_services
            .abort_staged_create_completion(&write.completion, &write.abort_handle)
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
        if request.context.cancellation().is_cancelled() {
            return Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Cancelled,
                    "Iceberg staged-create prepare was cancelled before dispatch",
                ),
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

        let entry = &self.entry;
        if !matches!(
            entry.kind,
            super::catalog::registry::IcebergCatalogKind::Rest
        ) {
            self.operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?
                .remove(&request.operation_id);
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Iceberg catalog has no atomic staged-create publication capability",
            ));
        }
        let columns = request
            .columns
            .iter()
            .map(super::provider::lower_column)
            .collect::<Result<Vec<_>, _>>()?;
        let partitioning = request
            .partitioning
            .iter()
            .map(super::provider::lower_partition)
            .collect::<Result<Vec<_>, _>>()?;
        let mut properties = request
            .properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<Vec<_>>();
        properties.push((
            CTAS_OPERATION_MARKER.to_string(),
            operation_marker(request.operation_id),
        ));
        match prepare_rest_staged_table(
            entry,
            &request.table.namespace,
            &request.table.table,
            &columns,
            &partitioning,
            &properties,
        ) {
            Ok(staged) => {
                let payload = Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes());
                let handle = ConnectorStagedTableHandle::try_new(
                    self.owner(),
                    request.operation_id,
                    payload.clone(),
                )?;
                let prepared = PreparedOperation {
                    handle_digest: handle.digest(),
                    staged,
                    policy: request.policy,
                    planning: None,
                    write: None,
                };
                self.operations
                    .lock()
                    .map_err(|error| internal(format!("staged-create operation lock: {error}")))?
                    .insert(request.operation_id, OperationState::Prepared(prepared));
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
            Err(RestStagedPrepareFailure::KnownUncommitted(message)) => {
                self.operations
                    .lock()
                    .map_err(|error| internal(format!("staged-create operation lock: {error}")))?
                    .remove(&request.operation_id);
                Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Internal,
                        message,
                    ),
                })
            }
            Err(RestStagedPrepareFailure::Conflict(message)) => {
                self.operations
                    .lock()
                    .map_err(|error| internal(format!("staged-create operation lock: {error}")))?
                    .remove(&request.operation_id);
                Ok(ConnectorStagedCreatePrepareOutcome::Conflict {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::AlreadyExists,
                        message,
                    ),
                })
            }
            Err(RestStagedPrepareFailure::CommitUnknown(message)) => {
                let evidence = self.evidence(
                    request.operation_id,
                    ConnectorStagedCreateReconcilePhase::Prepare,
                    Bytes::copy_from_slice(request.operation_id.to_bytes().as_slice()),
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
        let mut prepared = {
            let mut operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            let Some(OperationState::Prepared(prepared)) = operations.remove(&operation_id) else {
                return Err(invalid(
                    "Iceberg staged-create write binding requires an unpublished exact handle",
                ));
            };
            if prepared.handle_digest != handle.digest()
                || prepared.write.is_some()
                || prepared.planning.as_ref().is_none_or(|planning| {
                    planning.operation_id() != completion.sealed().operation_id()
                })
            {
                operations.insert(operation_id, OperationState::Prepared(prepared));
                return Err(invalid(
                    "Iceberg staged-create write binding handle is stale or already bound",
                ));
            }
            prepared
        };
        prepared.write = Some(StagedWrite {
            completion,
            updates: Vec::new(),
            expected_snapshot_id: None,
            abort_handle: Arc::new(super::commit::AbortLog::new()),
            action_built: false,
        });
        self.operations
            .lock()
            .map_err(|error| internal(format!("staged-create operation lock: {error}")))?
            .insert(operation_id, OperationState::Prepared(prepared));
        Ok(())
    }

    fn plan_write(
        &self,
        request: ConnectorStagedWritePlanningRequest,
    ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
        if request.handle.owner() != &self.owner() {
            return Err(invalid(
                "Iceberg staged writer planning has a foreign owner",
            ));
        }
        let target_operation_id = request.handle.operation_id();
        let mut prepared = {
            let mut operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            let Some(OperationState::Prepared(prepared)) = operations.remove(&target_operation_id)
            else {
                return Err(invalid(
                    "Iceberg staged writer planning requires an unpublished exact handle",
                ));
            };
            if prepared.handle_digest != request.handle.digest() || prepared.write.is_some() {
                operations.insert(target_operation_id, OperationState::Prepared(prepared));
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
                    operations.insert(target_operation_id, OperationState::Prepared(prepared));
                    return Ok(existing);
                }
                operations.insert(target_operation_id, OperationState::Prepared(prepared));
                return Err(invalid(
                    "Iceberg staged target already has a different writer planning binding",
                ));
            }
            prepared
        };

        let result = (|| {
            let entry = &self.entry;
            let staged_table = &prepared.staged.table;
            let ident = staged_table.identifier();
            let target = crate::engine::backend_resolver::TargetBackend {
                backend_name: "iceberg",
                catalog: self.descriptor.instance_id.as_str().to_string(),
                namespace: ident.namespace.to_url_string(),
                table: ident.name.clone(),
            };
            let columns = crate::engine::iceberg_writer::iceberg_insert_columns_from_schema(
                staged_table.metadata().current_schema(),
            )
            .map_err(internal)?;
            let resolved = crate::connector::backend::ResolvedTable {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
                columns: columns.clone(),
                statistics_pin: None,
            };
            let sink_spec = crate::engine::iceberg_writer::build_insert_write_sink_spec(
                &target,
                &resolved,
                staged_table,
                entry,
                &columns,
            )
            .map_err(internal)?;
            let writer_handle_payload =
                super::write_contract::encode_data_sink_spec_handle_payload(&sink_spec)
                    .map_err(internal)?;
            let metadata = staged_table.metadata();
            let table_ident = staged_table.identifier().clone();
            let collector = Arc::new(
                super::commit::IcebergCommitCollector::new(
                    super::commit::CommitOpKind::FastAppend,
                    table_ident,
                    None,
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
            let cleanup =
                crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(entry)
                    .map_err(internal)?;
            let catalog: Arc<dyn iceberg::Catalog> = prepared.staged.catalog.clone();
            let commit_executor = Arc::new(crate::engine::IcebergWriteCommitExecutor {
                state: std::sync::Weak::new(),
                target: target.clone(),
                catalog,
                table: staged_table.clone(),
                collector,
                fs: cleanup.fs,
                cleanup_path_mapper: cleanup.path_mapper,
                cow_update_rewrite: None,
                target_ref: "main".to_string(),
                snapshot_properties: Default::default(),
            });
            let plan_payload = super::write_control::IcebergWritePlanPayloadV1 {
                version: 1,
                target: format!("{}.{}.{}", target.catalog, target.namespace, target.table),
                target_ref: "main".to_string(),
            };
            let provider_payload = plan_payload.encode()?;
            let committer: Arc<dyn super::write_service::IcebergWriteReportCommitter> =
                commit_executor;
            let service = super::write_service::IcebergWriteControlService::new(
                super::write_service::IcebergWriteControlServiceContext::new_with_handle_payload(
                    writer_handle_payload,
                    plan_payload,
                    committer,
                )?,
            );
            let table =
                crate::engine::iceberg_writer::iceberg_connector_table_handle(&target, "main")
                    .map_err(internal)?;
            let binding = ConnectorStagedWritePlanningBinding::try_new(
                &request.handle,
                request.operation_id,
                request.intent,
                Arc::clone(&request.input_schema),
                table,
                provider_payload,
                request.context.clone(),
            )?;
            self.write_services
                .register(request.operation_id, service)?;
            Ok(binding)
        })();

        match result {
            Ok(binding) => {
                prepared.planning = Some(binding.clone());
                self.operations
                    .lock()
                    .map_err(|error| internal(format!("staged-create operation lock: {error}")))?
                    .insert(target_operation_id, OperationState::Prepared(prepared));
                Ok(binding)
            }
            Err(error) => {
                self.operations
                    .lock()
                    .map_err(|lock| internal(format!("staged-create operation lock: {lock}")))?
                    .insert(target_operation_id, OperationState::Prepared(prepared));
                Err(error)
            }
        }
    }

    fn publish(
        &self,
        request: ConnectorStagedCreatePublishRequest,
    ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
        if request.handle.owner() != &self.owner() || request.completion.owner() != &self.owner() {
            return Err(invalid("Iceberg staged-create publish has a foreign owner"));
        }
        let operation_id = request.handle.operation_id();
        let mut prepared = {
            let mut operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            let Some(OperationState::Prepared(prepared)) = operations.remove(&operation_id) else {
                return Err(invalid(
                    "Iceberg staged-create publish requires an unpublished exact handle",
                ));
            };
            if prepared.handle_digest != request.handle.digest() {
                operations.insert(operation_id, OperationState::Prepared(prepared));
                return Err(invalid("Iceberg staged-create handle digest mismatch"));
            }
            prepared
        };
        let Some(write) = prepared.write.as_ref() else {
            self.operations
                .lock()
                .map_err(|lock| internal(format!("staged-create operation lock: {lock}")))?
                .insert(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create publish requires a bound writer aggregate",
            ));
        };
        if write.completion.aggregate_digest() != request.completion.aggregate_digest()
            || write.completion.sealed().operation_id()
                != request.completion.sealed().operation_id()
        {
            self.operations
                .lock()
                .map_err(|lock| internal(format!("staged-create operation lock: {lock}")))?
                .insert(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create publish completion is not bound to this target",
            ));
        }
        if !write.action_built {
            let abort_handle = Arc::clone(&write.abort_handle);
            match self
                .write_services
                .build_staged_create_action(&request.completion, &abort_handle)
            {
                Ok(mut built) => {
                    let write = prepared
                        .write
                        .as_mut()
                        .expect("validated staged write remains attached");
                    write.updates = built.action.take_updates();
                    write.expected_snapshot_id = built
                        .outcome
                        .as_ref()
                        .map(|outcome| outcome.new_snapshot_id);
                    write.abort_handle = built.abort_handle;
                    write.action_built = true;
                }
                Err(build_failure) => {
                    debug_assert!(Arc::ptr_eq(&abort_handle, &build_failure.abort_handle));
                    let expected_snapshot_id = {
                        let write = prepared
                            .write
                            .as_mut()
                            .expect("validated staged write remains attached");
                        write.abort_handle = build_failure.abort_handle;
                        write.expected_snapshot_id
                    };
                    let message = format!(
                        "build staged CTAS action: {}",
                        build_failure.error.message()
                    );
                    if build_failure.error.is_unknown()
                        || build_failure.error.is_finalize_failed_known_committed()
                    {
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
                        return Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown {
                            failure: ConnectorMutationFailure::new(
                                ConnectorMutationFailureKind::Unavailable,
                                message,
                            ),
                            evidence,
                        });
                    }
                    self.operations
                        .lock()
                        .map_err(|lock| internal(format!("staged-create operation lock: {lock}")))?
                        .insert(operation_id, OperationState::Prepared(prepared));
                    return Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Internal,
                            message,
                        ),
                    });
                }
            }
        }
        let write = prepared
            .write
            .as_ref()
            .expect("built staged write remains attached");
        let mut updates = std::mem::take(&mut prepared.staged.initialization_updates);
        updates.extend(write.updates.clone());
        let commit = TableCommit::builder()
            .ident(prepared.staged.table.identifier().clone())
            .requirements(vec![TableRequirement::NotExist])
            .updates(updates)
            .build();
        let expected_snapshot_id = write.expected_snapshot_id;
        let result = block_on_iceberg(async {
            prepared
                .staged
                .catalog
                .commit_staged_table_typed(commit)
                .await
        });
        match result {
            Ok(Ok(table))
                if publication_matches(&table, operation_id, &prepared, expected_snapshot_id) =>
            {
                let snapshot_id = expected_snapshot_id.unwrap_or(0);
                let mut payload = Vec::with_capacity(24);
                payload.extend_from_slice(table.metadata().uuid().as_bytes());
                payload.extend_from_slice(&snapshot_id.to_be_bytes());
                let receipt = self.bounded_receipt(
                    request.operation_id,
                    ConnectorStagedCreateReceiptPhase::Published,
                    ExternalMutationEffect::Applied,
                    Bytes::from(payload),
                );
                self.entry.invalidate_table_cache(
                    &prepared.staged.table.identifier().namespace.to_url_string(),
                    &prepared.staged.table.identifier().name,
                );
                self.record_terminal(operation_id, OperationState::Published);
                Ok(ConnectorStagedCreatePublishOutcome::Applied {
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
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
                        "REST commit response did not prove the exact staged CTAS publication",
                    ),
                    evidence,
                })
            }
            Ok(Err(iceberg_catalog_rest::StagedCommitError::Conflict(error))) => {
                if prepared.policy == CreatePolicy::NoOpIfExists {
                    self.operations
                        .lock()
                        .map_err(|lock| internal(format!("staged-create operation lock: {lock}")))?
                        .insert(operation_id, OperationState::Prepared(prepared));
                    Ok(ConnectorStagedCreatePublishOutcome::NoOp {
                        receipt: self.receipt(
                            request.operation_id,
                            ConnectorStagedCreateReceiptPhase::Published,
                            ExternalMutationEffect::NoOp,
                            Bytes::new(),
                        )?,
                        finalization: ExternalMutationFinalization::Failed(
                            ConnectorMutationFailure::new(
                                ConnectorMutationFailureKind::Unavailable,
                                "staged CTAS artifacts require explicit durable abort cleanup",
                            ),
                        ),
                    })
                } else {
                    self.operations
                        .lock()
                        .map_err(|lock| internal(format!("staged-create operation lock: {lock}")))?
                        .insert(operation_id, OperationState::Prepared(prepared));
                    Ok(ConnectorStagedCreatePublishOutcome::Conflict {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Conflict,
                            error.to_string(),
                        ),
                    })
                }
            }
            Ok(Err(iceberg_catalog_rest::StagedCommitError::PossiblyDispatched(error))) => {
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
            Ok(Err(iceberg_catalog_rest::StagedCommitError::CommittedResponseInvalid(error))) => {
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
            Err(error) => {
                self.operations
                    .lock()
                    .map_err(|lock| internal(format!("staged-create operation lock: {lock}")))?
                    .insert(operation_id, OperationState::Prepared(prepared));
                Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Internal,
                        error,
                    ),
                })
            }
            Ok(Err(iceberg_catalog_rest::StagedCommitError::KnownNotDispatched(error))) => {
                self.operations
                    .lock()
                    .map_err(|lock| internal(format!("staged-create operation lock: {lock}")))?
                    .insert(operation_id, OperationState::Prepared(prepared));
                Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Internal,
                        error.to_string(),
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
        let prepared = {
            let mut operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            let Some(OperationState::Prepared(prepared)) = operations.remove(&operation_id) else {
                return Err(invalid(
                    "Iceberg staged-create abort requires an unpublished exact handle",
                ));
            };
            if prepared.handle_digest != request.handle.digest() {
                operations.insert(operation_id, OperationState::Prepared(prepared));
                return Err(invalid(
                    "Iceberg staged-create abort handle digest mismatch",
                ));
            }
            prepared
        };
        if request.completion.as_ref().is_some_and(|completion| {
            prepared.write.as_ref().is_none_or(|write| {
                write.completion.aggregate_digest() != completion.aggregate_digest()
            })
        }) {
            self.operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?
                .insert(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create abort completion digest mismatch",
            ));
        }
        match self.abort_prepared(&prepared) {
            Ok(finalization) => {
                let receipt = self.bounded_receipt(
                    request.operation_id,
                    ConnectorStagedCreateReceiptPhase::Aborted,
                    ExternalMutationEffect::Applied,
                    Bytes::new(),
                );
                self.record_terminal(operation_id, OperationState::Aborted);
                Ok(ConnectorStagedCreateAbortOutcome::Aborted {
                    receipt,
                    finalization,
                })
            }
            Err(error) if error.retryable_before_progress() => {
                self.operations
                    .lock()
                    .map_err(|lock| internal(format!("staged-create operation lock: {lock}")))?
                    .insert(operation_id, OperationState::Prepared(prepared));
                Ok(ConnectorStagedCreateAbortOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Internal,
                        format!("staged CTAS abort was not dispatched: {error}"),
                    ),
                })
            }
            Err(error) => {
                let evidence =
                    self.abort_evidence(request.operation_id, operation_id, &prepared)?;
                self.set_unknown(
                    operation_id,
                    ConnectorStagedCreateReconcilePhase::Abort,
                    &evidence,
                    Some(prepared),
                );
                Ok(ConnectorStagedCreateAbortOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        format!("staged CTAS abort may have been dispatched: {error}"),
                    ),
                    evidence,
                })
            }
        }
    }

    fn reconcile(
        &self,
        request: ConnectorStagedCreateReconcileRequest,
    ) -> Result<ConnectorStagedCreateReconcileOutcome, ConnectorError> {
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
                    "Iceberg staged-create outcome remains unresolved",
                ),
                evidence: request.evidence,
            });
        }
        let Some(prepared) = unknown.prepared else {
            return Err(invalid(
                "Iceberg staged-create publish reconcile lost its exact prepared operation",
            ));
        };
        if request.phase == ConnectorStagedCreateReconcilePhase::Abort {
            self.validate_abort_evidence(operation_id, &prepared, &request.evidence)?;
            return match self.abort_prepared(&prepared) {
                Ok(finalization) => {
                    let receipt = self.bounded_receipt(
                        dispatch_operation_id,
                        ConnectorStagedCreateReceiptPhase::Aborted,
                        ExternalMutationEffect::Applied,
                        Bytes::new(),
                    );
                    self.record_terminal(operation_id, OperationState::Aborted);
                    Ok(ConnectorStagedCreateReconcileOutcome::Aborted {
                        receipt,
                        finalization,
                    })
                }
                Err(error) => Ok(abort_reconcile_unknown(error, request.evidence)),
            };
        }
        let publish_evidence: PublishEvidenceV1 =
            serde_json::from_slice(request.evidence.provider_payload()).map_err(|error| {
                invalid(format!(
                    "Iceberg staged-create publish evidence is invalid: {error}"
                ))
            })?;
        let staged_ident = prepared.staged.table.identifier();
        if publish_evidence.version != EVIDENCE_VERSION
            || publish_evidence.operation_marker != operation_marker(operation_id)
            || publish_evidence.handle_digest != prepared.handle_digest
            || publish_evidence.table_uuid != prepared.staged.table.metadata().uuid().to_string()
            || publish_evidence.namespace != staged_ident.namespace.to_url_string()
            || publish_evidence.table != staged_ident.name
        {
            return Err(invalid(
                "Iceberg staged-create publish evidence does not match the exact prepared operation",
            ));
        }

        let load_result =
            block_on_iceberg(async { prepared.staged.catalog.load_table(staged_ident).await });
        match load_result {
            Ok(Ok(table)) => {
                let metadata = table.metadata();
                let exact_publication = metadata.uuid().to_string() == publish_evidence.table_uuid
                    && metadata
                        .properties()
                        .get(CTAS_OPERATION_MARKER)
                        .is_some_and(|marker| marker == &publish_evidence.operation_marker)
                    && publish_evidence
                        .expected_snapshot_id
                        .is_none_or(|snapshot_id| metadata.snapshot_by_id(snapshot_id).is_some());
                if exact_publication {
                    let snapshot_id = publish_evidence.expected_snapshot_id.unwrap_or(0);
                    let mut payload = Vec::with_capacity(24);
                    payload.extend_from_slice(metadata.uuid().as_bytes());
                    payload.extend_from_slice(&snapshot_id.to_be_bytes());
                    let receipt = self.bounded_receipt(
                        dispatch_operation_id,
                        ConnectorStagedCreateReceiptPhase::Published,
                        ExternalMutationEffect::Applied,
                        Bytes::from(payload),
                    );
                    self.entry.invalidate_table_cache(
                        &staged_ident.namespace.to_url_string(),
                        &staged_ident.name,
                    );
                    self.record_terminal(operation_id, OperationState::Published);
                    Ok(ConnectorStagedCreateReconcileOutcome::Published {
                        receipt,
                        finalization: ExternalMutationFinalization::Complete,
                    })
                } else if metadata.uuid().to_string() != publish_evidence.table_uuid {
                    self.operations
                        .lock()
                        .map_err(|error| {
                            internal(format!("staged-create operation lock: {error}"))
                        })?
                        .insert(operation_id, OperationState::Prepared(prepared));
                    Ok(ConnectorStagedCreateReconcileOutcome::KnownUncommitted {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Conflict,
                            "a different table publication is authoritative at the CTAS target",
                        ),
                    })
                } else {
                    Ok(ConnectorStagedCreateReconcileOutcome::CommitUnknown {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Unavailable,
                            "authoritative target has the staged UUID but does not yet prove the exact CTAS publication",
                        ),
                        evidence: request.evidence,
                    })
                }
            }
            Ok(Err(error)) if error.kind() == ErrorKind::TableNotFound => {
                self.operations
                    .lock()
                    .map_err(|lock| internal(format!("staged-create operation lock: {lock}")))?
                    .insert(operation_id, OperationState::Prepared(prepared));
                Ok(ConnectorStagedCreateReconcileOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "the CTAS target is authoritatively absent after publish uncertainty",
                    ),
                })
            }
            Ok(Err(error)) => Ok(ConnectorStagedCreateReconcileOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    format!("authoritative CTAS target reload failed: {error}"),
                ),
                evidence: request.evidence,
            }),
            Err(error) => Ok(ConnectorStagedCreateReconcileOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    format!("authoritative CTAS target reload runtime failed: {error}"),
                ),
                evidence: request.evidence,
            }),
        }
    }
}

fn publication_matches(
    table: &iceberg::table::Table,
    target_operation_id: novarocks_spi::connector::ConnectorStagedCreateOperationId,
    prepared: &PreparedOperation,
    expected_snapshot_id: Option<i64>,
) -> bool {
    let metadata = table.metadata();
    metadata.uuid() == prepared.staged.table.metadata().uuid()
        && metadata
            .properties()
            .get(CTAS_OPERATION_MARKER)
            .is_some_and(|marker| marker == &operation_marker(target_operation_id))
        && expected_snapshot_id
            .is_none_or(|snapshot_id| metadata.snapshot_by_id(snapshot_id).is_some())
}

fn abort_reconcile_unknown(
    error: ConnectorError,
    evidence: ExternalMutationEvidence,
) -> ConnectorStagedCreateReconcileOutcome {
    // A retryable-before-progress error only classifies the current reconcile
    // attempt. It cannot disprove possible progress from the original abort,
    // so every retry failure retains the same durable unknown evidence.
    ConnectorStagedCreateReconcileOutcome::CommitUnknown {
        failure: ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            format!("staged CTAS abort remains unresolved: {error}"),
        ),
        evidence,
    }
}

fn operation_kind(phase: ConnectorStagedCreateReconcilePhase) -> &'static str {
    match phase {
        ConnectorStagedCreateReconcilePhase::Prepare => "staged-create-prepare",
        ConnectorStagedCreateReconcilePhase::Publish => "staged-create-publish",
        ConnectorStagedCreateReconcilePhase::Abort => "staged-create-abort",
    }
}

fn operation_marker(
    operation_id: novarocks_spi::connector::ConnectorStagedCreateOperationId,
) -> String {
    uuid::Uuid::from_bytes(operation_id.to_bytes()).to_string()
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorMutationOperationId, ConnectorProviderId,
    };

    #[test]
    fn abort_reconcile_pre_progress_failure_retains_same_unknown_evidence() {
        let operation_id = ConnectorMutationOperationId::new();
        let evidence = ExternalMutationEvidence::try_new(
            EVIDENCE_VERSION,
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("rest").unwrap(),
            },
            ConnectorInstanceIncarnation::new(),
            operation_id,
            "staged-create-abort",
            Bytes::from_static(b"exact-abort-evidence"),
        )
        .unwrap();
        let outcome = abort_reconcile_unknown(
            ConnectorError::new(ConnectorErrorKind::Unavailable, "retry unavailable")
                .with_retryable_before_progress(),
            evidence.clone(),
        );
        let ConnectorStagedCreateReconcileOutcome::CommitUnknown {
            evidence: retained, ..
        } = outcome
        else {
            panic!("abort reconcile must remain unknown")
        };
        assert_eq!(retained, evidence);
    }
}
