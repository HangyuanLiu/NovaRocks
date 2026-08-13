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

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::state_store::{
    Direction, Key, KeyRange, Precondition, RangeRequest, StateRecord, StateStore, Value,
    WriteTransaction,
};
use novarocks_state_store::metrics::StateStoreMetrics;
use novarocks_state_store::{OperationId, RunFailure, run_side_effect_free};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::runtime::{Handle, RuntimeFlavor};
use uuid::Uuid;

use crate::dml::error::DmlError;
use crate::dml::journal::{DmlIntentAdmissionValidator, DmlMutationAuthority, OperationJournal};
use crate::dml::model::{
    AddFilesArtifact, AddFilesArtifactDescriptor, AddFilesDispatchCertainty,
    AddFilesLifecyclePhase, AddFilesLifecycleRecord, AddFilesMutationRequest, AddFilesSourceAction,
    CTAS_CREATE_POLICY_FAIL_IF_EXISTS, CTAS_CREATE_POLICY_NO_OP_IF_EXISTS,
    ConnectorWriteFinalizationRecord, ConnectorWriteLifecycleRecord, CreatePreparingRequest,
    CreateStatementOperationRequest, CtasSagaPhase, CtasSagaRecord,
    DML_COORDINATION_RESOURCE_CODEC_VERSION, DML_CTAS_FACT_ENCODED_LIMIT,
    DML_CTAS_TOTAL_FACT_ENCODED_LIMIT, DML_EXTERNAL_FACT_ENCODED_LIMIT,
    DML_FOREGROUND_RECOVERY_VISIBILITY_MS, DML_OPERATION_SCHEMA_VERSION,
    DML_RECOVERY_DUE_SCHEMA_VERSION, DML_RECOVERY_PAGE_SIZE, DML_RECOVERY_SHARD_COUNT,
    DML_UNFINISHED_SCHEMA_VERSION, DmlCoordinationClaimRequest, DmlCtasRecoveryMutationRequest,
    DmlCtasRecoveryRecord, DmlDirectMutationFenceMutationRequest,
    DmlDirectMutationFenceReceiptRecord, DmlExternalFenceMutationRequest,
    DmlExternalFenceReceiptRecord, DmlHistoricalDataMutationRecoveryMutationRequest,
    DmlHistoricalDataMutationRecoveryRecord, DmlHistoricalWriteRecoveryMutationRequest,
    DmlHistoricalWriteRecoveryRecord, DmlOperationId, DmlRecoveryCandidate,
    DmlRecoveryDueRescheduleRequest, DurableExternalFact, ExternalFactOutcome, OperationFact,
    OperationKind, OperationMutationRequest, OperationPayload, OperationState, OperationTarget,
    SourceScopeOwnership, StatementNextAction, StoredOperation, operation_requires_recovery_scan,
    operation_requires_recovery_scan_with_direct_mutation, validate_ctas_recovery,
    validate_ctas_recovery_transition, validate_direct_mutation_fence_receipt,
    validate_direct_mutation_fence_transition, validate_external_fence_receipt,
    validate_external_fence_transition, validate_historical_data_mutation_recovery,
    validate_historical_data_mutation_recovery_transition, validate_historical_write_recovery,
    validate_historical_write_recovery_transition, validate_operation_transition,
    validate_statement_operation_transition,
};
use crate::dml::now_unix_millis;

const OPERATION_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/operations/";
const UNFINISHED_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/unfinished/";
const RECOVERY_DUE_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/recovery-due/";
const ADD_FILES_ARTIFACT_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/add-files-artifacts/";
const ADD_FILES_SOURCE_SCOPE_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/add-files-source-scopes/";
const EXTERNAL_FENCE_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/external-fences/";
const HISTORICAL_WRITE_RECOVERY_PREFIX: &[u8] =
    b"novarocks/frontend/dml/v1/historical-write-recoveries/";
const DIRECT_MUTATION_FENCE_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/direct-mutation-fences/";
const HISTORICAL_DATA_MUTATION_RECOVERY_PREFIX: &[u8] =
    b"novarocks/frontend/dml/v1/historical-data-mutation-recoveries/";
const CTAS_RECOVERY_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/ctas-recoveries/";
const ADD_FILES_ARTIFACT_CHUNK_BYTES: usize = 8 * 1024;

/// Operation states in which an external operation fence receipt may still be
/// attached. A fence must be confirmed before any writer or commit dispatch, so
/// an operation that already carries an external outcome cannot accept one.
///
/// TRUNCATE and ADD FILES seal their fence in `Preparing` or `Committing`, so
/// the direct-mutation receipt shares this rule instead of restating it.
const EXTERNAL_FENCE_ALLOWED_STATES: [OperationState; 4] = [
    OperationState::Preparing,
    OperationState::Writing,
    OperationState::Collecting,
    OperationState::Committing,
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredAddFilesSourceScopeV1 {
    schema_version: u8,
    provider_id: String,
    scope_digest: String,
    operation_id: DmlOperationId,
    target: OperationTarget,
    plan_digest: String,
    ownership: SourceScopeOwnership,
    updated_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredUnfinishedOperationV1 {
    schema_version: u8,
    operation_id: DmlOperationId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredRecoveryDueV1 {
    schema_version: u8,
    operation_id: DmlOperationId,
    operation_revision: u64,
    last_mutation_id: Uuid,
    coordination_attempt_id: Option<Uuid>,
    recovery_due_at_ms: i64,
}

#[derive(Clone)]
pub struct StateStoreOperationJournal {
    store: Arc<dyn StateStore>,
    runtime: Handle,
    metrics: Arc<StateStoreMetrics>,
}

impl StateStoreOperationJournal {
    pub async fn open(store: Arc<dyn StateStore>, runtime: Handle) -> Result<Self, DmlError> {
        let provider = store.metrics_snapshot().provider;
        Ok(Self {
            store,
            runtime,
            metrics: Arc::new(StateStoreMetrics::new(provider)),
        })
    }

    fn blocking<T>(
        &self,
        future: impl Future<Output = Result<T, DmlError>>,
    ) -> Result<T, DmlError> {
        match Handle::try_current() {
            Ok(current) if current.runtime_flavor() == RuntimeFlavor::CurrentThread => {
                Err(DmlError::journal_unavailable(
                    "DML journal synchronous commands cannot run on a current-thread Tokio runtime",
                ))
            }
            Ok(_) => tokio::task::block_in_place(|| self.runtime.block_on(future)),
            Err(_) => self.runtime.block_on(future),
        }
    }

    async fn create_preparing_async(
        &self,
        request: CreatePreparingRequest,
        admission: Option<Arc<dyn DmlIntentAdmissionValidator>>,
    ) -> Result<DmlOperationId, DmlError> {
        let operation_id = DmlOperationId::new_v7();
        let mutation_id = Uuid::now_v7();
        let now_ms = request.created_at_ms;
        let recovery_due_at_ms = now_ms.saturating_add(DML_FOREGROUND_RECOVERY_VISIBILITY_MS);
        let operation = StoredOperation {
            schema_version: DML_OPERATION_SCHEMA_VERSION,
            operation_id,
            revision: 1,
            last_mutation_id: mutation_id,
            operation_kind: request.operation_kind,
            operation_subkind: request.operation_subkind,
            target: request.target,
            state: OperationState::Preparing,
            attempt_id: request.attempt_id,
            base_snapshot_id: request.base_snapshot_id,
            base_snapshot_map: request.base_snapshot_map,
            staged_artifacts: request.staged_artifacts,
            payload: OperationPayload::ConnectorWriteLifecycle(
                ConnectorWriteLifecycleRecord::Pending,
            ),
            coordination_provenance: None,
            recovery_due_at_ms: Some(recovery_due_at_ms),
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            finished_at_ms: None,
        };
        let operation_key = operation_key(operation_id)?;
        let unfinished_key = unfinished_key(operation_id)?;
        let recovery_due_key = recovery_due_key(operation_id, recovery_due_at_ms)?;
        let stored = operation.clone();
        let max_value_bytes = self.store.limits().max_value_bytes;
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            OperationId::from(mutation_id),
            "create frontend DML operation",
            |transaction| {
                let operation_key = operation_key.clone();
                let unfinished_key = unfinished_key.clone();
                let recovery_due_key = recovery_due_key.clone();
                let stored = stored.clone();
                let admission = admission.clone();
                Box::pin(async move {
                    if let Some(admission) = admission
                        && let Err(error) = admission.validate_in(transaction).await
                    {
                        return Ok(Err(error));
                    }
                    if transaction.get(&operation_key).await?.is_some()
                        || transaction.get(&unfinished_key).await?.is_some()
                        || transaction.get(&recovery_due_key).await?.is_some()
                    {
                        return Ok(Err(DmlError::journal_corruption(format!(
                            "duplicate DML operation id {}",
                            stored.operation_id
                        ))));
                    }
                    let operation_value =
                        match encode_operation_with_limit(&stored, max_value_bytes) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                    let unfinished_value = match encode_unfinished(stored.operation_id) {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    let recovery_due_value = match encode_recovery_due(&stored) {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    transaction
                        .put(operation_key, operation_value, Precondition::Absent)
                        .await?;
                    transaction
                        .put(unfinished_key, unfinished_value, Precondition::Absent)
                        .await?;
                    transaction
                        .put(recovery_due_key, recovery_due_value, Precondition::Absent)
                        .await?;
                    Ok(Ok(operation_id))
                })
            },
        )
        .await;
        self.finish_mutation(result, operation_key, mutation_id, operation_id, "create")
            .await
    }

    async fn create_statement_operation_async(
        &self,
        request: CreateStatementOperationRequest,
        admission: Option<Arc<dyn DmlIntentAdmissionValidator>>,
    ) -> Result<StoredOperation, DmlError> {
        let recovery_due_at_ms = request
            .created_at_ms
            .saturating_add(DML_FOREGROUND_RECOVERY_VISIBILITY_MS);
        let operation = StoredOperation {
            schema_version: DML_OPERATION_SCHEMA_VERSION,
            operation_id: request.operation_id,
            revision: 1,
            last_mutation_id: request.mutation_id,
            operation_kind: request.operation_kind,
            operation_subkind: None,
            target: request.target,
            state: OperationState::Preparing,
            attempt_id: request.attempt_id,
            base_snapshot_id: None,
            base_snapshot_map: std::collections::BTreeMap::new(),
            staged_artifacts: Vec::new(),
            payload: request.payload,
            coordination_provenance: None,
            recovery_due_at_ms: Some(recovery_due_at_ms),
            created_at_ms: request.created_at_ms,
            updated_at_ms: request.created_at_ms,
            finished_at_ms: None,
        };
        validate_operation(&operation)?;
        // A statement operation is being created, so no side record can exist.
        validate_recovery_due_scope(&operation, None, None, None)?;
        let operation_id = operation.operation_id;
        let mutation_id = operation.last_mutation_id;
        let operation_key = operation_key(operation_id)?;
        let unfinished_key = unfinished_key(operation_id)?;
        let recovery_due_key = recovery_due_key(operation_id, recovery_due_at_ms)?;
        let stored = operation.clone();
        let max_value_bytes = self.store.limits().max_value_bytes;
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            OperationId::from(mutation_id),
            "create statement-specific frontend DML operation",
            |transaction| {
                let operation_key = operation_key.clone();
                let unfinished_key = unfinished_key.clone();
                let recovery_due_key = recovery_due_key.clone();
                let stored = stored.clone();
                let admission = admission.clone();
                Box::pin(async move {
                    if let Some(admission) = admission
                        && let Err(error) = admission.validate_in(transaction).await
                    {
                        return Ok(Err(error));
                    }
                    let existing_operation = transaction.get(&operation_key).await?;
                    let existing_unfinished = transaction.get(&unfinished_key).await?;
                    let existing_due = transaction.get(&recovery_due_key).await?;
                    if let Some(record) = existing_operation {
                        let existing = match decode_operation(record.key, record.value) {
                            Ok(existing) => existing,
                            Err(error) => return Ok(Err(error)),
                        };
                        match (
                            existing.state.is_finished(),
                            existing_unfinished,
                            existing_due,
                        ) {
                            (false, Some(index), Some(due)) => {
                                let indexed_id = match decode_unfinished(index.key, index.value) {
                                    Ok(indexed_id) => indexed_id,
                                    Err(error) => return Ok(Err(error)),
                                };
                                if indexed_id != stored.operation_id {
                                    return Ok(Err(DmlError::journal_corruption(format!(
                                        "unfinished DML operation index identity mismatch for {}",
                                        stored.operation_id
                                    ))));
                                }
                                if let Err(error) = validate_recovery_due_record(&existing, due) {
                                    return Ok(Err(error));
                                }
                            }
                            (false, _, _) => {
                                return Ok(Err(DmlError::journal_corruption(format!(
                                    "unfinished DML operation {} is missing an index",
                                    stored.operation_id
                                ))));
                            }
                            (true, Some(_), _) => {
                                return Ok(Err(DmlError::journal_corruption(format!(
                                    "terminal DML operation {} remains in the unfinished index",
                                    stored.operation_id
                                ))));
                            }
                            (true, None, Some(due)) => {
                                if let Err(error) = validate_recovery_due_record(&existing, due) {
                                    return Ok(Err(error));
                                }
                            }
                            (true, None, None) => {}
                        }
                        if existing == stored {
                            return Ok(Ok(existing));
                        }
                        return Ok(Err(DmlError::journal_unresolved(format!(
                            "conflicting DML statement create replay for operation {}",
                            stored.operation_id
                        ))));
                    }
                    if existing_unfinished.is_some() || existing_due.is_some() {
                        return Ok(Err(DmlError::journal_corruption(format!(
                            "unfinished DML operation index {} has no operation record",
                            stored.operation_id
                        ))));
                    }
                    let operation_value =
                        match encode_operation_with_limit(&stored, max_value_bytes) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                    let unfinished_value = match encode_unfinished(stored.operation_id) {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    let recovery_due_value = match encode_recovery_due(&stored) {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    transaction
                        .put(operation_key, operation_value, Precondition::Absent)
                        .await?;
                    transaction
                        .put(unfinished_key, unfinished_value, Precondition::Absent)
                        .await?;
                    transaction
                        .put(recovery_due_key, recovery_due_value, Precondition::Absent)
                        .await?;
                    Ok(Ok(stored))
                })
            },
        )
        .await;
        self.finish_statement_mutation(result, operation_key, mutation_id, "create")
            .await
    }

    async fn mutate_statement_operation_async(
        &self,
        request: OperationMutationRequest,
        authority: Option<DmlMutationAuthority>,
        requested_recovery_due_at_ms: Option<Option<i64>>,
    ) -> Result<StoredOperation, DmlError> {
        let operation_id = request.operation_id;
        let mutation_id = request.mutation_id;
        let operation_key = operation_key(operation_id)?;
        let unfinished_key = unfinished_key(operation_id)?;
        let max_value_bytes = self.store.limits().max_value_bytes;
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            OperationId::from(mutation_id),
            "mutate statement-specific frontend DML operation",
            |transaction| {
                let operation_key = operation_key.clone();
                let unfinished_key = unfinished_key.clone();
                let request = request.clone();
                let authority = authority.clone();
                Box::pin(async move {
                    if let Some(authority) = &authority
                        && let Err(error) = authority.validator().validate_in(transaction).await
                    {
                        return Ok(Err(error));
                    }
                    let Some(record) = transaction.get(&operation_key).await? else {
                        return Ok(Err(DmlError::journal_unavailable(format!(
                            "DML operation {operation_id} not found"
                        ))));
                    };
                    let operation_version = record.version.clone();
                    let mut operation = match decode_operation(record.key, record.value) {
                        Ok(operation) => operation,
                        Err(error) => return Ok(Err(error)),
                    };
                    if let Some(authority) = &authority
                        && let Err(error) = validate_persisted_authority(&operation, authority)
                    {
                        return Ok(Err(error));
                    }
                    if operation.last_mutation_id == request.mutation_id {
                        let expected_applied_revision =
                            request.expected_revision.checked_add(1).ok_or_else(|| {
                                DmlError::journal_corruption(format!(
                                    "DML operation {operation_id} revision overflow"
                                ))
                            });
                        let expected_applied_revision = match expected_applied_revision {
                            Ok(revision) => revision,
                            Err(error) => return Ok(Err(error)),
                        };
                        if operation.revision == expected_applied_revision
                            && operation.state == request.state
                            && operation.payload == request.payload
                        {
                            return Ok(Ok(operation));
                        }
                        return Ok(Err(DmlError::journal_unresolved(format!(
                            "conflicting DML statement mutation replay for operation {operation_id}"
                        ))));
                    }
                    if operation.revision != request.expected_revision {
                        return Ok(Err(DmlError::journal_unresolved(format!(
                            "DML operation {operation_id} revision changed from expected {} to {}",
                            request.expected_revision, operation.revision
                        ))));
                    }
                    if let Err(error) = validate_statement_operation_transition(
                        operation.operation_kind,
                        operation.state,
                        request.state,
                    )
                    .map_err(DmlError::journal_unavailable)
                    {
                        return Ok(Err(error));
                    }
                    let direct_mutation = match load_historical_data_mutation_recovery_in(
                        transaction,
                        operation_id,
                    )
                    .await
                    {
                        Ok(direct_mutation) => direct_mutation,
                        Err(error) => return Ok(Err(error)),
                    };
                    let ctas = match load_ctas_recovery_in(transaction, operation_id).await {
                        Ok(ctas) => ctas,
                        Err(error) => return Ok(Err(error)),
                    };
                    let previous = operation.clone();
                    operation.schema_version = DML_OPERATION_SCHEMA_VERSION;
                    operation.revision = match operation.revision.checked_add(1) {
                        Some(revision) => revision,
                        None => {
                            return Ok(Err(DmlError::journal_corruption(format!(
                                "DML operation {operation_id} revision overflow"
                            ))));
                        }
                    };
                    operation.last_mutation_id = request.mutation_id;
                    operation.state = request.state;
                    operation.payload = request.payload;
                    operation.recovery_due_at_ms = requested_recovery_due_at_ms
                        .unwrap_or_else(|| legacy_recovery_due_after_mutation(&operation, &previous));
                    operation.updated_at_ms = now_unix_millis();
                    if operation.state.is_finished() {
                        operation.finished_at_ms = Some(operation.updated_at_ms);
                    }
                    // Statement families do not own a CP-3B historical write
                    // recovery record, but TRUNCATE and ADD FILES do own a
                    // CP-3C historical data-mutation recovery, so a terminal
                    // statement result must not drop that obligation.
                    if let Err(error) = validate_historical_retention(
                        operation_id,
                        None,
                        direct_mutation.as_ref(),
                        ctas.as_ref(),
                        operation.recovery_due_at_ms,
                    ) {
                        return Ok(Err(error));
                    }
                    if let Err(error) =
                        validate_recovery_due_scope(
                            &operation,
                            None,
                            direct_mutation.as_ref(),
                            ctas.as_ref(),
                        )
                    {
                        return Ok(Err(error));
                    }
                    let operation_value =
                        match encode_operation_with_limit(&operation, max_value_bytes) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                    transaction
                        .put(
                            operation_key,
                            operation_value,
                            Precondition::Version(operation_version),
                        )
                        .await?;
                    if let Err(error) =
                        update_recovery_due_index(transaction, &previous, &operation).await
                    {
                        return Ok(Err(error));
                    }
                    if operation.state.is_finished() {
                        let Some(index) = transaction.get(&unfinished_key).await? else {
                            return Ok(Err(DmlError::journal_corruption(format!(
                                "DML operation {operation_id} is missing its unfinished index"
                            ))));
                        };
                        let indexed_id = match decode_unfinished(index.key, index.value) {
                            Ok(indexed_id) => indexed_id,
                            Err(error) => return Ok(Err(error)),
                        };
                        if indexed_id != operation_id {
                            return Ok(Err(DmlError::journal_corruption(format!(
                                "unfinished DML operation index identity mismatch for {operation_id}"
                            ))));
                        }
                        transaction
                            .delete(unfinished_key, Precondition::Version(index.version))
                            .await?;
                    } else {
                        let Some(index) = transaction.get(&unfinished_key).await? else {
                            return Ok(Err(DmlError::journal_corruption(format!(
                                "DML operation {operation_id} is missing its unfinished index"
                            ))));
                        };
                        let indexed_id = match decode_unfinished(index.key, index.value) {
                            Ok(indexed_id) => indexed_id,
                            Err(error) => return Ok(Err(error)),
                        };
                        if indexed_id != operation_id {
                            return Ok(Err(DmlError::journal_corruption(format!(
                                "unfinished DML operation index identity mismatch for {operation_id}"
                            ))));
                        }
                        let value = match encode_unfinished(operation_id) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                        transaction
                            .put(
                                unfinished_key,
                                value,
                                Precondition::Version(index.version),
                            )
                            .await?;
                    }
                    Ok(Ok(operation))
                })
            },
        )
        .await;
        self.finish_statement_mutation(result, operation_key, mutation_id, "mutate")
            .await
    }

    async fn apply_add_files_mutation_async(
        &self,
        request: AddFilesMutationRequest,
        authority: Option<DmlMutationAuthority>,
        requested_recovery_due_at_ms: Option<Option<i64>>,
    ) -> Result<StoredOperation, DmlError> {
        self.preflight_add_files_mutation_shape(&request)?;
        let operation_id = request.operation.operation_id;
        let mutation_id = request.operation.mutation_id;
        let operation_key = operation_key(operation_id)?;
        let unfinished_key = unfinished_key(operation_id)?;
        let max_value_bytes = self.store.limits().max_value_bytes;
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            OperationId::from(mutation_id),
            "apply atomic frontend ADD FILES mutation",
            |transaction| {
                let operation_key = operation_key.clone();
                let unfinished_key = unfinished_key.clone();
                let request = request.clone();
                let authority = authority.clone();
                Box::pin(async move {
                    if let Some(authority) = &authority
                        && let Err(error) = authority.validator().validate_in(transaction).await
                    {
                        return Ok(Err(error));
                    }
                    let Some(record) = transaction.get(&operation_key).await? else {
                        return Ok(Err(DmlError::journal_unavailable(format!(
                            "DML operation {operation_id} not found"
                        ))));
                    };
                    let operation_version = record.version.clone();
                    let mut operation = match decode_operation(record.key, record.value) {
                        Ok(operation) => operation,
                        Err(error) => return Ok(Err(error)),
                    };
                    if let Some(authority) = &authority
                        && let Err(error) = validate_persisted_authority(&operation, authority)
                    {
                        return Ok(Err(error));
                    }
                    if operation.last_mutation_id == request.operation.mutation_id {
                        if operation.revision == request.operation.expected_revision.saturating_add(1)
                            && operation.state == request.operation.state
                            && operation.payload == request.operation.payload
                        {
                            return Ok(Ok(operation));
                        }
                        return Ok(Err(DmlError::journal_unresolved(format!(
                            "conflicting ADD FILES mutation replay for operation {operation_id}"
                        ))));
                    }
                    if operation.revision != request.operation.expected_revision {
                        return Ok(Err(DmlError::journal_unresolved(format!(
                            "DML operation {operation_id} revision changed from expected {} to {}",
                            request.operation.expected_revision, operation.revision
                        ))));
                    }
                    if operation.operation_kind != OperationKind::AddFiles {
                        return Ok(Err(DmlError::journal_corruption(
                            "ADD FILES atomic mutation was applied to another operation kind",
                        )));
                    }
                    if let Err(error) = validate_statement_operation_transition(
                        operation.operation_kind,
                        operation.state,
                        request.operation.state,
                    )
                    .map_err(DmlError::journal_unavailable)
                    {
                        return Ok(Err(error));
                    }
                    let direct_mutation = match load_historical_data_mutation_recovery_in(
                        transaction,
                        operation_id,
                    )
                    .await
                    {
                        Ok(direct_mutation) => direct_mutation,
                        Err(error) => return Ok(Err(error)),
                    };
                    let previous = operation.clone();
                    operation.schema_version = DML_OPERATION_SCHEMA_VERSION;
                    operation.revision = match operation.revision.checked_add(1) {
                        Some(revision) => revision,
                        None => return Ok(Err(DmlError::journal_corruption("DML operation revision overflow"))),
                    };
                    operation.last_mutation_id = request.operation.mutation_id;
                    operation.state = request.operation.state;
                    operation.payload = request.operation.payload;
                    operation.recovery_due_at_ms = requested_recovery_due_at_ms
                        .unwrap_or_else(|| legacy_recovery_due_after_mutation(&operation, &previous));
                    operation.updated_at_ms = now_unix_millis();
                    if operation.state.is_finished() {
                        operation.finished_at_ms = Some(operation.updated_at_ms);
                    }
                    if let Err(error) = validate_operation(&operation) {
                        return Ok(Err(error));
                    }
                    // ADD FILES does not own a CP-3B historical write recovery
                    // record, but it does own a CP-3C historical data-mutation
                    // recovery whose source-scope obligation outlives the
                    // user-visible statement result.
                    if let Err(error) = validate_historical_retention(
                        operation_id,
                        None,
                        direct_mutation.as_ref(),
                        None,
                        operation.recovery_due_at_ms,
                    ) {
                        return Ok(Err(error));
                    }
                    if let Err(error) =
                        validate_recovery_due_scope(
                            &operation,
                            None,
                            direct_mutation.as_ref(),
                            None,
                        )
                    {
                        return Ok(Err(error));
                    }
                    // `run_side_effect_free` can report a closure error after
                    // staged transaction writes. Validate every source-owner
                    // conflict before staging artifact chunks so a rejected
                    // competing reservation cannot leave orphan artifacts.
                    if let Some(action) = &request.source_action {
                        let (provider_id, scope_digest) = match action {
                            AddFilesSourceAction::Reserve {
                                provider_id,
                                scope_digest,
                                ..
                            }
                            | AddFilesSourceAction::Transition {
                                provider_id,
                                scope_digest,
                                ..
                            }
                            | AddFilesSourceAction::Release {
                                provider_id,
                                scope_digest,
                            } => (provider_id, scope_digest),
                        };
                        let key = match add_files_source_scope_key(provider_id, scope_digest) {
                            Ok(key) => key,
                            Err(error) => return Ok(Err(error)),
                        };
                        let existing = transaction.get(&key).await?;
                        match (action, existing) {
                            (AddFilesSourceAction::Reserve { .. }, Some(existing)) => {
                                let source = match decode_add_files_source_scope(&key, existing.value) {
                                    Ok(source) => source,
                                    Err(error) => return Ok(Err(error)),
                                };
                                return Ok(Err(DmlError::journal_unresolved(format!(
                                    "ADD FILES source scope is owned by operation {}",
                                    source.operation_id
                                ))));
                            }
                            (AddFilesSourceAction::Reserve { .. }, None) => {}
                            (AddFilesSourceAction::Transition { expected, .. }, Some(existing)) => {
                                let source = match decode_add_files_source_scope(&key, existing.value) {
                                    Ok(source) => source,
                                    Err(error) => return Ok(Err(error)),
                                };
                                if source.operation_id != operation_id || source.ownership != *expected {
                                    return Ok(Err(DmlError::journal_unresolved(
                                        "ADD FILES source scope transition conflicts with its owner",
                                    )));
                                }
                            }
                            (AddFilesSourceAction::Release { .. }, Some(existing)) => {
                                let source = match decode_add_files_source_scope(&key, existing.value) {
                                    Ok(source) => source,
                                    Err(error) => return Ok(Err(error)),
                                };
                                if source.operation_id != operation_id
                                    || source.ownership != SourceScopeOwnership::ReservedImmutable
                                {
                                    return Ok(Err(DmlError::journal_unresolved(
                                        "ADD FILES source scope release conflicts with its owner",
                                    )));
                                }
                            }
                            (AddFilesSourceAction::Transition { .. } | AddFilesSourceAction::Release { .. }, None) => {
                                return Ok(Err(DmlError::journal_corruption(
                                    "ADD FILES source scope is missing",
                                )));
                            }
                        }
                    }
                    for artifact in &request.artifacts {
                        if let Err(error) = validate_add_files_artifact_bytes(artifact) {
                            return Ok(Err(error));
                        }
                        for (index, chunk) in artifact
                            .bytes
                            .chunks(ADD_FILES_ARTIFACT_CHUNK_BYTES)
                            .enumerate()
                        {
                            let chunk_index = match u16::try_from(index) {
                                Ok(index) => index,
                                Err(_) => return Ok(Err(DmlError::journal_corruption("ADD FILES artifact chunk index overflow"))),
                            };
                            let key = match add_files_artifact_chunk_key(operation_id, &artifact.descriptor, chunk_index) {
                                Ok(key) => key,
                                Err(error) => return Ok(Err(error)),
                            };
                            match transaction.get(&key).await? {
                                Some(existing) if existing.value.as_bytes() == chunk => {}
                                Some(_) => return Ok(Err(DmlError::journal_corruption(
                                    "ADD FILES artifact chunk conflicts with an existing value",
                                ))),
                                None => {
                                    let value = match Value::try_from(Bytes::copy_from_slice(chunk)) {
                                        Ok(value) => value,
                                        Err(error) => return Ok(Err(DmlError::journal_unavailable(error))),
                                    };
                                    transaction.put(key, value, Precondition::Absent).await?;
                                }
                            }
                        }
                    }
                    if let Some(action) = &request.source_action {
                        let (provider_id, scope_digest) = match action {
                            AddFilesSourceAction::Reserve { provider_id, scope_digest, .. }
                            | AddFilesSourceAction::Transition { provider_id, scope_digest, .. }
                            | AddFilesSourceAction::Release { provider_id, scope_digest } => {
                                (provider_id.clone(), scope_digest.clone())
                            }
                        };
                        let key = match add_files_source_scope_key(&provider_id, &scope_digest) {
                            Ok(key) => key,
                            Err(error) => return Ok(Err(error)),
                        };
                        let existing = transaction.get(&key).await?;
                        match action {
                            AddFilesSourceAction::Reserve { ownership, .. } => {
                                if let Some(existing) = existing {
                                    let source = match decode_add_files_source_scope(&key, existing.value) {
                                        Ok(source) => source,
                                        Err(error) => return Ok(Err(error)),
                                    };
                                    return Ok(Err(DmlError::journal_unresolved(format!(
                                        "ADD FILES source scope is owned by operation {}",
                                        source.operation_id
                                    ))));
                                }
                                let source = match source_scope_record_from_operation(
                                    &operation, provider_id, scope_digest, *ownership,
                                ) {
                                    Ok(source) => source,
                                    Err(error) => return Ok(Err(error)),
                                };
                                let value = match encode_add_files_source_scope(&source, max_value_bytes) {
                                    Ok(value) => value,
                                    Err(error) => return Ok(Err(error)),
                                };
                                transaction.put(key, value, Precondition::Absent).await?;
                            }
                            AddFilesSourceAction::Transition { expected, ownership, .. } => {
                                let Some(existing) = existing else {
                                    return Ok(Err(DmlError::journal_corruption("ADD FILES source scope is missing")));
                                };
                                let source = match decode_add_files_source_scope(&key, existing.value) {
                                    Ok(source) => source,
                                    Err(error) => return Ok(Err(error)),
                                };
                                if source.operation_id != operation_id || source.ownership != *expected {
                                    return Ok(Err(DmlError::journal_unresolved(
                                        "ADD FILES source scope transition conflicts with its owner",
                                    )));
                                }
                                let source = match source_scope_record_from_operation(
                                    &operation, provider_id, scope_digest, *ownership,
                                ) {
                                    Ok(source) => source,
                                    Err(error) => return Ok(Err(error)),
                                };
                                let value = match encode_add_files_source_scope(&source, max_value_bytes) {
                                    Ok(value) => value,
                                    Err(error) => return Ok(Err(error)),
                                };
                                transaction.put(key, value, Precondition::Version(existing.version)).await?;
                            }
                            AddFilesSourceAction::Release { .. } => {
                                let Some(existing) = existing else {
                                    return Ok(Err(DmlError::journal_corruption("ADD FILES source scope is missing")));
                                };
                                let source = match decode_add_files_source_scope(&key, existing.value) {
                                    Ok(source) => source,
                                    Err(error) => return Ok(Err(error)),
                                };
                                if source.operation_id != operation_id
                                    || source.ownership != SourceScopeOwnership::ReservedImmutable
                                {
                                    return Ok(Err(DmlError::journal_unresolved(
                                        "ADD FILES source scope release conflicts with its owner",
                                    )));
                                }
                                transaction.delete(key, Precondition::Version(existing.version)).await?;
                            }
                        }
                    }
                    let operation_value = match encode_operation_with_limit(&operation, max_value_bytes) {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    transaction.put(operation_key, operation_value, Precondition::Version(operation_version)).await?;
                    if let Err(error) =
                        update_recovery_due_index(transaction, &previous, &operation).await
                    {
                        return Ok(Err(error));
                    }
                    let Some(index) = transaction.get(&unfinished_key).await? else {
                        return Ok(Err(DmlError::journal_corruption("DML operation is missing its unfinished index")));
                    };
                    if let Err(error) = decode_unfinished(index.key, index.value).and_then(|id| {
                        if id == operation_id { Ok(()) } else { Err(DmlError::journal_corruption("unfinished DML index identity mismatch")) }
                    }) {
                        return Ok(Err(error));
                    }
                    if operation.state.is_finished() {
                        transaction
                            .delete(unfinished_key, Precondition::Version(index.version))
                            .await?;
                    } else {
                        let value = match encode_unfinished(operation_id) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                        transaction
                            .put(unfinished_key, value, Precondition::Version(index.version))
                            .await?;
                    }
                    Ok(Ok(operation))
                })
            },
        )
        .await;
        self.finish_statement_mutation(result, operation_key, mutation_id, "apply ADD FILES")
            .await
    }

    async fn claim_operation_async(
        &self,
        request: DmlCoordinationClaimRequest,
        admission: Option<Arc<dyn DmlIntentAdmissionValidator>>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        if request.provenance.coordination_attempt_id != authority.coordination_attempt_id() {
            return Err(DmlError::journal_unresolved(
                "DML coordination claim attempt does not match its live authority",
            ));
        }
        validate_coordination_provenance(&request.provenance)?;
        let provenance = request.provenance.clone();
        self.mutate_operation_authorized_async(
            request.operation_id,
            request.expected_revision,
            request.mutation_id,
            Some(request.recovery_due_at_ms),
            admission,
            authority,
            true,
            move |operation| {
                operation.coordination_provenance = Some(provenance.clone());
                Ok(())
            },
        )
        .await
    }

    async fn transition_authorized_async(
        &self,
        operation_id: DmlOperationId,
        expected_revision: u64,
        mutation_id: Uuid,
        to: OperationState,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.mutate_operation_authorized_async(
            operation_id,
            expected_revision,
            mutation_id,
            recovery_due_at_ms,
            None,
            authority,
            false,
            move |operation| {
                validate_operation_transition(operation.state, to)
                    .map_err(DmlError::journal_unavailable)?;
                operation.state = to;
                if to.is_finished() {
                    operation.finished_at_ms = Some(now_unix_millis());
                }
                Ok(())
            },
        )
        .await
    }

    async fn record_fact_authorized_async(
        &self,
        operation_id: DmlOperationId,
        expected_revision: u64,
        mutation_id: Uuid,
        fact: OperationFact,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.mutate_operation_authorized_async(
            operation_id,
            expected_revision,
            mutation_id,
            recovery_due_at_ms,
            None,
            authority,
            false,
            move |operation| {
                validate_operation_transition(operation.state, fact.state)
                    .map_err(DmlError::journal_unavailable)?;
                if operation.state == fact.state {
                    let identical = matches!(
                        &operation.payload,
                        OperationPayload::ConnectorWriteLifecycle(existing)
                            if existing == &fact.lifecycle
                    );
                    if !identical {
                        return Err(DmlError::journal_unavailable(format!(
                            "conflicting DML operation fact replay for operation {operation_id} in state {}",
                            fact.state.as_str()
                        )));
                    }
                }
                operation.state = fact.state;
                operation.payload =
                    OperationPayload::ConnectorWriteLifecycle(fact.lifecycle.clone());
                if fact.state.is_finished() {
                    operation.finished_at_ms = Some(now_unix_millis());
                }
                Ok(())
            },
        )
        .await
    }

    async fn reschedule_recovery_due_async(
        &self,
        request: DmlRecoveryDueRescheduleRequest,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.mutate_operation_authorized_async(
            request.operation_id,
            request.expected_revision,
            request.mutation_id,
            request.recovery_due_at_ms,
            None,
            authority,
            false,
            |_| Ok(()),
        )
        .await
    }

    async fn record_external_fence_async(
        &self,
        request: DmlExternalFenceMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.apply_side_record_mutation_async(
            request.operation_id,
            request.expected_revision,
            request.mutation_id,
            recovery_due_at_ms,
            DmlSideRecord::ExternalFence(Box::new(request.fence)),
            authority,
        )
        .await
    }

    async fn record_historical_write_recovery_async(
        &self,
        request: DmlHistoricalWriteRecoveryMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.apply_side_record_mutation_async(
            request.operation_id,
            request.expected_revision,
            request.mutation_id,
            recovery_due_at_ms,
            DmlSideRecord::HistoricalWriteRecovery(Box::new(request.recovery)),
            authority,
        )
        .await
    }

    async fn record_direct_mutation_fence_async(
        &self,
        request: DmlDirectMutationFenceMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.apply_side_record_mutation_async(
            request.operation_id,
            request.expected_revision,
            request.mutation_id,
            recovery_due_at_ms,
            DmlSideRecord::DirectMutationFence(Box::new(request.fence)),
            authority,
        )
        .await
    }

    async fn record_historical_data_mutation_recovery_async(
        &self,
        request: DmlHistoricalDataMutationRecoveryMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.apply_side_record_mutation_async(
            request.operation_id,
            request.expected_revision,
            request.mutation_id,
            recovery_due_at_ms,
            DmlSideRecord::HistoricalDataMutationRecovery(Box::new(request.recovery)),
            authority,
        )
        .await
    }

    async fn record_ctas_recovery_async(
        &self,
        request: DmlCtasRecoveryMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.apply_side_record_mutation_async(
            request.operation_id,
            request.expected_revision,
            request.mutation_id,
            recovery_due_at_ms,
            DmlSideRecord::CtasRecovery(Box::new(request.recovery)),
            authority,
        )
        .await
    }

    /// Publish one CP-3B or CP-3C side record atomically with the operation
    /// revision it belongs to.
    ///
    /// The single StateStore transaction validates the dynamic latest lease
    /// fence, the expected operation revision, and the persisted coordination
    /// attempt before it writes anything, so a stale response can never change
    /// the journal.
    async fn apply_side_record_mutation_async(
        &self,
        operation_id: DmlOperationId,
        expected_revision: u64,
        mutation_id: Uuid,
        recovery_due_at_ms: Option<i64>,
        side: DmlSideRecord,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        let operation_key = operation_key(operation_id)?;
        let unfinished_key = unfinished_key(operation_id)?;
        let side_key = side.key(operation_id)?;
        let action = side.action();
        let max_value_bytes = self.store.limits().max_value_bytes;
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            OperationId::from(mutation_id),
            action,
            |transaction| {
                let operation_key = operation_key.clone();
                let unfinished_key = unfinished_key.clone();
                let side_key = side_key.clone();
                let side = side.clone();
                let authority = authority.clone();
                Box::pin(async move {
                    if let Err(error) = authority.validator().validate_in(transaction).await {
                        return Ok(Err(error));
                    }
                    let Some(record) = transaction.get(&operation_key).await? else {
                        return Ok(Err(DmlError::journal_unavailable(format!(
                            "DML operation {operation_id} not found"
                        ))));
                    };
                    let operation_version = record.version.clone();
                    let mut operation = match decode_operation(record.key, record.value) {
                        Ok(operation) => operation,
                        Err(error) => return Ok(Err(error)),
                    };
                    let stored_side = transaction.get(&side_key).await?;
                    let side_version = stored_side
                        .as_ref()
                        .map(|record| record.version.clone());
                    let existing_side = match stored_side
                        .map(|record| side.decode(&side_key, record.value))
                        .transpose()
                    {
                        Ok(existing_side) => existing_side,
                        Err(error) => return Ok(Err(error)),
                    };
                    if operation.last_mutation_id == mutation_id {
                        let applied_revision = match expected_revision.checked_add(1) {
                            Some(revision) => revision,
                            None => {
                                return Ok(Err(DmlError::journal_corruption(format!(
                                    "DML operation {operation_id} revision overflow"
                                ))));
                            }
                        };
                        if operation.revision == applied_revision
                            && operation.recovery_due_at_ms == recovery_due_at_ms
                            && existing_side.as_ref() == Some(&side)
                            && operation
                                .coordination_provenance
                                .as_ref()
                                .map(|provenance| provenance.coordination_attempt_id)
                                == Some(authority.coordination_attempt_id())
                        {
                            return Ok(Ok(operation));
                        }
                        return Ok(Err(DmlError::journal_unresolved(format!(
                            "conflicting authorized DML {} replay for operation {operation_id}",
                            side.label()
                        ))));
                    }
                    if operation.revision != expected_revision {
                        return Ok(Err(DmlError::journal_unresolved(format!(
                            "DML operation {operation_id} revision changed from expected {expected_revision} to {}",
                            operation.revision
                        ))));
                    }
                    if let Err(error) = validate_persisted_authority(&operation, &authority) {
                        return Ok(Err(error));
                    }
                    if let Err(error) =
                        side.validate_against(&operation, existing_side.as_ref(), &authority)
                    {
                        return Ok(Err(error));
                    }
                    // Writing a historical recovery record supersedes whatever
                    // was durable, so a mutation that resolves the recovery may
                    // release the bounded scan in the same transaction. Every
                    // other side record must read both durable recoveries.
                    let historical = match &side {
                        DmlSideRecord::HistoricalWriteRecovery(recovery) => {
                            Some(recovery.as_ref().clone())
                        }
                        _ => match load_historical_write_recovery_in(transaction, operation_id).await
                        {
                            Ok(historical) => historical,
                            Err(error) => return Ok(Err(error)),
                        },
                    };
                    let direct_mutation = match &side {
                        DmlSideRecord::HistoricalDataMutationRecovery(recovery) => {
                            Some(recovery.as_ref().clone())
                        }
                        _ => match load_historical_data_mutation_recovery_in(
                            transaction,
                            operation_id,
                        )
                        .await
                        {
                            Ok(direct_mutation) => direct_mutation,
                            Err(error) => return Ok(Err(error)),
                        },
                    };
                    let ctas = match &side {
                        DmlSideRecord::CtasRecovery(recovery) => {
                            Some(recovery.as_ref().clone())
                        }
                        _ => match load_ctas_recovery_in(transaction, operation_id).await {
                            Ok(ctas) => ctas,
                            Err(error) => return Ok(Err(error)),
                        },
                    };
                    if let Err(error) = validate_historical_retention(
                        operation_id,
                        historical.as_ref(),
                        direct_mutation.as_ref(),
                        ctas.as_ref(),
                        recovery_due_at_ms,
                    ) {
                        return Ok(Err(error));
                    }
                    let previous = operation.clone();
                    operation.schema_version = DML_OPERATION_SCHEMA_VERSION;
                    operation.revision = match operation.revision.checked_add(1) {
                        Some(revision) => revision,
                        None => {
                            return Ok(Err(DmlError::journal_corruption(format!(
                                "DML operation {operation_id} revision overflow"
                            ))));
                        }
                    };
                    operation.last_mutation_id = mutation_id;
                    operation.recovery_due_at_ms = recovery_due_at_ms;
                    operation.updated_at_ms = now_unix_millis();
                    if let Err(error) = validate_operation(&operation) {
                        return Ok(Err(error));
                    }
                    if let Err(error) = validate_recovery_due_scope(
                        &operation,
                        historical.as_ref(),
                        direct_mutation.as_ref(),
                        ctas.as_ref(),
                    ) {
                        return Ok(Err(error));
                    }
                    let operation_value =
                        match encode_operation_with_limit(&operation, max_value_bytes) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                    let side_value = match side.encode(max_value_bytes) {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    transaction
                        .put(
                            operation_key,
                            operation_value,
                            Precondition::Version(operation_version),
                        )
                        .await?;
                    transaction
                        .put(
                            side_key,
                            side_value,
                            match side_version {
                                Some(version) => Precondition::Version(version),
                                None => Precondition::Absent,
                            },
                        )
                        .await?;
                    if let Err(error) =
                        update_recovery_due_index(transaction, &previous, &operation).await
                    {
                        return Ok(Err(error));
                    }
                    if let Err(error) =
                        update_unfinished_index(transaction, &operation, &unfinished_key).await
                    {
                        return Ok(Err(error));
                    }
                    Ok(Ok(operation))
                })
            },
        )
        .await;
        self.finish_statement_mutation(result, operation_key, mutation_id, action)
            .await
    }

    async fn load_external_fence_async(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<DmlExternalFenceReceiptRecord>, DmlError> {
        let key = external_fence_key(operation_id)?;
        self.load_side_record(&key)
            .await?
            .map(|value| decode_external_fence(&key, value))
            .transpose()
    }

    async fn load_historical_write_recovery_async(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<DmlHistoricalWriteRecoveryRecord>, DmlError> {
        let key = historical_write_recovery_key(operation_id)?;
        self.load_side_record(&key)
            .await?
            .map(|value| decode_historical_write_recovery(&key, value))
            .transpose()
    }

    async fn load_direct_mutation_fence_async(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<DmlDirectMutationFenceReceiptRecord>, DmlError> {
        let key = direct_mutation_fence_key(operation_id)?;
        self.load_side_record(&key)
            .await?
            .map(|value| decode_direct_mutation_fence(&key, value))
            .transpose()
    }

    async fn load_historical_data_mutation_recovery_async(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<DmlHistoricalDataMutationRecoveryRecord>, DmlError> {
        let key = historical_data_mutation_recovery_key(operation_id)?;
        self.load_side_record(&key)
            .await?
            .map(|value| decode_historical_data_mutation_recovery(&key, value))
            .transpose()
    }

    async fn load_ctas_recovery_async(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<DmlCtasRecoveryRecord>, DmlError> {
        let key = ctas_recovery_key(operation_id)?;
        self.load_side_record(&key)
            .await?
            .map(|value| decode_ctas_recovery(&key, value))
            .transpose()
    }

    async fn load_side_record(&self, key: &Key) -> Result<Option<Value>, DmlError> {
        let mut transaction = self
            .store
            .begin_read()
            .await
            .map_err(DmlError::journal_unavailable)?;
        let record = transaction
            .get(key)
            .await
            .map_err(DmlError::journal_unavailable)?;
        transaction
            .abort()
            .await
            .map_err(DmlError::journal_unavailable)?;
        Ok(record.map(|record| record.value))
    }

    // Design: ADR-0054 (docs/adr/ADR-0054-frontend-dml-operation-authority-boundary.md)
    async fn mutate_operation_authorized_async(
        &self,
        operation_id: DmlOperationId,
        expected_revision: u64,
        mutation_id: Uuid,
        recovery_due_at_ms: Option<i64>,
        admission: Option<Arc<dyn DmlIntentAdmissionValidator>>,
        authority: DmlMutationAuthority,
        allow_new_attempt: bool,
        mutation: impl Fn(&mut StoredOperation) -> Result<(), DmlError> + Clone + Send + Sync + 'static,
    ) -> Result<StoredOperation, DmlError> {
        let operation_key = operation_key(operation_id)?;
        let unfinished_key = unfinished_key(operation_id)?;
        let max_value_bytes = self.store.limits().max_value_bytes;
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            OperationId::from(mutation_id),
            "mutate authorized frontend DML operation",
            |transaction| {
                let operation_key = operation_key.clone();
                let unfinished_key = unfinished_key.clone();
                let admission = admission.clone();
                let authority = authority.clone();
                let mutation = mutation.clone();
                Box::pin(async move {
                    if let Some(admission) = admission
                        && let Err(error) = admission.validate_in(transaction).await
                    {
                        return Ok(Err(error));
                    }
                    if let Err(error) = authority.validator().validate_in(transaction).await {
                        return Ok(Err(error));
                    }
                    let Some(record) = transaction.get(&operation_key).await? else {
                        return Ok(Err(DmlError::journal_unavailable(format!(
                            "DML operation {operation_id} not found"
                        ))));
                    };
                    let operation_version = record.version.clone();
                    let mut operation = match decode_operation(record.key, record.value) {
                        Ok(operation) => operation,
                        Err(error) => return Ok(Err(error)),
                    };
                    if operation.last_mutation_id == mutation_id {
                        let applied_revision = expected_revision.checked_add(1).ok_or_else(|| {
                            DmlError::journal_corruption(format!(
                                "DML operation {operation_id} revision overflow"
                            ))
                        });
                        let applied_revision = match applied_revision {
                            Ok(revision) => revision,
                            Err(error) => return Ok(Err(error)),
                        };
                        if operation.revision == applied_revision
                            && operation.recovery_due_at_ms == recovery_due_at_ms
                            && operation
                                .coordination_provenance
                                .as_ref()
                                .map(|provenance| provenance.coordination_attempt_id)
                                == Some(authority.coordination_attempt_id())
                        {
                            return Ok(Ok(operation));
                        }
                        return Ok(Err(DmlError::journal_unresolved(format!(
                            "conflicting authorized DML mutation replay for operation {operation_id}"
                        ))));
                    }
                    if operation.revision != expected_revision {
                        return Ok(Err(DmlError::journal_unresolved(format!(
                            "DML operation {operation_id} revision changed from expected {expected_revision} to {}",
                            operation.revision
                        ))));
                    }
                    if !allow_new_attempt
                        && let Err(error) = validate_persisted_authority(&operation, &authority)
                    {
                        return Ok(Err(error));
                    }
                    let historical =
                        match load_historical_write_recovery_in(transaction, operation_id).await {
                            Ok(historical) => historical,
                            Err(error) => return Ok(Err(error)),
                        };
                    let direct_mutation = match load_historical_data_mutation_recovery_in(
                        transaction,
                        operation_id,
                    )
                    .await
                    {
                        Ok(direct_mutation) => direct_mutation,
                        Err(error) => return Ok(Err(error)),
                    };
                    let ctas = match load_ctas_recovery_in(transaction, operation_id).await {
                        Ok(ctas) => ctas,
                        Err(error) => return Ok(Err(error)),
                    };
                    if let Err(error) = validate_historical_retention(
                        operation_id,
                        historical.as_ref(),
                        direct_mutation.as_ref(),
                        ctas.as_ref(),
                        recovery_due_at_ms,
                    ) {
                        return Ok(Err(error));
                    }
                    let previous = operation.clone();
                    if let Err(error) = mutation(&mut operation) {
                        return Ok(Err(error));
                    }
                    operation.schema_version = DML_OPERATION_SCHEMA_VERSION;
                    operation.revision = match operation.revision.checked_add(1) {
                        Some(revision) => revision,
                        None => {
                            return Ok(Err(DmlError::journal_corruption(format!(
                                "DML operation {operation_id} revision overflow"
                            ))));
                        }
                    };
                    operation.last_mutation_id = mutation_id;
                    operation.recovery_due_at_ms = recovery_due_at_ms;
                    operation.updated_at_ms = now_unix_millis();
                    if let Err(error) = validate_operation(&operation) {
                        return Ok(Err(error));
                    }
                    if let Err(error) = validate_recovery_due_scope(
                        &operation,
                        historical.as_ref(),
                        direct_mutation.as_ref(),
                        ctas.as_ref(),
                    ) {
                        return Ok(Err(error));
                    }
                    let value = match encode_operation_with_limit(&operation, max_value_bytes) {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    transaction
                        .put(operation_key, value, Precondition::Version(operation_version))
                        .await?;
                    if let Err(error) =
                        update_recovery_due_index(transaction, &previous, &operation).await
                    {
                        return Ok(Err(error));
                    }
                    if let Err(error) =
                        update_unfinished_index(transaction, &operation, &unfinished_key).await
                    {
                        return Ok(Err(error));
                    }
                    Ok(Ok(operation))
                })
            },
        )
        .await;
        self.finish_statement_mutation(result, operation_key, mutation_id, "authorized mutation")
            .await
    }

    async fn transition_async(
        &self,
        operation_id: DmlOperationId,
        to: OperationState,
    ) -> Result<(), DmlError> {
        self.mutate_operation(operation_id, "transition", move |operation| {
            validate_operation_transition(operation.state, to)
                .map_err(DmlError::journal_unavailable)?;
            operation.state = to;
            if to.is_finished() {
                operation.finished_at_ms = Some(now_unix_millis());
            }
            Ok(())
        })
        .await
    }

    async fn record_fact_async(
        &self,
        operation_id: DmlOperationId,
        fact: OperationFact,
    ) -> Result<(), DmlError> {
        self.mutate_operation(operation_id, "record fact", move |operation| {
            validate_operation_transition(operation.state, fact.state)
                .map_err(DmlError::journal_unavailable)?;
            if operation.state == fact.state {
                let identical = matches!(
                    &operation.payload,
                    OperationPayload::ConnectorWriteLifecycle(existing) if existing == &fact.lifecycle
                );
                if !identical {
                    return Err(DmlError::journal_unavailable(format!(
                        "conflicting DML operation fact replay for operation {operation_id} in state {}",
                        fact.state.as_str()
                    )));
                }
            }
            operation.state = fact.state;
            operation.payload = OperationPayload::ConnectorWriteLifecycle(fact.lifecycle.clone());
            if fact.state.is_finished() {
                operation.finished_at_ms = Some(now_unix_millis());
            }
            Ok(())
        })
        .await
    }

    async fn mutate_operation(
        &self,
        operation_id: DmlOperationId,
        action: &'static str,
        mutation: impl Fn(&mut StoredOperation) -> Result<(), DmlError> + Clone + Send + Sync + 'static,
    ) -> Result<(), DmlError> {
        let mutation_id = Uuid::now_v7();
        let operation_key = operation_key(operation_id)?;
        let unfinished_key = unfinished_key(operation_id)?;
        let max_value_bytes = self.store.limits().max_value_bytes;
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            OperationId::from(mutation_id),
            "mutate frontend DML operation",
            |transaction| {
                let operation_key = operation_key.clone();
                let unfinished_key = unfinished_key.clone();
                let mutation = mutation.clone();
                Box::pin(async move {
                    let Some(record) = transaction.get(&operation_key).await? else {
                        return Ok(Err(DmlError::journal_unavailable(format!(
                            "DML operation {operation_id} not found"
                        ))));
                    };
                    let operation_version = record.version.clone();
                    let mut operation = match decode_operation(record.key, record.value) {
                        Ok(operation) => operation,
                        Err(error) => return Ok(Err(error)),
                    };
                    let previous = operation.clone();
                    if let Err(error) = mutation(&mut operation) {
                        return Ok(Err(error));
                    }
                    operation.revision = match operation.revision.checked_add(1) {
                        Some(revision) => revision,
                        None => {
                            return Ok(Err(DmlError::journal_corruption(format!(
                                "DML operation {operation_id} revision overflow"
                            ))));
                        }
                    };
                    operation.schema_version = DML_OPERATION_SCHEMA_VERSION;
                    operation.last_mutation_id = mutation_id;
                    operation.recovery_due_at_ms =
                        legacy_recovery_due_after_mutation(&operation, &previous);
                    operation.updated_at_ms = now_unix_millis();
                    if let Err(error) = validate_operation(&operation) {
                        return Ok(Err(error));
                    }
                    let operation_value = match encode_operation_with_limit(&operation, max_value_bytes) {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    transaction
                        .put(
                            operation_key,
                            operation_value,
                            Precondition::Version(operation_version),
                        )
                        .await?;
                    if let Err(error) =
                        update_recovery_due_index(transaction, &previous, &operation).await
                    {
                        return Ok(Err(error));
                    }
                    if operation.state.is_finished() {
                        let Some(index) = transaction.get(&unfinished_key).await? else {
                            return Ok(Err(DmlError::journal_corruption(format!(
                                "DML operation {operation_id} is missing its unfinished index"
                            ))));
                        };
                        if let Err(error) = decode_unfinished(index.key, index.value) {
                            return Ok(Err(error));
                        }
                        transaction
                            .delete(unfinished_key, Precondition::Version(index.version))
                            .await?;
                    } else {
                        let existing = transaction.get(&unfinished_key).await?;
                        let precondition = match existing {
                            Some(index) => {
                                let indexed_id = match decode_unfinished(index.key, index.value) {
                                    Ok(indexed_id) => indexed_id,
                                    Err(error) => return Ok(Err(error)),
                                };
                                if indexed_id != operation_id {
                                    return Ok(Err(DmlError::journal_corruption(format!(
                                        "unfinished DML operation index identity mismatch for {operation_id}"
                                    ))));
                                }
                                Precondition::Version(index.version)
                            }
                            None => {
                                return Ok(Err(DmlError::journal_corruption(format!(
                                    "DML operation {operation_id} is missing its unfinished index"
                                ))));
                            }
                        };
                        let value = match encode_unfinished(operation_id) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                        transaction.put(unfinished_key, value, precondition).await?;
                    }
                    Ok(Ok(()))
                })
            },
        )
        .await;
        self.finish_mutation(result, operation_key, mutation_id, (), action)
            .await
    }

    async fn finish_mutation<T>(
        &self,
        result: Result<novarocks_state_store::RunSuccess<Result<T, DmlError>>, RunFailure>,
        operation_key: Key,
        mutation_id: Uuid,
        committed_value: T,
        action: &str,
    ) -> Result<T, DmlError> {
        match result {
            Ok(success) => success.value,
            Err(RunFailure::CommitUnknown { .. }) => {
                match self
                    .load_authoritative_mutation(&operation_key, mutation_id)
                    .await?
                {
                    Some(_) => Ok(committed_value),
                    _ => Err(DmlError::journal_unresolved(format!(
                        "DML journal {action} commit outcome is unresolved"
                    ))),
                }
            }
            Err(failure) => Err(format_run_failure(action, failure)),
        }
    }

    async fn finish_statement_mutation(
        &self,
        result: Result<
            novarocks_state_store::RunSuccess<Result<StoredOperation, DmlError>>,
            RunFailure,
        >,
        operation_key: Key,
        mutation_id: Uuid,
        action: &str,
    ) -> Result<StoredOperation, DmlError> {
        match result {
            Ok(success) => success.value,
            Err(RunFailure::CommitUnknown { .. }) => {
                match self
                    .load_authoritative_mutation(&operation_key, mutation_id)
                    .await?
                {
                    Some(operation) => Ok(operation),
                    _ => Err(DmlError::journal_unresolved(format!(
                        "DML journal statement {action} commit outcome is unresolved"
                    ))),
                }
            }
            Err(failure) => Err(format_run_failure(action, failure)),
        }
    }

    async fn load_authoritative_mutation(
        &self,
        operation_key: &Key,
        mutation_id: Uuid,
    ) -> Result<Option<StoredOperation>, DmlError> {
        let mut transaction = self
            .store
            .begin_read()
            .await
            .map_err(DmlError::journal_unavailable)?;
        let Some(record) = transaction
            .get(operation_key)
            .await
            .map_err(DmlError::journal_unavailable)?
        else {
            transaction
                .abort()
                .await
                .map_err(DmlError::journal_unavailable)?;
            return Ok(None);
        };
        let operation = decode_operation(record.key, record.value)?;
        if operation.last_mutation_id != mutation_id {
            transaction
                .abort()
                .await
                .map_err(DmlError::journal_unavailable)?;
            return Ok(None);
        }
        if let Some(due_at_ms) = operation.recovery_due_at_ms {
            let due_key = recovery_due_key(operation.operation_id, due_at_ms)?;
            let Some(indexed) = transaction
                .get(&due_key)
                .await
                .map_err(DmlError::journal_unavailable)?
            else {
                transaction
                    .abort()
                    .await
                    .map_err(DmlError::journal_unavailable)?;
                return Ok(None);
            };
            if validate_recovery_due_record(&operation, indexed).is_err() {
                transaction
                    .abort()
                    .await
                    .map_err(DmlError::journal_unavailable)?;
                return Ok(None);
            }
        }
        transaction
            .abort()
            .await
            .map_err(DmlError::journal_unavailable)?;
        Ok(Some(operation))
    }

    async fn load_async(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<StoredOperation>, DmlError> {
        self.load_by_key(&operation_key(operation_id)?).await
    }

    async fn load_by_key(&self, key: &Key) -> Result<Option<StoredOperation>, DmlError> {
        let mut transaction = self
            .store
            .begin_read()
            .await
            .map_err(DmlError::journal_unavailable)?;
        let record = transaction
            .get(key)
            .await
            .map_err(DmlError::journal_unavailable)?;
        transaction
            .abort()
            .await
            .map_err(DmlError::journal_unavailable)?;
        record
            .map(|record| decode_operation(record.key, record.value))
            .transpose()
    }

    async fn scan_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
        let records = self.scan_prefix(OPERATION_PREFIX).await?;
        records
            .into_iter()
            .map(|record| decode_operation(record.key, record.value))
            .collect()
    }

    async fn scan_unfinished_ids(&self) -> Result<Vec<DmlOperationId>, DmlError> {
        let records = self.scan_prefix(UNFINISHED_PREFIX).await?;
        records
            .into_iter()
            .map(|record| decode_unfinished(record.key, record.value))
            .collect()
    }

    async fn list_unfinished_async(&self) -> Result<Vec<StoredOperation>, DmlError> {
        let operation_ids = self.scan_unfinished_ids().await?;
        let mut operations = Vec::with_capacity(operation_ids.len());
        for operation_id in operation_ids {
            let operation = self.load_async(operation_id).await?.ok_or_else(|| {
                DmlError::journal_corruption(format!(
                    "unfinished DML operation index {operation_id} has no operation record"
                ))
            })?;
            if operation.state.is_finished() {
                return Err(DmlError::journal_corruption(format!(
                    "terminal DML operation {operation_id} remains in the unfinished index"
                )));
            }
            operations.push(operation);
        }
        Ok(operations)
    }

    async fn recovery_candidates_async(
        &self,
        shard: u8,
        due_at_or_before_ms: i64,
    ) -> Result<Vec<DmlRecoveryCandidate>, DmlError> {
        if shard >= DML_RECOVERY_SHARD_COUNT {
            return Err(DmlError::journal_unavailable(format!(
                "DML recovery shard {shard} is outside 0..{DML_RECOVERY_SHARD_COUNT}"
            )));
        }
        let prefix = recovery_due_shard_prefix(shard)?;
        let range = KeyRange::for_prefix(prefix).map_err(DmlError::journal_unavailable)?;
        let mut transaction = self
            .store
            .begin_read()
            .await
            .map_err(DmlError::journal_unavailable)?;
        let page = transaction
            .range(&RangeRequest {
                range,
                direction: Direction::Forward,
                page_size: DML_RECOVERY_PAGE_SIZE.min(self.store.limits().max_page_size),
                continuation: None,
            })
            .await
            .map_err(DmlError::journal_unavailable)?;
        let mut candidates = Vec::with_capacity(page.records.len());
        for record in page.records {
            let indexed = decode_recovery_due(&record.key, record.value)?;
            if indexed.recovery_due_at_ms > due_at_or_before_ms {
                break;
            }
            let operation_record = transaction
                .get(&operation_key(indexed.operation_id)?)
                .await
                .map_err(DmlError::journal_unavailable)?
                .ok_or_else(|| {
                    DmlError::journal_corruption(format!(
                        "DML recovery due index {} has no operation",
                        indexed.operation_id
                    ))
                })?;
            let operation = decode_operation(operation_record.key, operation_record.value)?;
            validate_recovery_due_identity(&operation, &indexed)?;
            candidates.push(DmlRecoveryCandidate {
                operation_id: indexed.operation_id,
                operation_revision: indexed.operation_revision,
                last_mutation_id: indexed.last_mutation_id,
                coordination_attempt_id: indexed.coordination_attempt_id,
                recovery_due_at_ms: indexed.recovery_due_at_ms,
            });
        }
        transaction
            .abort()
            .await
            .map_err(DmlError::journal_unavailable)?;
        Ok(candidates)
    }

    async fn load_add_files_artifact_async(
        &self,
        operation_id: DmlOperationId,
        descriptor: AddFilesArtifactDescriptor,
    ) -> Result<AddFilesArtifact, DmlError> {
        validate_add_files_artifact(&descriptor)?;
        let mut transaction = self
            .store
            .begin_read()
            .await
            .map_err(DmlError::journal_unavailable)?;
        let mut bytes = Vec::with_capacity(descriptor.total_length as usize);
        for chunk_index in 0..descriptor.chunk_count {
            let key = add_files_artifact_chunk_key(operation_id, &descriptor, chunk_index)?;
            let Some(record) = transaction
                .get(&key)
                .await
                .map_err(DmlError::journal_unavailable)?
            else {
                return Err(DmlError::journal_corruption(
                    "ADD FILES artifact descriptor references a missing chunk",
                ));
            };
            if record.value.as_bytes().is_empty()
                || record.value.as_bytes().len() > ADD_FILES_ARTIFACT_CHUNK_BYTES
            {
                return Err(DmlError::journal_corruption(
                    "ADD FILES artifact chunk is invalid",
                ));
            }
            bytes.extend_from_slice(record.value.as_bytes());
        }
        transaction
            .abort()
            .await
            .map_err(DmlError::journal_unavailable)?;
        let artifact = AddFilesArtifact { descriptor, bytes };
        validate_add_files_artifact_bytes(&artifact)?;
        Ok(artifact)
    }

    async fn scan_prefix(&self, prefix: &'static [u8]) -> Result<Vec<StateRecord>, DmlError> {
        let prefix =
            Key::try_from(Bytes::from_static(prefix)).map_err(DmlError::journal_corruption)?;
        let range = KeyRange::for_prefix(prefix).map_err(DmlError::journal_corruption)?;
        let mut transaction = self
            .store
            .begin_read()
            .await
            .map_err(DmlError::journal_unavailable)?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size: self.store.limits().max_page_size,
            continuation: None,
        };
        let mut records = Vec::new();
        loop {
            let page = transaction
                .range(&request)
                .await
                .map_err(DmlError::journal_unavailable)?;
            records.extend(page.records);
            let Some(continuation) = page.continuation else {
                break;
            };
            request.continuation = Some(continuation);
        }
        transaction
            .abort()
            .await
            .map_err(DmlError::journal_unavailable)?;
        Ok(records)
    }
}

impl OperationJournal for StateStoreOperationJournal {
    fn create_preparing_admitted(
        &self,
        request: CreatePreparingRequest,
        admission: Arc<dyn DmlIntentAdmissionValidator>,
    ) -> Result<DmlOperationId, DmlError> {
        self.blocking(self.create_preparing_async(request, Some(admission)))
    }

    fn create_statement_operation_admitted(
        &self,
        request: CreateStatementOperationRequest,
        admission: Arc<dyn DmlIntentAdmissionValidator>,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.create_statement_operation_async(request, Some(admission)))
    }

    fn claim_operation(
        &self,
        request: DmlCoordinationClaimRequest,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.claim_operation_async(request, None, authority))
    }

    fn claim_operation_admitted(
        &self,
        request: DmlCoordinationClaimRequest,
        admission: Arc<dyn DmlIntentAdmissionValidator>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.claim_operation_async(request, Some(admission), authority))
    }

    fn transition_authorized(
        &self,
        operation_id: DmlOperationId,
        expected_revision: u64,
        mutation_id: Uuid,
        to: OperationState,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.transition_authorized_async(
            operation_id,
            expected_revision,
            mutation_id,
            to,
            recovery_due_at_ms,
            authority,
        ))
    }

    fn record_fact_authorized(
        &self,
        operation_id: DmlOperationId,
        expected_revision: u64,
        mutation_id: Uuid,
        fact: OperationFact,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.record_fact_authorized_async(
            operation_id,
            expected_revision,
            mutation_id,
            fact,
            recovery_due_at_ms,
            authority,
        ))
    }

    fn mutate_statement_operation_authorized(
        &self,
        request: OperationMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.mutate_statement_operation_async(
            request,
            Some(authority),
            Some(recovery_due_at_ms),
        ))
    }

    fn apply_add_files_mutation_authorized(
        &self,
        request: AddFilesMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.apply_add_files_mutation_async(
            request,
            Some(authority),
            Some(recovery_due_at_ms),
        ))
    }

    fn record_external_fence_authorized(
        &self,
        request: DmlExternalFenceMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.record_external_fence_async(request, recovery_due_at_ms, authority))
    }

    fn record_historical_write_recovery_authorized(
        &self,
        request: DmlHistoricalWriteRecoveryMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.record_historical_write_recovery_async(
            request,
            recovery_due_at_ms,
            authority,
        ))
    }

    fn load_external_fence(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<DmlExternalFenceReceiptRecord>, DmlError> {
        self.blocking(self.load_external_fence_async(operation_id))
    }

    fn load_historical_write_recovery(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<DmlHistoricalWriteRecoveryRecord>, DmlError> {
        self.blocking(self.load_historical_write_recovery_async(operation_id))
    }

    fn preflight_external_fence(
        &self,
        request: &DmlExternalFenceMutationRequest,
    ) -> Result<(), DmlError> {
        validate_external_fence_receipt(&request.fence).map_err(DmlError::journal_corruption)?;
        DmlSideRecord::ExternalFence(Box::new(request.fence.clone()))
            .encode(self.store.limits().max_value_bytes)
            .map(|_| ())
    }

    fn preflight_historical_write_recovery(
        &self,
        request: &DmlHistoricalWriteRecoveryMutationRequest,
    ) -> Result<(), DmlError> {
        validate_historical_write_recovery(&request.recovery)
            .map_err(DmlError::journal_corruption)?;
        DmlSideRecord::HistoricalWriteRecovery(Box::new(request.recovery.clone()))
            .encode(self.store.limits().max_value_bytes)
            .map(|_| ())
    }

    fn record_direct_mutation_fence_authorized(
        &self,
        request: DmlDirectMutationFenceMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.record_direct_mutation_fence_async(
            request,
            recovery_due_at_ms,
            authority,
        ))
    }

    fn record_historical_data_mutation_recovery_authorized(
        &self,
        request: DmlHistoricalDataMutationRecoveryMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.record_historical_data_mutation_recovery_async(
            request,
            recovery_due_at_ms,
            authority,
        ))
    }

    fn load_direct_mutation_fence(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<DmlDirectMutationFenceReceiptRecord>, DmlError> {
        self.blocking(self.load_direct_mutation_fence_async(operation_id))
    }

    fn load_historical_data_mutation_recovery(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<DmlHistoricalDataMutationRecoveryRecord>, DmlError> {
        self.blocking(self.load_historical_data_mutation_recovery_async(operation_id))
    }

    fn preflight_direct_mutation_fence(
        &self,
        request: &DmlDirectMutationFenceMutationRequest,
    ) -> Result<(), DmlError> {
        validate_direct_mutation_fence_receipt(&request.fence)
            .map_err(DmlError::journal_corruption)?;
        DmlSideRecord::DirectMutationFence(Box::new(request.fence.clone()))
            .encode(self.store.limits().max_value_bytes)
            .map(|_| ())
    }

    fn preflight_historical_data_mutation_recovery(
        &self,
        request: &DmlHistoricalDataMutationRecoveryMutationRequest,
    ) -> Result<(), DmlError> {
        validate_historical_data_mutation_recovery(&request.recovery)
            .map_err(DmlError::journal_corruption)?;
        DmlSideRecord::HistoricalDataMutationRecovery(Box::new(request.recovery.clone()))
            .encode(self.store.limits().max_value_bytes)
            .map(|_| ())
    }

    fn record_ctas_recovery_authorized(
        &self,
        request: DmlCtasRecoveryMutationRequest,
        recovery_due_at_ms: Option<i64>,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.record_ctas_recovery_async(request, recovery_due_at_ms, authority))
    }

    fn load_ctas_recovery(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<DmlCtasRecoveryRecord>, DmlError> {
        self.blocking(self.load_ctas_recovery_async(operation_id))
    }

    fn preflight_ctas_recovery(
        &self,
        request: &DmlCtasRecoveryMutationRequest,
    ) -> Result<(), DmlError> {
        validate_ctas_recovery(&request.recovery).map_err(DmlError::journal_corruption)?;
        DmlSideRecord::CtasRecovery(Box::new(request.recovery.clone()))
            .encode(self.store.limits().max_value_bytes)
            .map(|_| ())
    }

    fn recovery_candidates(
        &self,
        shard: u8,
        due_at_or_before_ms: i64,
    ) -> Result<Vec<DmlRecoveryCandidate>, DmlError> {
        self.blocking(self.recovery_candidates_async(shard, due_at_or_before_ms))
    }

    fn reschedule_recovery_due(
        &self,
        request: DmlRecoveryDueRescheduleRequest,
        authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.reschedule_recovery_due_async(request, authority))
    }

    fn create_preparing(
        &self,
        request: CreatePreparingRequest,
    ) -> Result<DmlOperationId, DmlError> {
        self.blocking(self.create_preparing_async(request, None))
    }

    fn transition(&self, operation_id: DmlOperationId, to: OperationState) -> Result<(), DmlError> {
        self.blocking(self.transition_async(operation_id, to))
    }

    fn record_fact(
        &self,
        operation_id: DmlOperationId,
        fact: OperationFact,
    ) -> Result<(), DmlError> {
        self.blocking(self.record_fact_async(operation_id, fact))
    }

    fn load(&self, operation_id: DmlOperationId) -> Result<Option<StoredOperation>, DmlError> {
        self.blocking(self.load_async(operation_id))
    }

    fn list_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
        self.blocking(self.scan_operations())
    }

    fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError> {
        self.blocking(self.list_unfinished_async())
    }

    fn create_statement_operation(
        &self,
        request: CreateStatementOperationRequest,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.create_statement_operation_async(request, None))
    }

    fn mutate_statement_operation(
        &self,
        request: OperationMutationRequest,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.mutate_statement_operation_async(request, None, None))
    }

    fn preflight_statement_operation(&self, operation: &StoredOperation) -> Result<(), DmlError> {
        encode_operation_with_limit(operation, self.store.limits().max_value_bytes).map(|_| ())
    }

    fn preflight_add_files_mutation(
        &self,
        request: &AddFilesMutationRequest,
    ) -> Result<(), DmlError> {
        self.preflight_add_files_mutation_shape(request)?;
        let mut operation = self
            .load(request.operation.operation_id)?
            .ok_or_else(|| DmlError::journal_unavailable("DML operation not found"))?;
        operation.state = request.operation.state;
        operation.payload = request.operation.payload.clone();
        operation.schema_version = DML_OPERATION_SCHEMA_VERSION;
        encode_operation_with_limit(&operation, self.store.limits().max_value_bytes)?;
        Ok(())
    }

    fn apply_add_files_mutation(
        &self,
        request: AddFilesMutationRequest,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.apply_add_files_mutation_async(request, None, None))
    }

    fn load_add_files_artifact(
        &self,
        operation_id: DmlOperationId,
        artifact: &AddFilesArtifactDescriptor,
    ) -> Result<AddFilesArtifact, DmlError> {
        self.blocking(self.load_add_files_artifact_async(operation_id, artifact.clone()))
    }
}

impl StateStoreOperationJournal {
    fn preflight_add_files_mutation_shape(
        &self,
        request: &AddFilesMutationRequest,
    ) -> Result<(), DmlError> {
        let artifact_chunks = request
            .artifacts
            .iter()
            .try_fold(0usize, |count, artifact| {
                validate_add_files_artifact_bytes(artifact)?;
                count
                    .checked_add(artifact.descriptor.chunk_count as usize)
                    .ok_or_else(|| {
                        DmlError::journal_unavailable("ADD FILES artifact chunk count overflow")
                    })
            })?;
        let source_writes = usize::from(request.source_action.is_some());
        let required_operations = artifact_chunks
            .checked_add(source_writes)
            .and_then(|count| count.checked_add(3))
            .ok_or_else(|| {
                DmlError::journal_unavailable("ADD FILES transaction operation count overflow")
            })?;
        if required_operations > self.store.limits().max_transaction_operations {
            return Err(DmlError::journal_unavailable(format!(
                "ADD FILES atomic mutation needs {required_operations} StateStore operations, exceeding the configured limit {}",
                self.store.limits().max_transaction_operations
            )));
        }
        Ok(())
    }
}

/// One CP-3B, CP-3C, or CP-3D durable record that lives beside its DML operation.
///
/// Every kind is published in the same StateStore transaction that advances the
/// operation revision, so the operation record itself keeps its v8 shape and a
/// fenced mutation is the only way to change any of them.
#[derive(Clone, Debug, Eq, PartialEq)]
enum DmlSideRecord {
    ExternalFence(Box<DmlExternalFenceReceiptRecord>),
    HistoricalWriteRecovery(Box<DmlHistoricalWriteRecoveryRecord>),
    DirectMutationFence(Box<DmlDirectMutationFenceReceiptRecord>),
    HistoricalDataMutationRecovery(Box<DmlHistoricalDataMutationRecoveryRecord>),
    CtasRecovery(Box<DmlCtasRecoveryRecord>),
}

impl DmlSideRecord {
    const fn label(&self) -> &'static str {
        match self {
            Self::ExternalFence(_) => "external fence receipt",
            Self::HistoricalWriteRecovery(_) => "historical write recovery",
            Self::DirectMutationFence(_) => "direct mutation fence receipt",
            Self::HistoricalDataMutationRecovery(_) => "historical data mutation recovery",
            Self::CtasRecovery(_) => "CTAS recovery",
        }
    }

    const fn action(&self) -> &'static str {
        match self {
            Self::ExternalFence(_) => "record frontend DML external fence receipt",
            Self::HistoricalWriteRecovery(_) => "record frontend DML historical write recovery",
            Self::DirectMutationFence(_) => "record frontend DML direct mutation fence receipt",
            Self::HistoricalDataMutationRecovery(_) => {
                "record frontend DML historical data mutation recovery"
            }
            Self::CtasRecovery(_) => "record frontend CTAS recovery",
        }
    }

    fn key(&self, operation_id: DmlOperationId) -> Result<Key, DmlError> {
        match self {
            Self::ExternalFence(_) => external_fence_key(operation_id),
            Self::HistoricalWriteRecovery(_) => historical_write_recovery_key(operation_id),
            Self::DirectMutationFence(_) => direct_mutation_fence_key(operation_id),
            Self::HistoricalDataMutationRecovery(_) => {
                historical_data_mutation_recovery_key(operation_id)
            }
            Self::CtasRecovery(_) => ctas_recovery_key(operation_id),
        }
    }

    fn decode(&self, key: &Key, value: Value) -> Result<Self, DmlError> {
        match self {
            Self::ExternalFence(_) => decode_external_fence(key, value)
                .map(|record| Self::ExternalFence(Box::new(record))),
            Self::HistoricalWriteRecovery(_) => decode_historical_write_recovery(key, value)
                .map(|record| Self::HistoricalWriteRecovery(Box::new(record))),
            Self::DirectMutationFence(_) => decode_direct_mutation_fence(key, value)
                .map(|record| Self::DirectMutationFence(Box::new(record))),
            Self::HistoricalDataMutationRecovery(_) => {
                decode_historical_data_mutation_recovery(key, value)
                    .map(|record| Self::HistoricalDataMutationRecovery(Box::new(record)))
            }
            Self::CtasRecovery(_) => {
                decode_ctas_recovery(key, value).map(|record| Self::CtasRecovery(Box::new(record)))
            }
        }
    }

    fn encode(&self, max_value_bytes: usize) -> Result<Value, DmlError> {
        let encoded = match self {
            Self::ExternalFence(record) => serde_json::to_vec(record.as_ref()),
            Self::HistoricalWriteRecovery(record) => serde_json::to_vec(record.as_ref()),
            Self::DirectMutationFence(record) => serde_json::to_vec(record.as_ref()),
            Self::HistoricalDataMutationRecovery(record) => serde_json::to_vec(record.as_ref()),
            Self::CtasRecovery(record) => serde_json::to_vec(record.as_ref()),
        }
        .map_err(DmlError::journal_corruption)?;
        if encoded.len() > max_value_bytes {
            return Err(DmlError::journal_unavailable(format!(
                "DML {} encoded size {} exceeds StateStore value limit {max_value_bytes}",
                self.label(),
                encoded.len()
            )));
        }
        Value::try_from(Bytes::from(encoded)).map_err(DmlError::journal_unavailable)
    }

    fn validate_against(
        &self,
        operation: &StoredOperation,
        existing: Option<&Self>,
        authority: &DmlMutationAuthority,
    ) -> Result<(), DmlError> {
        match (self, existing) {
            (Self::ExternalFence(fence), existing) => {
                let existing = match existing {
                    Some(Self::ExternalFence(existing)) => Some(existing.as_ref()),
                    Some(_) => {
                        return Err(DmlError::journal_corruption(
                            "DML external fence key holds another record kind",
                        ));
                    }
                    None => None,
                };
                if fence.identity.coordination_attempt_id != authority.coordination_attempt_id() {
                    return Err(DmlError::journal_unresolved(format!(
                        "DML operation {} external fence receipt was minted by another coordination attempt",
                        operation.operation_id
                    )));
                }
                if !EXTERNAL_FENCE_ALLOWED_STATES.contains(&operation.state) {
                    return Err(DmlError::journal_unresolved(format!(
                        "DML operation {} cannot accept an external fence receipt in state {}",
                        operation.operation_id,
                        operation.state.as_str()
                    )));
                }
                validate_external_fence_transition(existing, fence)
                    .map_err(DmlError::journal_corruption)
            }
            (Self::HistoricalWriteRecovery(recovery), existing) => {
                let existing = match existing {
                    Some(Self::HistoricalWriteRecovery(existing)) => Some(existing.as_ref()),
                    Some(_) => {
                        return Err(DmlError::journal_corruption(
                            "DML historical write recovery key holds another record kind",
                        ));
                    }
                    None => None,
                };
                if recovery.recovery_attempt_id != authority.coordination_attempt_id() {
                    return Err(DmlError::journal_unresolved(format!(
                        "DML operation {} historical write recovery belongs to another coordination attempt",
                        operation.operation_id
                    )));
                }
                validate_historical_write_recovery_transition(existing, recovery)
                    .map_err(DmlError::journal_corruption)
            }
            (Self::DirectMutationFence(fence), existing) => {
                let existing = match existing {
                    Some(Self::DirectMutationFence(existing)) => Some(existing.as_ref()),
                    Some(_) => {
                        return Err(DmlError::journal_corruption(
                            "DML direct mutation fence key holds another record kind",
                        ));
                    }
                    None => None,
                };
                if fence.fence.identity.coordination_attempt_id
                    != authority.coordination_attempt_id()
                {
                    return Err(DmlError::journal_unresolved(format!(
                        "DML operation {} direct mutation fence receipt was minted by another coordination attempt",
                        operation.operation_id
                    )));
                }
                if operation.operation_kind != fence.operation_kind.operation_kind() {
                    return Err(DmlError::journal_corruption(format!(
                        "DML operation {} cannot accept a {} fence receipt",
                        operation.operation_id,
                        fence.operation_kind.as_str()
                    )));
                }
                if !EXTERNAL_FENCE_ALLOWED_STATES.contains(&operation.state) {
                    return Err(DmlError::journal_unresolved(format!(
                        "DML operation {} cannot accept a direct mutation fence receipt in state {}",
                        operation.operation_id,
                        operation.state.as_str()
                    )));
                }
                validate_direct_mutation_fence_transition(existing, fence)
                    .map_err(DmlError::journal_corruption)
            }
            (Self::HistoricalDataMutationRecovery(recovery), existing) => {
                let existing = match existing {
                    Some(Self::HistoricalDataMutationRecovery(existing)) => Some(existing.as_ref()),
                    Some(_) => {
                        return Err(DmlError::journal_corruption(
                            "DML historical data mutation recovery key holds another record kind",
                        ));
                    }
                    None => None,
                };
                if recovery.recovery_attempt_id != authority.coordination_attempt_id() {
                    return Err(DmlError::journal_unresolved(format!(
                        "DML operation {} historical data mutation recovery belongs to another coordination attempt",
                        operation.operation_id
                    )));
                }
                if operation.operation_kind != recovery.request.operation_kind.operation_kind() {
                    return Err(DmlError::journal_corruption(format!(
                        "DML operation {} cannot accept a {} historical data mutation recovery",
                        operation.operation_id,
                        recovery.request.operation_kind.as_str()
                    )));
                }
                validate_historical_data_mutation_recovery_transition(existing, recovery)
                    .map_err(DmlError::journal_corruption)
            }
            (Self::CtasRecovery(recovery), existing) => {
                let existing = match existing {
                    Some(Self::CtasRecovery(existing)) => Some(existing.as_ref()),
                    Some(_) => {
                        return Err(DmlError::journal_corruption(
                            "CTAS recovery key holds another record kind",
                        ));
                    }
                    None => None,
                };
                if operation.operation_kind != OperationKind::CreateTableAsSelect {
                    return Err(DmlError::journal_corruption(format!(
                        "DML operation {} cannot accept a CTAS recovery record",
                        operation.operation_id
                    )));
                }
                let OperationPayload::CtasSaga(saga) = &operation.payload else {
                    return Err(DmlError::journal_corruption(format!(
                        "DML operation {} has no CTAS saga payload",
                        operation.operation_id
                    )));
                };
                validate_ctas_recovery_against_saga(recovery, saga)?;
                if recovery.recovery_attempt_id != authority.coordination_attempt_id() {
                    return Err(DmlError::journal_unresolved(format!(
                        "DML operation {} CTAS recovery belongs to another coordination attempt",
                        operation.operation_id
                    )));
                }
                validate_ctas_recovery_transition(existing, recovery)
                    .map_err(DmlError::journal_corruption)
            }
        }
    }
}

fn validate_ctas_recovery_against_saga(
    recovery: &DmlCtasRecoveryRecord,
    saga: &CtasSagaRecord,
) -> Result<(), DmlError> {
    use crate::dml::model::{DmlCtasActionKind, DmlCtasDispatchCertainty};

    let mut chains = [
        (
            DmlCtasActionKind::AdvanceFence,
            recovery
                .catalog_fence_history
                .iter()
                .chain(recovery.catalog_fence.iter())
                .map(|fence| fence.action_id)
                .collect(),
        ),
        (DmlCtasActionKind::Stage, vec![saga.prepare_operation_id]),
        (DmlCtasActionKind::Write, vec![saga.write_operation_id]),
        (DmlCtasActionKind::Publish, vec![saga.publish_operation_id]),
        (
            DmlCtasActionKind::Abort,
            vec![saga.abort_staging_operation_id],
        ),
    ];
    let mut child_ids = BTreeSet::from([
        saga.prepare_operation_id,
        saga.write_operation_id,
        saga.publish_operation_id,
        saga.abort_staging_operation_id,
    ]);
    for supersession in &recovery.child_supersessions {
        let chain = chains
            .iter_mut()
            .find(|(action, _)| *action == supersession.action)
            .expect("all CTAS actions have a base child");
        if chain.1.last().copied() != Some(supersession.predecessor_child_operation_id) {
            return Err(DmlError::journal_corruption(
                "CTAS child supersession does not continue the durable action chain",
            ));
        }
        if !child_ids.insert(supersession.successor_child_operation_id) {
            return Err(DmlError::journal_corruption(
                "CTAS child supersession reuses an operation id",
            ));
        }
        chain.1.push(supersession.successor_child_operation_id);
    }
    for checkpoint in &recovery.dispatch_checkpoints {
        let belongs_to_action = chains
            .iter()
            .find(|(action, _)| *action == checkpoint.action)
            .map(|(_, children)| children.contains(&checkpoint.child_operation_id))
            .expect("all CTAS actions have a base child");
        if !belongs_to_action {
            return Err(DmlError::journal_corruption(
                "CTAS dispatch checkpoint is not bound to its action supersession chain",
            ));
        }
    }
    for observation in &recovery.historical_observations {
        if observation.action == DmlCtasActionKind::Write {
            return Err(DmlError::journal_corruption(
                "CTAS catalog recovery cannot classify the distributed write child",
            ));
        }
        let possibly_dispatched = observation.action == DmlCtasActionKind::AdvanceFence
            && recovery
                .catalog_fence_history
                .iter()
                .chain(recovery.catalog_fence.iter())
                .any(|fence| {
                    fence.action_id == observation.child_operation_id
                        && fence.receipt_payload.is_some()
                })
            || recovery.dispatch_checkpoints.iter().any(|checkpoint| {
                checkpoint.action == observation.action
                    && checkpoint.child_operation_id == observation.child_operation_id
                    && checkpoint.dispatch_certainty == DmlCtasDispatchCertainty::PossiblyDispatched
            });
        if !possibly_dispatched {
            return Err(DmlError::journal_corruption(
                "CTAS historical observation requires a possibly-dispatched action checkpoint",
            ));
        }
    }
    Ok(())
}

fn external_fence_key(operation_id: DmlOperationId) -> Result<Key, DmlError> {
    key_for(EXTERNAL_FENCE_PREFIX, operation_id)
}

fn historical_write_recovery_key(operation_id: DmlOperationId) -> Result<Key, DmlError> {
    key_for(HISTORICAL_WRITE_RECOVERY_PREFIX, operation_id)
}

fn direct_mutation_fence_key(operation_id: DmlOperationId) -> Result<Key, DmlError> {
    key_for(DIRECT_MUTATION_FENCE_PREFIX, operation_id)
}

fn historical_data_mutation_recovery_key(operation_id: DmlOperationId) -> Result<Key, DmlError> {
    key_for(HISTORICAL_DATA_MUTATION_RECOVERY_PREFIX, operation_id)
}

fn ctas_recovery_key(operation_id: DmlOperationId) -> Result<Key, DmlError> {
    key_for(CTAS_RECOVERY_PREFIX, operation_id)
}

fn decode_external_fence(
    key: &Key,
    value: Value,
) -> Result<DmlExternalFenceReceiptRecord, DmlError> {
    decode_key(EXTERNAL_FENCE_PREFIX, key)?;
    let record: DmlExternalFenceReceiptRecord =
        serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?;
    validate_external_fence_receipt(&record).map_err(DmlError::journal_corruption)?;
    Ok(record)
}

fn decode_historical_write_recovery(
    key: &Key,
    value: Value,
) -> Result<DmlHistoricalWriteRecoveryRecord, DmlError> {
    decode_key(HISTORICAL_WRITE_RECOVERY_PREFIX, key)?;
    let record: DmlHistoricalWriteRecoveryRecord =
        serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?;
    validate_historical_write_recovery(&record).map_err(DmlError::journal_corruption)?;
    Ok(record)
}

fn decode_direct_mutation_fence(
    key: &Key,
    value: Value,
) -> Result<DmlDirectMutationFenceReceiptRecord, DmlError> {
    decode_key(DIRECT_MUTATION_FENCE_PREFIX, key)?;
    let record: DmlDirectMutationFenceReceiptRecord =
        serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?;
    validate_direct_mutation_fence_receipt(&record).map_err(DmlError::journal_corruption)?;
    Ok(record)
}

fn decode_historical_data_mutation_recovery(
    key: &Key,
    value: Value,
) -> Result<DmlHistoricalDataMutationRecoveryRecord, DmlError> {
    decode_key(HISTORICAL_DATA_MUTATION_RECOVERY_PREFIX, key)?;
    let record: DmlHistoricalDataMutationRecoveryRecord =
        serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?;
    validate_historical_data_mutation_recovery(&record).map_err(DmlError::journal_corruption)?;
    Ok(record)
}

fn decode_ctas_recovery(key: &Key, value: Value) -> Result<DmlCtasRecoveryRecord, DmlError> {
    decode_key(CTAS_RECOVERY_PREFIX, key)?;
    let record: DmlCtasRecoveryRecord =
        serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?;
    validate_ctas_recovery(&record).map_err(DmlError::journal_corruption)?;
    Ok(record)
}

/// Read the durable historical write recovery record inside an open
/// transaction, so a fenced mutation can weigh it before it changes anything.
async fn load_historical_write_recovery_in(
    transaction: &mut dyn WriteTransaction,
    operation_id: DmlOperationId,
) -> Result<Option<DmlHistoricalWriteRecoveryRecord>, DmlError> {
    let key = historical_write_recovery_key(operation_id)?;
    let Some(record) = transaction
        .get(&key)
        .await
        .map_err(DmlError::journal_unavailable)?
    else {
        return Ok(None);
    };
    decode_historical_write_recovery(&key, record.value).map(Some)
}

/// Read the durable historical data-mutation recovery record inside an open
/// transaction. Every fenced mutation of a TRUNCATE or ADD FILES operation must
/// weigh it, otherwise a terminal statement result could silently drop an open
/// provider inspection or an ADD FILES source-scope obligation.
async fn load_historical_data_mutation_recovery_in(
    transaction: &mut dyn WriteTransaction,
    operation_id: DmlOperationId,
) -> Result<Option<DmlHistoricalDataMutationRecoveryRecord>, DmlError> {
    let key = historical_data_mutation_recovery_key(operation_id)?;
    let Some(record) = transaction
        .get(&key)
        .await
        .map_err(DmlError::journal_unavailable)?
    else {
        return Ok(None);
    };
    decode_historical_data_mutation_recovery(&key, record.value).map(Some)
}

async fn load_ctas_recovery_in(
    transaction: &mut dyn WriteTransaction,
    operation_id: DmlOperationId,
) -> Result<Option<DmlCtasRecoveryRecord>, DmlError> {
    let key = ctas_recovery_key(operation_id)?;
    let Some(record) = transaction
        .get(&key)
        .await
        .map_err(DmlError::journal_unavailable)?
    else {
        return Ok(None);
    };
    decode_ctas_recovery(&key, record.value).map(Some)
}

/// Refuse to drop the bounded recovery scan while a durable historical recovery
/// record still needs it.
///
/// A terminal user-visible statement result must not discard an outstanding
/// external finalization, a proof-bound cleanup, or an ADD FILES source-scope
/// obligation. Both arguments are the records that will be durable after this
/// mutation, so a mutation that resolves a recovery may release the scan in the
/// same transaction.
fn validate_historical_retention(
    operation_id: DmlOperationId,
    historical: Option<&DmlHistoricalWriteRecoveryRecord>,
    direct_mutation: Option<&DmlHistoricalDataMutationRecoveryRecord>,
    ctas: Option<&DmlCtasRecoveryRecord>,
    recovery_due_at_ms: Option<i64>,
) -> Result<(), DmlError> {
    if recovery_due_at_ms.is_some() {
        return Ok(());
    }
    if let Some(recovery) = historical
        && recovery.requires_recovery_scan()
    {
        return Err(DmlError::journal_unresolved(format!(
            "DML operation {operation_id} cannot drop its recovery due while historical write recovery phase {} is still open",
            recovery.phase.as_str()
        )));
    }
    if let Some(recovery) = direct_mutation
        && recovery.requires_recovery_scan()
    {
        return Err(DmlError::journal_unresolved(format!(
            "DML operation {operation_id} cannot drop its recovery due while historical data mutation recovery phase {} is still open",
            recovery.phase.as_str()
        )));
    }
    if let Some(recovery) = ctas
        && recovery.requires_recovery_scan()
    {
        return Err(DmlError::journal_unresolved(format!(
            "DML operation {operation_id} cannot drop its recovery due while CTAS catalog recovery or cleanup retention is still open"
        )));
    }
    Ok(())
}

/// Enforce that the recovery due matches exactly the obligations the operation
/// and its CP-3B/CP-3C historical recovery records still carry.
///
/// `validate_operation` can only prove the "an obligation always keeps a due"
/// half, because a decoded operation cannot see its side records. This is the
/// mutation-time check that also proves the converse.
fn validate_recovery_due_scope(
    operation: &StoredOperation,
    historical: Option<&DmlHistoricalWriteRecoveryRecord>,
    direct_mutation: Option<&DmlHistoricalDataMutationRecoveryRecord>,
    ctas: Option<&DmlCtasRecoveryRecord>,
) -> Result<(), DmlError> {
    let required = operation_requires_recovery_scan_with_direct_mutation(
        operation.state,
        &operation.payload,
        historical,
        direct_mutation,
    ) || ctas.is_some_and(DmlCtasRecoveryRecord::requires_recovery_scan);
    if required != operation.recovery_due_at_ms.is_some() {
        return Err(DmlError::journal_corruption(format!(
            "DML operation {} has inconsistent recovery due eligibility",
            operation.operation_id
        )));
    }
    Ok(())
}

fn validate_persisted_authority(
    operation: &StoredOperation,
    authority: &DmlMutationAuthority,
) -> Result<(), DmlError> {
    let Some(provenance) = &operation.coordination_provenance else {
        return Err(DmlError::journal_unresolved(format!(
            "DML operation {} has not been claimed",
            operation.operation_id
        )));
    };
    if provenance.coordination_attempt_id != authority.coordination_attempt_id() {
        return Err(DmlError::journal_unresolved(format!(
            "DML operation {} is owned by another coordination attempt",
            operation.operation_id
        )));
    }
    Ok(())
}

fn validate_coordination_provenance(
    provenance: &crate::dml::model::DmlCoordinationProvenance,
) -> Result<(), DmlError> {
    if provenance.resource_codec_version != DML_COORDINATION_RESOURCE_CODEC_VERSION {
        return Err(DmlError::journal_corruption(format!(
            "unsupported DML operation resource codec version: {}",
            provenance.resource_codec_version
        )));
    }
    if !is_uuid_v7(provenance.holder_id) || !is_uuid_v7(provenance.coordination_attempt_id) {
        return Err(DmlError::journal_corruption(
            "DML coordination holder and attempt ids must be UUIDv7",
        ));
    }
    provenance
        .fencing_token
        .try_decode()
        .map_err(DmlError::journal_corruption)?;
    if provenance.acquired_at_ms < 0 {
        return Err(DmlError::journal_corruption(
            "DML coordination acquired timestamp must be nonnegative",
        ));
    }
    Ok(())
}

/// Obligations visible from the operation record alone. A decoded operation
/// cannot see its CP-3B side records, so this is a lower bound on the real
/// obligation; `validate_recovery_due_scope` proves the exact answer at
/// mutation time.
fn operation_requires_recovery(operation: &StoredOperation) -> bool {
    operation_requires_recovery_scan(operation.state, &operation.payload, None)
}

fn legacy_recovery_due_after_mutation(
    operation: &StoredOperation,
    previous: &StoredOperation,
) -> Option<i64> {
    operation_requires_recovery(operation).then_some(
        previous
            .recovery_due_at_ms
            .unwrap_or(operation.updated_at_ms),
    )
}

async fn update_unfinished_index(
    transaction: &mut dyn WriteTransaction,
    operation: &StoredOperation,
    unfinished_key: &Key,
) -> Result<(), DmlError> {
    let existing = transaction
        .get(unfinished_key)
        .await
        .map_err(DmlError::journal_unavailable)?;
    match (operation.state.is_finished(), existing) {
        (true, Some(index)) => {
            let indexed_id = decode_unfinished(index.key, index.value)?;
            if indexed_id != operation.operation_id {
                return Err(DmlError::journal_corruption(
                    "unfinished DML index identity mismatch",
                ));
            }
            transaction
                .delete(unfinished_key.clone(), Precondition::Version(index.version))
                .await
                .map_err(DmlError::journal_unavailable)?;
        }
        (true, None) => {}
        (false, Some(index)) => {
            let indexed_id = decode_unfinished(index.key, index.value)?;
            if indexed_id != operation.operation_id {
                return Err(DmlError::journal_corruption(
                    "unfinished DML index identity mismatch",
                ));
            }
            transaction
                .put(
                    unfinished_key.clone(),
                    encode_unfinished(operation.operation_id)?,
                    Precondition::Version(index.version),
                )
                .await
                .map_err(DmlError::journal_unavailable)?;
        }
        (false, None) => {
            return Err(DmlError::journal_corruption(
                "unfinished DML operation is missing its index",
            ));
        }
    }
    Ok(())
}

async fn update_recovery_due_index(
    transaction: &mut dyn WriteTransaction,
    previous: &StoredOperation,
    operation: &StoredOperation,
) -> Result<(), DmlError> {
    let previous_entry = previous
        .recovery_due_at_ms
        .map(|due| recovery_due_key(previous.operation_id, due))
        .transpose()?;
    let next_entry = operation
        .recovery_due_at_ms
        .map(|due| recovery_due_key(operation.operation_id, due))
        .transpose()?;
    let existing = match &previous_entry {
        Some(key) => transaction
            .get(key)
            .await
            .map_err(DmlError::journal_unavailable)?,
        None => None,
    };
    if let Some(existing) = &existing {
        validate_recovery_due_record(previous, existing.clone())?;
    } else if previous_entry.is_some() {
        return Err(DmlError::journal_corruption(format!(
            "DML operation {} is missing its recovery due index",
            previous.operation_id
        )));
    }

    match (previous_entry, next_entry, existing) {
        (Some(previous_key), Some(next_key), Some(existing)) if previous_key == next_key => {
            transaction
                .put(
                    next_key,
                    encode_recovery_due(operation)?,
                    Precondition::Version(existing.version),
                )
                .await
                .map_err(DmlError::journal_unavailable)?;
        }
        (Some(previous_key), Some(next_key), Some(existing)) => {
            transaction
                .delete(previous_key, Precondition::Version(existing.version))
                .await
                .map_err(DmlError::journal_unavailable)?;
            if transaction
                .get(&next_key)
                .await
                .map_err(DmlError::journal_unavailable)?
                .is_some()
            {
                return Err(DmlError::journal_corruption(
                    "DML recovery due index target already exists",
                ));
            }
            transaction
                .put(
                    next_key,
                    encode_recovery_due(operation)?,
                    Precondition::Absent,
                )
                .await
                .map_err(DmlError::journal_unavailable)?;
        }
        (Some(previous_key), None, Some(existing)) => {
            transaction
                .delete(previous_key, Precondition::Version(existing.version))
                .await
                .map_err(DmlError::journal_unavailable)?;
        }
        (None, Some(next_key), None) => {
            if transaction
                .get(&next_key)
                .await
                .map_err(DmlError::journal_unavailable)?
                .is_some()
            {
                return Err(DmlError::journal_corruption(
                    "DML recovery due index target already exists",
                ));
            }
            transaction
                .put(
                    next_key,
                    encode_recovery_due(operation)?,
                    Precondition::Absent,
                )
                .await
                .map_err(DmlError::journal_unavailable)?;
        }
        (None, None, None) => {}
        _ => {
            return Err(DmlError::journal_corruption(
                "DML recovery due index mutation has inconsistent state",
            ));
        }
    }
    Ok(())
}

fn operation_key(operation_id: DmlOperationId) -> Result<Key, DmlError> {
    key_for(OPERATION_PREFIX, operation_id)
}

fn unfinished_key(operation_id: DmlOperationId) -> Result<Key, DmlError> {
    key_for(UNFINISHED_PREFIX, operation_id)
}

fn recovery_due_shard(operation_id: DmlOperationId) -> u8 {
    Sha256::digest(operation_id.as_uuid().as_bytes())[0] & (DML_RECOVERY_SHARD_COUNT - 1)
}

fn recovery_due_shard_prefix(shard: u8) -> Result<Key, DmlError> {
    if shard >= DML_RECOVERY_SHARD_COUNT {
        return Err(DmlError::journal_corruption(
            "DML recovery shard is outside the configured range",
        ));
    }
    let mut key = Vec::with_capacity(RECOVERY_DUE_PREFIX.len() + 3);
    key.extend_from_slice(RECOVERY_DUE_PREFIX);
    key.extend_from_slice(format!("{shard:02x}/").as_bytes());
    Key::try_from(Bytes::from(key)).map_err(DmlError::journal_corruption)
}

fn recovery_due_key(
    operation_id: DmlOperationId,
    recovery_due_at_ms: i64,
) -> Result<Key, DmlError> {
    let shard = recovery_due_shard(operation_id);
    let sortable_due = (recovery_due_at_ms as u64) ^ (1_u64 << 63);
    let mut key = recovery_due_shard_prefix(shard)?.as_bytes().to_vec();
    key.extend_from_slice(format!("{sortable_due:016x}/").as_bytes());
    key.extend_from_slice(operation_id.as_uuid().simple().to_string().as_bytes());
    Key::try_from(Bytes::from(key)).map_err(DmlError::journal_corruption)
}

fn decode_recovery_due_key(key: &Key) -> Result<(u8, i64, DmlOperationId), DmlError> {
    let suffix = key
        .as_bytes()
        .strip_prefix(RECOVERY_DUE_PREFIX)
        .ok_or_else(|| DmlError::journal_corruption("DML recovery key has an unknown prefix"))?;
    let text = std::str::from_utf8(suffix)
        .map_err(|_| DmlError::journal_corruption("DML recovery key is not UTF-8"))?;
    let mut fields = text.split('/');
    let shard_text = fields.next().unwrap_or_default();
    let due_text = fields.next().unwrap_or_default();
    let operation_text = fields.next().unwrap_or_default();
    if fields.next().is_some()
        || shard_text.len() != 2
        || due_text.len() != 16
        || operation_text.len() != 32
    {
        return Err(DmlError::journal_corruption(
            "DML recovery key has a malformed shape",
        ));
    }
    let shard = u8::from_str_radix(shard_text, 16)
        .map_err(|_| DmlError::journal_corruption("DML recovery key has an invalid shard"))?;
    let sortable_due = u64::from_str_radix(due_text, 16)
        .map_err(|_| DmlError::journal_corruption("DML recovery key has an invalid due time"))?;
    let recovery_due_at_ms = (sortable_due ^ (1_u64 << 63)) as i64;
    let operation_uuid = Uuid::parse_str(operation_text).map_err(|_| {
        DmlError::journal_corruption("DML recovery key has an invalid operation id")
    })?;
    let operation_id = DmlOperationId::from(operation_uuid);
    if shard != recovery_due_shard(operation_id)
        || recovery_due_key(operation_id, recovery_due_at_ms)? != *key
    {
        return Err(DmlError::journal_corruption(
            "DML recovery key is not canonical",
        ));
    }
    Ok((shard, recovery_due_at_ms, operation_id))
}

fn recovery_due_record(operation: &StoredOperation) -> Result<StoredRecoveryDueV1, DmlError> {
    let recovery_due_at_ms = operation.recovery_due_at_ms.ok_or_else(|| {
        DmlError::journal_corruption("DML operation has no recovery due timestamp")
    })?;
    Ok(StoredRecoveryDueV1 {
        schema_version: DML_RECOVERY_DUE_SCHEMA_VERSION,
        operation_id: operation.operation_id,
        operation_revision: operation.revision,
        last_mutation_id: operation.last_mutation_id,
        coordination_attempt_id: operation
            .coordination_provenance
            .as_ref()
            .map(|provenance| provenance.coordination_attempt_id),
        recovery_due_at_ms,
    })
}

fn encode_recovery_due(operation: &StoredOperation) -> Result<Value, DmlError> {
    let bytes = serde_json::to_vec(&recovery_due_record(operation)?)
        .map_err(DmlError::journal_corruption)?;
    Value::try_from(Bytes::from(bytes)).map_err(DmlError::journal_unavailable)
}

fn decode_recovery_due(key: &Key, value: Value) -> Result<StoredRecoveryDueV1, DmlError> {
    let (_, due_at_ms, operation_id) = decode_recovery_due_key(key)?;
    let indexed: StoredRecoveryDueV1 =
        serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?;
    if indexed.schema_version != DML_RECOVERY_DUE_SCHEMA_VERSION
        || indexed.operation_id != operation_id
        || indexed.recovery_due_at_ms != due_at_ms
        || indexed.operation_revision == 0
        || !is_uuid_v7(indexed.last_mutation_id)
        || indexed
            .coordination_attempt_id
            .is_some_and(|attempt| !is_uuid_v7(attempt))
    {
        return Err(DmlError::journal_corruption(
            "DML recovery due index identity is invalid",
        ));
    }
    Ok(indexed)
}

fn is_uuid_v7(value: Uuid) -> bool {
    value.get_version() == Some(uuid::Version::SortRand)
        && value.get_variant() == uuid::Variant::RFC4122
}

fn validate_recovery_due_identity(
    operation: &StoredOperation,
    indexed: &StoredRecoveryDueV1,
) -> Result<(), DmlError> {
    let expected = recovery_due_record(operation)?;
    if &expected != indexed {
        return Err(DmlError::journal_corruption(format!(
            "DML recovery due index conflicts with operation {}",
            operation.operation_id
        )));
    }
    Ok(())
}

fn validate_recovery_due_record(
    operation: &StoredOperation,
    indexed: StateRecord,
) -> Result<(), DmlError> {
    let due = decode_recovery_due(&indexed.key, indexed.value)?;
    validate_recovery_due_identity(operation, &due)
}

fn add_files_artifact_chunk_key(
    operation_id: DmlOperationId,
    artifact: &AddFilesArtifactDescriptor,
    chunk_index: u16,
) -> Result<Key, DmlError> {
    let kind = match artifact.kind {
        crate::dml::model::AddFilesArtifactKind::Plan => "plan",
        crate::dml::model::AddFilesArtifactKind::Receipt => "receipt",
        crate::dml::model::AddFilesArtifactKind::Evidence => "evidence",
    };
    let suffix = format!(
        "{}/{kind}/{chunk_index:05}",
        operation_id.as_uuid().simple()
    );
    let mut key = Vec::with_capacity(ADD_FILES_ARTIFACT_PREFIX.len() + suffix.len());
    key.extend_from_slice(ADD_FILES_ARTIFACT_PREFIX);
    key.extend_from_slice(suffix.as_bytes());
    Key::try_from(Bytes::from(key)).map_err(DmlError::journal_corruption)
}

fn add_files_source_scope_key(provider_id: &str, scope_digest: &str) -> Result<Key, DmlError> {
    if provider_id.is_empty() || !is_sha256(Some(scope_digest)) {
        return Err(DmlError::journal_corruption(
            "ADD FILES source scope key is invalid",
        ));
    }
    let suffix = format!("{provider_id}/{scope_digest}");
    let mut key = Vec::with_capacity(ADD_FILES_SOURCE_SCOPE_PREFIX.len() + suffix.len());
    key.extend_from_slice(ADD_FILES_SOURCE_SCOPE_PREFIX);
    key.extend_from_slice(suffix.as_bytes());
    Key::try_from(Bytes::from(key)).map_err(DmlError::journal_corruption)
}

fn encode_add_files_source_scope(
    value: &StoredAddFilesSourceScopeV1,
    max_value_bytes: usize,
) -> Result<Value, DmlError> {
    let encoded = serde_json::to_vec(value).map_err(DmlError::journal_corruption)?;
    if encoded.len() > max_value_bytes {
        return Err(DmlError::journal_unavailable(
            "ADD FILES source scope record exceeds StateStore value limit",
        ));
    }
    Value::try_from(Bytes::from(encoded)).map_err(DmlError::journal_unavailable)
}

fn decode_add_files_source_scope(
    key: &Key,
    value: Value,
) -> Result<StoredAddFilesSourceScopeV1, DmlError> {
    let record: StoredAddFilesSourceScopeV1 =
        serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?;
    if record.schema_version != 1
        || !is_sha256(Some(&record.scope_digest))
        || add_files_source_scope_key(&record.provider_id, &record.scope_digest)? != *key
    {
        return Err(DmlError::journal_corruption(
            "ADD FILES source scope record is invalid",
        ));
    }
    Ok(record)
}

fn validate_add_files_artifact_bytes(artifact: &AddFilesArtifact) -> Result<(), DmlError> {
    validate_add_files_artifact(&artifact.descriptor)?;
    if artifact.bytes.len() != artifact.descriptor.total_length as usize
        || Sha256::digest(&artifact.bytes).as_slice()
            != hex::decode(&artifact.descriptor.sha256)
                .map_err(DmlError::journal_corruption)?
                .as_slice()
    {
        return Err(DmlError::journal_corruption(
            "ADD FILES artifact bytes do not match their descriptor",
        ));
    }
    let expected_chunks = artifact
        .bytes
        .len()
        .div_ceil(ADD_FILES_ARTIFACT_CHUNK_BYTES);
    if expected_chunks != artifact.descriptor.chunk_count as usize {
        return Err(DmlError::journal_corruption(
            "ADD FILES artifact chunk count does not match its descriptor",
        ));
    }
    Ok(())
}

fn source_scope_record_from_operation(
    operation: &StoredOperation,
    provider_id: String,
    scope_digest: String,
    ownership: SourceScopeOwnership,
) -> Result<StoredAddFilesSourceScopeV1, DmlError> {
    let OperationPayload::AddFilesLifecycle(record) = &operation.payload else {
        return Err(DmlError::journal_corruption(
            "ADD FILES source action requires an ADD FILES operation payload",
        ));
    };
    if record.provider_id.as_deref() != Some(provider_id.as_str())
        || record.source_scope_digest.as_deref() != Some(scope_digest.as_str())
    {
        return Err(DmlError::journal_corruption(
            "ADD FILES source action does not match the durable operation facts",
        ));
    }
    Ok(StoredAddFilesSourceScopeV1 {
        schema_version: 1,
        provider_id,
        scope_digest,
        operation_id: operation.operation_id,
        target: operation.target.clone(),
        plan_digest: record.plan_digest.clone().unwrap_or_default(),
        ownership,
        updated_at_ms: operation.updated_at_ms,
    })
}

fn key_for(prefix: &[u8], operation_id: DmlOperationId) -> Result<Key, DmlError> {
    let mut key = Vec::with_capacity(prefix.len() + 32);
    key.extend_from_slice(prefix);
    key.extend_from_slice(operation_id.as_uuid().simple().to_string().as_bytes());
    Key::try_from(Bytes::from(key)).map_err(DmlError::journal_corruption)
}

fn decode_key(prefix: &[u8], key: &Key) -> Result<DmlOperationId, DmlError> {
    let suffix = key
        .as_bytes()
        .strip_prefix(prefix)
        .ok_or_else(|| DmlError::journal_corruption("DML journal key has an unknown prefix"))?;
    if suffix.len() != 32 || !suffix.iter().all(u8::is_ascii_hexdigit) {
        return Err(DmlError::journal_corruption(
            "DML journal key has a malformed operation id",
        ));
    }
    let text = std::str::from_utf8(suffix)
        .map_err(|_| DmlError::journal_corruption("DML journal key is not UTF-8"))?;
    let uuid = Uuid::parse_str(text)
        .map_err(|_| DmlError::journal_corruption("DML journal key has an invalid operation id"))?;
    let operation_id = DmlOperationId::from(uuid);
    if key_for(prefix, operation_id)? != *key {
        return Err(DmlError::journal_corruption(
            "DML journal key is not canonical",
        ));
    }
    Ok(operation_id)
}

fn encode_operation(operation: &StoredOperation) -> Result<Value, DmlError> {
    if operation.schema_version != DML_OPERATION_SCHEMA_VERSION {
        return Err(DmlError::journal_corruption(format!(
            "cannot encode frontend DML operation schema version {}",
            operation.schema_version
        )));
    }
    validate_operation(operation)?;
    let bytes = serde_json::to_vec(operation).map_err(DmlError::journal_corruption)?;
    Value::try_from(Bytes::from(bytes)).map_err(DmlError::journal_unavailable)
}

fn encode_operation_with_limit(
    operation: &StoredOperation,
    max_value_bytes: usize,
) -> Result<Value, DmlError> {
    let value = encode_operation(operation)?;
    if value.as_bytes().len() > max_value_bytes {
        return Err(DmlError::journal_unavailable(format!(
            "DML operation {} encoded size {} exceeds StateStore value limit {max_value_bytes}",
            operation.operation_id,
            value.as_bytes().len()
        )));
    }
    Ok(value)
}

fn decode_operation(key: Key, value: Value) -> Result<StoredOperation, DmlError> {
    let key_id = decode_key(OPERATION_PREFIX, &key)?;
    let operation: StoredOperation =
        serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?;
    validate_operation(&operation)?;
    if operation.operation_id != key_id {
        return Err(DmlError::journal_corruption(format!(
            "DML operation identity mismatch: key is {key_id}, value is {}",
            operation.operation_id
        )));
    }
    Ok(operation)
}

fn encode_unfinished(operation_id: DmlOperationId) -> Result<Value, DmlError> {
    let record = StoredUnfinishedOperationV1 {
        schema_version: DML_UNFINISHED_SCHEMA_VERSION,
        operation_id,
    };
    let bytes = serde_json::to_vec(&record).map_err(DmlError::journal_corruption)?;
    Value::try_from(Bytes::from(bytes)).map_err(DmlError::journal_unavailable)
}

fn decode_unfinished(key: Key, value: Value) -> Result<DmlOperationId, DmlError> {
    let key_id = decode_key(UNFINISHED_PREFIX, &key)?;
    let record: StoredUnfinishedOperationV1 =
        serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?;
    if record.schema_version != DML_UNFINISHED_SCHEMA_VERSION {
        return Err(DmlError::journal_corruption(format!(
            "unsupported frontend DML unfinished schema version: {}",
            record.schema_version
        )));
    }
    if record.operation_id != key_id {
        return Err(DmlError::journal_corruption(format!(
            "unfinished DML operation identity mismatch: key is {key_id}, value is {}",
            record.operation_id
        )));
    }
    Ok(record.operation_id)
}

fn validate_operation(operation: &StoredOperation) -> Result<(), DmlError> {
    if operation.schema_version != DML_OPERATION_SCHEMA_VERSION {
        return Err(DmlError::journal_corruption(format!(
            "unsupported frontend DML operation schema version: {}",
            operation.schema_version
        )));
    }
    validate_payload_shape(operation)?;
    if operation.operation_id.as_uuid().get_version_num() != 7 {
        return Err(DmlError::journal_corruption(format!(
            "DML operation {} does not use a UUIDv7 operation id",
            operation.operation_id
        )));
    }
    if operation.last_mutation_id.get_version_num() != 7 {
        return Err(DmlError::journal_corruption(format!(
            "DML operation {} does not use a UUIDv7 mutation id",
            operation.operation_id
        )));
    }
    if operation.revision == 0 {
        return Err(DmlError::journal_corruption(format!(
            "DML operation {} has zero revision",
            operation.operation_id
        )));
    }
    if operation.updated_at_ms < operation.created_at_ms {
        return Err(DmlError::journal_corruption(format!(
            "DML operation {} has invalid timestamps",
            operation.operation_id
        )));
    }
    if let Some(provenance) = &operation.coordination_provenance {
        validate_coordination_provenance(provenance)?;
        if provenance.acquired_at_ms < operation.created_at_ms {
            return Err(DmlError::journal_corruption(format!(
                "DML operation {} was acquired before it was created",
                operation.operation_id
            )));
        }
    }
    // Only the "an obligation always keeps a due" half is provable here: a
    // decoded operation cannot see whether a CP-3B historical write recovery
    // still needs the scan, and that record legitimately keeps a due alive on a
    // terminal operation. Every mutation additionally proves the converse
    // through `validate_recovery_due_scope`, which does see the side record.
    if operation_requires_recovery(operation) && operation.recovery_due_at_ms.is_none() {
        return Err(DmlError::journal_corruption(format!(
            "DML operation {} has inconsistent recovery due eligibility",
            operation.operation_id
        )));
    }
    if operation.state.is_finished() != operation.finished_at_ms.is_some() {
        return Err(DmlError::journal_corruption(format!(
            "DML operation {} has inconsistent terminal timestamp",
            operation.operation_id
        )));
    }
    validate_fact_shape(operation)?;
    Ok(())
}

fn validate_payload_shape(operation: &StoredOperation) -> Result<(), DmlError> {
    match (&operation.operation_kind, &operation.payload) {
        (
            OperationKind::InsertAppend
            | OperationKind::InsertOverwrite
            | OperationKind::RowDelta
            | OperationKind::MvRefresh
            | OperationKind::Maintenance,
            OperationPayload::ConnectorWriteLifecycle(_),
        )
        | (OperationKind::CreateTableAsSelect, OperationPayload::CtasSaga(_))
        | (OperationKind::Truncate, OperationPayload::TruncateLifecycle(_))
        | (OperationKind::AddFiles, OperationPayload::AddFilesLifecycle(_)) => {}
        _ => {
            return Err(DmlError::journal_corruption(format!(
                "DML operation {} kind and payload disagree",
                operation.operation_id
            )));
        }
    }
    match &operation.payload {
        OperationPayload::ConnectorWriteLifecycle(record) => {
            validate_connector_write_lifecycle(record)
        }
        OperationPayload::CtasSaga(record) => {
            validate_exact_connector_owner(
                record.provider_id.as_deref(),
                record.connector_instance_id.as_deref(),
                record.connector_incarnation.as_deref(),
            )?;
            validate_ctas_record(record)
        }
        OperationPayload::TruncateLifecycle(record) => {
            validate_exact_connector_owner(
                record.provider_id.as_deref(),
                record.connector_instance_id.as_deref(),
                record.connector_incarnation.as_deref(),
            )?;
            validate_external_fact(record.outcome.as_ref())
        }
        OperationPayload::AddFilesLifecycle(record) => validate_add_files_record(record),
    }
}

fn validate_add_files_record(record: &AddFilesLifecycleRecord) -> Result<(), DmlError> {
    if record.connector_operation_id.is_nil() || record.source_location.is_empty() {
        return Err(DmlError::journal_corruption(
            "ADD FILES record requires a non-empty source and operation ID",
        ));
    }
    if record.provider_id.is_some()
        || record.connector_instance_id.is_some()
        || record.connector_incarnation.is_some()
    {
        validate_exact_connector_owner(
            record.provider_id.as_deref(),
            record.connector_instance_id.as_deref(),
            record.connector_incarnation.as_deref(),
        )?;
    }
    validate_external_fact(record.outcome.as_ref())?;
    for artifact in [
        record.plan_artifact.as_ref(),
        record.receipt_artifact.as_ref(),
        record.evidence_artifact.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        validate_add_files_artifact(artifact)?;
    }
    let has_scope = record.source_scope_version.is_some()
        || record.source_scope_kind.is_some()
        || record.source_scope_digest.is_some();
    if has_scope
        && (record.source_scope_version != Some(1)
            || record.source_scope_kind.as_deref() != Some("DIRECTORY")
            || !is_sha256(record.source_scope_digest.as_deref()))
    {
        return Err(DmlError::journal_corruption(
            "ADD FILES source scope is incomplete or invalid",
        ));
    }
    if matches!(
        record.phase,
        AddFilesLifecyclePhase::Planned
            | AddFilesLifecyclePhase::Executing
            | AddFilesLifecyclePhase::CommitUnknown
            | AddFilesLifecyclePhase::Reconciling
            | AddFilesLifecyclePhase::Committed
    ) && (!has_scope || record.plan_artifact.is_none())
    {
        return Err(DmlError::journal_corruption(
            "ADD FILES dispatched lifecycle phase requires durable plan and source scope",
        ));
    }
    if matches!(
        record.phase,
        AddFilesLifecyclePhase::Planned
            | AddFilesLifecyclePhase::Executing
            | AddFilesLifecyclePhase::CommitUnknown
            | AddFilesLifecyclePhase::Reconciling
            | AddFilesLifecyclePhase::Committed
    ) && record.provider_id.is_none()
    {
        return Err(DmlError::journal_corruption(
            "ADD FILES dispatched lifecycle phase requires an exact connector owner",
        ));
    }
    if matches!(
        record.phase,
        AddFilesLifecyclePhase::CommitUnknown | AddFilesLifecyclePhase::Reconciling
    ) && record.source_ownership != SourceScopeOwnership::Frozen
    {
        return Err(DmlError::journal_corruption(
            "ADD FILES unknown lifecycle must keep its source scope frozen",
        ));
    }
    if record.phase == AddFilesLifecyclePhase::Committed
        && record.source_ownership != SourceScopeOwnership::TableOwned
    {
        return Err(DmlError::journal_corruption(
            "ADD FILES committed lifecycle must transfer source ownership to the table",
        ));
    }
    if record.dispatch_certainty == AddFilesDispatchCertainty::PossiblyDispatched
        && record.source_ownership == SourceScopeOwnership::Unclaimed
    {
        return Err(DmlError::journal_corruption(
            "possibly dispatched ADD FILES operation cannot release its source scope",
        ));
    }
    Ok(())
}

fn validate_add_files_artifact(artifact: &AddFilesArtifactDescriptor) -> Result<(), DmlError> {
    if artifact.codec_version == 0
        || artifact.total_length == 0
        || artifact.chunk_count == 0
        || !is_sha256(Some(&artifact.sha256))
    {
        return Err(DmlError::journal_corruption(
            "ADD FILES artifact descriptor is invalid",
        ));
    }
    Ok(())
}

fn is_sha256(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn validate_ctas_record(record: &CtasSagaRecord) -> Result<(), DmlError> {
    let child_ids = [
        record.prepare_operation_id,
        record.write_operation_id,
        record.publish_operation_id,
        record.abort_staging_operation_id,
    ];
    if child_ids.iter().any(Uuid::is_nil)
        || child_ids.iter().copied().collect::<BTreeSet<_>>().len() != child_ids.len()
    {
        return Err(DmlError::journal_corruption(
            "CTAS child operation IDs must be non-nil and pairwise distinct",
        ));
    }
    if !matches!(
        record.create_policy.as_str(),
        CTAS_CREATE_POLICY_FAIL_IF_EXISTS | CTAS_CREATE_POLICY_NO_OP_IF_EXISTS
    ) {
        return Err(DmlError::journal_corruption(
            "CTAS create policy must be FAIL_IF_EXISTS or NO_OP_IF_EXISTS",
        ));
    }
    for (label, value) in [
        ("source plan digest", record.source_plan_digest.as_deref()),
        (
            "source schema digest",
            record.source_schema_digest.as_deref(),
        ),
        (
            "source execution identity",
            record.source_execution_identity.as_deref(),
        ),
        ("write cohort ID", record.write_cohort_id.as_deref()),
        (
            "staged handle digest",
            record.staged_handle_digest.as_deref(),
        ),
        (
            "aggregate write digest",
            record.aggregate_write_digest.as_deref(),
        ),
    ] {
        if value.is_some_and(str::is_empty) {
            return Err(DmlError::journal_corruption(format!(
                "CTAS {label} must not be empty"
            )));
        }
    }

    let facts = [
        ("prepare", record.prepare_fact.as_ref()),
        ("write", record.write_fact.as_ref()),
        ("publish", record.publish_fact.as_ref()),
        ("abort staging", record.abort_staging_fact.as_ref()),
    ];
    let mut total_fact_bytes = 0usize;
    for (label, fact) in facts {
        validate_external_fact(fact)?;
        let Some(fact) = fact else {
            continue;
        };
        validate_ctas_external_fact_shape(label, fact)?;
        if fact.outcome == ExternalFactOutcome::CommitUnknown
            && fact.evidence.is_none()
            && record.next_action != StatementNextAction::ManualInspect
        {
            return Err(DmlError::journal_corruption(format!(
                "CTAS {label} unknown fact without provider evidence requires MANUAL_INSPECT"
            )));
        }
        let encoded = serde_json::to_vec(fact).map_err(DmlError::journal_corruption)?;
        if encoded.len() > DML_CTAS_FACT_ENCODED_LIMIT {
            return Err(DmlError::journal_unavailable(format!(
                "CTAS {label} fact encoded size {} exceeds per-fact limit {DML_CTAS_FACT_ENCODED_LIMIT}",
                encoded.len()
            )));
        }
        total_fact_bytes = total_fact_bytes.checked_add(encoded.len()).ok_or_else(|| {
            DmlError::journal_unavailable("CTAS total fact encoded size overflow")
        })?;
    }
    if total_fact_bytes > DML_CTAS_TOTAL_FACT_ENCODED_LIMIT {
        return Err(DmlError::journal_unavailable(format!(
            "CTAS total fact encoded size {total_fact_bytes} exceeds limit {DML_CTAS_TOTAL_FACT_ENCODED_LIMIT}"
        )));
    }

    match record.phase {
        CtasSagaPhase::PrepareUnknown => require_ctas_outcome(
            "prepare unknown",
            record.prepare_fact.as_ref(),
            ExternalFactOutcome::CommitUnknown,
        ),
        CtasSagaPhase::Staged | CtasSagaPhase::Writing => require_ctas_outcome(
            "prepared target",
            record.prepare_fact.as_ref(),
            ExternalFactOutcome::KnownCommitted,
        ),
        CtasSagaPhase::WriteUnknown => require_ctas_outcome(
            "write unknown",
            record.write_fact.as_ref(),
            ExternalFactOutcome::CommitUnknown,
        ),
        CtasSagaPhase::Publishing => require_ctas_outcome(
            "completed write",
            record.write_fact.as_ref(),
            ExternalFactOutcome::KnownCommitted,
        ),
        CtasSagaPhase::PublishUnknown => require_ctas_outcome(
            "publish unknown",
            record.publish_fact.as_ref(),
            ExternalFactOutcome::CommitUnknown,
        ),
        CtasSagaPhase::AbortUnknown => require_ctas_outcome(
            "abort unknown",
            record.abort_staging_fact.as_ref(),
            ExternalFactOutcome::CommitUnknown,
        ),
        CtasSagaPhase::Committed => require_ctas_outcome(
            "committed publish",
            record.publish_fact.as_ref(),
            ExternalFactOutcome::KnownCommitted,
        ),
        CtasSagaPhase::NoOp
            if record
                .publish_fact
                .as_ref()
                .is_some_and(|fact| fact.outcome == ExternalFactOutcome::NoOp) =>
        {
            Ok(())
        }
        CtasSagaPhase::NoOp
            if record.create_policy == CTAS_CREATE_POLICY_NO_OP_IF_EXISTS
                && record
                    .prepare_fact
                    .as_ref()
                    .is_some_and(|fact| fact.outcome == ExternalFactOutcome::Conflict) =>
        {
            Ok(())
        }
        CtasSagaPhase::NoOp => Err(DmlError::journal_corruption(
            "CTAS no-op phase requires publish NO_OP or NO_OP_IF_EXISTS prepare CONFLICT fact",
        )),
        CtasSagaPhase::Conflict
            if record
                .prepare_fact
                .as_ref()
                .is_some_and(|fact| fact.outcome == ExternalFactOutcome::Conflict)
                || record
                    .publish_fact
                    .as_ref()
                    .is_some_and(|fact| fact.outcome == ExternalFactOutcome::Conflict) =>
        {
            Ok(())
        }
        CtasSagaPhase::Conflict => Err(DmlError::journal_corruption(
            "CTAS conflict phase requires prepare or publish CONFLICT fact",
        )),
        CtasSagaPhase::PreparingSource
        | CtasSagaPhase::PreparingStagedTable
        | CtasSagaPhase::AbortingStaging
        | CtasSagaPhase::Failed
        | CtasSagaPhase::Unsupported => Ok(()),
    }
}

fn require_ctas_outcome(
    label: &str,
    fact: Option<&DurableExternalFact>,
    expected: ExternalFactOutcome,
) -> Result<(), DmlError> {
    if fact.is_some_and(|fact| fact.outcome == expected) {
        Ok(())
    } else {
        Err(DmlError::journal_corruption(format!(
            "CTAS {label} phase requires {expected:?} fact"
        )))
    }
}

fn validate_ctas_external_fact_shape(
    label: &str,
    fact: &DurableExternalFact,
) -> Result<(), DmlError> {
    if fact.receipt.as_deref().is_some_and(str::is_empty)
        || fact.evidence.as_deref().is_some_and(str::is_empty)
        || fact
            .finalization_failure
            .as_deref()
            .is_some_and(str::is_empty)
        || fact.failure.as_deref().is_some_and(str::is_empty)
    {
        return Err(DmlError::journal_corruption(format!(
            "CTAS {label} fact contains an empty durable field"
        )));
    }
    if fact.receipt.is_some() && fact.evidence.is_some() {
        return Err(DmlError::journal_corruption(format!(
            "CTAS {label} fact cannot contain both receipt and evidence"
        )));
    }
    match fact.outcome {
        ExternalFactOutcome::CommitUnknown if fact.evidence.is_none() && fact.failure.is_none() => {
            Err(DmlError::journal_corruption(format!(
                "CTAS {label} unknown fact requires provider evidence or a durable failure"
            )))
        }
        ExternalFactOutcome::KnownCommitted | ExternalFactOutcome::NoOp
            if fact.receipt.is_none() =>
        {
            Err(DmlError::journal_corruption(format!(
                "CTAS {label} committed/no-op fact requires a receipt"
            )))
        }
        ExternalFactOutcome::KnownUncommitted
        | ExternalFactOutcome::Unsupported
        | ExternalFactOutcome::Conflict
            if fact.failure.is_none() =>
        {
            Err(DmlError::journal_corruption(format!(
                "CTAS {label} uncommitted fact requires a failure"
            )))
        }
        ExternalFactOutcome::CommitUnknown if fact.finalization_failure.is_some() => {
            Err(DmlError::journal_corruption(format!(
                "CTAS {label} unknown fact cannot contain finalization failure"
            )))
        }
        _ => Ok(()),
    }
}

fn validate_exact_connector_owner(
    provider_id: Option<&str>,
    instance_id: Option<&str>,
    incarnation_hex: Option<&str>,
) -> Result<(), DmlError> {
    let present = [
        provider_id.is_some(),
        instance_id.is_some(),
        incarnation_hex.is_some(),
    ];
    if present.iter().any(|present| *present) && !present.iter().all(|present| *present) {
        return Err(DmlError::journal_corruption(
            "DML connector owner must include provider, instance, and incarnation together",
        ));
    }
    if provider_id.is_some_and(str::is_empty) || instance_id.is_some_and(str::is_empty) {
        return Err(DmlError::journal_corruption(
            "DML connector provider and instance IDs must not be empty",
        ));
    }
    if incarnation_hex.is_some_and(|value| {
        value.len() != 32
            || !value
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || matches!(*byte, b'a'..=b'f'))
    }) {
        return Err(DmlError::journal_corruption(
            "DML connector incarnation must be 32 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn validate_external_fact(fact: Option<&DurableExternalFact>) -> Result<(), DmlError> {
    let Some(fact) = fact else {
        return Ok(());
    };
    for (label, value) in [
        ("receipt", &fact.receipt),
        ("evidence", &fact.evidence),
        ("finalization failure", &fact.finalization_failure),
        ("failure", &fact.failure),
    ] {
        if value
            .as_ref()
            .is_some_and(|value| value.len() > DML_EXTERNAL_FACT_ENCODED_LIMIT)
        {
            return Err(DmlError::journal_unavailable(format!(
                "DML external {label} exceeds encoded limit {DML_EXTERNAL_FACT_ENCODED_LIMIT}"
            )));
        }
    }
    Ok(())
}

fn validate_fact_shape(operation: &StoredOperation) -> Result<(), DmlError> {
    let OperationPayload::ConnectorWriteLifecycle(record) = &operation.payload else {
        return Ok(());
    };
    let state_allows = match record {
        ConnectorWriteLifecycleRecord::Pending => !matches!(
            operation.state,
            OperationState::Committed
                | OperationState::CommitUnknown
                | OperationState::FailedKnownUncommitted
                | OperationState::FinalizeFailedKnownCommitted
                | OperationState::Finalized
        ),
        ConnectorWriteLifecycleRecord::KnownEmpty => matches!(
            operation.state,
            OperationState::Committed | OperationState::Finalizing | OperationState::Finalized
        ),
        ConnectorWriteLifecycleRecord::KnownCommitted { finalization, .. } => match finalization {
            ConnectorWriteFinalizationRecord::Complete => matches!(
                operation.state,
                OperationState::Committed | OperationState::Finalizing | OperationState::Finalized
            ),
            ConnectorWriteFinalizationRecord::Failed(_) => {
                operation.state == OperationState::FinalizeFailedKnownCommitted
            }
        },
        ConnectorWriteLifecycleRecord::KnownUncommitted { .. } => {
            operation.state == OperationState::FailedKnownUncommitted
        }
        ConnectorWriteLifecycleRecord::CommitUnknown { .. } => {
            operation.state == OperationState::CommitUnknown
        }
    };
    if state_allows {
        Ok(())
    } else {
        Err(DmlError::journal_corruption(format!(
            "DML operation {} has lifecycle facts incompatible with state {}",
            operation.operation_id,
            operation.state.as_str()
        )))
    }
}

fn validate_connector_write_lifecycle(
    record: &ConnectorWriteLifecycleRecord,
) -> Result<(), DmlError> {
    match record {
        ConnectorWriteLifecycleRecord::Pending | ConnectorWriteLifecycleRecord::KnownEmpty => {
            Ok(())
        }
        ConnectorWriteLifecycleRecord::KnownCommitted { receipt_wire, .. } => receipt_wire
            .try_decode()
            .map(|_| ())
            .map_err(DmlError::journal_corruption),
        ConnectorWriteLifecycleRecord::KnownUncommitted { failure }
        | ConnectorWriteLifecycleRecord::CommitUnknown { failure, .. } => {
            if failure.message.is_empty() {
                Err(DmlError::journal_corruption(
                    "connector write lifecycle failure message must not be empty",
                ))
            } else {
                Ok(())
            }
        }
    }?;
    if let ConnectorWriteLifecycleRecord::CommitUnknown { evidence_wire, .. } = record {
        evidence_wire
            .try_decode()
            .map(|_| ())
            .map_err(DmlError::journal_corruption)?;
    }
    Ok(())
}

fn format_run_failure(action: &str, failure: RunFailure) -> DmlError {
    let detail = match failure {
        RunFailure::Begin(error) => format!("begin failed: {error}"),
        RunFailure::Operation(error) => format!("operation failed: {error}"),
        RunFailure::RetryExhausted(error) => format!("retry exhausted: {error}"),
        RunFailure::DefiniteFailure(error) => format!("commit failed: {error}"),
        RunFailure::CommitUnknown { error, .. } => format!("commit unknown: {error}"),
        RunFailure::DeadlineExceeded => "deadline exceeded".to_string(),
    };
    DmlError::journal_unavailable(format!("DML journal {action} failed: {detail}"))
}
