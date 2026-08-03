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

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::state_store::{
    Direction, Key, KeyRange, Precondition, RangeRequest, StateRecord, StateStore, Value,
};
use novarocks_state_store::metrics::StateStoreMetrics;
use novarocks_state_store::{OperationId, RunFailure, run_side_effect_free};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::runtime::{Handle, RuntimeFlavor};
use uuid::Uuid;

use crate::dml::error::DmlError;
use crate::dml::journal::OperationJournal;
use crate::dml::model::{
    AddFilesArtifact, AddFilesArtifactDescriptor, AddFilesDispatchCertainty,
    AddFilesLifecyclePhase, AddFilesLifecycleRecord, AddFilesMutationRequest, AddFilesSourceAction,
    CTAS_CREATE_POLICY_FAIL_IF_EXISTS, CTAS_CREATE_POLICY_NO_OP_IF_EXISTS, CreatePreparingRequest,
    CreateStatementOperationRequest, CtasSagaPhase, CtasSagaRecord, DML_CTAS_FACT_ENCODED_LIMIT,
    DML_CTAS_TOTAL_FACT_ENCODED_LIMIT, DML_EXTERNAL_FACT_ENCODED_LIMIT,
    DML_LEGACY_OPERATION_SCHEMA_VERSION, DML_OPERATION_SCHEMA_VERSION,
    DML_PREVIOUS_OPERATION_SCHEMA_VERSION, DML_UNFINISHED_SCHEMA_VERSION, DmlOperationId,
    DurableExternalFact, ExternalFactOutcome, IcebergCleanupOutcomeRecord,
    IcebergCommitOutcomeRecord, IcebergOperationFailureRecord, IcebergRecoveryEvidenceRecord,
    OperationFact, OperationKind, OperationMutationRequest, OperationPayload, OperationState,
    OperationTarget, SourceScopeOwnership, StatementNextAction, StoredOperation,
    validate_operation_transition, validate_statement_operation_transition,
};
use crate::dml::now_unix_millis;

const OPERATION_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/operations/";
const UNFINISHED_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/unfinished/";
const ADD_FILES_ARTIFACT_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/add-files-artifacts/";
const ADD_FILES_SOURCE_SCOPE_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/add-files-source-scopes/";
const ADD_FILES_ARTIFACT_CHUNK_BYTES: usize = 8 * 1024;

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

#[derive(Deserialize)]
struct OperationSchemaProbe {
    schema_version: u8,
}

#[derive(Deserialize)]
struct StoredOperationV1 {
    schema_version: u8,
    operation_id: DmlOperationId,
    revision: u64,
    last_mutation_id: Uuid,
    operation_kind: OperationKind,
    #[serde(default)]
    operation_subkind: Option<String>,
    target: OperationTarget,
    state: OperationState,
    attempt_id: String,
    base_snapshot_id: Option<i64>,
    base_snapshot_map: std::collections::BTreeMap<String, i64>,
    staged_artifacts: Vec<String>,
    #[serde(default)]
    commit_outcome: Option<IcebergCommitOutcomeRecord>,
    #[serde(default)]
    cleanup_outcome: Option<IcebergCleanupOutcomeRecord>,
    #[serde(default)]
    recovery_evidence: Option<IcebergRecoveryEvidenceRecord>,
    #[serde(default)]
    failure: Option<IcebergOperationFailureRecord>,
    created_at_ms: i64,
    updated_at_ms: i64,
    finished_at_ms: Option<i64>,
}

impl From<StoredOperationV1> for StoredOperation {
    fn from(value: StoredOperationV1) -> Self {
        Self {
            schema_version: value.schema_version,
            operation_id: value.operation_id,
            revision: value.revision,
            last_mutation_id: value.last_mutation_id,
            operation_kind: value.operation_kind,
            operation_subkind: value.operation_subkind,
            target: value.target,
            state: value.state,
            attempt_id: value.attempt_id,
            base_snapshot_id: value.base_snapshot_id,
            base_snapshot_map: value.base_snapshot_map,
            staged_artifacts: value.staged_artifacts,
            commit_outcome: value.commit_outcome,
            cleanup_outcome: value.cleanup_outcome,
            recovery_evidence: value.recovery_evidence,
            failure: value.failure,
            payload: OperationPayload::WriteV1,
            created_at_ms: value.created_at_ms,
            updated_at_ms: value.updated_at_ms,
            finished_at_ms: value.finished_at_ms,
        }
    }
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
        let journal = Self {
            store,
            runtime,
            metrics: Arc::new(StateStoreMetrics::new(provider)),
        };
        journal.validate_open_state().await?;
        journal.recover_add_files_open_state().await?;
        journal.validate_open_state().await?;
        Ok(journal)
    }

    async fn validate_open_state(&self) -> Result<(), DmlError> {
        let operations = self.scan_operations().await?;
        let unfinished = self.scan_unfinished_ids().await?;
        let indexed = unfinished.into_iter().collect::<BTreeSet<_>>();
        for operation in &operations {
            if operation.state.is_finished() && indexed.contains(&operation.operation_id) {
                return Err(DmlError::journal_corruption(format!(
                    "terminal DML operation {} remains in the unfinished index",
                    operation.operation_id
                )));
            }
            if !operation.state.is_finished() && !indexed.contains(&operation.operation_id) {
                return Err(DmlError::journal_corruption(format!(
                    "unfinished DML operation {} is missing its index",
                    operation.operation_id
                )));
            }
        }
        let operation_ids = operations
            .iter()
            .map(|operation| operation.operation_id)
            .collect::<BTreeSet<_>>();
        if let Some(orphan) = indexed.difference(&operation_ids).next() {
            return Err(DmlError::journal_corruption(format!(
                "unfinished DML operation index {orphan} has no operation record"
            )));
        }
        self.validate_add_files_artifact_state(&operations).await?;
        Ok(())
    }

    async fn validate_add_files_artifact_state(
        &self,
        operations: &[StoredOperation],
    ) -> Result<(), DmlError> {
        let mut expected_artifact_keys = BTreeSet::new();
        let mut expected_source_keys = BTreeSet::new();
        for operation in operations {
            let OperationPayload::AddFilesLifecycle(record) = &operation.payload else {
                continue;
            };
            for descriptor in [
                record.plan_artifact.as_ref(),
                record.receipt_artifact.as_ref(),
                record.evidence_artifact.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                self.load_add_files_artifact_async(operation.operation_id, descriptor.clone())
                    .await?;
                for index in 0..descriptor.chunk_count {
                    expected_artifact_keys.insert(
                        add_files_artifact_chunk_key(operation.operation_id, descriptor, index)?
                            .as_bytes()
                            .to_vec(),
                    );
                }
            }
            // A contract failure may become possibly-dispatched before core
            // returns a canonical source scope.  Freeze that operation for
            // manual inspection, but never invent a source-index key from an
            // untrusted/raw statement location.
            if record.source_ownership != SourceScopeOwnership::Unclaimed
                && record.source_scope_digest.is_some()
            {
                let provider_id = record.provider_id.as_deref().ok_or_else(|| {
                    DmlError::journal_corruption("ADD FILES source owner has no provider")
                })?;
                let scope_digest = record.source_scope_digest.as_deref().ok_or_else(|| {
                    DmlError::journal_corruption("ADD FILES source owner has no scope digest")
                })?;
                let key = add_files_source_scope_key(provider_id, scope_digest)?;
                expected_source_keys.insert(key.as_bytes().to_vec());
            }
        }
        for record in self.scan_prefix(ADD_FILES_ARTIFACT_PREFIX).await? {
            if !expected_artifact_keys.contains(record.key.as_bytes()) {
                return Err(DmlError::journal_corruption(
                    "ADD FILES journal contains an orphan artifact chunk",
                ));
            }
        }
        let operation_by_id = operations
            .iter()
            .map(|operation| (operation.operation_id, operation))
            .collect::<BTreeMap<_, _>>();
        let mut actual_source_keys = BTreeSet::new();
        for stored_source in self.scan_prefix(ADD_FILES_SOURCE_SCOPE_PREFIX).await? {
            actual_source_keys.insert(stored_source.key.as_bytes().to_vec());
            let source = decode_add_files_source_scope(&stored_source.key, stored_source.value)?;
            let Some(operation) = operation_by_id.get(&source.operation_id) else {
                return Err(DmlError::journal_corruption(
                    "ADD FILES journal contains a source scope with no operation",
                ));
            };
            let OperationPayload::AddFilesLifecycle(record) = &operation.payload else {
                return Err(DmlError::journal_corruption(
                    "ADD FILES source scope owner has a non-ADD-FILES operation",
                ));
            };
            if record.provider_id.as_deref() != Some(source.provider_id.as_str())
                || record.source_scope_digest.as_deref() != Some(source.scope_digest.as_str())
                || record.source_ownership != source.ownership
                || operation.target != source.target
                || record.plan_digest.as_deref() != Some(source.plan_digest.as_str())
                || !expected_source_keys.contains(stored_source.key.as_bytes())
            {
                return Err(DmlError::journal_corruption(
                    "ADD FILES source scope conflicts with its durable operation",
                ));
            }
        }
        if actual_source_keys != expected_source_keys {
            return Err(DmlError::journal_corruption(
                "ADD FILES source scope ownership index is incomplete",
            ));
        }
        Ok(())
    }

    /// No provider call is allowed while restoring the frontend journal.  A
    /// prepared statement has not acquired a source scope yet and can be
    /// failed closed; a planned but undispatched statement releases its
    /// reservation; anything that might have crossed the provider boundary is
    /// frozen for explicit operator inspection.
    async fn recover_add_files_open_state(&self) -> Result<(), DmlError> {
        for stored in self.scan_operations().await? {
            if stored.operation_kind != OperationKind::AddFiles || stored.state.is_finished() {
                continue;
            }
            let OperationPayload::AddFilesLifecycle(mut record) = stored.payload.clone() else {
                return Err(DmlError::journal_corruption(
                    "ADD FILES operation has a non-ADD-FILES lifecycle payload",
                ));
            };
            let (state, source_action) = match record.phase {
                AddFilesLifecyclePhase::Preparing => {
                    record.phase = AddFilesLifecyclePhase::Failed;
                    record.next_action = StatementNextAction::None;
                    record.outcome = Some(DurableExternalFact {
                        outcome: ExternalFactOutcome::KnownUncommitted,
                        receipt: None,
                        evidence: None,
                        finalization_failure: None,
                        failure: Some("frontend restart before ADD FILES planning".to_string()),
                    });
                    (OperationState::FailedKnownUncommitted, None)
                }
                AddFilesLifecyclePhase::Planned => {
                    let provider_id = record.provider_id.clone().ok_or_else(|| {
                        DmlError::journal_corruption("planned ADD FILES operation has no provider")
                    })?;
                    let scope_digest = record.source_scope_digest.clone().ok_or_else(|| {
                        DmlError::journal_corruption(
                            "planned ADD FILES operation has no source scope",
                        )
                    })?;
                    if record.source_ownership != SourceScopeOwnership::ReservedImmutable {
                        return Err(DmlError::journal_corruption(
                            "planned ADD FILES operation does not own a reserved source scope",
                        ));
                    }
                    record.phase = AddFilesLifecyclePhase::Failed;
                    record.source_ownership = SourceScopeOwnership::Unclaimed;
                    record.next_action = StatementNextAction::None;
                    record.outcome = Some(DurableExternalFact {
                        outcome: ExternalFactOutcome::KnownUncommitted,
                        receipt: None,
                        evidence: None,
                        finalization_failure: None,
                        failure: Some("frontend restart before ADD FILES dispatch".to_string()),
                    });
                    (
                        OperationState::FailedKnownUncommitted,
                        Some(AddFilesSourceAction::Release {
                            provider_id,
                            scope_digest,
                        }),
                    )
                }
                AddFilesLifecyclePhase::Executing
                | AddFilesLifecyclePhase::CommitUnknown
                | AddFilesLifecyclePhase::Reconciling => {
                    let source_action = match record.source_ownership {
                        SourceScopeOwnership::ReservedImmutable => {
                            let provider_id = record.provider_id.clone().ok_or_else(|| {
                                DmlError::journal_corruption("ADD FILES operation has no provider")
                            })?;
                            let scope_digest =
                                record.source_scope_digest.clone().ok_or_else(|| {
                                    DmlError::journal_corruption(
                                        "ADD FILES operation has no source scope",
                                    )
                                })?;
                            Some(AddFilesSourceAction::Transition {
                                provider_id,
                                scope_digest,
                                expected: SourceScopeOwnership::ReservedImmutable,
                                ownership: SourceScopeOwnership::Frozen,
                            })
                        }
                        SourceScopeOwnership::Frozen => None,
                        SourceScopeOwnership::Unclaimed | SourceScopeOwnership::TableOwned => {
                            return Err(DmlError::journal_corruption(
                                "possibly dispatched ADD FILES operation has invalid source ownership",
                            ));
                        }
                    };
                    record.phase = AddFilesLifecyclePhase::CommitUnknown;
                    record.dispatch_certainty = AddFilesDispatchCertainty::PossiblyDispatched;
                    record.source_ownership = SourceScopeOwnership::Frozen;
                    record.next_action = StatementNextAction::ManualInspect;
                    record.outcome = Some(DurableExternalFact {
                        outcome: ExternalFactOutcome::CommitUnknown,
                        receipt: None,
                        evidence: record
                            .evidence_artifact
                            .as_ref()
                            .map(|artifact| format!("sha256:{}", artifact.sha256)),
                        finalization_failure: None,
                        failure: Some(
                            "frontend restart after ADD FILES dispatch boundary".to_string(),
                        ),
                    });
                    (OperationState::CommitUnknown, source_action)
                }
                AddFilesLifecyclePhase::Committed | AddFilesLifecyclePhase::Failed => continue,
            };
            self.apply_add_files_mutation_async(AddFilesMutationRequest {
                operation: OperationMutationRequest {
                    operation_id: stored.operation_id,
                    expected_revision: stored.revision,
                    mutation_id: Uuid::now_v7(),
                    state,
                    payload: OperationPayload::AddFilesLifecycle(record),
                },
                artifacts: Vec::new(),
                source_action,
            })
            .await?;
        }
        Ok(())
    }

    fn blocking<T>(
        &self,
        future: impl Future<Output = Result<T, DmlError>>,
    ) -> Result<T, DmlError> {
        match Handle::try_current() {
            Ok(_) if self.runtime.runtime_flavor() == RuntimeFlavor::CurrentThread => {
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
    ) -> Result<DmlOperationId, DmlError> {
        let operation_id = DmlOperationId::new_v7();
        let mutation_id = Uuid::now_v7();
        let now_ms = request.created_at_ms;
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
            commit_outcome: None,
            cleanup_outcome: None,
            recovery_evidence: None,
            failure: None,
            payload: OperationPayload::WriteV1,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            finished_at_ms: None,
        };
        let operation_key = operation_key(operation_id)?;
        let unfinished_key = unfinished_key(operation_id)?;
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
                let stored = stored.clone();
                Box::pin(async move {
                    if transaction.get(&operation_key).await?.is_some()
                        || transaction.get(&unfinished_key).await?.is_some()
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
                    transaction
                        .put(operation_key, operation_value, Precondition::Absent)
                        .await?;
                    transaction
                        .put(unfinished_key, unfinished_value, Precondition::Absent)
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
    ) -> Result<StoredOperation, DmlError> {
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
            commit_outcome: None,
            cleanup_outcome: None,
            recovery_evidence: None,
            failure: None,
            payload: request.payload,
            created_at_ms: request.created_at_ms,
            updated_at_ms: request.created_at_ms,
            finished_at_ms: None,
        };
        validate_operation(&operation)?;
        let operation_id = operation.operation_id;
        let mutation_id = operation.last_mutation_id;
        let operation_key = operation_key(operation_id)?;
        let unfinished_key = unfinished_key(operation_id)?;
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
                let stored = stored.clone();
                Box::pin(async move {
                    let existing_operation = transaction.get(&operation_key).await?;
                    let existing_unfinished = transaction.get(&unfinished_key).await?;
                    if let Some(record) = existing_operation {
                        let existing = match decode_operation(record.key, record.value) {
                            Ok(existing) => existing,
                            Err(error) => return Ok(Err(error)),
                        };
                        match (existing.state.is_finished(), existing_unfinished) {
                            (false, Some(index)) => {
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
                            }
                            (false, None) => {
                                return Ok(Err(DmlError::journal_corruption(format!(
                                    "unfinished DML operation {} is missing its index",
                                    stored.operation_id
                                ))));
                            }
                            (true, Some(_)) => {
                                return Ok(Err(DmlError::journal_corruption(format!(
                                    "terminal DML operation {} remains in the unfinished index",
                                    stored.operation_id
                                ))));
                            }
                            (true, None) => {}
                        }
                        if existing == stored {
                            return Ok(Ok(existing));
                        }
                        return Ok(Err(DmlError::journal_unresolved(format!(
                            "conflicting DML statement create replay for operation {}",
                            stored.operation_id
                        ))));
                    }
                    if existing_unfinished.is_some() {
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
                    transaction
                        .put(operation_key, operation_value, Precondition::Absent)
                        .await?;
                    transaction
                        .put(unfinished_key, unfinished_value, Precondition::Absent)
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
                    operation.updated_at_ms = now_unix_millis();
                    if operation.state.is_finished() {
                        operation.finished_at_ms = Some(operation.updated_at_ms);
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
                    operation.schema_version = DML_OPERATION_SCHEMA_VERSION;
                    operation.revision = match operation.revision.checked_add(1) {
                        Some(revision) => revision,
                        None => return Ok(Err(DmlError::journal_corruption("DML operation revision overflow"))),
                    };
                    operation.last_mutation_id = request.operation.mutation_id;
                    operation.state = request.operation.state;
                    operation.payload = request.operation.payload;
                    operation.updated_at_ms = now_unix_millis();
                    if operation.state.is_finished() {
                        operation.finished_at_ms = Some(operation.updated_at_ms);
                    }
                    if let Err(error) = validate_operation(&operation) {
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
                let identical = operation.commit_outcome == fact.commit_outcome
                    && operation.cleanup_outcome == fact.cleanup_outcome
                    && operation.recovery_evidence == fact.recovery_evidence
                    && operation.failure == fact.failure;
                if !identical {
                    return Err(DmlError::journal_unavailable(format!(
                        "conflicting DML operation fact replay for operation {operation_id} in state {}",
                        fact.state.as_str()
                    )));
                }
            }
            operation.state = fact.state;
            operation.commit_outcome = fact
                .commit_outcome
                .clone()
                .or_else(|| operation.commit_outcome.clone());
            operation.cleanup_outcome = fact
                .cleanup_outcome
                .clone()
                .or_else(|| operation.cleanup_outcome.clone());
            operation.recovery_evidence = fact
                .recovery_evidence
                .clone()
                .or_else(|| operation.recovery_evidence.clone());
            operation.failure = fact.failure.clone().or_else(|| operation.failure.clone());
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
                    operation.updated_at_ms = now_unix_millis();
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
                let authoritative = self.load_by_key(&operation_key).await?;
                match authoritative {
                    Some(operation) if operation.last_mutation_id == mutation_id => {
                        Ok(committed_value)
                    }
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
                let authoritative = self.load_by_key(&operation_key).await?;
                match authoritative {
                    Some(operation) if operation.last_mutation_id == mutation_id => Ok(operation),
                    _ => Err(DmlError::journal_unresolved(format!(
                        "DML journal statement {action} commit outcome is unresolved"
                    ))),
                }
            }
            Err(failure) => Err(format_run_failure(action, failure)),
        }
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
    fn create_preparing(
        &self,
        request: CreatePreparingRequest,
    ) -> Result<DmlOperationId, DmlError> {
        self.blocking(self.create_preparing_async(request))
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
        self.blocking(self.create_statement_operation_async(request))
    }

    fn mutate_statement_operation(
        &self,
        request: OperationMutationRequest,
    ) -> Result<StoredOperation, DmlError> {
        self.blocking(self.mutate_statement_operation_async(request))
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
        self.blocking(self.apply_add_files_mutation_async(request))
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

fn operation_key(operation_id: DmlOperationId) -> Result<Key, DmlError> {
    key_for(OPERATION_PREFIX, operation_id)
}

fn unfinished_key(operation_id: DmlOperationId) -> Result<Key, DmlError> {
    key_for(UNFINISHED_PREFIX, operation_id)
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
    let probe: OperationSchemaProbe =
        serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?;
    let operation = match probe.schema_version {
        DML_LEGACY_OPERATION_SCHEMA_VERSION => {
            let legacy: StoredOperationV1 =
                serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?;
            StoredOperation::from(legacy)
        }
        DML_PREVIOUS_OPERATION_SCHEMA_VERSION | DML_OPERATION_SCHEMA_VERSION => {
            serde_json::from_slice(value.as_bytes()).map_err(DmlError::journal_corruption)?
        }
        version => {
            return Err(DmlError::journal_corruption(format!(
                "unsupported frontend DML operation schema version: {version}"
            )));
        }
    };
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
    if !matches!(
        operation.schema_version,
        DML_LEGACY_OPERATION_SCHEMA_VERSION
            | DML_PREVIOUS_OPERATION_SCHEMA_VERSION
            | DML_OPERATION_SCHEMA_VERSION
    ) {
        return Err(DmlError::journal_corruption(format!(
            "unsupported frontend DML operation schema version: {}",
            operation.schema_version
        )));
    }
    if operation.schema_version == DML_LEGACY_OPERATION_SCHEMA_VERSION
        && operation.payload != OperationPayload::WriteV1
    {
        return Err(DmlError::journal_corruption(format!(
            "legacy DML operation {} has a non-write payload",
            operation.operation_id
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
            OperationPayload::WriteV1,
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
        OperationPayload::WriteV1 => Ok(()),
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
    ] {
        if let Some(artifact) = artifact {
            validate_add_files_artifact(artifact)?;
        }
    }
    let has_scope = record.source_scope_version.is_some()
        || record.source_scope_kind.is_some()
        || record.source_scope_digest.is_some();
    if has_scope {
        if record.source_scope_version != Some(1)
            || record.source_scope_kind.as_deref() != Some("DIRECTORY")
            || !is_sha256(record.source_scope_digest.as_deref())
        {
            return Err(DmlError::journal_corruption(
                "ADD FILES source scope is incomplete or invalid",
            ));
        }
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
    if operation.commit_outcome.is_some()
        && !matches!(
            operation.state,
            OperationState::Committed
                | OperationState::Finalizing
                | OperationState::Finalized
                | OperationState::FinalizeFailedKnownCommitted
        )
    {
        return Err(DmlError::journal_corruption(format!(
            "DML operation {} has a commit outcome in state {}",
            operation.operation_id,
            operation.state.as_str()
        )));
    }
    if operation.failure.is_some()
        && !matches!(
            operation.state,
            OperationState::CommitUnknown
                | OperationState::FailedKnownUncommitted
                | OperationState::FinalizeFailedKnownCommitted
        )
    {
        return Err(DmlError::journal_corruption(format!(
            "DML operation {} has failure evidence in state {}",
            operation.operation_id,
            operation.state.as_str()
        )));
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
