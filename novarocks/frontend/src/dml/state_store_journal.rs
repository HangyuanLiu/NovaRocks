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
};
use novarocks_state_store::metrics::StateStoreMetrics;
use novarocks_state_store::{OperationId, RunFailure, run_side_effect_free};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Handle, RuntimeFlavor};
use uuid::Uuid;

use crate::dml::error::DmlError;
use crate::dml::journal::OperationJournal;
use crate::dml::model::{
    CreatePreparingRequest, CreateStatementOperationRequest, DML_EXTERNAL_FACT_ENCODED_LIMIT,
    DML_LEGACY_OPERATION_SCHEMA_VERSION, DML_OPERATION_SCHEMA_VERSION,
    DML_UNFINISHED_SCHEMA_VERSION, DmlOperationId, DurableExternalFact,
    IcebergCleanupOutcomeRecord, IcebergCommitOutcomeRecord, IcebergOperationFailureRecord,
    IcebergRecoveryEvidenceRecord, OperationFact, OperationKind, OperationMutationRequest,
    OperationPayload, OperationState, OperationTarget, StoredOperation,
    validate_operation_transition,
};
use crate::dml::now_unix_millis;

const OPERATION_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/operations/";
const UNFINISHED_PREFIX: &[u8] = b"novarocks/frontend/dml/v1/unfinished/";

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
                    if let Err(error) = validate_operation_transition(operation.state, request.state)
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
}

fn operation_key(operation_id: DmlOperationId) -> Result<Key, DmlError> {
    key_for(OPERATION_PREFIX, operation_id)
}

fn unfinished_key(operation_id: DmlOperationId) -> Result<Key, DmlError> {
    key_for(UNFINISHED_PREFIX, operation_id)
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
        DML_OPERATION_SCHEMA_VERSION => {
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
        DML_LEGACY_OPERATION_SCHEMA_VERSION | DML_OPERATION_SCHEMA_VERSION
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
        | (OperationKind::Truncate, OperationPayload::TruncateLifecycle(_)) => {}
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
            validate_external_fact(record.prepare_fact.as_ref())?;
            validate_external_fact(record.publish_fact.as_ref())?;
            validate_external_fact(record.abort_staging_fact.as_ref())
        }
        OperationPayload::TruncateLifecycle(record) => {
            validate_external_fact(record.outcome.as_ref())
        }
    }
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
