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
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use novarocks::engine::table_maintenance::{MaintenanceTarget, OptimizeJobState};
use novarocks_spi::connector::{
    ConnectorDistributedRewriteAttemptCheckpoint as SpiRewriteCheckpoint,
    ConnectorDistributedRewriteAttemptDisposition as SpiRewriteDisposition, ConnectorInstanceId,
    ConnectorWriteCohortId, ConnectorWriteExecutionId,
};
use novarocks_spi::state_store::{
    CommitResolution, Direction, Key, KeyRange, Precondition, RangeRequest, StateRecord,
    StateStore, StateStoreError, StateStoreErrorKind, TransactionId, Value, VersionToken,
    WriteTransaction,
};
use novarocks_state_store::metrics::StateStoreMetrics;
use novarocks_state_store::{OperationId, RunFailure, run_side_effect_free};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::model::{
    DISTRIBUTED_REWRITE_MAX_ATTEMPT_HANDLE_BYTES, DISTRIBUTED_REWRITE_MAX_PAYLOAD_BYTES,
    DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION, DistributedRewriteAttemptCheckpoint,
    DistributedRewriteAttemptDisposition, DistributedRewriteOpaquePayload,
    DistributedRewriteOperation, DistributedRewriteOperationCreate,
    DistributedRewriteOperationState, DistributedRewritePlanPayload,
    METADATA_MAINTENANCE_MAX_PAYLOAD_BYTES, METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION,
    MetadataMaintenanceExactOwner, MetadataMaintenanceOpaquePayload, MetadataMaintenanceOperation,
    MetadataMaintenanceOperationCreate, MetadataMaintenanceOperationState,
    MetadataMaintenancePlanPayload, OPTIMIZE_JOB_SCHEMA_VERSION, OptimizeJob, OptimizeJobCreate,
    OptimizeJobOutcome, StoredDistributedRewriteAttemptV3, StoredDistributedRewriteOperationV3,
    StoredDistributedRewritePayloadKindV3, StoredDistributedRewritePayloadV3,
    StoredDistributedRewriteTransactionActionV3, StoredDistributedRewriteTransactionV3,
    StoredMaintenanceTargetV1, StoredMetadataMaintenanceOperationV2,
    StoredMetadataMaintenancePayloadKindV2, StoredMetadataMaintenancePayloadV2,
    StoredMetadataMaintenanceTransactionActionV2, StoredMetadataMaintenanceTransactionV2,
    StoredOptimizeCounterV1, StoredOptimizeJobStateV1, StoredOptimizeJobV1,
    StoredOptimizeOperationActionV1, StoredOptimizeOperationV1, StoredOptimizeOutcomeV1,
};

const COUNTER_KEY: &str = "novarocks/frontend/table-maintenance/v1/counter";
const JOB_PREFIX: &str = "novarocks/frontend/table-maintenance/v1/jobs/";
const PENDING_PREFIX: &str = "novarocks/frontend/table-maintenance/v1/state/pending/";
const RUNNING_PREFIX: &str = "novarocks/frontend/table-maintenance/v1/state/running/";
const ACTIVE_PREFIX: &str = "novarocks/frontend/table-maintenance/v1/active/";
const OPERATION_PREFIX: &str = "novarocks/frontend/table-maintenance/v1/operations/";

const METADATA_OPERATION_PREFIX: &str = "novarocks/frontend/table-maintenance/v2/operations/";
const METADATA_PAYLOAD_PREFIX: &str = "novarocks/frontend/table-maintenance/v2/payloads/";
const METADATA_STATE_PREFIX: &str = "novarocks/frontend/table-maintenance/v2/state/";
const METADATA_ACTIVE_PREFIX: &str = "novarocks/frontend/table-maintenance/v2/active/";
const METADATA_TRANSACTION_PREFIX: &str = "novarocks/frontend/table-maintenance/v2/transactions/";

const DISTRIBUTED_REWRITE_OPERATION_PREFIX: &str =
    "novarocks/frontend/table-maintenance/v3/rewrite/operations/";
const DISTRIBUTED_REWRITE_PAYLOAD_PREFIX: &str =
    "novarocks/frontend/table-maintenance/v3/rewrite/payloads/";
const DISTRIBUTED_REWRITE_ATTEMPT_PREFIX: &str =
    "novarocks/frontend/table-maintenance/v3/rewrite/attempts/";
const DISTRIBUTED_REWRITE_STATE_PREFIX: &str =
    "novarocks/frontend/table-maintenance/v3/rewrite/state/";
const DISTRIBUTED_REWRITE_TRANSACTION_PREFIX: &str =
    "novarocks/frontend/table-maintenance/v3/rewrite/transactions/";
const SHARED_ACTIVE_PREFIX: &str = "novarocks/frontend/table-maintenance/v3/active/";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryErrorKind {
    AlreadyActive,
    NotFound,
    InvalidTransition,
    Corruption,
    CommitUnknown,
    Store,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryError {
    kind: RepositoryErrorKind,
    message: String,
}

impl RepositoryError {
    pub const fn kind(&self) -> RepositoryErrorKind {
        self.kind
    }

    fn new(kind: RepositoryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn corruption(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::Corruption, message)
    }

    fn store(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::Store, message)
    }

    fn with_context(self, context: impl fmt::Display) -> Self {
        Self::new(self.kind, format!("{context}: {}", self.message))
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RepositoryError {}

// Stateful transaction closures return StateStoreError at their outer layer.
// Repository validation failures are intentionally translated to a stable
// corruption marker so a closure can return the original typed error through
// its inner RepositoryResult without leaking a partial write.
impl From<RepositoryError> for StateStoreError {
    fn from(_: RepositoryError) -> Self {
        StateStoreError::new(
            StateStoreErrorKind::Corruption,
            "table maintenance repository invariant failed",
        )
    }
}

type RepositoryResult<T> = Result<T, RepositoryError>;
type TransactionResult<T> = Result<RepositoryResult<T>, StateStoreError>;

#[derive(Clone)]
pub struct OptimizeJobRepository {
    store: Arc<dyn StateStore>,
    metrics: Arc<StateStoreMetrics>,
}

impl fmt::Debug for OptimizeJobRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OptimizeJobRepository")
            .field("provider", &self.metrics.provider())
            .finish_non_exhaustive()
    }
}

impl OptimizeJobRepository {
    pub async fn open(store: Arc<dyn StateStore>) -> Result<Self, RepositoryError> {
        let provider_id = store.metrics_snapshot().provider;
        let repository = Self {
            metrics: Arc::new(StateStoreMetrics::new(provider_id)),
            store,
        };
        repository.list().await?;
        Ok(repository)
    }

    pub async fn create(&self, request: OptimizeJobCreate) -> RepositoryResult<OptimizeJob> {
        let operation_id = OperationId::new_v7();
        let context = target_context(&request.target);
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            operation_id,
            "create frontend optimize job",
            |transaction| {
                let request = request.clone();
                Box::pin(async move { apply_create(transaction, operation_id, request).await })
            },
        )
        .await;

        match result {
            Ok(success) => success.value,
            Err(RunFailure::CommitUnknown {
                transaction_id,
                error,
            }) => {
                self.resolve_commit_unknown(
                    transaction_id,
                    operation_id,
                    StoredOptimizeOperationActionV1::Create,
                    None,
                    &format!("create optimize job for {context}"),
                    error,
                )
                .await
            }
            Err(failure) => Err(format_run_failure(
                &format!("create optimize job for {context}"),
                failure,
            )),
        }
    }

    pub async fn list(&self) -> RepositoryResult<Vec<OptimizeJob>> {
        let prefix = make_key(JOB_PREFIX, "build optimize job range")?;
        let range = KeyRange::for_prefix(prefix).map_err(|error| {
            RepositoryError::store(format!("build optimize job range failed: {error}"))
        })?;
        let mut transaction = self.store.begin_read().await.map_err(|error| {
            RepositoryError::store(format!("begin optimize job list failed: {error}"))
        })?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size: self.store.limits().max_page_size,
            continuation: None,
        };
        let mut jobs = Vec::new();
        let mut ids = BTreeSet::new();

        loop {
            let page = transaction.range(&request).await.map_err(|error| {
                RepositoryError::store(format!("list optimize job page failed: {error}"))
            })?;
            for record in page.records {
                let stored = decode_job_record(record)?;
                if !ids.insert(stored.job_id) {
                    return Err(RepositoryError::corruption(format!(
                        "list optimize jobs failed: duplicate job id {}",
                        stored.job_id
                    )));
                }
                jobs.push(OptimizeJob::from(&stored));
            }
            let Some(continuation) = page.continuation else {
                break;
            };
            request.continuation = Some(continuation);
        }

        transaction.abort().await.map_err(|error| {
            RepositoryError::store(format!("finish optimize job list failed: {error}"))
        })?;
        jobs.sort_by_key(|job| job.job_id);
        Ok(jobs)
    }

    pub async fn list_pending(&self) -> RepositoryResult<Vec<OptimizeJob>> {
        self.list_indexed_jobs(PENDING_PREFIX, OptimizeJobState::Pending)
            .await
    }

    pub async fn claim(&self, job_id: i64, now_ms: i64) -> RepositoryResult<Option<OptimizeJob>> {
        validate_job_id(job_id, "claim optimize job")?;
        let operation_id = OperationId::new_v7();
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            operation_id,
            "claim frontend optimize job",
            |transaction| {
                Box::pin(
                    async move { apply_claim(transaction, operation_id, job_id, now_ms).await },
                )
            },
        )
        .await;

        match result {
            Ok(success) => success.value,
            Err(RunFailure::CommitUnknown {
                transaction_id,
                error,
            }) => {
                let recovered = self
                    .resolve_commit_unknown(
                        transaction_id,
                        operation_id,
                        StoredOptimizeOperationActionV1::Claim,
                        Some(job_id),
                        &format!("claim optimize job {job_id}"),
                        error,
                    )
                    .await?;
                if recovered.state != OptimizeJobState::Running {
                    return Err(RepositoryError::corruption(format!(
                        "claim optimize job {job_id} authoritative result is not RUNNING"
                    )));
                }
                Ok(Some(recovered))
            }
            Err(failure) => Err(format_run_failure(
                &format!("claim optimize job {job_id}"),
                failure,
            )),
        }
    }

    pub async fn record_outcome(
        &self,
        job_id: i64,
        outcome: OptimizeJobOutcome,
    ) -> RepositoryResult<()> {
        validate_job_id(job_id, "record optimize job outcome")?;
        let operation_id = OperationId::new_v7();
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            operation_id,
            "record frontend optimize job outcome",
            |transaction| {
                let outcome = outcome.clone();
                Box::pin(async move {
                    apply_record_outcome(transaction, operation_id, job_id, outcome).await
                })
            },
        )
        .await;
        self.resolve_unit_mutation(
            result,
            operation_id,
            StoredOptimizeOperationActionV1::RecordOutcome,
            job_id,
            "record outcome for optimize job",
        )
        .await
    }

    pub async fn finish(&self, job_id: i64, now_ms: i64) -> RepositoryResult<()> {
        validate_job_id(job_id, "finish optimize job")?;
        let operation_id = OperationId::new_v7();
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            operation_id,
            "finish frontend optimize job",
            |transaction| {
                Box::pin(
                    async move { apply_finish(transaction, operation_id, job_id, now_ms).await },
                )
            },
        )
        .await;
        self.resolve_unit_mutation(
            result,
            operation_id,
            StoredOptimizeOperationActionV1::Finish,
            job_id,
            "finish optimize job",
        )
        .await
    }

    pub async fn fail(&self, job_id: i64, now_ms: i64, message: String) -> RepositoryResult<()> {
        validate_job_id(job_id, "fail optimize job")?;
        let operation_id = OperationId::new_v7();
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            operation_id,
            "fail frontend optimize job",
            |transaction| {
                let message = message.clone();
                Box::pin(async move {
                    apply_fail(transaction, operation_id, job_id, now_ms, message).await
                })
            },
        )
        .await;
        self.resolve_unit_mutation(
            result,
            operation_id,
            StoredOptimizeOperationActionV1::Fail,
            job_id,
            "fail optimize job",
        )
        .await
    }

    pub async fn reconcile_startup(&self, now_ms: i64) -> RepositoryResult<usize> {
        let running = self
            .list_indexed_jobs(RUNNING_PREFIX, OptimizeJobState::Running)
            .await?;
        let mut reconciled = 0;
        for job in running {
            if job.outcome.is_some() {
                self.finish(job.job_id, now_ms).await?;
            } else {
                self.fail(
                    job.job_id,
                    now_ms,
                    "optimize job failed during frontend restart reconciliation".to_string(),
                )
                .await?;
            }
            reconciled += 1;
        }
        Ok(reconciled)
    }

    async fn list_indexed_jobs(
        &self,
        prefix_text: &str,
        expected_state: OptimizeJobState,
    ) -> RepositoryResult<Vec<OptimizeJob>> {
        let prefix = make_key(prefix_text, "build optimize job state range")?;
        let range = KeyRange::for_prefix(prefix).map_err(|error| {
            RepositoryError::store(format!("build optimize job state range failed: {error}"))
        })?;
        let mut transaction = self.store.begin_read().await.map_err(|error| {
            RepositoryError::store(format!("begin optimize job state list failed: {error}"))
        })?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size: self.store.limits().max_page_size,
            continuation: None,
        };
        let mut jobs = Vec::new();
        let mut ids = BTreeSet::new();

        loop {
            let page = transaction.range(&request).await.map_err(|error| {
                RepositoryError::store(format!("list optimize job state page failed: {error}"))
            })?;
            for index in page.records {
                let job_id = decode_index_key(prefix_text, &index.key).map_err(|error| {
                    error.with_context(format!(
                        "list {} optimize jobs: decode state index key",
                        expected_state.as_str()
                    ))
                })?;
                let value_job_id = decode_index_value(&index.value).map_err(|error| {
                    error.with_context(format!(
                        "list {} optimize jobs: decode state index for job {job_id}",
                        expected_state.as_str()
                    ))
                })?;
                if job_id != value_job_id {
                    return Err(RepositoryError::corruption(format!(
                        "optimize job state index identity mismatch: key job {job_id}, value job {value_job_id}"
                    )));
                }
                if !ids.insert(job_id) {
                    return Err(RepositoryError::corruption(format!(
                        "duplicate optimize job state index for job {job_id}"
                    )));
                }
                let stored = load_job_from_transaction(transaction.as_mut(), job_id)
                    .await
                    .map_err(|error| {
                        RepositoryError::store(format!(
                            "load indexed optimize job {job_id} failed: {error}"
                        ))
                    })??
                    .ok_or_else(|| {
                        RepositoryError::corruption(format!(
                            "optimize job state index references missing job {job_id}"
                        ))
                    })?
                    .stored;
                let job = OptimizeJob::from(&stored);
                if job.state != expected_state {
                    return Err(RepositoryError::corruption(format!(
                        "optimize job {job_id} state index expects {}, found {}",
                        expected_state.as_str(),
                        job.state.as_str()
                    )));
                }
                jobs.push(job);
            }
            let Some(continuation) = page.continuation else {
                break;
            };
            request.continuation = Some(continuation);
        }

        transaction.abort().await.map_err(|error| {
            RepositoryError::store(format!("finish optimize job state list failed: {error}"))
        })?;
        jobs.sort_by_key(|job| job.job_id);
        Ok(jobs)
    }

    async fn resolve_unit_mutation(
        &self,
        result: Result<novarocks_state_store::RunSuccess<RepositoryResult<()>>, RunFailure>,
        operation_id: OperationId,
        action: StoredOptimizeOperationActionV1,
        job_id: i64,
        action_context: &str,
    ) -> RepositoryResult<()> {
        match result {
            Ok(success) => success.value,
            Err(RunFailure::CommitUnknown {
                transaction_id,
                error,
            }) => {
                self.resolve_commit_unknown(
                    transaction_id,
                    operation_id,
                    action,
                    Some(job_id),
                    &format!("{action_context} {job_id}"),
                    error,
                )
                .await?;
                Ok(())
            }
            Err(failure) => Err(format_run_failure(
                &format!("{action_context} {job_id}"),
                failure,
            )),
        }
    }

    async fn resolve_commit_unknown(
        &self,
        transaction_id: TransactionId,
        operation_id: OperationId,
        expected_action: StoredOptimizeOperationActionV1,
        expected_job_id: Option<i64>,
        context: &str,
        commit_error: StateStoreError,
    ) -> RepositoryResult<OptimizeJob> {
        let resolution = self
            .store
            .resolve_commit(&transaction_id)
            .await
            .map_err(|error| {
                commit_unknown_error(
                    context,
                    transaction_id,
                    &commit_error,
                    &format!("commit resolution failed: {error}"),
                )
            })?;
        let certainty = match resolution {
            CommitResolution::Committed(receipt) => {
                if receipt.transaction_id != transaction_id {
                    return Err(RepositoryError::corruption(format!(
                        "{context} commit resolution returned receipt for transaction {}, expected {}",
                        receipt.transaction_id.as_uuid(),
                        transaction_id.as_uuid()
                    )));
                }
                CommitCertainty::Committed
            }
            CommitResolution::NotCommitted => {
                return Err(RepositoryError::store(format!(
                    "{context} transaction {} was not committed after commit-unknown: {commit_error}",
                    transaction_id.as_uuid()
                )));
            }
            CommitResolution::Unresolved => CommitCertainty::Unresolved,
        };
        self.recover_operation(
            transaction_id,
            certainty,
            operation_id,
            expected_action,
            expected_job_id,
            context,
            commit_error,
        )
        .await
    }

    async fn recover_operation(
        &self,
        transaction_id: TransactionId,
        certainty: CommitCertainty,
        operation_id: OperationId,
        expected_action: StoredOptimizeOperationActionV1,
        expected_job_id: Option<i64>,
        context: &str,
        commit_error: StateStoreError,
    ) -> RepositoryResult<OptimizeJob> {
        let key = operation_key(operation_id)?;
        let mut transaction = self.store.begin_read().await.map_err(|error| {
            commit_recovery_error(
                certainty,
                context,
                transaction_id,
                &commit_error,
                &format!("authoritative read begin failed: {error}"),
            )
        })?;
        let operation_record = transaction.get(&key).await.map_err(|error| {
            commit_recovery_error(
                certainty,
                context,
                transaction_id,
                &commit_error,
                &format!("authoritative operation read failed: {error}"),
            )
        })?;
        let Some(operation_record) = operation_record else {
            transaction.abort().await.map_err(|error| {
                commit_recovery_error(
                    certainty,
                    context,
                    transaction_id,
                    &commit_error,
                    &format!("authoritative read finish failed: {error}"),
                )
            })?;
            return Err(match certainty {
                CommitCertainty::Committed => RepositoryError::corruption(format!(
                    "{context} transaction {} is committed but its atomic operation marker is absent",
                    transaction_id.as_uuid()
                )),
                CommitCertainty::Unresolved => commit_unknown_error(
                    context,
                    transaction_id,
                    &commit_error,
                    "operation marker is absent",
                ),
            });
        };
        let marker_context = format!(
            "{context} authoritative operation {} ({expected_action:?})",
            operation_id.as_uuid()
        );
        let marker: StoredOptimizeOperationV1 = decode_json(
            operation_record.value.as_bytes(),
            "optimize operation marker",
        )
        .map_err(|error| error.with_context(&marker_context))?;
        validate_operation_marker(&marker).map_err(|error| error.with_context(&marker_context))?;
        if marker.operation_id != *operation_id.as_uuid()
            || marker.action != expected_action
            || expected_job_id.is_some_and(|job_id| job_id != marker.job_id)
        {
            return Err(RepositoryError::corruption(format!(
                "{context} authoritative operation marker does not match the requested operation"
            )));
        }
        let current = load_job_from_transaction(transaction.as_mut(), marker.job_id)
            .await
            .map_err(|error| {
                commit_recovery_error(
                    certainty,
                    context,
                    transaction_id,
                    &commit_error,
                    &format!("authoritative job read failed: {error}"),
                )
            })??
            .ok_or_else(|| {
                RepositoryError::corruption(format!(
                    "{context} operation marker references missing job {}",
                    marker.job_id
                ))
            })?
            .stored;
        transaction.abort().await.map_err(|error| {
            commit_recovery_error(
                certainty,
                context,
                transaction_id,
                &commit_error,
                &format!("authoritative read finish failed: {error}"),
            )
        })?;
        validate_operation_successor(&marker, &current)
            .map_err(|error| error.with_context(&marker_context))?;
        Ok(OptimizeJob::from(&marker.post_job))
    }
}

#[derive(Clone, Copy)]
enum CommitCertainty {
    Committed,
    Unresolved,
}

struct VersionedStoredJob {
    stored: StoredOptimizeJobV1,
    version: VersionToken,
}

async fn apply_create(
    transaction: &mut dyn WriteTransaction,
    operation_id: OperationId,
    request: OptimizeJobCreate,
) -> TransactionResult<OptimizeJob> {
    let active_key = match active_target_key(&request.target) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    if let Some(active) = transaction.get(&active_key).await? {
        let active_job_id = match decode_index_value(&active.value) {
            Ok(job_id) => job_id,
            Err(error) => {
                return Ok(Err(error.with_context(format!(
                    "create optimize job for {} failed: decode active target index",
                    target_context(&request.target)
                ))));
            }
        };
        let active_job = match load_job_from_transaction(transaction, active_job_id).await? {
            Ok(Some(job)) => job.stored,
            Ok(None) => {
                return Ok(Err(RepositoryError::corruption(format!(
                    "create optimize job for {} failed: active target index references missing job {active_job_id}",
                    target_context(&request.target)
                ))));
            }
            Err(error) => return Ok(Err(error)),
        };
        if active_job.target != StoredMaintenanceTargetV1::from(&request.target)
            || !matches!(
                active_job.state,
                StoredOptimizeJobStateV1::Pending | StoredOptimizeJobStateV1::Running
            )
        {
            return Ok(Err(RepositoryError::corruption(format!(
                "create optimize job for {} failed: active target index references inconsistent job {active_job_id}",
                target_context(&request.target)
            ))));
        }
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::AlreadyActive,
            format!(
                "create optimize job for {} failed: target already has active job {active_job_id}",
                target_context(&request.target)
            ),
        )));
    }

    // The v1 OPTIMIZE and v2 metadata-maintenance repositories own separate
    // records, but must never run concurrently for one Iceberg table.
    let metadata_active_key = match metadata_active_target_key(&request.target) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    if let Some(active) = transaction.get(&metadata_active_key).await? {
        let operation_id = match decode_uuid_index_value(&active.value, "metadata active target") {
            Ok(operation_id) => operation_id,
            Err(error) => return Ok(Err(error)),
        };
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::AlreadyActive,
            format!(
                "create optimize job for {} failed: target has active metadata maintenance operation {operation_id}",
                target_context(&request.target)
            ),
        )));
    }
    let shared_active_key = match shared_active_target_key(&request.target) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    if let Some(active) = transaction.get(&shared_active_key).await? {
        let fence: StoredSharedActiveFenceV3 =
            match decode_rewrite_json(active.value.as_bytes(), "shared maintenance active fence") {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::AlreadyActive,
            format!(
                "create optimize job for {} failed: target has active {:?} maintenance operation {}",
                target_context(&request.target),
                fence.family,
                fence.operation_id
            ),
        )));
    }

    let counter_key = match make_key(COUNTER_KEY, "build optimize job counter key") {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let counter_record = transaction.get(&counter_key).await?;
    let (last_job_id, counter_precondition) = match counter_record {
        Some(record) => {
            let counter: StoredOptimizeCounterV1 =
                match decode_json(record.value.as_bytes(), "optimize job counter") {
                    Ok(counter) => counter,
                    Err(error) => return Ok(Err(error)),
                };
            if counter.schema_version != OPTIMIZE_JOB_SCHEMA_VERSION || counter.last_job_id < 0 {
                return Ok(Err(RepositoryError::corruption(
                    "optimize job counter is corrupt",
                )));
            }
            (counter.last_job_id, Precondition::Version(record.version))
        }
        None => (0, Precondition::Absent),
    };
    let Some(job_id) = last_job_id.checked_add(1) else {
        return Ok(Err(RepositoryError::corruption(
            "optimize job id counter overflow",
        )));
    };
    let stored = StoredOptimizeJobV1 {
        schema_version: OPTIMIZE_JOB_SCHEMA_VERSION,
        job_id,
        target: StoredMaintenanceTargetV1::from(&request.target),
        base_snapshot_id: request.base_snapshot_id,
        state: StoredOptimizeJobStateV1::Pending,
        outcome: None,
        error_message: None,
        created_at_ms: request.created_at_ms,
        started_at_ms: None,
        finished_at_ms: None,
        last_operation_id: *operation_id.as_uuid(),
    };
    let counter = StoredOptimizeCounterV1 {
        schema_version: OPTIMIZE_JOB_SCHEMA_VERSION,
        last_job_id: job_id,
    };
    let counter_value = match encode_json(&counter, "optimize job counter") {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let job_value = match encode_job(&stored) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let job_key = match job_key(job_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let pending_key = match state_key(PENDING_PREFIX, job_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let index_value = match encode_index_value(job_id) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let (operation_key, operation_value) = match operation_record(
        operation_id,
        StoredOptimizeOperationActionV1::Create,
        &stored,
    ) {
        Ok(record) => record,
        Err(error) => return Ok(Err(error)),
    };

    transaction
        .put(counter_key, counter_value, counter_precondition)
        .await?;
    transaction
        .put(job_key, job_value, Precondition::Absent)
        .await?;
    transaction
        .put(pending_key, index_value.clone(), Precondition::Absent)
        .await?;
    transaction
        .put(active_key, index_value, Precondition::Absent)
        .await?;
    transaction
        .put(operation_key, operation_value, Precondition::Absent)
        .await?;
    Ok(Ok(OptimizeJob::from(&stored)))
}

async fn apply_claim(
    transaction: &mut dyn WriteTransaction,
    operation_id: OperationId,
    job_id: i64,
    now_ms: i64,
) -> TransactionResult<Option<OptimizeJob>> {
    let Some(mut job) = (match load_job_from_transaction(transaction, job_id).await? {
        Ok(job) => job,
        Err(error) => return Ok(Err(error)),
    }) else {
        return Ok(Ok(None));
    };
    if job.stored.state != StoredOptimizeJobStateV1::Pending {
        return Ok(Ok(None));
    }
    if let Err(error) =
        require_index(transaction, PENDING_PREFIX, job_id, "claim optimize job").await?
    {
        return Ok(Err(error));
    }
    let running_key = match state_key(RUNNING_PREFIX, job_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    if transaction.get(&running_key).await?.is_some() {
        return Ok(Err(RepositoryError::corruption(format!(
            "claim optimize job {job_id} failed: running index already exists"
        ))));
    }
    if let Err(error) = require_active_index(transaction, &job.stored, "claim optimize job").await?
    {
        return Ok(Err(error));
    }

    job.stored.state = StoredOptimizeJobStateV1::Running;
    job.stored.started_at_ms = Some(now_ms);
    job.stored.last_operation_id = *operation_id.as_uuid();
    let value = match encode_job(&job.stored) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let key = match job_key(job_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let pending_key = match state_key(PENDING_PREFIX, job_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let index_value = match encode_index_value(job_id) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let (operation_key, operation_value) = match operation_record(
        operation_id,
        StoredOptimizeOperationActionV1::Claim,
        &job.stored,
    ) {
        Ok(record) => record,
        Err(error) => return Ok(Err(error)),
    };
    transaction
        .put(key, value, Precondition::Version(job.version))
        .await?;
    transaction
        .delete(pending_key, Precondition::Present)
        .await?;
    transaction
        .put(running_key, index_value, Precondition::Absent)
        .await?;
    transaction
        .put(operation_key, operation_value, Precondition::Absent)
        .await?;
    Ok(Ok(Some(OptimizeJob::from(&job.stored))))
}

async fn apply_record_outcome(
    transaction: &mut dyn WriteTransaction,
    operation_id: OperationId,
    job_id: i64,
    outcome: OptimizeJobOutcome,
) -> TransactionResult<()> {
    let mut job =
        match require_running_job(transaction, job_id, "record optimize job outcome").await? {
            Ok(job) => job,
            Err(error) => return Ok(Err(error)),
        };
    job.stored.outcome = Some(StoredOptimizeOutcomeV1::from(&outcome));
    job.stored.last_operation_id = *operation_id.as_uuid();
    let value = match encode_job(&job.stored) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let key = match job_key(job_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let (operation_key, operation_value) = match operation_record(
        operation_id,
        StoredOptimizeOperationActionV1::RecordOutcome,
        &job.stored,
    ) {
        Ok(record) => record,
        Err(error) => return Ok(Err(error)),
    };
    transaction
        .put(key, value, Precondition::Version(job.version))
        .await?;
    transaction
        .put(operation_key, operation_value, Precondition::Absent)
        .await?;
    Ok(Ok(()))
}

async fn apply_finish(
    transaction: &mut dyn WriteTransaction,
    operation_id: OperationId,
    job_id: i64,
    now_ms: i64,
) -> TransactionResult<()> {
    let mut job = match require_running_job(transaction, job_id, "finish optimize job").await? {
        Ok(job) => job,
        Err(error) => return Ok(Err(error)),
    };
    if job.stored.outcome.is_none() {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::InvalidTransition,
            format!("finish optimize job {job_id} failed: outcome has not been recorded"),
        )));
    }
    job.stored.state = StoredOptimizeJobStateV1::Finished;
    job.stored.error_message = None;
    job.stored.finished_at_ms = Some(now_ms);
    job.stored.last_operation_id = *operation_id.as_uuid();
    terminalize_job(
        transaction,
        operation_id,
        StoredOptimizeOperationActionV1::Finish,
        job,
    )
    .await
}

async fn apply_fail(
    transaction: &mut dyn WriteTransaction,
    operation_id: OperationId,
    job_id: i64,
    now_ms: i64,
    message: String,
) -> TransactionResult<()> {
    let mut job = match require_running_job(transaction, job_id, "fail optimize job").await? {
        Ok(job) => job,
        Err(error) => return Ok(Err(error)),
    };
    job.stored.state = StoredOptimizeJobStateV1::Failed;
    job.stored.error_message = Some(message);
    job.stored.finished_at_ms = Some(now_ms);
    job.stored.last_operation_id = *operation_id.as_uuid();
    terminalize_job(
        transaction,
        operation_id,
        StoredOptimizeOperationActionV1::Fail,
        job,
    )
    .await
}

async fn terminalize_job(
    transaction: &mut dyn WriteTransaction,
    operation_id: OperationId,
    action: StoredOptimizeOperationActionV1,
    job: VersionedStoredJob,
) -> TransactionResult<()> {
    let job_id = job.stored.job_id;
    let value = match encode_job(&job.stored) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let key = match job_key(job_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let running_key = match state_key(RUNNING_PREFIX, job_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let active_key = match active_target_key(&job.stored.target.clone().into()) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let (operation_key, operation_value) = match operation_record(operation_id, action, &job.stored)
    {
        Ok(record) => record,
        Err(error) => return Ok(Err(error)),
    };
    transaction
        .put(key, value, Precondition::Version(job.version))
        .await?;
    transaction
        .delete(running_key, Precondition::Present)
        .await?;
    transaction
        .delete(active_key, Precondition::Present)
        .await?;
    transaction
        .put(operation_key, operation_value, Precondition::Absent)
        .await?;
    Ok(Ok(()))
}

async fn require_running_job(
    transaction: &mut dyn WriteTransaction,
    job_id: i64,
    action: &str,
) -> TransactionResult<VersionedStoredJob> {
    let Some(job) = (match load_job_from_transaction(transaction, job_id).await? {
        Ok(job) => job,
        Err(error) => return Ok(Err(error)),
    }) else {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::NotFound,
            format!("{action} {job_id} failed: job not found"),
        )));
    };
    if job.stored.state != StoredOptimizeJobStateV1::Running {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::InvalidTransition,
            format!(
                "{action} {job_id} failed: expected RUNNING, found {}",
                OptimizeJobState::from(job.stored.state).as_str()
            ),
        )));
    }
    if let Err(error) = require_index(transaction, RUNNING_PREFIX, job_id, action).await? {
        return Ok(Err(error));
    }
    if let Err(error) = require_active_index(transaction, &job.stored, action).await? {
        return Ok(Err(error));
    }
    Ok(Ok(job))
}

async fn require_index(
    transaction: &mut dyn WriteTransaction,
    prefix: &str,
    job_id: i64,
    action: &str,
) -> TransactionResult<()> {
    let key = match state_key(prefix, job_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let Some(record) = transaction.get(&key).await? else {
        return Ok(Err(RepositoryError::corruption(format!(
            "{action} {job_id} failed: required state index is missing"
        ))));
    };
    match decode_index_value(&record.value) {
        Ok(index_job_id) if index_job_id == job_id => Ok(Ok(())),
        Ok(index_job_id) => Ok(Err(RepositoryError::corruption(format!(
            "{action} {job_id} failed: state index references job {index_job_id}"
        )))),
        Err(error) => Ok(Err(error.with_context(format!(
            "{action} {job_id} failed: decode required state index {prefix}"
        )))),
    }
}

async fn require_active_index(
    transaction: &mut dyn WriteTransaction,
    job: &StoredOptimizeJobV1,
    action: &str,
) -> TransactionResult<()> {
    let target: MaintenanceTarget = job.target.clone().into();
    let key = match active_target_key(&target) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let Some(record) = transaction.get(&key).await? else {
        return Ok(Err(RepositoryError::corruption(format!(
            "{action} {} for {} failed: active target index is missing",
            job.job_id,
            target_context(&target)
        ))));
    };
    match decode_index_value(&record.value) {
        Ok(index_job_id) if index_job_id == job.job_id => Ok(Ok(())),
        Ok(index_job_id) => Ok(Err(RepositoryError::corruption(format!(
            "{action} {} for {} failed: active target index references job {index_job_id}",
            job.job_id,
            target_context(&target)
        )))),
        Err(error) => Ok(Err(error.with_context(format!(
            "{action} {} for {} failed: decode active target index",
            job.job_id,
            target_context(&target)
        )))),
    }
}

async fn load_job_from_transaction(
    transaction: &mut dyn novarocks_spi::state_store::ReadTransaction,
    job_id: i64,
) -> TransactionResult<Option<VersionedStoredJob>> {
    let key = match job_key(job_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let Some(record) = transaction.get(&key).await? else {
        return Ok(Ok(None));
    };
    let version = record.version.clone();
    match decode_job_record(record) {
        Ok(stored) => Ok(Ok(Some(VersionedStoredJob { stored, version }))),
        Err(error) => Ok(Err(error)),
    }
}

fn decode_job_record(record: StateRecord) -> RepositoryResult<StoredOptimizeJobV1> {
    let key_job_id = decode_index_key(JOB_PREFIX, &record.key)?;
    let stored: StoredOptimizeJobV1 = decode_json(
        record.value.as_bytes(),
        &format!("optimize job {key_job_id}"),
    )?;
    validate_stored_job(&stored)?;
    if stored.job_id != key_job_id {
        return Err(RepositoryError::corruption(format!(
            "optimize job identity mismatch: key job {key_job_id}, value job {}",
            stored.job_id
        )));
    }
    Ok(stored)
}

fn validate_stored_job(stored: &StoredOptimizeJobV1) -> RepositoryResult<()> {
    if stored.schema_version != OPTIMIZE_JOB_SCHEMA_VERSION {
        return Err(RepositoryError::corruption(format!(
            "unsupported optimize job schema version: {}",
            stored.schema_version
        )));
    }
    validate_job_id(stored.job_id, "decode optimize job")?;
    match stored.state {
        StoredOptimizeJobStateV1::Pending => {
            if stored.started_at_ms.is_some()
                || stored.finished_at_ms.is_some()
                || stored.outcome.is_some()
                || stored.error_message.is_some()
            {
                return Err(RepositoryError::corruption(format!(
                    "pending optimize job {} contains lifecycle fields",
                    stored.job_id
                )));
            }
        }
        StoredOptimizeJobStateV1::Running => {
            if stored.started_at_ms.is_none()
                || stored.finished_at_ms.is_some()
                || stored.error_message.is_some()
            {
                return Err(RepositoryError::corruption(format!(
                    "running optimize job {} has invalid lifecycle fields",
                    stored.job_id
                )));
            }
        }
        StoredOptimizeJobStateV1::Finished => {
            if stored.started_at_ms.is_none()
                || stored.finished_at_ms.is_none()
                || stored.outcome.is_none()
                || stored.error_message.is_some()
            {
                return Err(RepositoryError::corruption(format!(
                    "finished optimize job {} has invalid lifecycle fields",
                    stored.job_id
                )));
            }
        }
        StoredOptimizeJobStateV1::Failed => {
            if stored.started_at_ms.is_none()
                || stored.finished_at_ms.is_none()
                || stored.error_message.is_none()
            {
                return Err(RepositoryError::corruption(format!(
                    "failed optimize job {} has invalid lifecycle fields",
                    stored.job_id
                )));
            }
        }
    }
    Ok(())
}

fn encode_job(stored: &StoredOptimizeJobV1) -> RepositoryResult<Value> {
    validate_stored_job(stored)?;
    encode_json(stored, &format!("optimize job {}", stored.job_id))
}

fn operation_record(
    operation_id: OperationId,
    action: StoredOptimizeOperationActionV1,
    post_job: &StoredOptimizeJobV1,
) -> RepositoryResult<(Key, Value)> {
    let marker = StoredOptimizeOperationV1 {
        schema_version: OPTIMIZE_JOB_SCHEMA_VERSION,
        operation_id: *operation_id.as_uuid(),
        action,
        job_id: post_job.job_id,
        post_job: post_job.clone(),
    };
    Ok((
        operation_key(operation_id)?,
        encode_json(&marker, "optimize operation marker")?,
    ))
}

fn validate_operation_marker(marker: &StoredOptimizeOperationV1) -> RepositoryResult<()> {
    if marker.schema_version != OPTIMIZE_JOB_SCHEMA_VERSION {
        return Err(RepositoryError::corruption(
            "optimize operation marker has an unsupported schema version",
        ));
    }
    validate_stored_job(&marker.post_job)?;
    if marker.job_id != marker.post_job.job_id {
        return Err(RepositoryError::corruption(format!(
            "optimize operation marker job id {} does not match post-job id {}",
            marker.job_id, marker.post_job.job_id
        )));
    }
    if marker.operation_id != marker.post_job.last_operation_id {
        return Err(RepositoryError::corruption(format!(
            "optimize operation marker {} does not match post-job last operation id",
            marker.operation_id
        )));
    }
    Ok(())
}

fn validate_operation_successor(
    marker: &StoredOptimizeOperationV1,
    current: &StoredOptimizeJobV1,
) -> RepositoryResult<()> {
    let post = &marker.post_job;
    let expected_post_state = match marker.action {
        StoredOptimizeOperationActionV1::Create => StoredOptimizeJobStateV1::Pending,
        StoredOptimizeOperationActionV1::Claim | StoredOptimizeOperationActionV1::RecordOutcome => {
            StoredOptimizeJobStateV1::Running
        }
        StoredOptimizeOperationActionV1::Finish => StoredOptimizeJobStateV1::Finished,
        StoredOptimizeOperationActionV1::Fail => StoredOptimizeJobStateV1::Failed,
    };
    if post.state != expected_post_state
        || (marker.action == StoredOptimizeOperationActionV1::Claim && post.outcome.is_some())
        || (marker.action == StoredOptimizeOperationActionV1::RecordOutcome
            && post.outcome.is_none())
    {
        return Err(RepositoryError::corruption(format!(
            "operation marker action {:?} has invalid post-job state {}",
            marker.action,
            OptimizeJobState::from(post.state).as_str()
        )));
    }
    if post.job_id != current.job_id
        || post.target != current.target
        || post.base_snapshot_id != current.base_snapshot_id
        || post.created_at_ms != current.created_at_ms
    {
        return Err(RepositoryError::corruption(format!(
            "current optimize job {} does not preserve the operation post-job identity",
            current.job_id
        )));
    }

    let legal = match marker.action {
        StoredOptimizeOperationActionV1::Create => match current.state {
            StoredOptimizeJobStateV1::Pending => current == post,
            StoredOptimizeJobStateV1::Running
            | StoredOptimizeJobStateV1::Finished
            | StoredOptimizeJobStateV1::Failed => true,
        },
        StoredOptimizeOperationActionV1::Claim => {
            current.started_at_ms == post.started_at_ms
                && match current.state {
                    StoredOptimizeJobStateV1::Running => {
                        current == post || current.outcome.is_some()
                    }
                    StoredOptimizeJobStateV1::Finished | StoredOptimizeJobStateV1::Failed => true,
                    StoredOptimizeJobStateV1::Pending => false,
                }
        }
        StoredOptimizeOperationActionV1::RecordOutcome => {
            current.started_at_ms == post.started_at_ms
                && current.outcome.is_some()
                && matches!(
                    current.state,
                    StoredOptimizeJobStateV1::Running
                        | StoredOptimizeJobStateV1::Finished
                        | StoredOptimizeJobStateV1::Failed
                )
        }
        StoredOptimizeOperationActionV1::Finish | StoredOptimizeOperationActionV1::Fail => {
            current == post
        }
    };
    if !legal {
        return Err(RepositoryError::corruption(format!(
            "current optimize job {} state {} is not a legal successor of {:?} post-state {}",
            current.job_id,
            OptimizeJobState::from(current.state).as_str(),
            marker.action,
            OptimizeJobState::from(post.state).as_str()
        )));
    }
    Ok(())
}

fn encode_json<T: Serialize>(value: &T, context: &str) -> RepositoryResult<Value> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        RepositoryError::corruption(format!("encode {context} failed: {error}"))
    })?;
    Value::try_from(Bytes::from(bytes))
        .map_err(|error| RepositoryError::store(format!("encode {context} failed: {error}")))
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8], context: &str) -> RepositoryResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| RepositoryError::corruption(format!("decode {context} failed: {error}")))
}

fn make_key(value: impl AsRef<[u8]>, context: &str) -> RepositoryResult<Key> {
    Key::try_from(Bytes::copy_from_slice(value.as_ref()))
        .map_err(|error| RepositoryError::store(format!("{context} failed: {error}")))
}

fn job_key(job_id: i64) -> RepositoryResult<Key> {
    validate_job_id(job_id, "build optimize job key")?;
    make_key(
        Bytes::from(format!("{JOB_PREFIX}{job_id:016x}")),
        "build optimize job key",
    )
}

fn state_key(prefix: &str, job_id: i64) -> RepositoryResult<Key> {
    validate_job_id(job_id, "build optimize job state key")?;
    make_key(
        Bytes::from(format!("{prefix}{job_id:016x}")),
        "build optimize job state key",
    )
}

fn active_target_key(target: &MaintenanceTarget) -> RepositoryResult<Key> {
    make_key(
        Bytes::from(format!(
            "{ACTIVE_PREFIX}{}/{}/{}",
            hex::encode(target.catalog.as_bytes()),
            hex::encode(target.namespace.as_bytes()),
            hex::encode(target.table.as_bytes())
        )),
        "build optimize job active target key",
    )
}

fn operation_key(operation_id: OperationId) -> RepositoryResult<Key> {
    make_key(
        Bytes::from(format!("{OPERATION_PREFIX}{}", operation_id.as_uuid())),
        "build optimize operation key",
    )
}

fn decode_index_key(prefix: &str, key: &Key) -> RepositoryResult<i64> {
    let suffix = key
        .as_bytes()
        .strip_prefix(prefix.as_bytes())
        .ok_or_else(|| RepositoryError::corruption("optimize job key has an unknown prefix"))?;
    if suffix.len() != 16
        || !suffix
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(RepositoryError::corruption(
            "optimize job key has a non-canonical id",
        ));
    }
    let text = std::str::from_utf8(suffix)
        .map_err(|_| RepositoryError::corruption("optimize job key id is not UTF-8"))?;
    let raw = u64::from_str_radix(text, 16)
        .map_err(|_| RepositoryError::corruption("optimize job key id is invalid"))?;
    let job_id = i64::try_from(raw)
        .map_err(|_| RepositoryError::corruption("optimize job key id exceeds i64"))?;
    validate_job_id(job_id, "decode optimize job key")?;
    Ok(job_id)
}

fn encode_index_value(job_id: i64) -> RepositoryResult<Value> {
    validate_job_id(job_id, "encode optimize job index")?;
    Value::try_from(Bytes::from(format!("{job_id:016x}"))).map_err(|error| {
        RepositoryError::store(format!("encode optimize job index failed: {error}"))
    })
}

fn decode_index_value(value: &Value) -> RepositoryResult<i64> {
    if value.as_bytes().len() != 16 {
        return Err(RepositoryError::corruption(
            "optimize job index value has a non-canonical id",
        ));
    }
    let text = std::str::from_utf8(value.as_bytes())
        .map_err(|_| RepositoryError::corruption("optimize job index value is not UTF-8"))?;
    if !text
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RepositoryError::corruption(
            "optimize job index value has a non-canonical id",
        ));
    }
    let raw = u64::from_str_radix(text, 16)
        .map_err(|_| RepositoryError::corruption("optimize job index value is invalid"))?;
    let job_id = i64::try_from(raw)
        .map_err(|_| RepositoryError::corruption("optimize job index value exceeds i64"))?;
    validate_job_id(job_id, "decode optimize job index")?;
    Ok(job_id)
}

fn validate_job_id(job_id: i64, action: &str) -> RepositoryResult<()> {
    if job_id <= 0 {
        return Err(RepositoryError::corruption(format!(
            "{action} failed: optimize job id must be positive, found {job_id}"
        )));
    }
    Ok(())
}

fn target_context(target: &MaintenanceTarget) -> String {
    format!(
        "target {}.{}.{}",
        target.catalog, target.namespace, target.table
    )
}

// ---------------------------------------------------------------------------
// V2 metadata-maintenance operation repository.
//
// A metadata operation is intentionally not represented as an OPTIMIZE job.
// Its durable plan is the dispatch fence: the caller may only invoke the
// external provider after `start` has committed, and recovery may only
// reconcile that exact plan.

#[derive(Clone)]
pub struct MetadataMaintenanceOperationRepository {
    store: Arc<dyn StateStore>,
    metrics: Arc<StateStoreMetrics>,
}

impl fmt::Debug for MetadataMaintenanceOperationRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataMaintenanceOperationRepository")
            .field("provider", &self.metrics.provider())
            .finish_non_exhaustive()
    }
}

/// SHA-256 v1 digest for an opaque durable payload.  The domain separator
/// prevents payload digests from being confused with the SPI request/plan
/// digests that include richer semantic inputs.
pub fn metadata_maintenance_payload_digest(payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.metadata-maintenance.payload.v1\0");
    hasher.update((payload.len() as u64).to_be_bytes());
    hasher.update(payload);
    hasher.finalize().into()
}

impl MetadataMaintenanceOperationRepository {
    pub async fn open(store: Arc<dyn StateStore>) -> RepositoryResult<Self> {
        let provider_id = store.metrics_snapshot().provider;
        let repository = Self {
            metrics: Arc::new(StateStoreMetrics::new(provider_id)),
            store,
        };
        repository.list().await?;
        Ok(repository)
    }

    pub async fn create(
        &self,
        request: MetadataMaintenanceOperationCreate,
    ) -> RepositoryResult<MetadataMaintenanceOperation> {
        validate_metadata_create(&request)?;
        let transaction_operation_id = OperationId::new_v7();
        let context = format!(
            "create metadata maintenance operation {}",
            request.operation_id
        );
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            transaction_operation_id,
            "create frontend metadata maintenance operation",
            |transaction| {
                let request = request.clone();
                Box::pin(async move {
                    apply_metadata_create(transaction, transaction_operation_id, request).await
                })
            },
        )
        .await;
        self.resolve_metadata_result(
            result,
            transaction_operation_id,
            StoredMetadataMaintenanceTransactionActionV2::Create,
            request.operation_id,
            &context,
        )
        .await
    }

    /// Atomically persists the opaque plan and changes PENDING to RUNNING.
    /// The returned record is the only state that authorizes provider execute.
    pub async fn start(
        &self,
        operation_id: Uuid,
        plan: MetadataMaintenancePlanPayload,
        now_ms: i64,
    ) -> RepositoryResult<MetadataMaintenanceOperation> {
        validate_payload(
            &plan.payload,
            plan.payload_digest,
            "metadata maintenance plan payload",
        )?;
        let transaction_operation_id = OperationId::new_v7();
        let context = format!("start metadata maintenance operation {operation_id}");
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            transaction_operation_id,
            "start frontend metadata maintenance operation",
            |transaction| {
                let plan = plan.clone();
                Box::pin(async move {
                    apply_metadata_start(
                        transaction,
                        transaction_operation_id,
                        operation_id,
                        plan,
                        now_ms,
                    )
                    .await
                })
            },
        )
        .await;
        self.resolve_metadata_result(
            result,
            transaction_operation_id,
            StoredMetadataMaintenanceTransactionActionV2::Start,
            operation_id,
            &context,
        )
        .await
    }

    pub async fn mark_reconcile_pending(
        &self,
        operation_id: Uuid,
        evidence: MetadataMaintenanceOpaquePayload,
    ) -> RepositoryResult<MetadataMaintenanceOperation> {
        validate_payload(
            &evidence.payload,
            evidence.digest,
            "metadata maintenance reconcile evidence",
        )?;
        let transaction_operation_id = OperationId::new_v7();
        let context =
            format!("record reconcile-pending metadata maintenance operation {operation_id}");
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            transaction_operation_id,
            "record frontend metadata maintenance reconcile evidence",
            |transaction| {
                let evidence = evidence.clone();
                Box::pin(async move {
                    apply_metadata_reconcile_pending(
                        transaction,
                        transaction_operation_id,
                        operation_id,
                        evidence,
                    )
                    .await
                })
            },
        )
        .await;
        self.resolve_metadata_result(
            result,
            transaction_operation_id,
            StoredMetadataMaintenanceTransactionActionV2::ReconcilePending,
            operation_id,
            &context,
        )
        .await
    }

    pub async fn finish(
        &self,
        operation_id: Uuid,
        receipt: MetadataMaintenanceOpaquePayload,
        now_ms: i64,
    ) -> RepositoryResult<MetadataMaintenanceOperation> {
        validate_payload(
            &receipt.payload,
            receipt.digest,
            "metadata maintenance receipt",
        )?;
        self.transition_terminal(
            operation_id,
            StoredMetadataMaintenanceTransactionActionV2::Finish,
            Some(receipt),
            None,
            now_ms,
        )
        .await
    }

    pub async fn fail(
        &self,
        operation_id: Uuid,
        message: String,
        now_ms: i64,
    ) -> RepositoryResult<MetadataMaintenanceOperation> {
        validate_metadata_error(&message)?;
        self.transition_terminal(
            operation_id,
            StoredMetadataMaintenanceTransactionActionV2::Fail,
            None,
            Some(message),
            now_ms,
        )
        .await
    }

    /// An unresolved operation retains its table fence.  A later incarnation
    /// must not silently turn it into a current-generation operation.
    pub async fn mark_unresolved(
        &self,
        operation_id: Uuid,
        message: String,
        now_ms: i64,
    ) -> RepositoryResult<MetadataMaintenanceOperation> {
        validate_metadata_error(&message)?;
        let transaction_operation_id = OperationId::new_v7();
        let context = format!("mark metadata maintenance operation {operation_id} unresolved");
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            transaction_operation_id,
            "mark frontend metadata maintenance operation unresolved",
            |transaction| {
                let message = message.clone();
                Box::pin(async move {
                    apply_metadata_unresolved(
                        transaction,
                        transaction_operation_id,
                        operation_id,
                        message,
                        now_ms,
                    )
                    .await
                })
            },
        )
        .await;
        self.resolve_metadata_result(
            result,
            transaction_operation_id,
            StoredMetadataMaintenanceTransactionActionV2::Unresolve,
            operation_id,
            &context,
        )
        .await
    }

    pub async fn get(
        &self,
        operation_id: Uuid,
    ) -> RepositoryResult<Option<MetadataMaintenanceOperation>> {
        let mut transaction = self.store.begin_read().await.map_err(|error| {
            RepositoryError::store(format!(
                "begin metadata maintenance operation read failed: {error}"
            ))
        })?;
        let result = load_metadata_operation(transaction.as_mut(), operation_id)
            .await
            .map_err(|error| {
                RepositoryError::store(format!(
                    "read metadata maintenance operation failed: {error}"
                ))
            })?;
        let finish = transaction.abort().await.map_err(|error| {
            RepositoryError::store(format!(
                "finish metadata maintenance operation read failed: {error}"
            ))
        });
        finish?;
        result.map(|record| record.map(|record| MetadataMaintenanceOperation::from(&record.stored)))
    }

    pub async fn list(&self) -> RepositoryResult<Vec<MetadataMaintenanceOperation>> {
        let prefix = make_key(
            METADATA_OPERATION_PREFIX,
            "build metadata maintenance operation range",
        )?;
        let range = KeyRange::for_prefix(prefix).map_err(|error| {
            RepositoryError::store(format!(
                "build metadata maintenance operation range failed: {error}"
            ))
        })?;
        let mut transaction = self.store.begin_read().await.map_err(|error| {
            RepositoryError::store(format!(
                "begin metadata maintenance operation list failed: {error}"
            ))
        })?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size: self.store.limits().max_page_size,
            continuation: None,
        };
        let mut operations = Vec::new();
        let mut ids = BTreeSet::new();
        loop {
            let page = transaction.range(&request).await.map_err(|error| {
                RepositoryError::store(format!(
                    "list metadata maintenance operation page failed: {error}"
                ))
            })?;
            for record in page.records {
                let stored = decode_metadata_operation_record(record)?;
                if !ids.insert(stored.operation_id) {
                    return Err(RepositoryError::corruption(format!(
                        "duplicate metadata maintenance operation {}",
                        stored.operation_id
                    )));
                }
                operations.push(MetadataMaintenanceOperation::from(&stored));
            }
            let Some(continuation) = page.continuation else {
                break;
            };
            request.continuation = Some(continuation);
        }
        transaction.abort().await.map_err(|error| {
            RepositoryError::store(format!(
                "finish metadata maintenance operation list failed: {error}"
            ))
        })?;
        operations.sort_by_key(|operation| operation.operation_id);
        Ok(operations)
    }

    pub async fn list_reconcile_candidates(
        &self,
    ) -> RepositoryResult<Vec<MetadataMaintenanceOperation>> {
        let mut operations = Vec::new();
        for state in [
            MetadataMaintenanceOperationState::Running,
            MetadataMaintenanceOperationState::ReconcilePending,
        ] {
            operations.extend(self.list_by_state(state).await?);
        }
        operations.sort_by_key(|operation| operation.operation_id);
        Ok(operations)
    }

    pub async fn load_plan(
        &self,
        operation_id: Uuid,
    ) -> RepositoryResult<Option<MetadataMaintenancePlanPayload>> {
        let Some(payload) = self
            .load_payload(operation_id, StoredMetadataMaintenancePayloadKindV2::Plan)
            .await?
        else {
            return Ok(None);
        };
        let Some(operation) = self.get(operation_id).await? else {
            return Ok(None);
        };
        let Some(plan_digest) = operation.plan_digest else {
            return Err(RepositoryError::corruption(
                "metadata maintenance plan payload exists without a plan digest",
            ));
        };
        Ok(Some(MetadataMaintenancePlanPayload {
            plan_digest,
            payload_digest: payload.digest,
            payload: payload.payload,
            summary: operation.plan_summary.ok_or_else(|| {
                RepositoryError::corruption("metadata maintenance operation has no plan summary")
            })?,
        }))
    }

    pub async fn load_evidence(
        &self,
        operation_id: Uuid,
    ) -> RepositoryResult<Option<MetadataMaintenanceOpaquePayload>> {
        self.load_payload(
            operation_id,
            StoredMetadataMaintenancePayloadKindV2::Evidence,
        )
        .await
    }

    pub async fn load_receipt(
        &self,
        operation_id: Uuid,
    ) -> RepositoryResult<Option<MetadataMaintenanceOpaquePayload>> {
        self.load_payload(
            operation_id,
            StoredMetadataMaintenancePayloadKindV2::Receipt,
        )
        .await
    }

    pub async fn has_active_target(&self, target: &MaintenanceTarget) -> RepositoryResult<bool> {
        let key = metadata_active_target_key(target)?;
        let mut transaction = self.store.begin_read().await.map_err(|error| {
            RepositoryError::store(format!(
                "begin metadata maintenance active check failed: {error}"
            ))
        })?;
        let record = transaction.get(&key).await.map_err(|error| {
            RepositoryError::store(format!(
                "read metadata maintenance active check failed: {error}"
            ))
        })?;
        transaction.abort().await.map_err(|error| {
            RepositoryError::store(format!(
                "finish metadata maintenance active check failed: {error}"
            ))
        })?;
        Ok(record.is_some())
    }

    async fn load_payload(
        &self,
        operation_id: Uuid,
        kind: StoredMetadataMaintenancePayloadKindV2,
    ) -> RepositoryResult<Option<MetadataMaintenanceOpaquePayload>> {
        let key = metadata_payload_key(operation_id, kind)?;
        let mut transaction = self.store.begin_read().await.map_err(|error| {
            RepositoryError::store(format!(
                "begin metadata maintenance payload read failed: {error}"
            ))
        })?;
        let record = transaction.get(&key).await.map_err(|error| {
            RepositoryError::store(format!("read metadata maintenance payload failed: {error}"))
        })?;
        transaction.abort().await.map_err(|error| {
            RepositoryError::store(format!(
                "finish metadata maintenance payload read failed: {error}"
            ))
        })?;
        record
            .map(decode_metadata_payload_record)
            .transpose()
            .map(|payload| {
                payload.map(|payload| MetadataMaintenanceOpaquePayload {
                    digest: payload.digest,
                    payload: payload.payload,
                })
            })
    }

    async fn list_by_state(
        &self,
        state: MetadataMaintenanceOperationState,
    ) -> RepositoryResult<Vec<MetadataMaintenanceOperation>> {
        let prefix_text = metadata_state_prefix(state);
        let prefix = make_key(&prefix_text, "build metadata maintenance state range")?;
        let range = KeyRange::for_prefix(prefix).map_err(|error| {
            RepositoryError::store(format!(
                "build metadata maintenance state range failed: {error}"
            ))
        })?;
        let mut transaction = self.store.begin_read().await.map_err(|error| {
            RepositoryError::store(format!(
                "begin metadata maintenance state list failed: {error}"
            ))
        })?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size: self.store.limits().max_page_size,
            continuation: None,
        };
        let mut operations = Vec::new();
        loop {
            let page = transaction.range(&request).await.map_err(|error| {
                RepositoryError::store(format!(
                    "list metadata maintenance state page failed: {error}"
                ))
            })?;
            for index in page.records {
                let operation_id =
                    decode_uuid_index_key(&prefix_text, &index.key, "metadata maintenance state")?;
                let indexed_id =
                    decode_uuid_index_value(&index.value, "metadata maintenance state")?;
                if operation_id != indexed_id {
                    return Err(RepositoryError::corruption(
                        "metadata maintenance state index identity mismatch",
                    ));
                }
                let Some(operation) = load_metadata_operation(transaction.as_mut(), operation_id)
                    .await
                    .map_err(|error| {
                        RepositoryError::store(format!(
                            "load metadata maintenance state operation failed: {error}"
                        ))
                    })??
                else {
                    return Err(RepositoryError::corruption(
                        "metadata maintenance state index references missing operation",
                    ));
                };
                if operation.stored.state != state {
                    return Err(RepositoryError::corruption(
                        "metadata maintenance state index references wrong operation state",
                    ));
                }
                operations.push(MetadataMaintenanceOperation::from(&operation.stored));
            }
            let Some(continuation) = page.continuation else {
                break;
            };
            request.continuation = Some(continuation);
        }
        transaction.abort().await.map_err(|error| {
            RepositoryError::store(format!(
                "finish metadata maintenance state list failed: {error}"
            ))
        })?;
        Ok(operations)
    }

    async fn transition_terminal(
        &self,
        operation_id: Uuid,
        action: StoredMetadataMaintenanceTransactionActionV2,
        receipt: Option<MetadataMaintenanceOpaquePayload>,
        message: Option<String>,
        now_ms: i64,
    ) -> RepositoryResult<MetadataMaintenanceOperation> {
        let transaction_operation_id = OperationId::new_v7();
        let context = format!("terminal metadata maintenance operation {operation_id}");
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            transaction_operation_id,
            "terminal frontend metadata maintenance operation",
            |transaction| {
                let receipt = receipt.clone();
                let message = message.clone();
                Box::pin(async move {
                    apply_metadata_terminal(
                        transaction,
                        transaction_operation_id,
                        operation_id,
                        action,
                        receipt,
                        message,
                        now_ms,
                    )
                    .await
                })
            },
        )
        .await;
        self.resolve_metadata_result(
            result,
            transaction_operation_id,
            action,
            operation_id,
            &context,
        )
        .await
    }

    async fn resolve_metadata_result(
        &self,
        result: Result<
            novarocks_state_store::RunSuccess<RepositoryResult<MetadataMaintenanceOperation>>,
            RunFailure,
        >,
        transaction_operation_id: OperationId,
        expected_action: StoredMetadataMaintenanceTransactionActionV2,
        operation_id: Uuid,
        context: &str,
    ) -> RepositoryResult<MetadataMaintenanceOperation> {
        match result {
            Ok(success) => success.value,
            Err(RunFailure::CommitUnknown {
                transaction_id,
                error,
            }) => {
                self.recover_metadata_transaction(
                    transaction_id,
                    transaction_operation_id,
                    expected_action,
                    operation_id,
                    context,
                    error,
                )
                .await
            }
            Err(failure) => Err(format_run_failure(context, failure)),
        }
    }

    async fn recover_metadata_transaction(
        &self,
        transaction_id: TransactionId,
        transaction_operation_id: OperationId,
        expected_action: StoredMetadataMaintenanceTransactionActionV2,
        operation_id: Uuid,
        context: &str,
        commit_error: StateStoreError,
    ) -> RepositoryResult<MetadataMaintenanceOperation> {
        let resolution = self
            .store
            .resolve_commit(&transaction_id)
            .await
            .map_err(|error| {
                commit_unknown_error(
                    context,
                    transaction_id,
                    &commit_error,
                    &format!("commit resolution failed: {error}"),
                )
            })?;
        match resolution {
            CommitResolution::NotCommitted => {
                return Err(RepositoryError::store(format!(
                    "{context} transaction {} was not committed after commit-unknown: {commit_error}",
                    transaction_id.as_uuid()
                )));
            }
            CommitResolution::Unresolved => {
                return Err(commit_unknown_error(
                    context,
                    transaction_id,
                    &commit_error,
                    "commit resolution remains unresolved",
                ));
            }
            CommitResolution::Committed(receipt) if receipt.transaction_id != transaction_id => {
                return Err(RepositoryError::corruption(format!(
                    "{context} commit resolution returned a different transaction"
                )));
            }
            CommitResolution::Committed(_) => {}
        }
        let key = metadata_transaction_key(transaction_operation_id)?;
        let mut transaction = self.store.begin_read().await.map_err(|error| {
            RepositoryError::store(format!("{context}: begin recovery read failed: {error}"))
        })?;
        let marker_record = transaction
            .get(&key)
            .await
            .map_err(|error| {
                RepositoryError::store(format!("{context}: read recovery marker failed: {error}"))
            })?
            .ok_or_else(|| {
                RepositoryError::corruption(format!(
                    "{context}: committed transaction marker is absent"
                ))
            })?;
        let marker: StoredMetadataMaintenanceTransactionV2 = decode_metadata_json(
            marker_record.value.as_bytes(),
            "metadata maintenance transaction marker",
        )?;
        validate_metadata_transaction_marker(&marker)?;
        if marker.transaction_operation_id != *transaction_operation_id.as_uuid()
            || marker.operation_id != operation_id
            || marker.action != expected_action
        {
            return Err(RepositoryError::corruption(format!(
                "{context}: recovery marker does not match request"
            )));
        }
        let Some(current) = load_metadata_operation(transaction.as_mut(), operation_id)
            .await
            .map_err(|error| {
                RepositoryError::store(format!(
                    "{context}: recovery operation read failed: {error}"
                ))
            })??
        else {
            return Err(RepositoryError::corruption(format!(
                "{context}: recovery marker references missing operation"
            )));
        };
        transaction.abort().await.map_err(|error| {
            RepositoryError::store(format!("{context}: finish recovery read failed: {error}"))
        })?;
        if !metadata_operation_is_legal_successor(&marker.post_operation, &current.stored) {
            return Err(RepositoryError::corruption(format!(
                "{context}: current operation is not a legal marker successor"
            )));
        }
        Ok(MetadataMaintenanceOperation::from(&current.stored))
    }
}

struct VersionedStoredMetadataOperation {
    stored: StoredMetadataMaintenanceOperationV2,
    version: VersionToken,
}

async fn apply_metadata_create(
    transaction: &mut dyn WriteTransaction,
    transaction_operation_id: OperationId,
    request: MetadataMaintenanceOperationCreate,
) -> TransactionResult<MetadataMaintenanceOperation> {
    if let Err(error) = validate_metadata_create(&request) {
        return Ok(Err(error));
    }
    let existing = match load_metadata_operation(transaction, request.operation_id).await? {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    if let Some(existing) = existing {
        if existing.stored.target == StoredMaintenanceTargetV1::from(&request.target)
            && existing.stored.owner == request.owner
            && existing.stored.kind == request.kind
            && existing.stored.request_digest == request.request_digest
            && existing.stored.request_payload_digest == request.request_payload_digest
            && existing.stored.base_state_digest == request.base_state_digest
            && metadata_payload_matches(
                transaction,
                request.operation_id,
                StoredMetadataMaintenancePayloadKindV2::Request,
                request.request_payload_digest,
                &request.request_payload,
            )
            .await?
        {
            return Ok(Ok(MetadataMaintenanceOperation::from(&existing.stored)));
        }
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::InvalidTransition,
            format!(
                "metadata maintenance operation {} conflicts with its durable request",
                request.operation_id
            ),
        )));
    }
    let v1_active_key = match active_target_key(&request.target) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    if transaction.get(&v1_active_key).await?.is_some() {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::AlreadyActive,
            format!(
                "create metadata maintenance operation for {} failed: target has active optimize job",
                target_context(&request.target)
            ),
        )));
    }
    let shared_active_key = match shared_active_target_key(&request.target) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    if let Some(active) = transaction.get(&shared_active_key).await? {
        let fence: StoredSharedActiveFenceV3 =
            match decode_rewrite_json(active.value.as_bytes(), "shared maintenance active fence") {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::AlreadyActive,
            format!(
                "create metadata maintenance operation for {} failed: target has active {:?} maintenance operation {}",
                target_context(&request.target),
                fence.family,
                fence.operation_id
            ),
        )));
    }
    let active_key = match metadata_active_target_key(&request.target) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    if let Some(active) = transaction.get(&active_key).await? {
        let active_id = match decode_uuid_index_value(&active.value, "metadata active target") {
            Ok(id) => id,
            Err(error) => return Ok(Err(error)),
        };
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::AlreadyActive,
            format!(
                "create metadata maintenance operation for {} failed: target already has active operation {active_id}",
                target_context(&request.target)
            ),
        )));
    }
    let stored = StoredMetadataMaintenanceOperationV2 {
        schema_version: METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION,
        operation_id: request.operation_id,
        target: StoredMaintenanceTargetV1::from(&request.target),
        owner: request.owner,
        kind: request.kind,
        request_digest: request.request_digest,
        request_payload_digest: request.request_payload_digest,
        base_state_digest: request.base_state_digest,
        plan_digest: None,
        plan_summary: None,
        state: MetadataMaintenanceOperationState::Pending,
        error_message: None,
        created_at_ms: request.created_at_ms,
        started_at_ms: None,
        finished_at_ms: None,
    };
    let operation_key = match metadata_operation_key(request.operation_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let pending_key = match metadata_state_key(
        MetadataMaintenanceOperationState::Pending,
        request.operation_id,
    ) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let request_key = match metadata_payload_key(
        request.operation_id,
        StoredMetadataMaintenancePayloadKindV2::Request,
    ) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let operation_value = match encode_metadata_operation(&stored) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let request_value = match encode_metadata_payload(
        StoredMetadataMaintenancePayloadKindV2::Request,
        request.request_payload_digest,
        request.request_payload,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let index_value = match encode_uuid_index_value(request.operation_id) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let (marker_key, marker_value) = match metadata_transaction_record(
        transaction_operation_id,
        StoredMetadataMaintenanceTransactionActionV2::Create,
        &stored,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    transaction
        .put(operation_key, operation_value, Precondition::Absent)
        .await?;
    transaction
        .put(request_key, request_value, Precondition::Absent)
        .await?;
    transaction
        .put(pending_key, index_value.clone(), Precondition::Absent)
        .await?;
    transaction
        .put(active_key, index_value, Precondition::Absent)
        .await?;
    transaction
        .put(marker_key, marker_value, Precondition::Absent)
        .await?;
    Ok(Ok(MetadataMaintenanceOperation::from(&stored)))
}

async fn apply_metadata_start(
    transaction: &mut dyn WriteTransaction,
    transaction_operation_id: OperationId,
    operation_id: Uuid,
    plan: MetadataMaintenancePlanPayload,
    now_ms: i64,
) -> TransactionResult<MetadataMaintenanceOperation> {
    let loaded = match load_metadata_operation(transaction, operation_id).await? {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let Some(mut operation) = loaded else {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::NotFound,
            format!(
                "start metadata maintenance operation {operation_id} failed: operation not found"
            ),
        )));
    };
    if operation.stored.state == MetadataMaintenanceOperationState::Running
        && operation.stored.plan_digest == Some(plan.plan_digest)
        && metadata_payload_matches(
            transaction,
            operation_id,
            StoredMetadataMaintenancePayloadKindV2::Plan,
            plan.payload_digest,
            &plan.payload,
        )
        .await?
    {
        return Ok(Ok(MetadataMaintenanceOperation::from(&operation.stored)));
    }
    if operation.stored.state != MetadataMaintenanceOperationState::Pending {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::InvalidTransition,
            format!("start metadata maintenance operation {operation_id} failed: expected PENDING"),
        )));
    }
    if let Err(error) = require_metadata_state_and_active(
        transaction,
        &operation.stored,
        MetadataMaintenanceOperationState::Pending,
        "start metadata maintenance operation",
    )
    .await?
    {
        return Ok(Err(error));
    }
    operation.stored.state = MetadataMaintenanceOperationState::Running;
    operation.stored.plan_digest = Some(plan.plan_digest);
    operation.stored.plan_summary = Some(plan.summary);
    operation.stored.started_at_ms = Some(now_ms);
    let plan_key =
        match metadata_payload_key(operation_id, StoredMetadataMaintenancePayloadKindV2::Plan) {
            Ok(key) => key,
            Err(error) => return Ok(Err(error)),
        };
    let plan_value = match encode_metadata_payload(
        StoredMetadataMaintenancePayloadKindV2::Plan,
        plan.payload_digest,
        plan.payload,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    metadata_transition_state(
        transaction,
        transaction_operation_id,
        StoredMetadataMaintenanceTransactionActionV2::Start,
        operation,
        MetadataMaintenanceOperationState::Pending,
        MetadataMaintenanceOperationState::Running,
        Some((plan_key, plan_value)),
    )
    .await
}

async fn apply_metadata_reconcile_pending(
    transaction: &mut dyn WriteTransaction,
    transaction_operation_id: OperationId,
    operation_id: Uuid,
    evidence: MetadataMaintenanceOpaquePayload,
) -> TransactionResult<MetadataMaintenanceOperation> {
    let loaded = match load_metadata_operation(transaction, operation_id).await? {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let Some(mut operation) = loaded else {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::NotFound,
            format!(
                "record metadata maintenance evidence {operation_id} failed: operation not found"
            ),
        )));
    };
    if operation.stored.state == MetadataMaintenanceOperationState::ReconcilePending
        && metadata_payload_matches(
            transaction,
            operation_id,
            StoredMetadataMaintenancePayloadKindV2::Evidence,
            evidence.digest,
            &evidence.payload,
        )
        .await?
    {
        return Ok(Ok(MetadataMaintenanceOperation::from(&operation.stored)));
    }
    if operation.stored.state != MetadataMaintenanceOperationState::Running {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::InvalidTransition,
            format!("record metadata maintenance evidence {operation_id} failed: expected RUNNING"),
        )));
    }
    if let Err(error) = require_metadata_state_and_active(
        transaction,
        &operation.stored,
        MetadataMaintenanceOperationState::Running,
        "record metadata maintenance evidence",
    )
    .await?
    {
        return Ok(Err(error));
    }
    operation.stored.state = MetadataMaintenanceOperationState::ReconcilePending;
    let evidence_key = match metadata_payload_key(
        operation_id,
        StoredMetadataMaintenancePayloadKindV2::Evidence,
    ) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let evidence_value = match encode_metadata_payload(
        StoredMetadataMaintenancePayloadKindV2::Evidence,
        evidence.digest,
        evidence.payload,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    metadata_transition_state(
        transaction,
        transaction_operation_id,
        StoredMetadataMaintenanceTransactionActionV2::ReconcilePending,
        operation,
        MetadataMaintenanceOperationState::Running,
        MetadataMaintenanceOperationState::ReconcilePending,
        Some((evidence_key, evidence_value)),
    )
    .await
}

async fn apply_metadata_terminal(
    transaction: &mut dyn WriteTransaction,
    transaction_operation_id: OperationId,
    operation_id: Uuid,
    action: StoredMetadataMaintenanceTransactionActionV2,
    receipt: Option<MetadataMaintenanceOpaquePayload>,
    message: Option<String>,
    now_ms: i64,
) -> TransactionResult<MetadataMaintenanceOperation> {
    let loaded = match load_metadata_operation(transaction, operation_id).await? {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let Some(mut operation) = loaded else {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::NotFound,
            format!(
                "terminal metadata maintenance operation {operation_id} failed: operation not found"
            ),
        )));
    };
    let target_state = match action {
        StoredMetadataMaintenanceTransactionActionV2::Finish => {
            MetadataMaintenanceOperationState::Finished
        }
        StoredMetadataMaintenanceTransactionActionV2::Fail => {
            MetadataMaintenanceOperationState::Failed
        }
        _ => {
            return Ok(Err(RepositoryError::corruption(
                "metadata terminal transition has invalid action",
            )));
        }
    };
    if operation.stored.state == target_state {
        return Ok(Ok(MetadataMaintenanceOperation::from(&operation.stored)));
    }
    if !matches!(
        operation.stored.state,
        MetadataMaintenanceOperationState::Pending
            | MetadataMaintenanceOperationState::Running
            | MetadataMaintenanceOperationState::ReconcilePending
    ) {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::InvalidTransition,
            format!(
                "terminal metadata maintenance operation {operation_id} failed: operation is not active"
            ),
        )));
    }
    let prior = operation.stored.state;
    if let Err(error) = require_metadata_state_and_active(
        transaction,
        &operation.stored,
        prior,
        "terminal metadata maintenance operation",
    )
    .await?
    {
        return Ok(Err(error));
    }
    if target_state == MetadataMaintenanceOperationState::Finished && receipt.is_none() {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::InvalidTransition,
            "finish metadata maintenance operation failed: receipt is required",
        )));
    }
    operation.stored.state = target_state;
    operation.stored.error_message = message;
    operation.stored.finished_at_ms = Some(now_ms);
    let extra = match receipt {
        Some(receipt) => {
            let key = match metadata_payload_key(
                operation_id,
                StoredMetadataMaintenancePayloadKindV2::Receipt,
            ) {
                Ok(key) => key,
                Err(error) => return Ok(Err(error)),
            };
            let value = match encode_metadata_payload(
                StoredMetadataMaintenancePayloadKindV2::Receipt,
                receipt.digest,
                receipt.payload,
            ) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
            Some((key, value))
        }
        None => None,
    };
    metadata_transition_state(
        transaction,
        transaction_operation_id,
        action,
        operation,
        prior,
        target_state,
        extra,
    )
    .await
}

async fn apply_metadata_unresolved(
    transaction: &mut dyn WriteTransaction,
    transaction_operation_id: OperationId,
    operation_id: Uuid,
    message: String,
    now_ms: i64,
) -> TransactionResult<MetadataMaintenanceOperation> {
    let loaded = match load_metadata_operation(transaction, operation_id).await? {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let Some(mut operation) = loaded else {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::NotFound,
            format!(
                "mark metadata maintenance operation {operation_id} unresolved failed: operation not found"
            ),
        )));
    };
    if operation.stored.state == MetadataMaintenanceOperationState::Unresolved {
        return Ok(Ok(MetadataMaintenanceOperation::from(&operation.stored)));
    }
    if !matches!(
        operation.stored.state,
        MetadataMaintenanceOperationState::Running
            | MetadataMaintenanceOperationState::ReconcilePending
    ) {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::InvalidTransition,
            format!(
                "mark metadata maintenance operation {operation_id} unresolved failed: expected RUNNING or RECONCILE_PENDING"
            ),
        )));
    }
    let prior = operation.stored.state;
    if let Err(error) = require_metadata_state_and_active(
        transaction,
        &operation.stored,
        prior,
        "mark metadata maintenance operation unresolved",
    )
    .await?
    {
        return Ok(Err(error));
    }
    operation.stored.state = MetadataMaintenanceOperationState::Unresolved;
    operation.stored.error_message = Some(message);
    operation.stored.finished_at_ms = Some(now_ms);
    metadata_transition_state(
        transaction,
        transaction_operation_id,
        StoredMetadataMaintenanceTransactionActionV2::Unresolve,
        operation,
        prior,
        MetadataMaintenanceOperationState::Unresolved,
        None,
    )
    .await
}

async fn metadata_transition_state(
    transaction: &mut dyn WriteTransaction,
    transaction_operation_id: OperationId,
    action: StoredMetadataMaintenanceTransactionActionV2,
    operation: VersionedStoredMetadataOperation,
    prior: MetadataMaintenanceOperationState,
    next: MetadataMaintenanceOperationState,
    payload: Option<(Key, Value)>,
) -> TransactionResult<MetadataMaintenanceOperation> {
    let operation_id = operation.stored.operation_id;
    let operation_key = match metadata_operation_key(operation_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let old_state_key = match metadata_state_key(prior, operation_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let next_state_key = match metadata_state_key(next, operation_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let operation_value = match encode_metadata_operation(&operation.stored) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let index_value = match encode_uuid_index_value(operation_id) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let (marker_key, marker_value) =
        match metadata_transaction_record(transaction_operation_id, action, &operation.stored) {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };
    transaction
        .put(
            operation_key,
            operation_value,
            Precondition::Version(operation.version),
        )
        .await?;
    transaction
        .delete(old_state_key, Precondition::Present)
        .await?;
    transaction
        .put(next_state_key, index_value, Precondition::Absent)
        .await?;
    if let Some((key, value)) = payload {
        transaction.put(key, value, Precondition::Absent).await?;
    }
    if next.is_terminal() {
        let active_key = match metadata_active_target_key(&operation.stored.target.clone().into()) {
            Ok(key) => key,
            Err(error) => return Ok(Err(error)),
        };
        transaction
            .delete(active_key, Precondition::Present)
            .await?;
    }
    transaction
        .put(marker_key, marker_value, Precondition::Absent)
        .await?;
    Ok(Ok(MetadataMaintenanceOperation::from(&operation.stored)))
}

async fn require_metadata_state_and_active(
    transaction: &mut dyn WriteTransaction,
    operation: &StoredMetadataMaintenanceOperationV2,
    expected: MetadataMaintenanceOperationState,
    context: &str,
) -> TransactionResult<()> {
    let state_key = match metadata_state_key(expected, operation.operation_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let Some(index) = transaction.get(&state_key).await? else {
        return Ok(Err(RepositoryError::corruption(format!(
            "{context} failed: state index is missing"
        ))));
    };
    let indexed_operation_id =
        match decode_uuid_index_value(&index.value, "metadata maintenance state") {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };
    if indexed_operation_id != operation.operation_id {
        return Ok(Err(RepositoryError::corruption(format!(
            "{context} failed: state index references a different operation"
        ))));
    }
    let active_key = match metadata_active_target_key(&operation.target.clone().into()) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let Some(active) = transaction.get(&active_key).await? else {
        return Ok(Err(RepositoryError::corruption(format!(
            "{context} failed: active target index is missing"
        ))));
    };
    let active_operation_id = match decode_uuid_index_value(&active.value, "metadata active target")
    {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    if active_operation_id != operation.operation_id {
        return Ok(Err(RepositoryError::corruption(format!(
            "{context} failed: active target index references a different operation"
        ))));
    }
    Ok(Ok(()))
}

async fn load_metadata_operation(
    transaction: &mut dyn novarocks_spi::state_store::ReadTransaction,
    operation_id: Uuid,
) -> TransactionResult<Option<VersionedStoredMetadataOperation>> {
    let key = match metadata_operation_key(operation_id) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let Some(record) = transaction.get(&key).await? else {
        return Ok(Ok(None));
    };
    let version = record.version.clone();
    let stored = match decode_metadata_operation_record(record) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    Ok(Ok(Some(VersionedStoredMetadataOperation {
        stored,
        version,
    })))
}

async fn metadata_payload_matches(
    transaction: &mut dyn novarocks_spi::state_store::ReadTransaction,
    operation_id: Uuid,
    kind: StoredMetadataMaintenancePayloadKindV2,
    digest: [u8; 32],
    payload: &[u8],
) -> Result<bool, StateStoreError> {
    let key = metadata_payload_key(operation_id, kind).map_err(repository_error_as_store)?;
    let Some(record) = transaction.get(&key).await? else {
        return Ok(false);
    };
    let decoded = decode_metadata_payload_record(record).map_err(repository_error_as_store)?;
    Ok(decoded.digest == digest && decoded.payload == payload)
}

fn decode_metadata_operation_record(
    record: StateRecord,
) -> RepositoryResult<StoredMetadataMaintenanceOperationV2> {
    let key_operation_id = decode_uuid_index_key(
        METADATA_OPERATION_PREFIX,
        &record.key,
        "metadata maintenance operation",
    )?;
    let stored: StoredMetadataMaintenanceOperationV2 =
        decode_metadata_json(record.value.as_bytes(), "metadata maintenance operation")?;
    validate_metadata_operation(&stored)?;
    if stored.operation_id != key_operation_id {
        return Err(RepositoryError::corruption(
            "metadata maintenance operation identity mismatch",
        ));
    }
    Ok(stored)
}

fn decode_metadata_payload_record(
    record: StateRecord,
) -> RepositoryResult<StoredMetadataMaintenancePayloadV2> {
    let stored: StoredMetadataMaintenancePayloadV2 =
        decode_metadata_json(record.value.as_bytes(), "metadata maintenance payload")?;
    validate_stored_metadata_payload(&stored)?;
    Ok(stored)
}

fn validate_metadata_create(request: &MetadataMaintenanceOperationCreate) -> RepositoryResult<()> {
    validate_metadata_target(&request.target)?;
    validate_metadata_owner(&request.owner)?;
    validate_payload(
        &request.request_payload,
        request.request_payload_digest,
        "metadata maintenance request payload",
    )
}

fn validate_metadata_owner(owner: &MetadataMaintenanceExactOwner) -> RepositoryResult<()> {
    ConnectorInstanceId::parse(&owner.instance_id).map_err(|_| {
        RepositoryError::corruption(
            "metadata maintenance owner has an invalid connector instance ID",
        )
    })?;
    Ok(())
}

fn validate_metadata_target(target: &MaintenanceTarget) -> RepositoryResult<()> {
    if target.catalog.is_empty() || target.namespace.is_empty() || target.table.is_empty() {
        return Err(RepositoryError::corruption(
            "metadata maintenance target has an empty component",
        ));
    }
    if [
        target.catalog.as_str(),
        target.namespace.as_str(),
        target.table.as_str(),
    ]
    .iter()
    .any(|part| part.len() > 4096 || part.contains('\0'))
    {
        return Err(RepositoryError::corruption(
            "metadata maintenance target has an invalid component",
        ));
    }
    Ok(())
}

fn validate_payload(payload: &[u8], digest: [u8; 32], context: &str) -> RepositoryResult<()> {
    if payload.len() > METADATA_MAINTENANCE_MAX_PAYLOAD_BYTES {
        return Err(RepositoryError::new(
            RepositoryErrorKind::Store,
            format!(
                "{context} exceeds {} byte StateStore payload limit",
                METADATA_MAINTENANCE_MAX_PAYLOAD_BYTES
            ),
        ));
    }
    if metadata_maintenance_payload_digest(payload) != digest {
        return Err(RepositoryError::corruption(format!(
            "{context} digest does not match payload"
        )));
    }
    Ok(())
}

fn validate_metadata_error(message: &str) -> RepositoryResult<()> {
    if message.is_empty() || message.len() > 8 * 1024 || message.contains('\0') {
        return Err(RepositoryError::corruption(
            "metadata maintenance error message is invalid",
        ));
    }
    Ok(())
}

fn validate_metadata_operation(
    stored: &StoredMetadataMaintenanceOperationV2,
) -> RepositoryResult<()> {
    if stored.schema_version != METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION {
        return Err(RepositoryError::corruption(
            "metadata maintenance operation has unsupported schema version",
        ));
    }
    validate_metadata_target(&stored.target.clone().into())?;
    validate_metadata_owner(&stored.owner)?;
    match stored.state {
        MetadataMaintenanceOperationState::Pending => {
            if stored.plan_digest.is_some()
                || stored.started_at_ms.is_some()
                || stored.finished_at_ms.is_some()
                || stored.error_message.is_some()
            {
                return Err(RepositoryError::corruption(
                    "pending metadata maintenance operation has lifecycle fields",
                ));
            }
        }
        MetadataMaintenanceOperationState::Running
        | MetadataMaintenanceOperationState::ReconcilePending => {
            if stored.plan_digest.is_none()
                || stored.started_at_ms.is_none()
                || stored.finished_at_ms.is_some()
                || stored.error_message.is_some()
            {
                return Err(RepositoryError::corruption(
                    "active metadata maintenance operation has invalid lifecycle fields",
                ));
            }
        }
        MetadataMaintenanceOperationState::Finished => {
            if stored.plan_digest.is_none()
                || stored.started_at_ms.is_none()
                || stored.finished_at_ms.is_none()
                || stored.error_message.is_some()
            {
                return Err(RepositoryError::corruption(
                    "finished metadata maintenance operation has invalid lifecycle fields",
                ));
            }
        }
        MetadataMaintenanceOperationState::Failed => {
            if stored.finished_at_ms.is_none() || stored.error_message.is_none() {
                return Err(RepositoryError::corruption(
                    "failed metadata maintenance operation has invalid lifecycle fields",
                ));
            }
        }
        MetadataMaintenanceOperationState::Unresolved => {
            if stored.plan_digest.is_none()
                || stored.started_at_ms.is_none()
                || stored.finished_at_ms.is_none()
                || stored.error_message.is_none()
            {
                return Err(RepositoryError::corruption(
                    "unresolved metadata maintenance operation has invalid lifecycle fields",
                ));
            }
        }
    }
    Ok(())
}

fn encode_metadata_operation(
    stored: &StoredMetadataMaintenanceOperationV2,
) -> RepositoryResult<Value> {
    validate_metadata_operation(stored)?;
    encode_metadata_json(stored, "metadata maintenance operation")
}

fn encode_metadata_payload(
    kind: StoredMetadataMaintenancePayloadKindV2,
    digest: [u8; 32],
    payload: Vec<u8>,
) -> RepositoryResult<Value> {
    validate_payload(&payload, digest, "metadata maintenance payload")?;
    encode_metadata_json(
        &StoredMetadataMaintenancePayloadV2 {
            schema_version: METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION,
            kind,
            digest,
            payload,
        },
        "metadata maintenance payload",
    )
}

fn validate_stored_metadata_payload(
    stored: &StoredMetadataMaintenancePayloadV2,
) -> RepositoryResult<()> {
    if stored.schema_version != METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION {
        return Err(RepositoryError::corruption(
            "metadata maintenance payload has unsupported schema version",
        ));
    }
    validate_payload(
        &stored.payload,
        stored.digest,
        "metadata maintenance payload",
    )
}

fn metadata_transaction_record(
    transaction_operation_id: OperationId,
    action: StoredMetadataMaintenanceTransactionActionV2,
    post_operation: &StoredMetadataMaintenanceOperationV2,
) -> RepositoryResult<(Key, Value)> {
    let marker = StoredMetadataMaintenanceTransactionV2 {
        schema_version: METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION,
        transaction_operation_id: *transaction_operation_id.as_uuid(),
        action,
        operation_id: post_operation.operation_id,
        post_operation: post_operation.clone(),
    };
    Ok((
        metadata_transaction_key(transaction_operation_id)?,
        encode_metadata_json(&marker, "metadata maintenance transaction marker")?,
    ))
}

fn validate_metadata_transaction_marker(
    marker: &StoredMetadataMaintenanceTransactionV2,
) -> RepositoryResult<()> {
    if marker.schema_version != METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION
        || marker.operation_id != marker.post_operation.operation_id
    {
        return Err(RepositoryError::corruption(
            "metadata maintenance transaction marker is invalid",
        ));
    }
    validate_metadata_operation(&marker.post_operation)
}

fn metadata_operation_is_legal_successor(
    post: &StoredMetadataMaintenanceOperationV2,
    current: &StoredMetadataMaintenanceOperationV2,
) -> bool {
    post.operation_id == current.operation_id
        && post.target == current.target
        && post.owner == current.owner
        && post.kind == current.kind
        && post.request_digest == current.request_digest
        && post.request_payload_digest == current.request_payload_digest
        && post.base_state_digest == current.base_state_digest
        && post.created_at_ms == current.created_at_ms
        && (post == current || (post.state.holds_active_fence() && current.state.is_terminal()))
}

fn encode_metadata_json<T: Serialize>(value: &T, context: &str) -> RepositoryResult<Value> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        RepositoryError::corruption(format!("encode {context} failed: {error}"))
    })?;
    if bytes.len() > METADATA_MAINTENANCE_MAX_PAYLOAD_BYTES {
        return Err(RepositoryError::new(
            RepositoryErrorKind::Store,
            format!("encode {context} exceeds StateStore payload limit"),
        ));
    }
    Value::try_from(Bytes::from(bytes))
        .map_err(|error| RepositoryError::store(format!("encode {context} failed: {error}")))
}

fn decode_metadata_json<T>(bytes: &[u8], context: &str) -> RepositoryResult<T>
where
    T: DeserializeOwned + Serialize,
{
    let decoded: T = serde_json::from_slice(bytes).map_err(|error| {
        RepositoryError::corruption(format!("decode {context} failed: {error}"))
    })?;
    let canonical = serde_json::to_vec(&decoded).map_err(|error| {
        RepositoryError::corruption(format!("re-encode {context} failed: {error}"))
    })?;
    if canonical != bytes {
        return Err(RepositoryError::corruption(format!(
            "decode {context} failed: non-canonical JSON"
        )));
    }
    Ok(decoded)
}

fn metadata_operation_key(operation_id: Uuid) -> RepositoryResult<Key> {
    make_key(
        format!("{METADATA_OPERATION_PREFIX}{operation_id}"),
        "build metadata maintenance operation key",
    )
}

fn metadata_payload_key(
    operation_id: Uuid,
    kind: StoredMetadataMaintenancePayloadKindV2,
) -> RepositoryResult<Key> {
    let component = match kind {
        StoredMetadataMaintenancePayloadKindV2::Request => "request",
        StoredMetadataMaintenancePayloadKindV2::Plan => "plan",
        StoredMetadataMaintenancePayloadKindV2::Receipt => "receipt",
        StoredMetadataMaintenancePayloadKindV2::Evidence => "evidence",
    };
    make_key(
        format!("{METADATA_PAYLOAD_PREFIX}{operation_id}/{component}"),
        "build metadata maintenance payload key",
    )
}

fn metadata_state_prefix(state: MetadataMaintenanceOperationState) -> String {
    format!("{METADATA_STATE_PREFIX}{}/", state.as_key_component())
}

fn metadata_state_key(
    state: MetadataMaintenanceOperationState,
    operation_id: Uuid,
) -> RepositoryResult<Key> {
    make_key(
        format!("{}{operation_id}", metadata_state_prefix(state)),
        "build metadata maintenance state key",
    )
}

fn metadata_active_target_key(target: &MaintenanceTarget) -> RepositoryResult<Key> {
    make_key(
        format!(
            "{METADATA_ACTIVE_PREFIX}{}/{}/{}",
            hex::encode(target.catalog.as_bytes()),
            hex::encode(target.namespace.as_bytes()),
            hex::encode(target.table.as_bytes())
        ),
        "build metadata maintenance active target key",
    )
}

fn metadata_transaction_key(transaction_operation_id: OperationId) -> RepositoryResult<Key> {
    make_key(
        format!(
            "{METADATA_TRANSACTION_PREFIX}{}",
            transaction_operation_id.as_uuid()
        ),
        "build metadata maintenance transaction key",
    )
}

fn encode_uuid_index_value(value: Uuid) -> RepositoryResult<Value> {
    Value::try_from(Bytes::from(value.to_string())).map_err(|error| {
        RepositoryError::store(format!("encode metadata maintenance index failed: {error}"))
    })
}

fn decode_uuid_index_value(value: &Value, context: &str) -> RepositoryResult<Uuid> {
    let text = std::str::from_utf8(value.as_bytes())
        .map_err(|_| RepositoryError::corruption(format!("{context} index is not UTF-8")))?;
    if text.len() != 36 || text != text.to_ascii_lowercase() {
        return Err(RepositoryError::corruption(format!(
            "{context} index has non-canonical UUID"
        )));
    }
    Uuid::parse_str(text)
        .map_err(|_| RepositoryError::corruption(format!("{context} index has invalid UUID")))
}

// -------------------------------------------------------------------------
// V3 distributed rewrite durable repository.
//
// V3 intentionally has its own record namespace: changing the E1 v2 JSON
// shape would make a merged recovery format needlessly risky.  The shared
// fence below is the compatibility boundary between v1, v2, and v3.

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum SharedMaintenanceOperationFamilyV3 {
    Optimize,
    Metadata,
    DistributedRewrite,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredSharedActiveFenceV3 {
    schema_version: u8,
    family: SharedMaintenanceOperationFamilyV3,
    operation_id: Uuid,
}

#[derive(Clone)]
pub struct DistributedRewriteOperationRepository {
    store: Arc<dyn StateStore>,
    metrics: Arc<StateStoreMetrics>,
}

impl fmt::Debug for DistributedRewriteOperationRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DistributedRewriteOperationRepository")
            .field("provider", &self.metrics.provider())
            .finish_non_exhaustive()
    }
}

/// SHA-256 v1 digest for bounded StateStore payloads and external artifact
/// handles.  It is deliberately separate from provider and C1 digests.
pub fn distributed_rewrite_payload_digest(payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.distributed-rewrite.payload.v1\0");
    hasher.update((payload.len() as u64).to_be_bytes());
    hasher.update(payload);
    hasher.finalize().into()
}

impl DistributedRewriteOperationRepository {
    pub async fn open(store: Arc<dyn StateStore>) -> RepositoryResult<Self> {
        let repository = Self {
            metrics: Arc::new(StateStoreMetrics::new(store.metrics_snapshot().provider)),
            store,
        };
        repository.list().await?;
        Ok(repository)
    }

    pub async fn create(
        &self,
        request: DistributedRewriteOperationCreate,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        self.create_inner(request, None).await
    }

    /// Create the rewrite operation owned by an already-claimed v1 OPTIMIZE
    /// job. The v1 active-target index remains an external fence; this narrow
    /// path merely proves, in the same transaction, that it belongs to the
    /// running job that is creating its child rewrite operation.
    pub async fn create_for_claimed_optimize_job(
        &self,
        request: DistributedRewriteOperationCreate,
        claimed_optimize_job_id: i64,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        self.create_inner(request, Some(claimed_optimize_job_id))
            .await
    }

    async fn create_inner(
        &self,
        request: DistributedRewriteOperationCreate,
        claimed_optimize_job_id: Option<i64>,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        validate_rewrite_create(&request)?;
        let transaction_operation_id = OperationId::new_v7();
        let operation_id = request.operation_id;
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            transaction_operation_id,
            "create frontend distributed rewrite operation",
            |transaction| {
                let request = request.clone();
                Box::pin(async move {
                    apply_rewrite_create(
                        transaction,
                        transaction_operation_id,
                        request,
                        claimed_optimize_job_id,
                    )
                    .await
                })
            },
        )
        .await;
        self.resolve_rewrite_result(
            result,
            transaction_operation_id,
            StoredDistributedRewriteTransactionActionV3::Create,
            operation_id,
            "create distributed rewrite operation",
        )
        .await
    }

    pub async fn plan(
        &self,
        operation_id: Uuid,
        plan: DistributedRewritePlanPayload,
        now_ms: i64,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        validate_rewrite_payload(
            &plan.payload,
            plan.payload_digest,
            "distributed rewrite plan payload",
        )?;
        if plan.cohort_count as usize
            > novarocks_spi::connector::MAX_CONNECTOR_DISTRIBUTED_REWRITE_COHORTS
        {
            return Err(RepositoryError::new(
                RepositoryErrorKind::Store,
                "distributed rewrite plan exceeds cohort limit",
            ));
        }
        self.rewrite_transition(
            operation_id,
            StoredDistributedRewriteTransactionActionV3::Plan,
            &[DistributedRewriteOperationState::Pending],
            DistributedRewriteOperationState::Planned,
            Some(RewriteTransitionPayload::Plan(plan)),
            None,
            now_ms,
        )
        .await
    }

    pub async fn start_staging(
        &self,
        operation_id: Uuid,
        now_ms: i64,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        self.rewrite_transition(
            operation_id,
            StoredDistributedRewriteTransactionActionV3::StartStaging,
            &[DistributedRewriteOperationState::Planned],
            DistributedRewriteOperationState::Staging,
            None,
            None,
            now_ms,
        )
        .await
    }

    pub async fn checkpoint_attempt(
        &self,
        operation_id: Uuid,
        checkpoint: DistributedRewriteAttemptCheckpoint,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        validate_rewrite_checkpoint(&checkpoint)?;
        self.rewrite_transition(
            operation_id,
            StoredDistributedRewriteTransactionActionV3::Checkpoint,
            &[DistributedRewriteOperationState::Staging],
            DistributedRewriteOperationState::Staging,
            None,
            Some(checkpoint),
            0,
        )
        .await
    }

    pub async fn mark_abort_pending(
        &self,
        operation_id: Uuid,
        now_ms: i64,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        self.rewrite_transition(
            operation_id,
            StoredDistributedRewriteTransactionActionV3::AbortPending,
            &[
                DistributedRewriteOperationState::Planned,
                DistributedRewriteOperationState::Staging,
                DistributedRewriteOperationState::CommitPending,
                DistributedRewriteOperationState::ReconcilePending,
            ],
            DistributedRewriteOperationState::AbortPending,
            None,
            None,
            now_ms,
        )
        .await
    }

    pub async fn mark_commit_pending(
        &self,
        operation_id: Uuid,
        now_ms: i64,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        self.rewrite_transition(
            operation_id,
            StoredDistributedRewriteTransactionActionV3::CommitPending,
            &[DistributedRewriteOperationState::Staging],
            DistributedRewriteOperationState::CommitPending,
            None,
            None,
            now_ms,
        )
        .await
    }

    pub async fn mark_reconcile_pending(
        &self,
        operation_id: Uuid,
        evidence: DistributedRewriteOpaquePayload,
        now_ms: i64,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        validate_rewrite_payload(
            &evidence.payload,
            evidence.digest,
            "distributed rewrite evidence",
        )?;
        self.rewrite_transition(
            operation_id,
            StoredDistributedRewriteTransactionActionV3::ReconcilePending,
            &[DistributedRewriteOperationState::CommitPending],
            DistributedRewriteOperationState::ReconcilePending,
            Some(RewriteTransitionPayload::Evidence(evidence)),
            None,
            now_ms,
        )
        .await
    }

    pub async fn finish(
        &self,
        operation_id: Uuid,
        receipt: DistributedRewriteOpaquePayload,
        now_ms: i64,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        validate_rewrite_payload(
            &receipt.payload,
            receipt.digest,
            "distributed rewrite receipt",
        )?;
        self.rewrite_transition(
            operation_id,
            StoredDistributedRewriteTransactionActionV3::Finish,
            &[
                DistributedRewriteOperationState::Planned,
                DistributedRewriteOperationState::AbortPending,
                DistributedRewriteOperationState::CommitPending,
                DistributedRewriteOperationState::ReconcilePending,
            ],
            DistributedRewriteOperationState::Finished,
            Some(RewriteTransitionPayload::Receipt(receipt)),
            None,
            now_ms,
        )
        .await
    }

    pub async fn fail(
        &self,
        operation_id: Uuid,
        message: String,
        now_ms: i64,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        validate_rewrite_error(&message)?;
        self.rewrite_transition(
            operation_id,
            StoredDistributedRewriteTransactionActionV3::Fail,
            &[
                DistributedRewriteOperationState::Pending,
                DistributedRewriteOperationState::Planned,
                DistributedRewriteOperationState::Staging,
                DistributedRewriteOperationState::AbortPending,
            ],
            DistributedRewriteOperationState::Failed,
            Some(RewriteTransitionPayload::Error(message)),
            None,
            now_ms,
        )
        .await
    }

    pub async fn mark_unresolved(
        &self,
        operation_id: Uuid,
        message: String,
        now_ms: i64,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        validate_rewrite_error(&message)?;
        self.rewrite_transition(
            operation_id,
            StoredDistributedRewriteTransactionActionV3::Unresolve,
            &[
                DistributedRewriteOperationState::Staging,
                DistributedRewriteOperationState::CommitPending,
                DistributedRewriteOperationState::ReconcilePending,
                DistributedRewriteOperationState::AbortPending,
            ],
            DistributedRewriteOperationState::Unresolved,
            Some(RewriteTransitionPayload::Error(message)),
            None,
            now_ms,
        )
        .await
    }

    pub async fn get(
        &self,
        operation_id: Uuid,
    ) -> RepositoryResult<Option<DistributedRewriteOperation>> {
        let mut transaction = self.store.begin_read().await.map_err(|e| {
            RepositoryError::store(format!("begin distributed rewrite read failed: {e}"))
        })?;
        let result = load_rewrite_operation(transaction.as_mut(), operation_id)
            .await
            .map_err(|e| {
                RepositoryError::store(format!("read distributed rewrite operation failed: {e}"))
            })??;
        transaction.abort().await.map_err(|e| {
            RepositoryError::store(format!("finish distributed rewrite read failed: {e}"))
        })?;
        Ok(result.map(|item| DistributedRewriteOperation::from(&item.stored)))
    }

    pub async fn list(&self) -> RepositoryResult<Vec<DistributedRewriteOperation>> {
        self.list_with_prefix(DISTRIBUTED_REWRITE_OPERATION_PREFIX, None)
            .await
    }

    pub async fn list_recovery_candidates(
        &self,
    ) -> RepositoryResult<Vec<DistributedRewriteOperation>> {
        let mut result = Vec::new();
        for state in [
            DistributedRewriteOperationState::Staging,
            DistributedRewriteOperationState::AbortPending,
            DistributedRewriteOperationState::CommitPending,
            DistributedRewriteOperationState::ReconcilePending,
        ] {
            result.extend(
                self.list_with_prefix(&rewrite_state_prefix(state), Some(state))
                    .await?,
            );
        }
        result.sort_by_key(|operation| operation.operation_id);
        Ok(result)
    }

    pub async fn load_plan(
        &self,
        operation_id: Uuid,
    ) -> RepositoryResult<Option<DistributedRewritePlanPayload>> {
        let Some(payload) = self
            .load_payload(operation_id, StoredDistributedRewritePayloadKindV3::Plan)
            .await?
        else {
            return Ok(None);
        };
        let Some(operation) = self.get(operation_id).await? else {
            return Ok(None);
        };
        Ok(Some(DistributedRewritePlanPayload {
            plan_digest: operation.plan_digest.ok_or_else(|| {
                RepositoryError::corruption("distributed rewrite plan has no digest")
            })?,
            manifest_digest: operation.manifest_digest.ok_or_else(|| {
                RepositoryError::corruption("distributed rewrite plan has no manifest digest")
            })?,
            cohort_set_digest: operation.cohort_set_digest.ok_or_else(|| {
                RepositoryError::corruption("distributed rewrite plan has no cohort set digest")
            })?,
            payload_digest: payload.digest,
            payload: payload.payload,
            cohort_count: operation.cohort_count.ok_or_else(|| {
                RepositoryError::corruption("distributed rewrite plan has no cohort count")
            })?,
        }))
    }

    pub async fn load_attempts(
        &self,
        operation_id: Uuid,
    ) -> RepositoryResult<Vec<DistributedRewriteAttemptCheckpoint>> {
        let prefix = make_key(
            format!("{DISTRIBUTED_REWRITE_ATTEMPT_PREFIX}{operation_id}/"),
            "build distributed rewrite attempt range",
        )?;
        let range = KeyRange::for_prefix(prefix).map_err(|e| {
            RepositoryError::store(format!(
                "build distributed rewrite attempt range failed: {e}"
            ))
        })?;
        let mut transaction = self.store.begin_read().await.map_err(|e| {
            RepositoryError::store(format!(
                "begin distributed rewrite attempt list failed: {e}"
            ))
        })?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size: self.store.limits().max_page_size,
            continuation: None,
        };
        let mut checkpoints = Vec::new();
        loop {
            let page = transaction.range(&request).await.map_err(|e| {
                RepositoryError::store(format!("list distributed rewrite attempts failed: {e}"))
            })?;
            for record in page.records {
                checkpoints.push(decode_rewrite_attempt(record)?.into_checkpoint());
            }
            let Some(next) = page.continuation else { break };
            request.continuation = Some(next);
        }
        transaction.abort().await.map_err(|e| {
            RepositoryError::store(format!(
                "finish distributed rewrite attempt list failed: {e}"
            ))
        })?;
        checkpoints
            .sort_by(|a, b| (a.cohort_id, &a.execution_id).cmp(&(b.cohort_id, &b.execution_id)));
        Ok(checkpoints)
    }

    async fn load_payload(
        &self,
        operation_id: Uuid,
        kind: StoredDistributedRewritePayloadKindV3,
    ) -> RepositoryResult<Option<DistributedRewriteOpaquePayload>> {
        let key = rewrite_payload_key(operation_id, kind)?;
        let mut transaction = self.store.begin_read().await.map_err(|e| {
            RepositoryError::store(format!(
                "begin distributed rewrite payload read failed: {e}"
            ))
        })?;
        let record = transaction.get(&key).await.map_err(|e| {
            RepositoryError::store(format!("read distributed rewrite payload failed: {e}"))
        })?;
        transaction.abort().await.map_err(|e| {
            RepositoryError::store(format!(
                "finish distributed rewrite payload read failed: {e}"
            ))
        })?;
        record.map(decode_rewrite_payload).transpose().map(|item| {
            item.map(|payload| DistributedRewriteOpaquePayload {
                digest: payload.digest,
                payload: payload.payload,
            })
        })
    }

    async fn list_with_prefix(
        &self,
        prefix_text: &str,
        expected_state: Option<DistributedRewriteOperationState>,
    ) -> RepositoryResult<Vec<DistributedRewriteOperation>> {
        let prefix = make_key(prefix_text, "build distributed rewrite range")?;
        let range = KeyRange::for_prefix(prefix).map_err(|e| {
            RepositoryError::store(format!("build distributed rewrite range failed: {e}"))
        })?;
        let mut transaction = self.store.begin_read().await.map_err(|e| {
            RepositoryError::store(format!("begin distributed rewrite list failed: {e}"))
        })?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size: self.store.limits().max_page_size,
            continuation: None,
        };
        let mut result = Vec::new();
        loop {
            let page = transaction.range(&request).await.map_err(|e| {
                RepositoryError::store(format!("list distributed rewrite operations failed: {e}"))
            })?;
            for record in page.records {
                let stored = if expected_state.is_some() {
                    let id = decode_uuid_index_key(
                        prefix_text,
                        &record.key,
                        "distributed rewrite state",
                    )?;
                    let id_value =
                        decode_uuid_index_value(&record.value, "distributed rewrite state")?;
                    if id != id_value {
                        return Err(RepositoryError::corruption(
                            "distributed rewrite state index identity mismatch",
                        ));
                    }
                    load_rewrite_operation(transaction.as_mut(), id)
                        .await
                        .map_err(|e| {
                            RepositoryError::store(format!(
                                "load distributed rewrite state operation failed: {e}"
                            ))
                        })??
                        .ok_or_else(|| {
                            RepositoryError::corruption(
                                "distributed rewrite state index references missing operation",
                            )
                        })?
                        .stored
                } else {
                    decode_rewrite_operation(record)?
                };
                if expected_state.is_some_and(|state| stored.state != state) {
                    return Err(RepositoryError::corruption(
                        "distributed rewrite state index references wrong state",
                    ));
                }
                result.push(DistributedRewriteOperation::from(&stored));
            }
            let Some(next) = page.continuation else { break };
            request.continuation = Some(next);
        }
        transaction.abort().await.map_err(|e| {
            RepositoryError::store(format!("finish distributed rewrite list failed: {e}"))
        })?;
        result.sort_by_key(|operation| operation.operation_id);
        Ok(result)
    }

    async fn rewrite_transition(
        &self,
        operation_id: Uuid,
        action: StoredDistributedRewriteTransactionActionV3,
        allowed: &[DistributedRewriteOperationState],
        next: DistributedRewriteOperationState,
        payload: Option<RewriteTransitionPayload>,
        checkpoint: Option<DistributedRewriteAttemptCheckpoint>,
        now_ms: i64,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        let transaction_operation_id = OperationId::new_v7();
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            transaction_operation_id,
            "transition frontend distributed rewrite operation",
            |transaction| {
                let payload = payload.clone();
                let checkpoint = checkpoint.clone();
                let allowed = allowed.to_vec();
                Box::pin(async move {
                    apply_rewrite_transition(
                        transaction,
                        transaction_operation_id,
                        operation_id,
                        action,
                        &allowed,
                        next,
                        payload,
                        checkpoint,
                        now_ms,
                    )
                    .await
                })
            },
        )
        .await;
        self.resolve_rewrite_result(
            result,
            transaction_operation_id,
            action,
            operation_id,
            "transition distributed rewrite operation",
        )
        .await
    }

    async fn resolve_rewrite_result(
        &self,
        result: Result<
            novarocks_state_store::RunSuccess<RepositoryResult<DistributedRewriteOperation>>,
            RunFailure,
        >,
        _transaction_operation_id: OperationId,
        _action: StoredDistributedRewriteTransactionActionV3,
        _operation_id: Uuid,
        context: &str,
    ) -> RepositoryResult<DistributedRewriteOperation> {
        match result {
            Ok(success) => success.value,
            Err(RunFailure::CommitUnknown { error, .. }) => Err(RepositoryError::new(
                RepositoryErrorKind::CommitUnknown,
                format!("{context} commit outcome is unknown: {error}"),
            )),
            Err(failure) => Err(format_run_failure(context, failure)),
        }
    }
}

#[derive(Clone)]
enum RewriteTransitionPayload {
    Plan(DistributedRewritePlanPayload),
    Evidence(DistributedRewriteOpaquePayload),
    Receipt(DistributedRewriteOpaquePayload),
    Error(String),
}

struct VersionedStoredRewriteOperation {
    stored: StoredDistributedRewriteOperationV3,
    version: VersionToken,
}

impl StoredDistributedRewriteAttemptV3 {
    fn into_checkpoint(self) -> DistributedRewriteAttemptCheckpoint {
        DistributedRewriteAttemptCheckpoint {
            cohort_id: self.cohort_id,
            execution_id: ConnectorWriteExecutionId::new(
                self.execution_query_id,
                self.execution_attempt_id,
            ),
            disposition: self.disposition,
            attempt_digest: self.attempt_digest,
            artifact_digest: self.artifact_digest,
            artifact_handle: self.artifact_handle,
            checkpoint_digest: self.checkpoint_digest,
        }
    }
}

async fn apply_rewrite_create(
    transaction: &mut dyn WriteTransaction,
    transaction_operation_id: OperationId,
    request: DistributedRewriteOperationCreate,
    claimed_optimize_job_id: Option<i64>,
) -> TransactionResult<DistributedRewriteOperation> {
    if let Err(error) = validate_rewrite_create(&request) {
        return Ok(Err(error));
    }
    let existing = match load_rewrite_operation(transaction, request.operation_id).await? {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    if let Some(existing) = existing {
        if existing.stored.target == StoredMaintenanceTargetV1::from(&request.target)
            && existing.stored.owner == request.owner
            && existing.stored.kind == request.kind
            && existing.stored.request_digest == request.request_digest
            && existing.stored.base_state_digest == request.base_state_digest
            && existing.stored.request_payload_digest == request.request_payload_digest
            && rewrite_payload_matches(
                transaction,
                request.operation_id,
                StoredDistributedRewritePayloadKindV3::Request,
                request.request_payload_digest,
                &request.request_payload,
            )
            .await?
        {
            return Ok(Ok(DistributedRewriteOperation::from(&existing.stored)));
        }
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::InvalidTransition,
            "distributed rewrite operation conflicts with its durable request",
        )));
    }
    let active_key = active_target_key(&request.target)?;
    if let Some(active) = transaction.get(&active_key).await? {
        let active_job_id =
            match decode_index_value(&active.value) {
                Ok(job_id) => job_id,
                Err(error) => return Ok(Err(error.with_context(
                    "create distributed rewrite operation failed: decode active optimize job index",
                ))),
            };
        if claimed_optimize_job_id != Some(active_job_id) {
            return Ok(Err(RepositoryError::new(
                RepositoryErrorKind::AlreadyActive,
                "distributed rewrite target has active optimize job",
            )));
        }
        let active_job = match load_job_from_transaction(transaction, active_job_id).await? {
            Ok(Some(job)) => job.stored,
            Ok(None) => {
                return Ok(Err(RepositoryError::corruption(format!(
                    "create distributed rewrite operation failed: active optimize job {active_job_id} is missing"
                ))));
            }
            Err(error) => return Ok(Err(error)),
        };
        if active_job.target != StoredMaintenanceTargetV1::from(&request.target)
            || active_job.state != StoredOptimizeJobStateV1::Running
        {
            return Ok(Err(RepositoryError::corruption(format!(
                "create distributed rewrite operation failed: active optimize job {active_job_id} is not the claimed running target"
            ))));
        }
    } else if let Some(claimed_job_id) = claimed_optimize_job_id {
        return Ok(Err(RepositoryError::corruption(format!(
            "create distributed rewrite operation failed: claimed optimize job {claimed_job_id} has no active target index"
        ))));
    }
    if transaction
        .get(&metadata_active_target_key(&request.target)?)
        .await?
        .is_some()
    {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::AlreadyActive,
            "distributed rewrite target has active metadata maintenance operation",
        )));
    }
    let shared_key = shared_active_target_key(&request.target)?;
    if transaction.get(&shared_key).await?.is_some() {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::AlreadyActive,
            "distributed rewrite target has active shared maintenance operation",
        )));
    }
    let stored = StoredDistributedRewriteOperationV3 {
        schema_version: DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION,
        operation_id: request.operation_id,
        target: StoredMaintenanceTargetV1::from(&request.target),
        owner: request.owner,
        kind: request.kind,
        request_digest: request.request_digest,
        base_state_digest: request.base_state_digest,
        request_payload_digest: request.request_payload_digest,
        plan_digest: None,
        manifest_digest: None,
        cohort_set_digest: None,
        cohort_count: None,
        state: DistributedRewriteOperationState::Pending,
        error_message: None,
        created_at_ms: request.created_at_ms,
        started_at_ms: None,
        finished_at_ms: None,
    };
    let operation_key = rewrite_operation_key(request.operation_id)?;
    let pending_key = rewrite_state_key(
        DistributedRewriteOperationState::Pending,
        request.operation_id,
    )?;
    let request_key = rewrite_payload_key(
        request.operation_id,
        StoredDistributedRewritePayloadKindV3::Request,
    )?;
    let index_value = encode_uuid_index_value(request.operation_id)?;
    let shared_value = encode_rewrite_json(
        &StoredSharedActiveFenceV3 {
            schema_version: DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION,
            family: SharedMaintenanceOperationFamilyV3::DistributedRewrite,
            operation_id: request.operation_id,
        },
        "shared maintenance active fence",
    )?;
    let (marker_key, marker_value) = rewrite_transaction_record(
        transaction_operation_id,
        StoredDistributedRewriteTransactionActionV3::Create,
        &stored,
    )?;
    transaction
        .put(
            operation_key,
            encode_rewrite_operation(&stored)?,
            Precondition::Absent,
        )
        .await?;
    transaction
        .put(
            request_key,
            encode_rewrite_payload(
                StoredDistributedRewritePayloadKindV3::Request,
                request.request_payload_digest,
                request.request_payload,
            )?,
            Precondition::Absent,
        )
        .await?;
    transaction
        .put(pending_key, index_value, Precondition::Absent)
        .await?;
    transaction
        .put(shared_key, shared_value, Precondition::Absent)
        .await?;
    transaction
        .put(marker_key, marker_value, Precondition::Absent)
        .await?;
    Ok(Ok(DistributedRewriteOperation::from(&stored)))
}

async fn apply_rewrite_transition(
    transaction: &mut dyn WriteTransaction,
    transaction_operation_id: OperationId,
    operation_id: Uuid,
    action: StoredDistributedRewriteTransactionActionV3,
    allowed: &[DistributedRewriteOperationState],
    next: DistributedRewriteOperationState,
    payload: Option<RewriteTransitionPayload>,
    checkpoint: Option<DistributedRewriteAttemptCheckpoint>,
    now_ms: i64,
) -> TransactionResult<DistributedRewriteOperation> {
    let loaded = match load_rewrite_operation(transaction, operation_id).await? {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let Some(mut operation) = loaded else {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::NotFound,
            format!("distributed rewrite operation {operation_id} not found"),
        )));
    };
    if operation.stored.state == next {
        if let Some(RewriteTransitionPayload::Plan(plan)) = &payload {
            if operation.stored.plan_digest == Some(plan.plan_digest)
                && rewrite_payload_matches(
                    transaction,
                    operation_id,
                    StoredDistributedRewritePayloadKindV3::Plan,
                    plan.payload_digest,
                    &plan.payload,
                )
                .await?
            {
                return Ok(Ok(DistributedRewriteOperation::from(&operation.stored)));
            }
        } else if checkpoint.is_none() {
            return Ok(Ok(DistributedRewriteOperation::from(&operation.stored)));
        }
    }
    if !allowed.contains(&operation.stored.state) {
        return Ok(Err(RepositoryError::new(
            RepositoryErrorKind::InvalidTransition,
            format!(
                "distributed rewrite operation {operation_id} cannot transition from {:?}",
                operation.stored.state
            ),
        )));
    }
    let prior = operation.stored.state;
    if let Err(error) = require_rewrite_state_and_active(
        transaction,
        &operation.stored,
        prior,
        "transition distributed rewrite operation",
    )
    .await?
    {
        return Ok(Err(error));
    }
    let mut extra = None;
    match payload {
        Some(RewriteTransitionPayload::Plan(plan)) => {
            operation.stored.plan_digest = Some(plan.plan_digest);
            operation.stored.manifest_digest = Some(plan.manifest_digest);
            operation.stored.cohort_set_digest = Some(plan.cohort_set_digest);
            operation.stored.cohort_count = Some(plan.cohort_count);
            operation.stored.started_at_ms = Some(now_ms);
            extra = Some((
                rewrite_payload_key(operation_id, StoredDistributedRewritePayloadKindV3::Plan)?,
                encode_rewrite_payload(
                    StoredDistributedRewritePayloadKindV3::Plan,
                    plan.payload_digest,
                    plan.payload,
                )?,
            ));
        }
        Some(RewriteTransitionPayload::Evidence(evidence)) => {
            extra = Some((
                rewrite_payload_key(
                    operation_id,
                    StoredDistributedRewritePayloadKindV3::Evidence,
                )?,
                encode_rewrite_payload(
                    StoredDistributedRewritePayloadKindV3::Evidence,
                    evidence.digest,
                    evidence.payload,
                )?,
            ))
        }
        Some(RewriteTransitionPayload::Receipt(receipt)) => {
            extra = Some((
                rewrite_payload_key(operation_id, StoredDistributedRewritePayloadKindV3::Receipt)?,
                encode_rewrite_payload(
                    StoredDistributedRewritePayloadKindV3::Receipt,
                    receipt.digest,
                    receipt.payload,
                )?,
            ))
        }
        Some(RewriteTransitionPayload::Error(message)) => {
            operation.stored.error_message = Some(message);
            operation.stored.finished_at_ms = Some(now_ms);
        }
        None => {}
    }
    if next == DistributedRewriteOperationState::Finished {
        operation.stored.finished_at_ms = Some(now_ms);
    }
    if next == DistributedRewriteOperationState::AbortPending
        || next == DistributedRewriteOperationState::CommitPending
        || next == DistributedRewriteOperationState::ReconcilePending
        || next == DistributedRewriteOperationState::Staging
    {
        if operation.stored.started_at_ms.is_none() {
            return Ok(Err(RepositoryError::corruption(
                "active distributed rewrite operation has no durable plan",
            )));
        }
    }
    operation.stored.state = next;
    rewrite_transition_state(
        transaction,
        transaction_operation_id,
        action,
        operation,
        prior,
        next,
        extra,
        checkpoint,
    )
    .await
}

async fn rewrite_transition_state(
    transaction: &mut dyn WriteTransaction,
    transaction_operation_id: OperationId,
    action: StoredDistributedRewriteTransactionActionV3,
    operation: VersionedStoredRewriteOperation,
    prior: DistributedRewriteOperationState,
    next: DistributedRewriteOperationState,
    payload: Option<(Key, Value)>,
    checkpoint: Option<DistributedRewriteAttemptCheckpoint>,
) -> TransactionResult<DistributedRewriteOperation> {
    let operation_id = operation.stored.operation_id;
    let operation_key = rewrite_operation_key(operation_id)?;
    let old_state_key = rewrite_state_key(prior, operation_id)?;
    let next_state_key = rewrite_state_key(next, operation_id)?;
    let (marker_key, marker_value) =
        rewrite_transaction_record(transaction_operation_id, action, &operation.stored)?;
    transaction
        .put(
            operation_key,
            encode_rewrite_operation(&operation.stored)?,
            Precondition::Version(operation.version),
        )
        .await?;
    if prior != next {
        transaction
            .delete(old_state_key, Precondition::Present)
            .await?;
        transaction
            .put(
                next_state_key,
                encode_uuid_index_value(operation_id)?,
                Precondition::Absent,
            )
            .await?;
    }
    if let Some((key, value)) = payload {
        transaction.put(key, value, Precondition::Absent).await?;
    }
    if let Some(checkpoint) = checkpoint {
        let key = rewrite_attempt_key(operation_id, checkpoint.cohort_id, checkpoint.execution_id)?;
        let stored = StoredDistributedRewriteAttemptV3 {
            schema_version: DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION,
            operation_id,
            cohort_id: checkpoint.cohort_id,
            execution_query_id: checkpoint.execution_id.query_id(),
            execution_attempt_id: checkpoint.execution_id.attempt_id(),
            disposition: checkpoint.disposition,
            attempt_digest: checkpoint.attempt_digest,
            artifact_digest: checkpoint.artifact_digest,
            artifact_handle: checkpoint.artifact_handle,
            checkpoint_digest: checkpoint.checkpoint_digest,
        };
        transaction
            .put(
                key,
                encode_rewrite_json(&stored, "distributed rewrite attempt")?,
                Precondition::Absent,
            )
            .await?;
    }
    if next.is_terminal() {
        transaction
            .delete(
                shared_active_target_key(&operation.stored.target.clone().into())?,
                Precondition::Present,
            )
            .await?;
    }
    transaction
        .put(marker_key, marker_value, Precondition::Absent)
        .await?;
    Ok(Ok(DistributedRewriteOperation::from(&operation.stored)))
}

async fn load_rewrite_operation(
    transaction: &mut dyn novarocks_spi::state_store::ReadTransaction,
    operation_id: Uuid,
) -> TransactionResult<Option<VersionedStoredRewriteOperation>> {
    let key = rewrite_operation_key(operation_id)?;
    let Some(record) = transaction.get(&key).await? else {
        return Ok(Ok(None));
    };
    let version = record.version.clone();
    let stored = decode_rewrite_operation(record)?;
    Ok(Ok(Some(VersionedStoredRewriteOperation {
        stored,
        version,
    })))
}

async fn require_rewrite_state_and_active(
    transaction: &mut dyn WriteTransaction,
    operation: &StoredDistributedRewriteOperationV3,
    expected: DistributedRewriteOperationState,
    context: &str,
) -> TransactionResult<()> {
    let key = rewrite_state_key(expected, operation.operation_id)?;
    let Some(index) = transaction.get(&key).await? else {
        return Ok(Err(RepositoryError::corruption(format!(
            "{context}: state index missing"
        ))));
    };
    if decode_uuid_index_value(&index.value, "distributed rewrite state")? != operation.operation_id
    {
        return Ok(Err(RepositoryError::corruption(format!(
            "{context}: state index mismatch"
        ))));
    }
    let active_key = shared_active_target_key(&operation.target.clone().into())?;
    let Some(active) = transaction.get(&active_key).await? else {
        return Ok(Err(RepositoryError::corruption(format!(
            "{context}: shared active fence missing"
        ))));
    };
    let decoded: StoredSharedActiveFenceV3 =
        decode_rewrite_json(active.value.as_bytes(), "shared maintenance active fence")?;
    if decoded.schema_version != DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION
        || decoded.family != SharedMaintenanceOperationFamilyV3::DistributedRewrite
        || decoded.operation_id != operation.operation_id
    {
        return Ok(Err(RepositoryError::corruption(format!(
            "{context}: shared active fence mismatch"
        ))));
    }
    Ok(Ok(()))
}

async fn rewrite_payload_matches(
    transaction: &mut dyn novarocks_spi::state_store::ReadTransaction,
    operation_id: Uuid,
    kind: StoredDistributedRewritePayloadKindV3,
    digest: [u8; 32],
    payload: &[u8],
) -> Result<bool, StateStoreError> {
    let key = rewrite_payload_key(operation_id, kind).map_err(repository_error_as_store)?;
    let Some(record) = transaction.get(&key).await? else {
        return Ok(false);
    };
    let stored = decode_rewrite_payload(record).map_err(repository_error_as_store)?;
    Ok(stored.digest == digest && stored.payload == payload)
}

fn validate_rewrite_create(request: &DistributedRewriteOperationCreate) -> RepositoryResult<()> {
    validate_metadata_target(&request.target)?;
    validate_metadata_owner(&request.owner)?;
    validate_rewrite_payload(
        &request.request_payload,
        request.request_payload_digest,
        "distributed rewrite request payload",
    )
}

fn validate_rewrite_checkpoint(
    checkpoint: &DistributedRewriteAttemptCheckpoint,
) -> RepositoryResult<()> {
    if checkpoint.artifact_handle.len() > DISTRIBUTED_REWRITE_MAX_ATTEMPT_HANDLE_BYTES {
        return Err(RepositoryError::new(
            RepositoryErrorKind::Store,
            "distributed rewrite checkpoint artifact handle exceeds StateStore payload limit",
        ));
    }
    let disposition = match checkpoint.disposition {
        DistributedRewriteAttemptDisposition::Accepted => SpiRewriteDisposition::Accepted,
        DistributedRewriteAttemptDisposition::Superseded => SpiRewriteDisposition::Superseded,
    };
    let expected = SpiRewriteCheckpoint::try_new(
        ConnectorWriteCohortId::from_bytes(checkpoint.cohort_id),
        checkpoint.execution_id,
        disposition,
        checkpoint.attempt_digest,
        checkpoint.artifact_digest,
        Bytes::copy_from_slice(&checkpoint.artifact_handle),
    )
    .map_err(|error| {
        RepositoryError::corruption(format!(
            "distributed rewrite checkpoint is invalid: {error}"
        ))
    })?;
    if expected.checkpoint_digest != checkpoint.checkpoint_digest {
        return Err(RepositoryError::corruption(
            "distributed rewrite checkpoint digest does not match durable fields",
        ));
    }
    Ok(())
}

fn validate_rewrite_payload(
    payload: &[u8],
    digest: [u8; 32],
    context: &str,
) -> RepositoryResult<()> {
    if payload.len() > DISTRIBUTED_REWRITE_MAX_PAYLOAD_BYTES {
        return Err(RepositoryError::new(
            RepositoryErrorKind::Store,
            format!(
                "{context} exceeds {DISTRIBUTED_REWRITE_MAX_PAYLOAD_BYTES} byte StateStore payload limit"
            ),
        ));
    }
    if distributed_rewrite_payload_digest(payload) != digest {
        return Err(RepositoryError::corruption(format!(
            "{context} digest does not match payload"
        )));
    }
    Ok(())
}

fn validate_rewrite_error(message: &str) -> RepositoryResult<()> {
    if message.is_empty() || message.len() > 8 * 1024 || message.contains('\0') {
        return Err(RepositoryError::corruption(
            "distributed rewrite error message is invalid",
        ));
    }
    Ok(())
}

fn validate_rewrite_operation(
    stored: &StoredDistributedRewriteOperationV3,
) -> RepositoryResult<()> {
    if stored.schema_version != DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION {
        return Err(RepositoryError::corruption(
            "distributed rewrite operation has unsupported schema version",
        ));
    }
    validate_metadata_target(&stored.target.clone().into())?;
    validate_metadata_owner(&stored.owner)?;
    let planned = stored.plan_digest.is_some()
        && stored.manifest_digest.is_some()
        && stored.cohort_set_digest.is_some()
        && stored.cohort_count.is_some();
    match stored.state {
        DistributedRewriteOperationState::Pending => {
            if planned
                || stored.started_at_ms.is_some()
                || stored.finished_at_ms.is_some()
                || stored.error_message.is_some()
            {
                return Err(RepositoryError::corruption(
                    "pending distributed rewrite operation has lifecycle fields",
                ));
            }
        }
        DistributedRewriteOperationState::Planned
        | DistributedRewriteOperationState::Staging
        | DistributedRewriteOperationState::AbortPending
        | DistributedRewriteOperationState::CommitPending
        | DistributedRewriteOperationState::ReconcilePending => {
            if !planned
                || stored.started_at_ms.is_none()
                || stored.finished_at_ms.is_some()
                || stored.error_message.is_some()
            {
                return Err(RepositoryError::corruption(
                    "active distributed rewrite operation has invalid lifecycle fields",
                ));
            }
        }
        DistributedRewriteOperationState::Finished => {
            if !planned
                || stored.started_at_ms.is_none()
                || stored.finished_at_ms.is_none()
                || stored.error_message.is_some()
            {
                return Err(RepositoryError::corruption(
                    "finished distributed rewrite operation has invalid lifecycle fields",
                ));
            }
        }
        DistributedRewriteOperationState::Failed => {
            if stored.finished_at_ms.is_none() || stored.error_message.is_none() {
                return Err(RepositoryError::corruption(
                    "failed distributed rewrite operation has invalid lifecycle fields",
                ));
            }
        }
        DistributedRewriteOperationState::Unresolved => {
            if !planned
                || stored.started_at_ms.is_none()
                || stored.finished_at_ms.is_none()
                || stored.error_message.is_none()
            {
                return Err(RepositoryError::corruption(
                    "unresolved distributed rewrite operation has invalid lifecycle fields",
                ));
            }
        }
    }
    Ok(())
}

fn encode_rewrite_operation(
    stored: &StoredDistributedRewriteOperationV3,
) -> RepositoryResult<Value> {
    validate_rewrite_operation(stored)?;
    encode_rewrite_json(stored, "distributed rewrite operation")
}
fn decode_rewrite_operation(
    record: StateRecord,
) -> RepositoryResult<StoredDistributedRewriteOperationV3> {
    let key_id = decode_uuid_index_key(
        DISTRIBUTED_REWRITE_OPERATION_PREFIX,
        &record.key,
        "distributed rewrite operation",
    )?;
    let stored: StoredDistributedRewriteOperationV3 =
        decode_rewrite_json(record.value.as_bytes(), "distributed rewrite operation")?;
    validate_rewrite_operation(&stored)?;
    if stored.operation_id != key_id {
        return Err(RepositoryError::corruption(
            "distributed rewrite operation identity mismatch",
        ));
    }
    Ok(stored)
}
fn encode_rewrite_payload(
    kind: StoredDistributedRewritePayloadKindV3,
    digest: [u8; 32],
    payload: Vec<u8>,
) -> RepositoryResult<Value> {
    validate_rewrite_payload(&payload, digest, "distributed rewrite payload")?;
    encode_rewrite_json(
        &StoredDistributedRewritePayloadV3 {
            schema_version: DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION,
            kind,
            digest,
            payload,
        },
        "distributed rewrite payload",
    )
}
fn decode_rewrite_payload(
    record: StateRecord,
) -> RepositoryResult<StoredDistributedRewritePayloadV3> {
    let stored: StoredDistributedRewritePayloadV3 =
        decode_rewrite_json(record.value.as_bytes(), "distributed rewrite payload")?;
    if stored.schema_version != DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION {
        return Err(RepositoryError::corruption(
            "distributed rewrite payload has unsupported schema version",
        ));
    }
    validate_rewrite_payload(
        &stored.payload,
        stored.digest,
        "distributed rewrite payload",
    )?;
    Ok(stored)
}
fn decode_rewrite_attempt(
    record: StateRecord,
) -> RepositoryResult<StoredDistributedRewriteAttemptV3> {
    let stored: StoredDistributedRewriteAttemptV3 =
        decode_rewrite_json(record.value.as_bytes(), "distributed rewrite attempt")?;
    if stored.schema_version != DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION {
        return Err(RepositoryError::corruption(
            "distributed rewrite attempt has unsupported schema version",
        ));
    }
    validate_rewrite_checkpoint(&DistributedRewriteAttemptCheckpoint {
        cohort_id: stored.cohort_id,
        execution_id: ConnectorWriteExecutionId::new(
            stored.execution_query_id,
            stored.execution_attempt_id,
        ),
        disposition: stored.disposition,
        attempt_digest: stored.attempt_digest,
        artifact_digest: stored.artifact_digest,
        artifact_handle: stored.artifact_handle.clone(),
        checkpoint_digest: stored.checkpoint_digest,
    })?;
    Ok(stored)
}
fn encode_rewrite_json<T: Serialize>(value: &T, context: &str) -> RepositoryResult<Value> {
    let bytes = serde_json::to_vec(value)
        .map_err(|e| RepositoryError::corruption(format!("encode {context} failed: {e}")))?;
    if bytes.len() > DISTRIBUTED_REWRITE_MAX_PAYLOAD_BYTES {
        return Err(RepositoryError::new(
            RepositoryErrorKind::Store,
            format!("encode {context} exceeds StateStore payload limit"),
        ));
    }
    Value::try_from(Bytes::from(bytes))
        .map_err(|e| RepositoryError::store(format!("encode {context} failed: {e}")))
}
fn decode_rewrite_json<T>(bytes: &[u8], context: &str) -> RepositoryResult<T>
where
    T: DeserializeOwned + Serialize,
{
    let decoded: T = serde_json::from_slice(bytes)
        .map_err(|e| RepositoryError::corruption(format!("decode {context} failed: {e}")))?;
    let canonical = serde_json::to_vec(&decoded)
        .map_err(|e| RepositoryError::corruption(format!("re-encode {context} failed: {e}")))?;
    if canonical != bytes {
        return Err(RepositoryError::corruption(format!(
            "decode {context} failed: non-canonical JSON"
        )));
    }
    Ok(decoded)
}
fn rewrite_transaction_record(
    transaction_operation_id: OperationId,
    action: StoredDistributedRewriteTransactionActionV3,
    post_operation: &StoredDistributedRewriteOperationV3,
) -> RepositoryResult<(Key, Value)> {
    let marker = StoredDistributedRewriteTransactionV3 {
        schema_version: DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION,
        transaction_operation_id: *transaction_operation_id.as_uuid(),
        action,
        operation_id: post_operation.operation_id,
        post_operation: post_operation.clone(),
    };
    Ok((
        rewrite_transaction_key(transaction_operation_id)?,
        encode_rewrite_json(&marker, "distributed rewrite transaction marker")?,
    ))
}
fn rewrite_operation_key(operation_id: Uuid) -> RepositoryResult<Key> {
    make_key(
        format!("{DISTRIBUTED_REWRITE_OPERATION_PREFIX}{operation_id}"),
        "build distributed rewrite operation key",
    )
}
fn rewrite_payload_key(
    operation_id: Uuid,
    kind: StoredDistributedRewritePayloadKindV3,
) -> RepositoryResult<Key> {
    let name = match kind {
        StoredDistributedRewritePayloadKindV3::Request => "request",
        StoredDistributedRewritePayloadKindV3::Plan => "plan",
        StoredDistributedRewritePayloadKindV3::Receipt => "receipt",
        StoredDistributedRewritePayloadKindV3::Evidence => "evidence",
    };
    make_key(
        format!("{DISTRIBUTED_REWRITE_PAYLOAD_PREFIX}{operation_id}/{name}"),
        "build distributed rewrite payload key",
    )
}
fn rewrite_attempt_key(
    operation_id: Uuid,
    cohort_id: [u8; 32],
    execution_id: ConnectorWriteExecutionId,
) -> RepositoryResult<Key> {
    make_key(
        format!(
            "{DISTRIBUTED_REWRITE_ATTEMPT_PREFIX}{operation_id}/{}/{}/{}",
            hex::encode(cohort_id),
            hex::encode(execution_id.query_id()),
            execution_id.attempt_id(),
        ),
        "build distributed rewrite attempt key",
    )
}
fn rewrite_state_prefix(state: DistributedRewriteOperationState) -> String {
    format!(
        "{DISTRIBUTED_REWRITE_STATE_PREFIX}{}/",
        state.as_key_component()
    )
}
fn rewrite_state_key(
    state: DistributedRewriteOperationState,
    operation_id: Uuid,
) -> RepositoryResult<Key> {
    make_key(
        format!("{}{operation_id}", rewrite_state_prefix(state)),
        "build distributed rewrite state key",
    )
}
fn rewrite_transaction_key(transaction_operation_id: OperationId) -> RepositoryResult<Key> {
    make_key(
        format!(
            "{DISTRIBUTED_REWRITE_TRANSACTION_PREFIX}{}",
            transaction_operation_id.as_uuid()
        ),
        "build distributed rewrite transaction key",
    )
}
fn shared_active_target_key(target: &MaintenanceTarget) -> RepositoryResult<Key> {
    make_key(
        format!(
            "{SHARED_ACTIVE_PREFIX}{}/{}/{}",
            hex::encode(target.catalog.as_bytes()),
            hex::encode(target.namespace.as_bytes()),
            hex::encode(target.table.as_bytes())
        ),
        "build shared maintenance active key",
    )
}

fn decode_uuid_index_key(
    prefix: impl AsRef<[u8]>,
    key: &Key,
    context: &str,
) -> RepositoryResult<Uuid> {
    let suffix = key
        .as_bytes()
        .strip_prefix(prefix.as_ref())
        .ok_or_else(|| RepositoryError::corruption(format!("{context} key has unknown prefix")))?;
    let value = Value::try_from(Bytes::copy_from_slice(suffix))
        .map_err(|error| RepositoryError::store(format!("decode {context} key failed: {error}")))?;
    decode_uuid_index_value(&value, context)
}

fn repository_error_as_store(error: RepositoryError) -> StateStoreError {
    let _ = error;
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "metadata maintenance repository invariant failed",
    )
}

fn commit_unknown_error(
    context: &str,
    transaction_id: TransactionId,
    commit_error: &StateStoreError,
    reason: &str,
) -> RepositoryError {
    RepositoryError::new(
        RepositoryErrorKind::CommitUnknown,
        format!(
            "{context} transaction {} commit outcome is unresolved: {commit_error}; authoritative reread: {reason}",
            transaction_id.as_uuid()
        ),
    )
}

fn commit_recovery_error(
    certainty: CommitCertainty,
    context: &str,
    transaction_id: TransactionId,
    commit_error: &StateStoreError,
    reason: &str,
) -> RepositoryError {
    match certainty {
        CommitCertainty::Committed => RepositoryError::store(format!(
            "{context} transaction {} is committed but its result could not be read: {reason}",
            transaction_id.as_uuid()
        )),
        CommitCertainty::Unresolved => {
            commit_unknown_error(context, transaction_id, commit_error, reason)
        }
    }
}

fn format_run_failure(context: &str, failure: RunFailure) -> RepositoryError {
    let (kind, detail) = match failure {
        RunFailure::Begin(error) => (store_error_kind(&error), format!("begin failed: {error}")),
        RunFailure::Operation(error) => (
            store_error_kind(&error),
            format!("operation failed: {error}"),
        ),
        RunFailure::RetryExhausted(error) => (
            store_error_kind(&error),
            format!("retry exhausted: {error}"),
        ),
        RunFailure::DefiniteFailure(error) => {
            (store_error_kind(&error), format!("commit failed: {error}"))
        }
        RunFailure::CommitUnknown { error, .. } => (
            RepositoryErrorKind::CommitUnknown,
            format!("commit unknown: {error}"),
        ),
        RunFailure::DeadlineExceeded => (
            RepositoryErrorKind::Store,
            "state store deadline exceeded".to_string(),
        ),
    };
    RepositoryError::new(kind, format!("{context} failed: {detail}"))
}

fn store_error_kind(error: &StateStoreError) -> RepositoryErrorKind {
    if error.kind() == StateStoreErrorKind::Corruption {
        RepositoryErrorKind::Corruption
    } else {
        RepositoryErrorKind::Store
    }
}
