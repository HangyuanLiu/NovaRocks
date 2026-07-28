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
use novarocks_spi::state_store::{
    Direction, Key, KeyRange, Precondition, RangeRequest, StateRecord, StateStore, StateStoreError,
    StateStoreErrorKind, Value, VersionToken, WriteTransaction,
};
use novarocks_state_store::metrics::StateStoreMetrics;
use novarocks_state_store::{OperationId, RunFailure, run_side_effect_free};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::model::{
    OPTIMIZE_JOB_SCHEMA_VERSION, OptimizeJob, OptimizeJobCreate, OptimizeJobOutcome,
    StoredMaintenanceTargetV1, StoredOptimizeCounterV1, StoredOptimizeJobStateV1,
    StoredOptimizeJobV1, StoredOptimizeOperationActionV1, StoredOptimizeOperationV1,
    StoredOptimizeOutcomeV1,
};

const COUNTER_KEY: &str = "novarocks/frontend/table-maintenance/v1/counter";
const JOB_PREFIX: &str = "novarocks/frontend/table-maintenance/v1/jobs/";
const PENDING_PREFIX: &str = "novarocks/frontend/table-maintenance/v1/state/pending/";
const RUNNING_PREFIX: &str = "novarocks/frontend/table-maintenance/v1/state/running/";
const ACTIVE_PREFIX: &str = "novarocks/frontend/table-maintenance/v1/active/";
const OPERATION_PREFIX: &str = "novarocks/frontend/table-maintenance/v1/operations/";

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
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RepositoryError {}

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
            .field("provider", &self.store.provider_name())
            .finish_non_exhaustive()
    }
}

impl OptimizeJobRepository {
    pub async fn open(store: Arc<dyn StateStore>) -> Result<Self, RepositoryError> {
        let repository = Self {
            metrics: Arc::new(StateStoreMetrics::new(store.provider_name())),
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
            Err(RunFailure::CommitUnknown { error, .. }) => {
                self.recover_operation(
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
            Err(RunFailure::CommitUnknown { error, .. }) => {
                let recovered = self
                    .recover_operation(
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
            self.fail(
                job.job_id,
                now_ms,
                "optimize job failed during frontend restart reconciliation".to_string(),
            )
            .await?;
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
                let job_id = decode_index_key(prefix_text, &index.key)?;
                let value_job_id = decode_index_value(&index.value)?;
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
            Err(RunFailure::CommitUnknown { error, .. }) => {
                self.recover_operation(
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

    async fn recover_operation(
        &self,
        operation_id: OperationId,
        expected_action: StoredOptimizeOperationActionV1,
        expected_job_id: Option<i64>,
        context: &str,
        commit_error: StateStoreError,
    ) -> RepositoryResult<OptimizeJob> {
        let key = operation_key(operation_id)?;
        let mut transaction = self.store.begin_read().await.map_err(|error| {
            commit_unknown_error(
                context,
                &commit_error,
                &format!("authoritative read begin failed: {error}"),
            )
        })?;
        let operation_record = transaction.get(&key).await.map_err(|error| {
            commit_unknown_error(
                context,
                &commit_error,
                &format!("authoritative operation read failed: {error}"),
            )
        })?;
        let Some(operation_record) = operation_record else {
            transaction.abort().await.map_err(|error| {
                commit_unknown_error(
                    context,
                    &commit_error,
                    &format!("authoritative read finish failed: {error}"),
                )
            })?;
            return Err(commit_unknown_error(
                context,
                &commit_error,
                "operation marker is absent",
            ));
        };
        let marker: StoredOptimizeOperationV1 = decode_json(
            operation_record.value.as_bytes(),
            "optimize operation marker",
        )?;
        validate_operation_marker(&marker)?;
        if marker.operation_id != *operation_id.as_uuid()
            || marker.action != expected_action
            || expected_job_id.is_some_and(|job_id| job_id != marker.job_id)
        {
            return Err(RepositoryError::corruption(format!(
                "{context} authoritative operation marker does not match the requested operation"
            )));
        }
        let stored = load_job_from_transaction(transaction.as_mut(), marker.job_id)
            .await
            .map_err(|error| {
                commit_unknown_error(
                    context,
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
            commit_unknown_error(
                context,
                &commit_error,
                &format!("authoritative read finish failed: {error}"),
            )
        })?;
        if stored.last_operation_id != *operation_id.as_uuid() {
            return Err(RepositoryError::corruption(format!(
                "{context} job {} does not contain the authoritative operation id",
                marker.job_id
            )));
        }
        Ok(OptimizeJob::from(&stored))
    }
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
            Err(error) => return Ok(Err(error)),
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
        job_id,
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
    let (operation_key, operation_value) =
        match operation_record(operation_id, StoredOptimizeOperationActionV1::Claim, job_id) {
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
        job_id,
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
    let (operation_key, operation_value) = match operation_record(operation_id, action, job_id) {
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
        Err(error) => Ok(Err(error)),
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
        Err(error) => Ok(Err(error)),
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
    job_id: i64,
) -> RepositoryResult<(Key, Value)> {
    let marker = StoredOptimizeOperationV1 {
        schema_version: OPTIMIZE_JOB_SCHEMA_VERSION,
        operation_id: *operation_id.as_uuid(),
        action,
        job_id,
    };
    Ok((
        operation_key(operation_id)?,
        encode_json(&marker, "optimize operation marker")?,
    ))
}

fn validate_operation_marker(marker: &StoredOptimizeOperationV1) -> RepositoryResult<()> {
    if marker.schema_version != OPTIMIZE_JOB_SCHEMA_VERSION || marker.job_id <= 0 {
        return Err(RepositoryError::corruption(
            "optimize operation marker is corrupt",
        ));
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

fn commit_unknown_error(
    context: &str,
    commit_error: &StateStoreError,
    reason: &str,
) -> RepositoryError {
    RepositoryError::new(
        RepositoryErrorKind::CommitUnknown,
        format!(
            "{context} commit outcome is unresolved: {commit_error}; authoritative reread: {reason}"
        ),
    )
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
