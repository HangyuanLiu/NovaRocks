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

//! Frontend-owned, current-process table maintenance service.
//!
//! No table-maintenance job, operation, lease, checkpoint, or recovery fact
//! survives a frontend restart. The sole durable exception is the separately
//! named GC first-observation accelerator; it never owns a catalog mutation.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use novarocks_spi::connector::{
    ConnectorCleanupCandidate, ConnectorCleanupOperationId, ConnectorCleanupOwnedRefSelection,
    ConnectorWriteOperationId, ExternalMutationFinalization, ExternalMutationOutcome,
};
use novarocks_state_store_api::StateStore;
use novarocks_state_store_runtime::StateStoreRunPolicy;
use novarocks_workload_control::RootAdmissionHandle;
use tokio::runtime::Handle;

pub(crate) use self::admission::{
    ParsedMaintenanceAction, ParsedMaintenanceStatement, ParsedShowOptimize,
    is_typed_spark_maintenance_call, lower_typed_maintenance_statement, lower_typed_show_optimize,
};
use self::gc_observation::{
    GcOwnedRefObservation, GcOwnedRefObservationAccelerator, GcOwnedRefObservationDecision,
};
use self::result::{action_result, optimize_jobs_result};
use self::worker::{OptimizeJobExecutor, OptimizeWorker};
use crate::connector::distributed_rewrite_application::DistributedRewriteIntent;
use crate::query_execution::maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceRequestContext,
    MaintenanceStatementResult, OptimizeSubmission, TableMaintenanceEngine,
    TableMaintenanceService,
};
use novarocks_table_maintenance::activity::{MaintenanceActivityFamily, TableMaintenanceActivity};
use novarocks_table_maintenance::runtime::{
    RuntimeErrorKind as OptimizeRuntimeErrorKind, TerminalError as OptimizeTerminalError,
};
use novarocks_table_maintenance::{
    MaintenanceTarget, OptimizeJob, OptimizeProcessRuntime,
};

pub mod admission;
pub mod gc_observation;
pub mod result;
pub mod worker;

enum WorkerLifecycle {
    NotStarted,
    Started(OptimizeWorker),
    Stopped(Result<(), String>),
}

// Design: ADR-0111 (docs/adr/ADR-0111-frontend-process-runtime-jobs-and-gc-observation-accelerator.md)
pub struct FrontendTableMaintenanceService {
    optimize_runtime: Arc<OptimizeProcessRuntime>,
    activity: TableMaintenanceActivity,
    gc_observations: Option<Arc<GcOwnedRefObservationAccelerator>>,
    worker: Mutex<WorkerLifecycle>,
    runtime: Handle,
    lake_publication_runtime_policy:
        Option<novarocks_query_application::publication::LakePublicationRuntimePolicy>,
    root_admission: RootAdmissionHandle,
}

impl FrontendTableMaintenanceService {
    /// Opens the service.
    ///
    /// The store and the policy that governs its use arrive together, so a
    /// half-configured owner holding one without the other cannot be built.
    pub async fn open(
        durable: Option<(Arc<dyn StateStore>, StateStoreRunPolicy)>,
        runtime: Handle,
        root_admission: RootAdmissionHandle,
    ) -> Result<Self, String> {
        Self::open_inner(durable, runtime, root_admission).await
    }

    async fn open_inner(
        durable: Option<(Arc<dyn StateStore>, StateStoreRunPolicy)>,
        runtime: Handle,
        root_admission: RootAdmissionHandle,
    ) -> Result<Self, String> {
        let gc_observations = match durable {
            Some((store, policy)) => Some(Arc::new(
                GcOwnedRefObservationAccelerator::open(store, policy)
                    .await
                    .map_err(|error| {
                        format!(
                            "open frontend GC owned-ref observation accelerator failed: {error}"
                        )
                    })?,
            )),
            None => None,
        };
        Ok(Self {
            optimize_runtime: Arc::new(OptimizeProcessRuntime::new()),
            activity: TableMaintenanceActivity::default(),
            gc_observations,
            worker: Mutex::new(WorkerLifecycle::NotStarted),
            runtime,
            lake_publication_runtime_policy: None,
            root_admission,
        })
    }

    pub fn with_lake_publication_runtime_policy(
        mut self,
        policy: novarocks_query_application::publication::LakePublicationRuntimePolicy,
    ) -> Self {
        self.lake_publication_runtime_policy = Some(policy);
        self
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    async fn execute_user_action(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
        action: ParsedMaintenanceAction,
        spark_result: bool,
    ) -> Result<MaintenanceStatementResult, String> {
        engine.reject_user_action_on_mv(&target)?;
        let outcome = match action {
            ParsedMaintenanceAction::RemoveOrphanFiles { older_than_ms } => {
                self.execute_cleanup(engine, target, older_than_ms).await?
            }
            ParsedMaintenanceAction::RewriteDataFiles { .. } => {
                let _permit = self
                    .activity
                    .acquire(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| error.to_string())?;
                execute_distributed_rewrite(
                    engine,
                    &target,
                    DistributedRewriteIntent::DataFiles { rewrite_all: true },
                )?
            }
            ParsedMaintenanceAction::RewritePositionDeleteFiles {
                options,
                where_clause,
            } => {
                let _permit = self
                    .activity
                    .acquire(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| error.to_string())?;
                execute_distributed_rewrite(
                    engine,
                    &target,
                    rewrite_position_delete_intent(&options, where_clause.as_deref())?,
                )?
            }
            action => {
                let _permit = self
                    .activity
                    .acquire(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| error.to_string())?;
                engine.execute_action(action.into_request(engine, target)?)?
            }
        };
        if spark_result {
            action_result(outcome)
        } else {
            Ok(MaintenanceStatementResult::Ok)
        }
    }

    fn cleanup_gc_timing(&self, older_than_ms: i64) -> Result<(i64, i64), String> {
        let policy = self.lake_publication_runtime_policy.ok_or_else(|| {
            "orphan cleanup is unsupported until the lake publication runtime policy is installed"
                .to_string()
        })?;
        let now_ms = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| "orphan cleanup is unsupported because wall clock is unsafe")?
                .as_millis(),
        )
        .map_err(|_| "orphan cleanup is unsupported because wall clock exceeds i64")?;
        let safe_age_ms = i64::try_from(policy.safe_gc_age().as_millis())
            .map_err(|_| "orphan cleanup is unsupported because safe GC age exceeds i64")?;
        let cutoff = now_ms.checked_sub(safe_age_ms).ok_or_else(|| {
            "orphan cleanup is unsupported because safe GC cutoff underflows".to_string()
        })?;
        if older_than_ms <= 0 || older_than_ms > cutoff {
            return Err(format!(
                "orphan cleanup cutoff {older_than_ms} is newer than the shared safe GC boundary {cutoff}"
            ));
        }
        Ok((now_ms, safe_age_ms))
    }

    async fn execute_cleanup(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
        older_than_ms: i64,
    ) -> Result<MaintenanceActionOutcome, String> {
        let _permit = self
            .activity
            .acquire(&target, MaintenanceActivityFamily::Cleanup)
            .map_err(|error| error.to_string())?;
        let (now_ms, safe_age_ms) = self.cleanup_gc_timing(older_than_ms)?;
        let observations = self.gc_observations.as_ref().ok_or_else(|| {
            "orphan cleanup is unavailable because the GC observation accelerator is not configured"
                .to_string()
        })?;
        let discovery = engine.plan_cleanup_maintenance(
            &target,
            ConnectorCleanupOperationId::new(),
            older_than_ms,
        )?;
        let candidates = cleanup_candidates_first_page(engine, &discovery)?;
        let owned = candidates
            .iter()
            .any(|candidate| matches!(candidate, ConnectorCleanupCandidate::OwnedRef { .. }));
        let objects = candidates
            .iter()
            .any(|candidate| matches!(candidate, ConnectorCleanupCandidate::Object { .. }));
        if owned && objects {
            return Err("cleanup discovery mixed owned-ref and object candidates".to_string());
        }
        let session = if owned {
            let selection = self
                .observe_mature_owned_refs(observations, &candidates, now_ms, safe_age_ms)
                .await?;
            if let Err(error) = engine.finalize_cleanup_terminal(&discovery) {
                tracing::warn!(%error, "orphan cleanup discovery finalization failed");
            }
            if selection.identities().is_empty() {
                return Ok(MaintenanceActionOutcome::RemoveOrphanFiles {
                    orphan_file_locations: Vec::new(),
                });
            }
            engine.plan_selected_owned_ref_cleanup_maintenance(
                &target,
                ConnectorCleanupOperationId::new(),
                older_than_ms,
                selection,
            )?
        } else {
            discovery
        };
        let batches = session.plan_ref().summary().batch_count();
        for ordinal in 0..batches {
            let prepared = engine.prepare_cleanup_batch(&session, ordinal)?;
            match engine.execute_cleanup_batch(&session, prepared)? {
                crate::connector::cleanup_maintenance::CleanupBatchExecution::Receipt(receipt) => {
                    if receipt.summary().unknown() != 0 {
                        return Err("orphan cleanup batch outcome is unknown; inspect the provider before retrying".to_string());
                    }
                }
                crate::connector::cleanup_maintenance::CleanupBatchExecution::Uncertain(error) => {
                    return Err(format!(
                        "orphan cleanup dispatch outcome is unknown: {error}"
                    ));
                }
            }
        }
        let locations = cleanup_candidate_locations(engine, &session)?;
        if let Err(error) = engine.finalize_cleanup_terminal(&session) {
            tracing::warn!(%error, "orphan cleanup terminal finalization failed");
        }
        Ok(MaintenanceActionOutcome::RemoveOrphanFiles {
            orphan_file_locations: locations,
        })
    }

    async fn observe_mature_owned_refs(
        &self,
        observations: &GcOwnedRefObservationAccelerator,
        candidates: &[ConnectorCleanupCandidate],
        now_ms: i64,
        safe_gc_age_ms: i64,
    ) -> Result<ConnectorCleanupOwnedRefSelection, String> {
        let mut identities = Vec::new();
        for candidate in candidates {
            let ConnectorCleanupCandidate::OwnedRef {
                table_uuid,
                name,
                head_snapshot_id,
                provenance_version,
                provenance_digest,
                ..
            } = candidate
            else {
                continue;
            };
            let observation = GcOwnedRefObservation::try_new(
                *table_uuid,
                name.to_string(),
                *head_snapshot_id,
                *provenance_version,
                *provenance_digest,
                now_ms,
            )
            .map_err(|error| format!("build GC owned-ref observation failed: {error}"))?;
            match observations
                .observe(observation, now_ms, safe_gc_age_ms)
                .await
                .map_err(|error| format!("record GC owned-ref observation failed: {error}"))?
            {
                GcOwnedRefObservationDecision::Mature { .. } => {
                    identities.push(candidate.owned_ref_identity().ok_or_else(|| {
                        "owned-ref candidate has no valid exact identity".to_string()
                    })?)
                }
                GcOwnedRefObservationDecision::NotMature { .. } => {}
            }
        }
        ConnectorCleanupOwnedRefSelection::try_new(identities)
            .map_err(|error| format!("build mature owned-ref cleanup selection failed: {error}"))
    }

    fn submit_optimize(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        // Acquire the target conflict right before observing any provider
        // identity or base version. Capturing first leaves a TOCTOU window in
        // which another maintenance operation can replace the exact object
        // that this job would later bind and dispatch against.
        let permit = self
            .activity
            .acquire(&target, MaintenanceActivityFamily::Optimize)
            .map_err(|_| "an optimize job is already active for this table".to_string())?;
        let object_id = engine.capture_target_object_id(&target)?;
        let base_snapshot_id = engine.current_snapshot_id(&target)?;
        let submitted = self.block_on(self.optimize_runtime.submit(
            novarocks_table_maintenance::runtime::JobCreate {
                target,
                object_id: object_id.as_bytes().to_vec(),
                base_snapshot_id,
                created_at_ms: now_unix_millis(),
            },
            permit,
        ));
        match submitted {
            Ok(job) => {
                self.wakeup_worker()?;
                Ok(OptimizeSubmission::Submitted { job_id: job.job_id })
            }
            Err(error) if error.kind() == OptimizeRuntimeErrorKind::AlreadyActive => {
                Ok(OptimizeSubmission::AlreadyActive)
            }
            Err(error) => Err(format!("create frontend optimize job failed: {error}")),
        }
    }

    fn show_optimize(
        &self,
        statement: ParsedShowOptimize,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        let mut jobs = self
            .block_on(self.optimize_runtime.list())
            .map_err(|error| format!("show frontend optimize jobs failed: {error}"))?;
        if let Some(catalog) = statement.catalog.as_deref().or(context.current_catalog) {
            jobs.retain(|job| job.target.catalog == catalog);
        }
        jobs.retain(|job| {
            job.target.namespace
                == statement
                    .database
                    .as_deref()
                    .unwrap_or(context.current_database)
        });
        if let Some(table) = statement.table_name.as_deref() {
            jobs.retain(|job| job.target.table == table);
        }
        if statement.order_by_create_time_desc {
            jobs.sort_by_key(|job| std::cmp::Reverse(job.created_at_ms));
        }
        if let Some(limit) = statement.limit {
            jobs.truncate(limit);
        }
        optimize_jobs_result(jobs)
    }

    fn wakeup_worker(&self) -> Result<(), String> {
        let worker = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        if let WorkerLifecycle::Started(worker) = &*worker {
            worker.wakeup();
        }
        Ok(())
    }

    pub(crate) async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        self.optimize_runtime.stop_admission();
        let previous = {
            let mut lifecycle = self
                .worker
                .lock()
                .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
            std::mem::replace(&mut *lifecycle, WorkerLifecycle::Stopped(Ok(())))
        };
        let (next, result) = match previous {
            WorkerLifecycle::NotStarted => (WorkerLifecycle::Stopped(Ok(())), Ok(())),
            WorkerLifecycle::Started(mut worker) => {
                let result = worker.shutdown_until(deadline).await;
                if result.is_err() && worker.has_join_owner() {
                    (WorkerLifecycle::Started(worker), result)
                } else {
                    (WorkerLifecycle::Stopped(result.clone()), result)
                }
            }
            WorkerLifecycle::Stopped(result) => (WorkerLifecycle::Stopped(result.clone()), result),
        };
        let mut lifecycle = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *lifecycle = next;
        result
    }

    pub(crate) fn request_shutdown_for_process_exit(&self) {
        self.optimize_runtime.stop_admission();
        let lifecycle = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let WorkerLifecycle::Started(worker) = &*lifecycle {
            worker.request_stop();
        }
    }
}

struct DirectOptimizeExecutor;

impl OptimizeJobExecutor for DirectOptimizeExecutor {
    fn execute(
        &self,
        _runtime: &Handle,
        engine: &dyn TableMaintenanceEngine,
        job: &OptimizeJob,
    ) -> Result<MaintenanceActionOutcome, OptimizeTerminalError> {
        execute_distributed_rewrite_terminal(
            engine,
            &job.target,
            DistributedRewriteIntent::DataFiles { rewrite_all: true },
        )
    }
}

/// Execute one current-process OPTIMIZE job through the frontend-owned native
/// distributed rewrite path. The process runtime deliberately retains no
/// recovery record, but a single attempt must still finish or abort its exact
/// frozen connector session before the worker reports a terminal job state.
fn execute_distributed_rewrite(
    engine: &dyn TableMaintenanceEngine,
    target: &MaintenanceTarget,
    intent: DistributedRewriteIntent,
) -> Result<MaintenanceActionOutcome, String> {
    execute_distributed_rewrite_terminal(engine, target, intent).map_err(|error| error.message)
}

/// Executes one rewrite and preserves the provider's terminal fact instead of
/// flattening it into a generic worker failure. Only the current-process job
/// runtime interprets these terminal classes; foreground SQL maps them to its
/// ordinary error surface.
fn execute_distributed_rewrite_terminal(
    engine: &dyn TableMaintenanceEngine,
    target: &MaintenanceTarget,
    intent: DistributedRewriteIntent,
) -> Result<MaintenanceActionOutcome, OptimizeTerminalError> {
    let session = engine
        .plan_distributed_rewrite(target, ConnectorWriteOperationId::new(), intent)
        .map_err(OptimizeTerminalError::failed)?;
    let plan = session.plan();
    if session.is_noop() {
        return rewrite_outcome(intent, None, plan.summary())
            .map_err(OptimizeTerminalError::failed);
    }

    for cohort in plan.cohorts() {
        let completion = engine
            .prepare_distributed_rewrite_cohort(&session, cohort.cohort_id())
            .and_then(|prepared| {
                // A rewrite group's plan carries writer nodes, so the bundle
                // has to be encoded against the session's sealed recipes.
                crate::native::fragment_encoder::encode_native_fragment_bundle_for_input(
                    prepared.encoding(),
                )
                .map_err(|error| format!("encode distributed rewrite fragments: {error}"))
                .and_then(|bundle| prepared.finish(bundle))
            });
        let completion = match completion {
            Ok(completion) => completion,
            Err(error) => {
                return abort_distributed_rewrite(engine, &session, error)
                    .map_err(OptimizeTerminalError::failed);
            }
        };
        // Each group runs as its own query, so the session collects their
        // prepared sets and commits the union once below.
        if let Err(error) = engine.accumulate_distributed_rewrite_group(&session, completion) {
            return abort_distributed_rewrite(engine, &session, error)
                .map_err(OptimizeTerminalError::failed);
        }
    }

    match engine
        .commit_distributed_rewrite(&session)
        .map_err(OptimizeTerminalError::failed)?
    {
        ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization,
            ..
        } => {
            if let ExternalMutationFinalization::Failed(error) = finalization {
                return Err(OptimizeTerminalError::known_committed_finalization_failed(
                    format!("distributed optimize committed but finalization failed: {error}"),
                ));
            }
            let receipt = engine
                .finalize_distributed_rewrite(&session, &receipt)
                .map_err(OptimizeTerminalError::failed)?;
            let summary = receipt.summary();
            rewrite_outcome(intent, Some(summary), plan.summary())
                .map_err(OptimizeTerminalError::failed)
        }
        ExternalMutationOutcome::KnownUncommitted { failure } => {
            let error = format!("distributed rewrite commit was not applied: {failure}");
            let error = match abort_distributed_rewrite(engine, &session, error) {
                Err(error) => error,
                Ok(_) => "distributed rewrite commit was not applied".to_string(),
            };
            Err(OptimizeTerminalError::known_uncommitted(error))
        }
        ExternalMutationOutcome::CommitUnknown { failure, .. } => {
            Err(OptimizeTerminalError::commit_unknown(format!(
                "distributed rewrite commit outcome is unknown: {failure}; do not retry automatically"
            )))
        }
    }
}

fn rewrite_outcome(
    intent: DistributedRewriteIntent,
    receipt: Option<novarocks_spi::connector::ConnectorDistributedRewriteReceiptSummary>,
    plan: novarocks_spi::connector::ConnectorDistributedRewritePlanSummary,
) -> Result<MaintenanceActionOutcome, String> {
    let receipt = receipt.unwrap_or_default();
    match intent {
        DistributedRewriteIntent::DataFiles { .. } => {
            Ok(MaintenanceActionOutcome::RewriteDataFiles {
                target_snapshot_id: receipt.target_version,
                rewritten_data_files_count: i32::try_from(receipt.input_data_files)
                    .map_err(|_| "distributed rewrite input data file count exceeds i32")?,
                added_data_files_count: receipt
                    .output_data_files
                    .map(|count| {
                        i32::try_from(count).map_err(|_| {
                            "distributed rewrite output data file count exceeds i32".to_string()
                        })
                    })
                    .transpose()?,
                added_delete_files_count: receipt
                    .output_delete_files
                    .map(|count| {
                        i32::try_from(count).map_err(|_| {
                            "distributed rewrite output delete file count exceeds i32".to_string()
                        })
                    })
                    .transpose()?,
                rewritten_bytes_count: i64::try_from(plan.input_bytes)
                    .map_err(|_| "distributed rewrite input byte count exceeds i64")?,
                failed_data_files_count: 0,
                removed_delete_files_count: i32::try_from(receipt.input_delete_files)
                    .map_err(|_| "distributed rewrite input delete file count exceeds i32")?,
                output_record_count: receipt
                    .output_rows
                    .map(|count| {
                        i64::try_from(count).map_err(|_| {
                            "distributed rewrite output row count exceeds i64".to_string()
                        })
                    })
                    .transpose()?,
            })
        }
        DistributedRewriteIntent::PositionDeletes { .. } => {
            Ok(MaintenanceActionOutcome::RewritePositionDeleteFiles {
                rewritten_delete_files_count: i32::try_from(receipt.input_delete_files)
                    .map_err(|_| "distributed rewrite input delete file count exceeds i32")?,
                added_delete_files_count: receipt
                    .output_delete_files
                    .map(|count| {
                        i32::try_from(count).map_err(|_| {
                            "distributed rewrite output delete file count exceeds i32".to_string()
                        })
                    })
                    .transpose()?,
                rewritten_bytes_count: i64::try_from(plan.input_bytes)
                    .map_err(|_| "distributed rewrite input byte count exceeds i64")?,
                added_bytes_count: 0,
            })
        }
    }
}

fn abort_distributed_rewrite(
    engine: &dyn TableMaintenanceEngine,
    session: &crate::query_execution::distributed_rewrite::DistributedRewriteMaintenanceSession,
    error: String,
) -> Result<MaintenanceActionOutcome, String> {
    match engine.abort_distributed_rewrite(session) {
        Ok(_) => Err(error),
        Err(abort) => Err(format!(
            "{error}; distributed rewrite abort failed: {abort}"
        )),
    }
}

fn rewrite_position_delete_intent(
    options: &std::collections::BTreeMap<String, String>,
    where_clause: Option<&str>,
) -> Result<DistributedRewriteIntent, String> {
    if where_clause.is_some() {
        return Err(
            "rewrite_position_delete_files where is not supported in NovaRocks yet".to_string(),
        );
    }
    let mut rewrite_all = false;
    let mut min_input_files = None;
    for (key, value) in options {
        match key.as_str() {
            "rewrite-all" if value.eq_ignore_ascii_case("true") => rewrite_all = true,
            "rewrite-all" => {
                return Err(
                    "rewrite_position_delete_files option `rewrite-all` must be `true`".to_string(),
                );
            }
            "min-input-files" => {
                min_input_files = Some(value.parse::<u32>().map_err(|_| {
                    "rewrite_position_delete_files option `min-input-files` must be a positive integer"
                        .to_string()
                })?);
                if min_input_files == Some(0) {
                    return Err(
                        "rewrite_position_delete_files option `min-input-files` must be a positive integer"
                            .to_string(),
                    );
                }
            }
            other => {
                return Err(format!(
                    "unsupported rewrite_position_delete_files option `{other}`"
                ));
            }
        }
    }
    Ok(DistributedRewriteIntent::PositionDeletes {
        rewrite_all,
        min_input_files,
    })
}

#[async_trait::async_trait]
impl TableMaintenanceService for FrontendTableMaintenanceService {
    fn start(&self, engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String> {
        let mut lifecycle = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        match &*lifecycle {
            WorkerLifecycle::NotStarted => {
                *lifecycle = WorkerLifecycle::Started(OptimizeWorker::start_with_executor(
                    &self.runtime,
                    Arc::clone(&self.optimize_runtime),
                    Arc::downgrade(&engine),
                    Arc::new(DirectOptimizeExecutor),
                    self.root_admission.clone(),
                )?);
                Ok(())
            }
            WorkerLifecycle::Started(_) => {
                Err("table maintenance service is already started".to_string())
            }
            WorkerLifecycle::Stopped(_) => {
                Err("table maintenance service cannot be restarted after shutdown".to_string())
            }
        }
    }

    async fn handle_typed_statement(
        &self,
        engine: &dyn TableMaintenanceEngine,
        statement: ParsedMaintenanceStatement,
        spark_procedure: bool,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        match statement {
            ParsedMaintenanceStatement::Execute { name_parts, action } => {
                self.execute_user_action(
                    engine,
                    engine.resolve_target(&name_parts, context)?,
                    action,
                    spark_procedure,
                )
                .await
            }
            ParsedMaintenanceStatement::SubmitOptimize { name_parts } => {
                engine.reject_user_action_on_mv(&engine.resolve_target(&name_parts, context)?)?;
                self.submit_optimize(engine, engine.resolve_target(&name_parts, context)?)
                    .map(|_| MaintenanceStatementResult::Ok)
            }
            ParsedMaintenanceStatement::ShowOptimize => Err(
                "SHOW ALTER TABLE OPTIMIZE belongs to the read-only maintenance owner".to_string(),
            ),
        }
    }

    #[expect(
        private_interfaces,
        reason = "The typed SHOW OPTIMIZE parser carrier is intentionally confined to the table-maintenance boundary."
    )]
    fn handle_typed_show_optimize(
        &self,
        statement: ParsedShowOptimize,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        self.show_optimize(statement, context)
    }

    async fn execute_automatic_action(
        &self,
        engine: &dyn TableMaintenanceEngine,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        match request {
            MaintenanceActionRequest::RewriteDataFiles { target, .. } => {
                execute_distributed_rewrite(
                    engine,
                    &target,
                    DistributedRewriteIntent::DataFiles { rewrite_all: true },
                )
            }
            MaintenanceActionRequest::RewritePositionDeleteFiles {
                target,
                options,
                where_clause,
            } => execute_distributed_rewrite(
                engine,
                &target,
                rewrite_position_delete_intent(&options, where_clause.as_deref())?,
            ),
            MaintenanceActionRequest::RemoveOrphanFiles {
                target,
                older_than_ms,
            } => self.execute_cleanup(engine, target, older_than_ms).await,
            request => {
                let target = match &request {
                    MaintenanceActionRequest::RewriteManifests { target, .. }
                    | MaintenanceActionRequest::ExpireSnapshots { target, .. } => target,
                    MaintenanceActionRequest::RewriteDataFiles { .. }
                    | MaintenanceActionRequest::RewritePositionDeleteFiles { .. } => {
                        unreachable!("handled by the frontend distributed rewrite owner")
                    }
                    MaintenanceActionRequest::RemoveOrphanFiles { .. } => {
                        unreachable!("handled above")
                    }
                };
                let _permit = self
                    .activity
                    .acquire(target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| error.to_string())?;
                engine.execute_action(request)
            }
        }
    }

    fn submit_automatic_optimize(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        self.submit_optimize(engine, target)
    }

    async fn wait_for_automatic_optimize(
        &self,
        handle: novarocks_table_maintenance::runtime::JobHandle,
    ) -> Result<novarocks_table_maintenance::runtime::MaintenanceJobState, String> {
        self.optimize_runtime
            .wait_for_completion(handle.job_id())
            .await
            .map(|job| job.state)
            .map_err(|error| format!("wait for frontend optimize job failed: {error}"))
    }

    fn execute_automatic_optimize_durably(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        let submission = self.submit_optimize(engine, target)?;
        let Some(handle) = submission.handle() else {
            return Ok(submission);
        };
        let terminal = self.block_on(self.wait_for_automatic_optimize(handle))?;
        if terminal == novarocks_table_maintenance::runtime::MaintenanceJobState::Finished {
            Ok(submission)
        } else {
            Err(format!(
                "optimize job {} completed with terminal state {}",
                handle.job_id(),
                terminal.as_str()
            ))
        }
    }

    async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        FrontendTableMaintenanceService::shutdown_until(self, deadline).await
    }

    fn request_shutdown_for_process_exit(&self) {
        FrontendTableMaintenanceService::request_shutdown_for_process_exit(self);
    }
}

impl ParsedMaintenanceAction {
    fn into_request(
        self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<MaintenanceActionRequest, String> {
        match self {
            Self::RewriteDataFiles {
                options,
                branch,
                where_clause,
            } => Ok(MaintenanceActionRequest::RewriteDataFiles {
                target: target.clone(),
                base_snapshot_id: engine.current_snapshot_id(&target)?,
                job_id: None,
                options,
                branch,
                where_clause,
            }),
            Self::RewriteManifests {
                use_caching,
                spec_id,
            } => Ok(MaintenanceActionRequest::RewriteManifests {
                target,
                use_caching,
                spec_id,
            }),
            Self::ExpireSnapshots {
                older_than_ms,
                retain_last,
            } => Ok(MaintenanceActionRequest::ExpireSnapshots {
                target,
                older_than_ms,
                retain_last,
            }),
            Self::RemoveOrphanFiles { older_than_ms } => {
                Ok(MaintenanceActionRequest::RemoveOrphanFiles {
                    target,
                    older_than_ms,
                })
            }
            Self::RewritePositionDeleteFiles {
                options,
                where_clause,
            } => Ok(MaintenanceActionRequest::RewritePositionDeleteFiles {
                target,
                options,
                where_clause,
            }),
        }
    }
}

fn cleanup_candidate_locations(
    engine: &dyn TableMaintenanceEngine,
    session: &crate::connector::cleanup_maintenance::CleanupMaintenanceSession,
) -> Result<Vec<String>, String> {
    let mut offset = 0_u64;
    let mut locations = Vec::new();
    loop {
        let page = engine.read_cleanup_candidate_page(session, offset, 1024)?;
        locations.extend(page.display_keys().iter().map(ToString::to_string));
        if page.complete() {
            return Ok(locations);
        }
        offset = offset
            .checked_add(page.candidates().len() as u64)
            .ok_or_else(|| "orphan cleanup candidate page offset overflow".to_string())?;
    }
}

fn cleanup_candidates_first_page(
    engine: &dyn TableMaintenanceEngine,
    session: &crate::connector::cleanup_maintenance::CleanupMaintenanceSession,
) -> Result<Vec<ConnectorCleanupCandidate>, String> {
    let page = engine.read_cleanup_candidate_page(session, 0, 1024)?;
    if page.candidates().is_empty() && !page.complete() {
        return Err("cleanup discovery returned a non-terminal empty candidate page".to_string());
    }
    Ok(page.candidates().to_vec())
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|value| i64::try_from(value.as_millis()).ok())
        .unwrap_or(0)
}
