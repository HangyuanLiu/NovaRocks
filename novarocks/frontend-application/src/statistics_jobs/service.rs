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
// software distributed under the Apache License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Frontend adapters for the statistics application product owner.
//!
//! This module translates typed frontend commands and connector target
//! captures. It deliberately owns neither a job ledger nor business attempt
//! orchestration. Role composition starts the product-owned job runtime with
//! the concrete attempt adapter; without the required bindings the entrypoint
//! fails closed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::application;
use super::model::StatisticsJobTarget;
use novarocks_statistics_application::{
    StatisticsAttemptExecutor, StatisticsColumns, StatisticsJob, StatisticsJobCreate,
    StatisticsJobId, StatisticsJobRuntime, StatisticsJobService, StatisticsTarget,
};
use novarocks_workload_control::{PendingQueryRoot, RootAdmissionHandle, WorkClass, WorkRequest};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsStatementResult {
    JobSubmitted(StatisticsJob),
    JobCancellationRequested(StatisticsJob),
    AnalyzeJobs(Vec<StatisticsJob>),
    TableStats(Vec<StatisticsTableStatRow>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTableStatRow {
    pub metric_name: String,
    pub value: Option<String>,
    pub status: String,
    pub basis_version: String,
    pub source: String,
    pub numeric_nature: String,
    pub basis_relation: String,
}

pub trait TableStatisticsReader: Send + Sync {
    fn show_table_stats(
        &self,
        target: &StatisticsJobTarget,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<StatisticsTableStatRow>, String>;
}

/// T12's role composition supplies a root for each independently-owned
/// background statistics business request. There is intentionally no default
/// implementation: borrowing the statement observer would let observer
/// cancellation destroy the job root.
pub trait StatisticsJobRootScopeSource: Send + Sync {
    fn begin_statistics_job(&self) -> Result<PendingQueryRoot, String>;
}

/// Role-local source for independently-owned ANALYZE jobs.
///
/// The Server injects only the narrow root-admission capability.  A submitted
/// job retains the returned owner in the statistics application repository;
/// the admission's transient business permit is deliberately released after
/// it has become that durable process-local responsibility.
#[derive(Clone)]
pub struct RootAdmissionStatisticsJobSource {
    admission: RootAdmissionHandle,
}

impl RootAdmissionStatisticsJobSource {
    pub fn new(admission: RootAdmissionHandle) -> Self {
        Self { admission }
    }
}

impl StatisticsJobRootScopeSource for RootAdmissionStatisticsJobSource {
    fn begin_statistics_job(&self) -> Result<PendingQueryRoot, String> {
        self.admission
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .map_err(|error| error.to_string())
    }
}

struct StatisticsTableReaderAdapter {
    inner: Arc<dyn application::StatisticsTableReader>,
}

impl TableStatisticsReader for StatisticsTableReaderAdapter {
    fn show_table_stats(
        &self,
        target: &StatisticsJobTarget,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<StatisticsTableStatRow>, String> {
        self.inner
            .show_table_stats(
                &application::StatisticsTableTarget {
                    catalog: target.catalog.clone(),
                    namespace: target.namespace.clone(),
                    table: target.table.clone(),
                },
                context,
            )
            .map_err(|error| error.to_string())
            .map(|rows| {
                rows.into_iter()
                    .map(|row| StatisticsTableStatRow {
                        metric_name: row.metric,
                        value: row.value,
                        status: row.status,
                        basis_version: row.basis_version,
                        source: row.source,
                        numeric_nature: row.numeric_nature,
                        basis_relation: row.basis_relation,
                    })
                    .collect()
            })
    }
}

/// Construct the sole role-local adapter for SQL statistics reads. The
/// product port receives it at construction and never accepts a later sink.
pub(crate) fn table_statistics_reader_for_role(
    controls: Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
) -> Arc<dyn TableStatisticsReader> {
    Arc::new(StatisticsTableReaderAdapter {
        inner: Arc::new(application::ConnectorStatisticsTableReader::new(controls)),
    })
}

pub struct FrontendStatisticsApplicationPort {
    job_runtime: StatisticsJobRuntime,
    target_resolver: Arc<dyn application::StatisticsTargetResolver>,
    root_scope: Arc<dyn StatisticsJobRootScopeSource>,
    table_statistics: Arc<dyn TableStatisticsReader>,
    runtime: tokio::runtime::Handle,
}
impl FrontendStatisticsApplicationPort {
    pub fn new(
        job_service: StatisticsJobService,
        target_resolver: Arc<dyn application::StatisticsTargetResolver>,
        root_scope: Arc<dyn StatisticsJobRootScopeSource>,
        table_statistics: Arc<dyn TableStatisticsReader>,
        core_executor: Arc<dyn StatisticsAttemptExecutor>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            job_runtime: StatisticsJobRuntime::start(job_service, core_executor, runtime.clone()),
            target_resolver,
            root_scope,
            table_statistics,
            runtime,
        }
    }
    pub async fn shutdown_worker_until(&self, _deadline: Instant) -> Result<(), String> {
        self.job_runtime.shutdown_until(_deadline).await
    }
    pub fn request_worker_stop_for_process_exit(&self) {
        self.job_runtime.request_stop_for_process_exit();
    }

    async fn execute_command(
        &self,
        command: application::StatisticsApplicationCommand,
        submitted_at_ms: i64,
        connector_context: Option<novarocks_spi::connector::ConnectorRequestContext>,
    ) -> Result<StatisticsStatementResult, application::StatisticsApplicationError> {
        match command {
            application::StatisticsApplicationCommand::AnalyzeTable { target, columns } => {
                let context = connector_context.ok_or_else(|| {
                    application::StatisticsApplicationError::new(
                        "ANALYZE target capture requires an admitted execution context",
                    )
                })?;
                let capture = self
                    .target_resolver
                    .capture_table_object(&target, context.clone())?;
                let root = self
                    .root_scope
                    .begin_statistics_job()
                    .map_err(application::StatisticsApplicationError::new)?;
                let (owner, permit) = admit_statistics_job_root(root, &context).await?;
                let columns = match columns {
                    application::StatisticsColumnIntent::AllColumns => StatisticsColumns::All,
                    application::StatisticsColumnIntent::Explicit(columns) => {
                        StatisticsColumns::Explicit(
                            columns.into_iter().map(Arc::<str>::from).collect(),
                        )
                    }
                };
                let submitted = self
                    .job_runtime
                    .submit_admitted(
                        StatisticsJobCreate {
                            target: StatisticsTarget {
                                catalog: Arc::from(target.catalog),
                                namespace: Arc::from(target.namespace),
                                table: Arc::from(target.table),
                                object_id: Arc::from(capture.object_id),
                            },
                            columns,
                            submitted_at_ms,
                        },
                        owner,
                        permit,
                    )
                    .await
                    .map_err(|error| {
                        application::StatisticsApplicationError::new(error.to_string())
                    })?;
                if capture.job_admission
                    == novarocks_spi::connector::ConnectorTableJobAdmission::Detached
                {
                    return Ok(StatisticsStatementResult::JobSubmitted(submitted));
                }
                let terminal =
                    await_statistics_conclusion(&self.job_runtime, submitted.id, &context).await?;
                Ok(StatisticsStatementResult::JobSubmitted(terminal))
            }
            application::StatisticsApplicationCommand::ShowAnalyzeJobs => self
                .job_runtime
                .list()
                .await
                .map(StatisticsStatementResult::AnalyzeJobs)
                .map_err(|error| application::StatisticsApplicationError::new(error.to_string())),
            application::StatisticsApplicationCommand::CancelAnalyze { job_id } => self
                .job_runtime
                .request_cancel(StatisticsJobId::from_uuid(job_id), submitted_at_ms)
                .await
                .map(StatisticsStatementResult::JobCancellationRequested)
                .map_err(|error| application::StatisticsApplicationError::new(error.to_string())),
            application::StatisticsApplicationCommand::ShowTableStats { target } => self
                .table_statistics
                .show_table_stats(
                    &target.into(),
                    connector_context.ok_or_else(|| {
                        application::StatisticsApplicationError::new(
                            "SHOW TABLE STATS requires an admitted execution context",
                        )
                    })?,
                )
                .map(StatisticsStatementResult::TableStats)
                .map_err(application::StatisticsApplicationError::new),
        }
    }
}

impl application::StatisticsApplicationPort for FrontendStatisticsApplicationPort {
    fn execute(
        &self,
        command: application::StatisticsApplicationCommand,
        execution: Option<
            &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        >,
    ) -> Result<application::StatisticsApplicationResult, application::StatisticsApplicationError>
    {
        let connector_context = match (&command, execution) {
            (application::StatisticsApplicationCommand::AnalyzeTable { .. }, Some(execution)) => {
                Some(statistics_connector_context(
                    execution,
                    true,
                    &self.runtime,
                )?)
            }
            (application::StatisticsApplicationCommand::ShowTableStats { .. }, Some(execution)) => {
                Some(statistics_connector_context(
                    execution,
                    false,
                    &self.runtime,
                )?)
            }
            (application::StatisticsApplicationCommand::AnalyzeTable { .. }, None) => {
                return Err(application::StatisticsApplicationError::new(
                    "ANALYZE requires an admitted execution context",
                ));
            }
            (application::StatisticsApplicationCommand::ShowTableStats { .. }, None) => {
                return Err(application::StatisticsApplicationError::new(
                    "SHOW TABLE STATS requires an admitted execution context",
                ));
            }
            _ => None,
        };
        let at_ms = now_ms().map_err(application::StatisticsApplicationError::new)?;
        let result = tokio::task::block_in_place(|| {
            self.runtime
                .block_on(self.execute_command(command, at_ms, connector_context))
        })?;
        Ok(map_application_result(result))
    }
}

fn statistics_connector_context(
    execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    require_deadline: bool,
    runtime: &tokio::runtime::Handle,
) -> Result<
    novarocks_spi::connector::ConnectorRequestContext,
    application::StatisticsApplicationError,
> {
    let deadline = match execution.deadline() {
        Some(value) => value,
        None if require_deadline => {
            return Err(application::StatisticsApplicationError::new(
                "ANALYZE target capture requires an admitted deadline",
            ));
        }
        None => Instant::now()
            .checked_add(Duration::from_secs(30))
            .ok_or_else(|| {
                application::StatisticsApplicationError::new(
                    "statistics metadata-read deadline overflow",
                )
            })?,
    };
    crate::connector::query_connector_request_context_on_runtime(
        runtime,
        deadline,
        execution.cancellation().clone(),
    )
    .map_err(application::StatisticsApplicationError::new)
}
fn map_application_result(
    result: StatisticsStatementResult,
) -> application::StatisticsApplicationResult {
    match result {
        StatisticsStatementResult::JobSubmitted(job) => {
            application::StatisticsApplicationResult::JobSubmitted(job_view(job))
        }
        StatisticsStatementResult::JobCancellationRequested(job) => {
            application::StatisticsApplicationResult::JobCancellationRequested(job_view(job))
        }
        StatisticsStatementResult::AnalyzeJobs(jobs) => {
            application::StatisticsApplicationResult::AnalyzeJobs(
                jobs.into_iter().map(job_view).collect(),
            )
        }
        StatisticsStatementResult::TableStats(rows) => {
            application::StatisticsApplicationResult::TableStats(
                rows.into_iter()
                    .map(|row| application::StatisticsTableStatView {
                        metric: row.metric_name,
                        value: row.value,
                        status: row.status,
                        basis_version: row.basis_version,
                        source: row.source,
                        numeric_nature: row.numeric_nature,
                        basis_relation: row.basis_relation,
                    })
                    .collect(),
            )
        }
    }
}
fn job_view(job: StatisticsJob) -> application::StatisticsJobView {
    let finalization_failure = job.publication_finalization_failure.clone();
    let failure = job.failure.clone().or(finalization_failure.clone());
    application::StatisticsJobView {
        job_id: job.id.as_uuid(),
        operation_id: novarocks_spi::connector::LakePublicationId::try_from_uuid(
            job.publication_id.as_uuid(),
        )
        .expect("statistics publication IDs are UUIDv7"),
        state: match job.state {
            novarocks_statistics_application::StatisticsJobState::Active(phase) => {
                format!("{phase:?}")
            }
            novarocks_statistics_application::StatisticsJobState::Terminal(conclusion) => {
                format!("{conclusion:?}")
            }
        }
        .to_ascii_uppercase(),
        attempt: u32::from(job.query_attempt_id.is_some()),
        target: application::StatisticsTableTarget {
            catalog: job.target.catalog.to_string(),
            namespace: job.target.namespace.to_string(),
            table: job.target.table.to_string(),
        },
        error_kind: failure.as_ref().map(|_| {
            if finalization_failure.is_some() {
                "FINALIZATION".into()
            } else {
                "STATISTICS".into()
            }
        }),
        error_message: failure.map(|failure| failure.message.to_string()),
    }
}
fn now_ms() -> Result<i64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_millis()
        .try_into()
        .map_err(|_| "statistics time exceeds i64 milliseconds".into())
}

// Design: ADR-0169 (docs/adr/ADR-0169-read-only-hms-and-single-writer-admission.md)
async fn await_statistics_conclusion(
    runtime: &StatisticsJobRuntime,
    id: StatisticsJobId,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatisticsJob, application::StatisticsApplicationError> {
    await_statistics_conclusion_with_clock(runtime, id, context, now_ms).await
}

async fn await_statistics_conclusion_with_clock(
    runtime: &StatisticsJobRuntime,
    id: StatisticsJobId,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    clock: impl Fn() -> Result<i64, String>,
) -> Result<StatisticsJob, application::StatisticsApplicationError> {
    use novarocks_statistics_application::{StatisticsJobConclusion, StatisticsJobState};
    let wait = runtime.wait_for_conclusion(id);
    tokio::pin!(wait);
    let stopped = context.stop().stopped();
    tokio::pin!(stopped);
    let deadline = tokio::time::sleep_until(context.deadline().into());
    tokio::pin!(deadline);
    let mut cancelled = false;
    let mut cancellation_error = None;
    let terminal = tokio::select! {
        terminal = &mut wait => terminal,
        _ = &mut stopped => {
            cancelled = true;
            cancellation_error = request_statistics_cancellation(runtime, id, &clock).await;
            wait.await
        },
        _ = &mut deadline => {
            cancelled = true;
            cancellation_error = request_statistics_cancellation(runtime, id, &clock).await;
            wait.await
        },
    }
    .map_err(|error| application::StatisticsApplicationError::new(error.to_string()))?;
    if let Some(error) = cancellation_error {
        return Err(application::StatisticsApplicationError::new(error));
    }
    if cancelled {
        return Err(application::StatisticsApplicationError::new(
            "ANALYZE statement cancelled; statistics job has actually converged",
        ));
    }
    if terminal.state != StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded) {
        return Err(application::StatisticsApplicationError::new(
            terminal
                .failure
                .as_ref()
                .map(|failure| failure.message.to_string())
                .unwrap_or_else(|| format!("statistics job completed with {:?}", terminal.state)),
        ));
    }
    Ok(terminal)
}

/// Stop-aware query admission for a job root that has not been submitted yet.
/// Dropping the admission releases its queue/grant before the root completes.
async fn admit_statistics_job_root(
    root: PendingQueryRoot,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    (
        novarocks_workload_control::WorkOwner,
        novarocks_workload_control::QueryConcurrencyPermit,
    ),
    application::StatisticsApplicationError,
> {
    use novarocks_workload_control::CancellationReason;
    let admission = match root.owner.scope().admit_query() {
        Ok(admission) => admission,
        Err(error) => {
            root.owner.complete();
            return Err(application::StatisticsApplicationError::new(
                error.to_string(),
            ));
        }
    };
    let permit = {
        tokio::pin!(admission);
        tokio::select! {
            biased;
            _ = context.stop().stopped() => {
                root.owner.cancel(CancellationReason::Requested);
                Err(application::StatisticsApplicationError::new("ANALYZE cancelled before job submission"))
            },
            _ = tokio::time::sleep_until(context.deadline().into()) => {
                root.owner.cancel(CancellationReason::DeadlineExceeded);
                Err(application::StatisticsApplicationError::new("ANALYZE deadline elapsed before job submission"))
            },
            permit = &mut admission => permit.map_err(|error| application::StatisticsApplicationError::new(error.to_string())),
        }
    };
    match permit {
        Ok(permit) => {
            if let Err(error) =
                novarocks_spi::connector::ConnectorOperationControl::check_active(context)
            {
                root.owner.cancel(if context.is_cancelled() {
                    CancellationReason::Requested
                } else {
                    CancellationReason::DeadlineExceeded
                });
                drop(permit);
                root.owner.complete_after_terminal_cancel_settled();
                return Err(application::StatisticsApplicationError::new(
                    error.to_string(),
                ));
            }
            Ok((root.owner, permit))
        }
        Err(error) => {
            root.owner.complete_after_terminal_cancel_settled();
            Err(error)
        }
    }
}

/// Clock failure cannot prevent cancellation or bypass actual convergence.
/// Without a new observation time, preserve the repository's existing times.
async fn request_statistics_cancellation(
    runtime: &StatisticsJobRuntime,
    id: StatisticsJobId,
    clock: &impl Fn() -> Result<i64, String>,
) -> Option<String> {
    use novarocks_statistics_application::StatisticsRepositoryErrorKind;
    let (result, clock_error) = match clock() {
        Ok(at_ms) => (runtime.request_cancel(id, at_ms).await, None),
        Err(error) => (runtime.request_cancel_without_time(id).await, Some(error)),
    };
    match result {
        Ok(_) => clock_error,
        Err(error) if error.kind() == StatisticsRepositoryErrorKind::NotFound => clock_error,
        Err(error) => Some(error.to_string()),
    }
}

#[cfg(test)]
mod admission_and_conclusion_tests {
    use std::future::Future;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    use novarocks_statistics_application::{
        StatisticsAttemptError, StatisticsFailure, StatisticsJobConclusion, StatisticsJobState,
        StatisticsPublicationFact, StatisticsPublicationOutcome,
    };
    use novarocks_workload_control::{ResourceConfig, WorkScope, WorkloadConfig, WorkloadControl};

    use super::*;

    fn control() -> WorkloadControl {
        let control = WorkloadControl::try_new(
            WorkloadConfig {
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 16 * 1024 * 1024,
                control_bytes: 1024 * 1024,
                per_scope_bytes: 8 * 1024 * 1024,
            },
        )
        .expect("control");
        control.mark_ready().expect("ready");
        control
    }

    fn context(
        stop: &novarocks_spi::connector::ConnectorStopOwner,
        deadline: Instant,
    ) -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            deadline,
            stop.view(),
            64 * 1024,
            1024 * 1024,
        )
        .expect("context")
    }

    async fn is_pending<T>(future: std::pin::Pin<&mut impl Future<Output = T>>) -> bool {
        let mut future = future;
        std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx).is_pending()))
            .await
    }

    #[tokio::test]
    async fn statement_stop_releases_the_unsubmitted_root_and_admission_record() {
        assert_unsubmitted_root_control(false).await;
    }

    #[tokio::test]
    async fn statement_deadline_releases_the_unsubmitted_root_and_admission_record() {
        assert_unsubmitted_root_control(true).await;
    }

    async fn assert_unsubmitted_root_control(deadline_expired: bool) {
        let control = control();
        let held = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .expect("held root");
        let held_permit = held.owner.scope().admit_query().unwrap().await.unwrap();
        let pending = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .expect("pending root");
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let request = context(&stop, Instant::now() + Duration::from_secs(30));
        let result = if deadline_expired {
            let expired = context(&stop, Instant::now());
            tokio::time::timeout(
                Duration::from_secs(1),
                admit_statistics_job_root(pending, &expired),
            )
            .await
            .expect("deadline interrupts admission")
        } else {
            let wait = admit_statistics_job_root(pending, &request);
            tokio::pin!(wait);
            assert!(is_pending(wait.as_mut()).await);
            assert_eq!(control.observation().snapshot().waiting_records, 1);
            stop.request_stop();
            tokio::time::timeout(Duration::from_secs(1), &mut wait)
                .await
                .expect("statement stop interrupts admission")
        };
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("an interrupted statement must not submit a root"),
        };
        assert!(error.to_string().contains(if deadline_expired {
            "deadline elapsed"
        } else {
            "cancelled"
        }));
        let snapshot = control.observation().snapshot();
        assert_eq!(snapshot.admitted_queries, 1, "only the held query remains");
        assert_eq!(snapshot.root_responsibilities, 1, "pending root completed");
        assert_eq!(snapshot.waiting_records, 0);
        assert_eq!(snapshot.admission_records, 0);
        drop(held_permit);
        held.owner.complete();
    }

    struct ControlledExecutor {
        started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
        exited: Arc<AtomicBool>,
        fail: bool,
    }

    impl StatisticsAttemptExecutor for ControlledExecutor {
        fn prepare(
            &self,
            _job: &StatisticsJob,
            scope: &WorkScope,
        ) -> Result<(), StatisticsAttemptError> {
            self.started
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .expect("test releases the actual executor");
            self.exited.store(true, Ordering::SeqCst);
            scope.check().map_err(|error| {
                StatisticsAttemptError::Cancelled(StatisticsFailure {
                    message: Arc::from(error.to_string()),
                })
            })?;
            if self.fail {
                Err(StatisticsAttemptError::Failed(StatisticsFailure {
                    message: Arc::from("controlled statistics failure"),
                }))
            } else {
                Ok(())
            }
        }

        fn collect(
            &self,
            _job: &StatisticsJob,
            _scope: &WorkScope,
        ) -> Result<(), StatisticsAttemptError> {
            Ok(())
        }

        fn publish(
            &self,
            _job: &StatisticsJob,
            _scope: &WorkScope,
        ) -> Result<StatisticsPublicationOutcome, StatisticsAttemptError> {
            Ok(StatisticsPublicationOutcome {
                fact: StatisticsPublicationFact::KnownCommitted,
                finalization_failure: None,
            })
        }
    }

    #[derive(Clone, Copy)]
    enum Outcome {
        Success,
        Failure,
        StatementStop,
        StatementDeadline,
        CancellationClockFailure,
    }

    #[tokio::test]
    async fn successful_analyze_wait_returns_after_actual_executor_exit() {
        assert_statement_wait(Outcome::Success).await;
    }

    #[tokio::test]
    async fn failed_analyze_wait_reports_failure_after_actual_executor_exit() {
        assert_statement_wait(Outcome::Failure).await;
    }

    #[tokio::test]
    async fn cancelled_analyze_wait_holds_the_statement_until_actual_executor_exit() {
        assert_statement_wait(Outcome::StatementStop).await;
    }

    #[tokio::test]
    async fn expired_analyze_wait_holds_the_statement_until_actual_executor_exit() {
        assert_statement_wait(Outcome::StatementDeadline).await;
    }

    #[tokio::test]
    async fn cancellation_clock_error_cannot_bypass_actual_executor_exit() {
        assert_statement_wait(Outcome::CancellationClockFailure).await;
    }

    async fn assert_statement_wait(outcome: Outcome) {
        let control = control();
        let service = StatisticsJobService::new();
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = mpsc::channel();
        let exited = Arc::new(AtomicBool::new(false));
        let runtime = StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(ControlledExecutor {
                started: Mutex::new(Some(started)),
                release: Mutex::new(release_rx),
                exited: Arc::clone(&exited),
                fail: matches!(outcome, Outcome::Failure),
            }),
            tokio::runtime::Handle::current(),
        );
        let root = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .expect("job root");
        let permit = root.owner.scope().admit_query().unwrap().await.unwrap();
        let submitted = runtime
            .submit_admitted(
                StatisticsJobCreate {
                    target: StatisticsTarget {
                        catalog: Arc::from("ice"),
                        namespace: Arc::from("db"),
                        table: Arc::from("t"),
                        object_id: Arc::from(&b"object"[..]),
                    },
                    columns: StatisticsColumns::All,
                    submitted_at_ms: 1,
                },
                root.owner,
                permit,
            )
            .await
            .expect("submit");
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .expect("executor starts")
            .expect("started message");
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let deadline = if matches!(outcome, Outcome::StatementDeadline) {
            Instant::now()
        } else {
            Instant::now() + Duration::from_secs(30)
        };
        let context = context(&stop, deadline);
        if matches!(
            outcome,
            Outcome::StatementStop | Outcome::CancellationClockFailure
        ) {
            stop.request_stop();
        }
        let clock = || {
            if matches!(outcome, Outcome::CancellationClockFailure) {
                Err("controlled wall-clock failure".to_string())
            } else {
                now_ms()
            }
        };
        let wait = await_statistics_conclusion_with_clock(&runtime, submitted.id, &context, clock);
        tokio::pin!(wait);
        assert!(is_pending(wait.as_mut()).await);
        if matches!(outcome, Outcome::StatementDeadline) {
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    assert!(is_pending(wait.as_mut()).await);
                    if service.list().await.unwrap().remove(0).cancel_requested {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("elapsed statement deadline requests job cancellation");
        }
        assert!(!exited.load(Ordering::SeqCst));
        let before = service.list().await.unwrap().remove(0);
        assert!(!before.convergence.is_complete());
        if matches!(
            outcome,
            Outcome::StatementStop | Outcome::CancellationClockFailure
        ) {
            assert!(before.cancel_requested);
        }
        release.send(()).expect("release actual executor");
        let result = tokio::time::timeout(Duration::from_secs(1), &mut wait)
            .await
            .expect("statement converges");
        assert!(exited.load(Ordering::SeqCst));
        let terminal = service.list().await.unwrap().remove(0);
        assert!(terminal.convergence.is_complete());
        if matches!(
            outcome,
            Outcome::StatementStop | Outcome::StatementDeadline | Outcome::CancellationClockFailure
        ) {
            assert_eq!(
                terminal.state,
                StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled)
            );
        }
        assert_eq!(control.observation().snapshot().admitted_queries, 0);
        match outcome {
            Outcome::Success => {
                assert_eq!(result.unwrap(), terminal);
                assert_eq!(
                    terminal.state,
                    StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
                );
            }
            Outcome::Failure => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("controlled statistics failure")
            ),
            Outcome::CancellationClockFailure => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("controlled wall-clock failure")
            ),
            Outcome::StatementStop | Outcome::StatementDeadline => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("actually converged")
            ),
        }
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }
}
