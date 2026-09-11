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
//! captures. It deliberately owns neither a job ledger nor a worker. A live
//! ANALYZE needs both a product-supplied root `WorkOwner` and a three-phase
//! executor; without them the entrypoint fails closed until role composition
//! (T12) supplies the binding.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use novarocks_statistics_application::{
    StatisticsAttemptExecutor, StatisticsColumns, StatisticsJob, StatisticsJobCreate,
    StatisticsJobId, StatisticsJobRepository, StatisticsTarget, StatisticsWorker,
};
use novarocks_workload_control::WorkOwner;
use uuid::Uuid;

use super::application;
use super::model::StatisticsJobTarget;
use crate::workload_lifecycle::FrontendServingLifecycle;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzeTableStatement {
    pub target: StatisticsJobTarget,
    pub columns: application::StatisticsColumnIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowAnalyzeJobsStatement {
    pub target: Option<StatisticsJobTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelAnalyzeStatement {
    pub job_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowTableStatsStatement {
    pub target: StatisticsJobTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsStatement {
    AnalyzeTable(AnalyzeTableStatement),
    ShowAnalyzeJobs(ShowAnalyzeJobsStatement),
    CancelAnalyze(CancelAnalyzeStatement),
    ShowTableStats(ShowTableStatsStatement),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsStatementResult {
    JobSubmitted(StatisticsJob),
    JobCompleted(StatisticsJob),
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

pub trait StatisticsJobTargetResolver: Send + Sync {
    fn capture_table_object(
        &self,
        target: &StatisticsJobTarget,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<application::StatisticsTargetCapture, String>;
}

/// T12's role composition supplies a root for each independently-owned
/// background statistics business request. There is intentionally no default
/// implementation: borrowing the statement observer would let observer
/// cancellation destroy the job root.
pub trait StatisticsJobRootScopeSource: Send + Sync {
    fn begin_statistics_job(&self) -> Result<WorkOwner, String>;
}

struct UnavailableStatisticsJobTargetResolver;

impl StatisticsJobTargetResolver for UnavailableStatisticsJobTargetResolver {
    fn capture_table_object(
        &self,
        _target: &StatisticsJobTarget,
        _context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<application::StatisticsTargetCapture, String> {
        Err("ANALYZE is unavailable until the frontend statistics target resolver is bound".into())
    }
}

struct StatisticsTargetResolverAdapter {
    inner: Arc<dyn application::StatisticsTargetResolver>,
}
struct StatisticsTableReaderAdapter {
    inner: Arc<dyn application::StatisticsTableReader>,
}

impl StatisticsJobTargetResolver for StatisticsTargetResolverAdapter {
    fn capture_table_object(
        &self,
        target: &StatisticsJobTarget,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<application::StatisticsTargetCapture, String> {
        self.inner
            .capture_table_object(
                &application::StatisticsTableTarget {
                    catalog: target.catalog.clone(),
                    namespace: target.namespace.clone(),
                    table: target.table.clone(),
                },
                context,
            )
            .map_err(|error| error.to_string())
    }
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

struct TargetResolverSlot {
    resolver: std::sync::RwLock<Arc<dyn StatisticsJobTargetResolver>>,
    bound: std::sync::atomic::AtomicBool,
}
struct RootScopeSlot {
    source: Mutex<Option<Arc<dyn StatisticsJobRootScopeSource>>>,
}

#[derive(Clone)]
pub struct StatisticsApplicationService {
    repository: StatisticsJobRepository,
    target_resolver: Arc<TargetResolverSlot>,
    root_scope: Arc<RootScopeSlot>,
}

impl StatisticsApplicationService {
    pub fn new() -> Self {
        Self {
            repository: StatisticsJobRepository::new(),
            target_resolver: Arc::new(TargetResolverSlot {
                resolver: std::sync::RwLock::new(Arc::new(UnavailableStatisticsJobTargetResolver)),
                bound: std::sync::atomic::AtomicBool::new(false),
            }),
            root_scope: Arc::new(RootScopeSlot {
                source: Mutex::new(None),
            }),
        }
    }
    pub fn repository(&self) -> StatisticsJobRepository {
        self.repository.clone()
    }
    pub fn bind_target_resolver(
        &self,
        resolver: Arc<dyn StatisticsJobTargetResolver>,
    ) -> Result<(), String> {
        if self
            .target_resolver
            .bound
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return Err("statistics target resolver is already bound".into());
        }
        *self
            .target_resolver
            .resolver
            .write()
            .map_err(|_| "statistics target resolver lock poisoned".to_string())? = resolver;
        Ok(())
    }
    pub fn bind_job_root_scope_source(
        &self,
        source: Arc<dyn StatisticsJobRootScopeSource>,
    ) -> Result<(), String> {
        let mut slot = self
            .root_scope
            .source
            .lock()
            .map_err(|_| "statistics root scope lock poisoned".to_string())?;
        if slot.replace(source).is_some() {
            return Err("statistics job root scope source is already bound".into());
        }
        Ok(())
    }
    pub async fn execute(
        &self,
        statement: StatisticsStatement,
        submitted_at_ms: i64,
        table_statistics: &dyn TableStatisticsReader,
        connector_context: Option<novarocks_spi::connector::ConnectorRequestContext>,
    ) -> Result<StatisticsStatementResult, StatisticsApplicationError> {
        match statement {
            StatisticsStatement::AnalyzeTable(statement) => {
                let context = connector_context.ok_or_else(|| {
                    StatisticsApplicationError::target_resolution(
                        "ANALYZE target capture requires an admitted execution context",
                    )
                })?;
                let resolver = self
                    .target_resolver
                    .resolver
                    .read()
                    .map_err(|_| {
                        StatisticsApplicationError::target_resolution(
                            "statistics target resolver lock poisoned",
                        )
                    })?
                    .clone();
                let capture = resolver
                    .capture_table_object(&statement.target, context)
                    .map_err(StatisticsApplicationError::target_resolution)?;
                let owner = self
                    .root_scope
                    .source
                    .lock()
                    .map_err(|_| {
                        StatisticsApplicationError::configuration(
                            "statistics root scope lock poisoned",
                        )
                    })?
                    .clone()
                    .ok_or_else(|| {
                        StatisticsApplicationError::configuration(
                            "ANALYZE requires a bound statistics WorkOwner source",
                        )
                    })?
                    .begin_statistics_job()
                    .map_err(StatisticsApplicationError::configuration)?;
                let columns = match statement.columns {
                    application::StatisticsColumnIntent::AllColumns => StatisticsColumns::All,
                    application::StatisticsColumnIntent::Explicit(columns) => {
                        StatisticsColumns::Explicit(
                            columns.into_iter().map(Arc::<str>::from).collect(),
                        )
                    }
                };
                self.repository
                    .create(
                        StatisticsJobCreate {
                            target: StatisticsTarget {
                                catalog: Arc::from(statement.target.catalog),
                                namespace: Arc::from(statement.target.namespace),
                                table: Arc::from(statement.target.table),
                                object_id: Arc::from(capture.object_id),
                            },
                            columns,
                            submitted_at_ms,
                        },
                        owner,
                    )
                    .await
                    .map(StatisticsStatementResult::JobSubmitted)
                    .map_err(StatisticsApplicationError::repository)
            }
            StatisticsStatement::ShowAnalyzeJobs(statement) => {
                let mut jobs = self
                    .repository
                    .list()
                    .await
                    .map_err(StatisticsApplicationError::repository)?;
                if let Some(target) = statement.target {
                    jobs.retain(|job| {
                        job.target.catalog.as_ref() == target.catalog
                            && job.target.namespace.as_ref() == target.namespace
                            && job.target.table.as_ref() == target.table
                    });
                }
                Ok(StatisticsStatementResult::AnalyzeJobs(jobs))
            }
            StatisticsStatement::CancelAnalyze(statement) => self
                .repository
                .request_cancel(
                    StatisticsJobId::from_uuid(statement.job_id),
                    submitted_at_ms,
                )
                .await
                .map(StatisticsStatementResult::JobCancellationRequested)
                .map_err(StatisticsApplicationError::repository),
            StatisticsStatement::ShowTableStats(statement) => table_statistics
                .show_table_stats(
                    &statement.target,
                    connector_context.ok_or_else(|| {
                        StatisticsApplicationError::table_statistics(
                            "SHOW TABLE STATS requires an admitted execution context",
                        )
                    })?,
                )
                .map(StatisticsStatementResult::TableStats)
                .map_err(StatisticsApplicationError::table_statistics),
        }
    }
}
impl Default for StatisticsApplicationService {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsApplicationErrorKind {
    Repository,
    TableStatistics,
    TargetResolution,
    Configuration,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsApplicationError {
    kind: StatisticsApplicationErrorKind,
    message: String,
}
impl StatisticsApplicationError {
    fn repository(error: novarocks_statistics_application::StatisticsRepositoryError) -> Self {
        Self {
            kind: StatisticsApplicationErrorKind::Repository,
            message: error.to_string(),
        }
    }
    fn table_statistics(error: impl Into<String>) -> Self {
        Self {
            kind: StatisticsApplicationErrorKind::TableStatistics,
            message: error.into(),
        }
    }
    fn target_resolution(error: impl Into<String>) -> Self {
        Self {
            kind: StatisticsApplicationErrorKind::TargetResolution,
            message: error.into(),
        }
    }
    fn configuration(error: impl Into<String>) -> Self {
        Self {
            kind: StatisticsApplicationErrorKind::Configuration,
            message: error.into(),
        }
    }
    pub const fn kind(&self) -> StatisticsApplicationErrorKind {
        self.kind
    }
}
impl fmt::Display for StatisticsApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for StatisticsApplicationError {}

pub struct FrontendStatisticsApplicationPort {
    service: StatisticsApplicationService,
    table_statistics: std::sync::RwLock<Option<Arc<dyn TableStatisticsReader>>>,
    runtime: tokio::runtime::Handle,
    core_executor: Mutex<Option<Arc<dyn StatisticsAttemptExecutor>>>,
}
impl FrontendStatisticsApplicationPort {
    pub fn new(service: StatisticsApplicationService, runtime: tokio::runtime::Handle) -> Self {
        Self {
            service,
            table_statistics: std::sync::RwLock::new(None),
            runtime,
            core_executor: Mutex::new(None),
        }
    }
    pub(crate) fn with_workload_lifecycle(self, _lifecycle: FrontendServingLifecycle) -> Self {
        self
    }
    pub fn bind_table_statistics_reader(
        &self,
        reader: Arc<dyn TableStatisticsReader>,
    ) -> Result<(), String> {
        let mut slot = self
            .table_statistics
            .write()
            .map_err(|_| "statistics table reader lock poisoned".to_string())?;
        if slot.replace(reader).is_some() {
            return Err("statistics table reader is already bound".into());
        }
        Ok(())
    }
    pub fn bind_statistics_target_resolver(
        &self,
        resolver: Arc<dyn application::StatisticsTargetResolver>,
    ) -> Result<(), String> {
        self.service
            .bind_target_resolver(Arc::new(StatisticsTargetResolverAdapter {
                inner: resolver,
            }))
    }
    pub fn bind_statistics_job_root_scope_source(
        &self,
        source: Arc<dyn StatisticsJobRootScopeSource>,
    ) -> Result<(), String> {
        self.service.bind_job_root_scope_source(source)
    }
    pub fn bind_statistics_core_executor(
        &self,
        executor: Arc<dyn StatisticsAttemptExecutor>,
    ) -> Result<(), String> {
        let mut slot = self
            .core_executor
            .lock()
            .map_err(|_| "statistics core executor lock poisoned".to_string())?;
        if slot.replace(executor).is_some() {
            return Err("statistics core executor is already bound".into());
        }
        Ok(())
    }
    pub async fn shutdown_worker_until(&self, _deadline: Instant) -> Result<(), String> {
        Ok(())
    }
    pub fn request_worker_stop_for_process_exit(&self) {}
    fn run_bound_job(&self, at_ms: i64) -> Result<Option<StatisticsJob>, String> {
        let executor = self
            .core_executor
            .lock()
            .map_err(|_| "statistics core executor lock poisoned".to_string())?
            .clone()
            .ok_or_else(|| {
                "ANALYZE requires a bound three-phase statistics executor".to_string()
            })?;
        tokio::task::block_in_place(|| {
            self.runtime
                .block_on(StatisticsWorker::new(self.service.repository(), executor).run_one(at_ms))
        })
        .map_err(|error| error.to_string())
    }
}

impl application::StatisticsApplicationPort for FrontendStatisticsApplicationPort {
    fn execute(
        &self,
        command: application::StatisticsApplicationCommand,
        execution: Option<&crate::common::admitted_query_context::QueryExecutionContext>,
    ) -> Result<application::StatisticsApplicationResult, application::StatisticsApplicationError>
    {
        let connector_context = match (&command, execution) {
            (application::StatisticsApplicationCommand::AnalyzeTable { .. }, Some(execution)) => {
                Some(statistics_connector_context(execution, true)?)
            }
            (application::StatisticsApplicationCommand::ShowTableStats { .. }, Some(execution)) => {
                Some(statistics_connector_context(execution, false)?)
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
        let statement = match command {
            application::StatisticsApplicationCommand::AnalyzeTable { target, columns } => {
                StatisticsStatement::AnalyzeTable(AnalyzeTableStatement {
                    target: target.into(),
                    columns,
                })
            }
            application::StatisticsApplicationCommand::ShowAnalyzeJobs => {
                StatisticsStatement::ShowAnalyzeJobs(ShowAnalyzeJobsStatement { target: None })
            }
            application::StatisticsApplicationCommand::CancelAnalyze { job_id } => {
                StatisticsStatement::CancelAnalyze(CancelAnalyzeStatement { job_id })
            }
            application::StatisticsApplicationCommand::ShowTableStats { target } => {
                StatisticsStatement::ShowTableStats(ShowTableStatsStatement {
                    target: target.into(),
                })
            }
        };
        let at_ms = now_ms().map_err(application::StatisticsApplicationError::new)?;
        if matches!(statement, StatisticsStatement::AnalyzeTable(_))
            && self
                .core_executor
                .lock()
                .map_err(|_| {
                    application::StatisticsApplicationError::new(
                        "statistics core executor lock poisoned",
                    )
                })?
                .is_none()
        {
            return Err(application::StatisticsApplicationError::new(
                "ANALYZE requires a bound statistics WorkOwner source and three-phase executor",
            ));
        }
        let reader = self
            .table_statistics
            .read()
            .map_err(|_| {
                application::StatisticsApplicationError::new(
                    "statistics table reader lock poisoned",
                )
            })?
            .clone()
            .unwrap_or_else(|| Arc::new(UnboundTableStatisticsReader));
        let result = tokio::task::block_in_place(|| {
            self.runtime.block_on(self.service.execute(
                statement,
                at_ms,
                reader.as_ref(),
                connector_context,
            ))
        })
        .map_err(|error| application::StatisticsApplicationError::new(error.to_string()))?;
        let result = match result {
            StatisticsStatementResult::JobSubmitted(job) => match self
                .run_bound_job(at_ms)
                .map_err(application::StatisticsApplicationError::new)?
            {
                Some(completed) => StatisticsStatementResult::JobCompleted(completed),
                None => StatisticsStatementResult::JobSubmitted(job),
            },
            other => other,
        };
        Ok(map_application_result(result))
    }
}

fn statistics_connector_context(
    execution: &crate::common::admitted_query_context::QueryExecutionContext,
    require_deadline: bool,
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
    novarocks_spi::connector::ConnectorRequestContext::try_new(
        deadline,
        Arc::new(StatisticsApplicationCancellation(
            execution.cancellation().clone(),
        )),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| application::StatisticsApplicationError::new(error.to_string()))
}
struct StatisticsApplicationCancellation(crate::common::query_cancellation::QueryCancellationView);
impl novarocks_spi::connector::ConnectorCancellation for StatisticsApplicationCancellation {
    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}
impl application::StatisticsTargetResolverSink for FrontendStatisticsApplicationPort {
    fn bind_statistics_target_resolver(
        &self,
        resolver: Arc<dyn application::StatisticsTargetResolver>,
    ) -> Result<(), String> {
        self.bind_statistics_target_resolver(resolver)
    }
}
impl application::StatisticsTableReaderSink for FrontendStatisticsApplicationPort {
    fn bind_statistics_table_reader(
        &self,
        reader: Arc<dyn application::StatisticsTableReader>,
    ) -> Result<(), String> {
        self.bind_table_statistics_reader(Arc::new(StatisticsTableReaderAdapter { inner: reader }))
    }
}
impl application::StatisticsAttemptExecutorSink for FrontendStatisticsApplicationPort {
    fn bind_statistics_attempt_executor(
        &self,
        _executor: Arc<dyn application::StatisticsAttemptExecutor>,
    ) -> Result<(), String> {
        Err("legacy combined statistics attempt executor cannot bind: role composition must provide a WorkOwner source and three-phase statistics executor".into())
    }
}
struct UnboundTableStatisticsReader;
impl TableStatisticsReader for UnboundTableStatisticsReader {
    fn show_table_stats(
        &self,
        _target: &StatisticsJobTarget,
        _context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<StatisticsTableStatRow>, String> {
        Err(
            "SHOW TABLE STATS is unavailable until the frontend statistics table reader is bound"
                .into(),
        )
    }
}
fn map_application_result(
    result: StatisticsStatementResult,
) -> application::StatisticsApplicationResult {
    match result {
        StatisticsStatementResult::JobSubmitted(job) => {
            application::StatisticsApplicationResult::JobSubmitted(job_view(job))
        }
        StatisticsStatementResult::JobCompleted(job) => {
            application::StatisticsApplicationResult::JobCompleted(job_view(job))
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
