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

//! Frontend-owned SQL admission and native assembly boundary.

use std::sync::Arc;

use crate::catalog_application::information_schema;
use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::catalog_application::query_materializer::{
    build_catalog_service_provider,
    build_catalog_service_provider_with_bindings_and_query_local_overlays,
};
use crate::connector::connector_planning_context_for_query;
use crate::mv::domain::readiness::{MvCandidateReader, MvReadinessPort};
use crate::native::fragment_encoder::encode_native_fragment_bundle;
use crate::query_execution::compiler::{
    freeze_query_mv_rewrite_definition_index, query_catalog_service_snapshot,
    query_statistics_snapshot,
};
use crate::query_execution::completion::{
    PreReadyRetryBoundary, PreparedDistributedAttempt, PreparedDistributedAttemptFactory,
};
use crate::query_execution::completion::{
    PreparedDistributedQuery as PreparedQueryDistributedOperation, PreparedQueryOperation,
};
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::kernels::{
    QueryPreparationKernel, SystemTableQueryKernel, ViewExecutionKernel,
};
use crate::query_execution::planning::sql_cancellation_observation;
use crate::query_execution::planning::time_travel::{
    TimeTravelRewriteError, has_time_travel_refs, rewrite_time_travel_refs,
};
use crate::query_execution::post_compile::PostCompileIntent;
use novarocks_parser::ast::{ExplainFormat, ExplainQuery, Query, Statement};
use novarocks_physical_plan::{
    MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, ScanReadBudget,
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::{
    QueryExecutionContext, RequestContext, StatementAdmissionContext,
};
use novarocks_query_application::statement_effect::StatementEffectTracker;
use novarocks_query_application::view::ViewRequestContext;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_sql::analyze_error::AnalyzeError;
use novarocks_sql::compiler::{
    ExplainLevel, SqlAnalyzeRequest, SqlCompileControl, SqlCompileError, SqlCompileIntent,
    SqlCompiler, SqlOptimizeRequest, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
};
use novarocks_sql::planning::catalog::TableLookupMode;

/// Preserves SQL analyze-domain facts until the session still has the original
/// SQL source required to render a user location.
#[derive(Debug)]
pub(crate) enum FrontendQueryCompilerError {
    Engine(String),
    Analyze(AnalyzeError),
}

impl FrontendQueryCompilerError {
    fn from_compile(error: SqlCompileError) -> Self {
        match error {
            SqlCompileError::Analyze(error) => Self::Analyze(error),
            error => Self::Engine(error.to_string()),
        }
    }
}

impl From<String> for FrontendQueryCompilerError {
    fn from(error: String) -> Self {
        Self::Engine(error)
    }
}

impl From<TimeTravelRewriteError> for FrontendQueryCompilerError {
    fn from(error: TimeTravelRewriteError) -> Self {
        match error {
            TimeTravelRewriteError::Engine(error) => Self::Engine(error),
            TimeTravelRewriteError::Analyze(error) => Self::Analyze(error),
        }
    }
}

/// A lake-native MV target is a physical provider table, but it is not an
/// ordinary external relation once this frontend has recorded it as an MV.
/// If startup quarantined that target, admitting its provider scan would let a
/// stale MV publication bypass the readiness boundary through plain SQL.
fn reject_quarantined_mv_targets(
    bindings: &QueryTableBindingStore,
    readiness: &MvReadinessPort,
) -> Result<(), FrontendQueryCompilerError> {
    for (_, binding) in bindings.captured_bindings() {
        let identity =
            novarocks_sql::planning::catalog::materialization_identity_facts(&binding.resolved);
        let target = novarocks_sql::planning::mv::SqlMvTarget {
            catalog: Some(identity.catalog().to_string()),
            database: identity.namespace().to_string(),
            name: identity.table().to_string(),
        };
        // A target closed to management is still readable: what this rejects
        // is a projection in doubt, not one whose owner has yet to readmit it.
        match readiness.query_admission(&target) {
            Ok(
                novarocks_mv_application::readiness::MvQueryAdmission::NotAnMv
                | novarocks_mv_application::readiness::MvQueryAdmission::Admitted,
            ) => {}
            Ok(novarocks_mv_application::readiness::MvQueryAdmission::Quarantined(_)) => {
                return Err(FrontendQueryCompilerError::Engine(format!(
                    "unknown table: {}.{}",
                    identity.namespace(),
                    identity.table()
                )));
            }
            Err(error) => return Err(FrontendQueryCompilerError::Engine(error.to_string())),
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct FrontendQueryCompiler {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    query: QueryPreparationKernel,
    view: ViewExecutionKernel,
    system_tables: SystemTableQueryKernel,
    mv_readiness: Arc<MvReadinessPort>,
    mv_candidate_reader: MvCandidateReader,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    /// The bounded lane every owner that may reach a remote catalog answers
    /// on, so one slow catalog cannot stall the statement threads.
    connector_blocking_io: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
}

enum RetryCompletionTemplate {
    Result,
    Profile {
        planning_started_at: std::time::Instant,
    },
}

impl RetryCompletionTemplate {
    fn from_first_round(intent: &PostCompileIntent) -> Self {
        match intent {
            PostCompileIntent::Result => Self::Result,
            PostCompileIntent::Profile {
                planning_elapsed, ..
            } => Self::Profile {
                planning_started_at: std::time::Instant::now() - *planning_elapsed,
            },
        }
    }

    fn next_round_intent(&self) -> PostCompileIntent {
        match self {
            Self::Result => PostCompileIntent::Result,
            Self::Profile {
                planning_started_at,
            } => PostCompileIntent::Profile {
                planning_elapsed: planning_started_at.elapsed(),
                execution_started_at: std::time::Instant::now(),
            },
        }
    }
}

/// Instantiates a new attempt from one already frozen logical execution.
///
/// It owns the exact logical request and immutable attempt template, not the
/// means to create either. A pre-ready topology retry derives only what an
/// attempt actually owns from the admitted topology. The coordinator remains
/// the sole owner that can mint the fresh attempt identity and credentials.
///
/// The previous shape returned to the compiler and re-ran analysis, the
/// optimizer, MV candidate discovery and a fresh statistics snapshot. That was
/// not merely wasteful: two rounds of one statement could disagree about plan
/// shape because the statistics under them had moved, while both still claimed
/// to be the same query.
struct FrontendDistributedAttemptFactory {
    /// The exact application request and static attempt template produced by
    /// the first and only finalization. A replacement has no compiler,
    /// Catalog observation, scan negotiation, or native template encoder.
    logical_execution: Arc<crate::query_execution::contract::RestartableReadExecution>,
    statement: StatementAdmissionContext,
    completion: RetryCompletionTemplate,
    effect_tracker: StatementEffectTracker,
}

impl PreparedDistributedAttemptFactory for FrontendDistributedAttemptFactory {
    fn instantiate(
        &mut self,
        topology: novarocks_query_application::api::BackendTopologySnapshot,
    ) -> Result<PreparedDistributedAttempt, DistributedQueryError> {
        let execution = self.statement.for_topology(topology);
        let request = self
            .logical_execution
            .instantiate_attempt(execution.execution());
        Ok(PreparedDistributedAttempt::new(
            request,
            match self.completion.next_round_intent() {
                PostCompileIntent::Result => {
                    crate::query_execution::completion::PreparedQueryCompletion::result()
                }
                PostCompileIntent::Profile {
                    planning_elapsed,
                    execution_started_at,
                } => crate::query_execution::completion::PreparedQueryCompletion::profile(
                    self.logical_execution.shared_plan().ok_or_else(|| {
                        DistributedQueryError::new(
                            DistributedQueryErrorKind::ContractViolation,
                            "EXPLAIN ANALYZE of a completed plan renders from the plan itself",
                        )
                    })?,
                    planning_elapsed,
                    execution_started_at,
                ),
            },
        ))
    }
}

impl PreReadyRetryBoundary for FrontendDistributedAttemptFactory {
    fn permit_pre_ready_retry(&self) -> Result<(), DistributedQueryError> {
        self.effect_tracker
            .issue_topology_retry_permit()
            .map(|_| ())
            .map_err(|error| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::Rejected,
                    format!("topology retry is not effect-free: {error:?}"),
                )
            })
    }

    fn close_after_control_ready(&self) {
        self.effect_tracker.close_after_control_ready();
    }

    fn close_after_stage_or_start(&self) {
        self.effect_tracker.close_after_stage_or_start();
    }
}

impl FrontendQueryCompiler {
    pub(crate) fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        query: QueryPreparationKernel,
        view: ViewExecutionKernel,
        system_tables: SystemTableQueryKernel,
        mv_readiness: Arc<MvReadinessPort>,
        mv_candidate_reader: MvCandidateReader,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        connector_blocking_io: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
    ) -> Self {
        Self {
            functions,
            query,
            view,
            system_tables,
            mv_readiness,
            mv_candidate_reader,
            mv_storage_observation,
            connector_blocking_io,
        }
    }

    pub(crate) fn prepare_statement(
        &self,
        statement: &Statement,
        context: &RequestContext,
        query_options: Option<QueryOptions>,
        scope: &novarocks_workload_control::WorkScope,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
        let connector_planning_context = connector_planning_context_for_query(
            query_options.as_ref(),
            context.execution().cancellation().clone(),
        )?;
        let connector_context = connector_planning_context.request();
        let current_catalog = context.session().current_catalog();
        let current_database = context.session().current_database();

        match statement {
            // A logical EXPLAIN asks about the statement before any plan
            // exists, so it stays with the compiler that answers that; every
            // other EXPLAIN asks about the plan, which is now the completed
            // one.
            Statement::ExplainQuery(explain)
                if explain.format != ExplainFormat::Analyze && !explain_mode(explain).1 =>
            {
                let (level, _) = explain_mode(explain);
                let query = self.prepare_explain_query(
                    explain.query.as_ref(),
                    current_catalog,
                    current_database,
                    connector_context,
                )?;
                self.complete_distributed_explain(
                    &query,
                    current_catalog,
                    current_database,
                    query_options,
                    connector_planning_context,
                    context.execution(),
                    context,
                    scope,
                    level,
                )
            }
            Statement::ExplainQuery(explain) if explain.format != ExplainFormat::Analyze => {
                let (level, force_logical_explain) = explain_mode(explain);
                let query = self.prepare_explain_query(
                    explain.query.as_ref(),
                    current_catalog,
                    current_database,
                    connector_context,
                )?;
                let catalog_service = query_catalog_service_snapshot(&self.query);
                let materializer = build_catalog_service_provider(
                    current_catalog,
                    &catalog_service,
                    self.query.connector_control().as_ref(),
                    connector_context.clone(),
                    TableLookupMode::ExplainStats,
                    self.query.catalog_application().map(Arc::as_ref),
                );
                let mv_definitions = if force_logical_explain {
                    None
                } else {
                    Some(freeze_query_mv_rewrite_definition_index(
                        &self.query,
                        &self.mv_candidate_reader,
                        self.mv_storage_observation.as_ref(),
                    )?)
                };
                let analyzed = SqlCompiler::analyze(self.analyze_request(
                    &query,
                    current_catalog,
                    current_database,
                    context.execution(),
                    &materializer,
                    mv_definitions.as_ref(),
                    if force_logical_explain {
                        SqlCompileIntent::LogicalOnly
                    } else {
                        SqlCompileIntent::Explain {
                            level,
                            analyze: false,
                        }
                    },
                )?)
                .map_err(FrontendQueryCompilerError::from_compile)?;
                reject_quarantined_mv_targets(
                    materializer.query_table_bindings().as_ref(),
                    self.mv_readiness.as_ref(),
                )?;
                let output = if force_logical_explain {
                    analyzed
                        .into_complete()
                        .map_err(FrontendQueryCompilerError::from_compile)?
                } else {
                    let analyzed = analyzed
                        .into_pending()
                        .map_err(FrontendQueryCompilerError::from_compile)?;
                    let statistics =
                        query_statistics_snapshot(&self.query, &materializer, connector_context)?;
                    SqlCompiler::optimize(SqlOptimizeRequest::new(
                        analyzed,
                        &statistics,
                        SqlCompileControl::new(
                            context.execution().deadline(),
                            sql_cancellation_observation(
                                context.execution().cancellation().clone(),
                            ),
                        ),
                    ))
                    .map_err(FrontendQueryCompilerError::from_compile)?
                };
                Ok(PreparedQueryOperation::explain_lines(
                    output
                        .into_explain_lines(level, force_logical_explain)
                        .map_err(FrontendQueryCompilerError::from_compile)?,
                )?)
            }
            Statement::ExplainQuery(explain) => self.prepare_explain_analyze(
                explain.query.as_ref(),
                current_catalog,
                current_database,
                query_options,
                &connector_planning_context,
                context.execution(),
            ),
            Statement::Query(query) => {
                if let Some(result) = information_schema::try_query_materialized_views(
                    self.system_tables.mv_readiness().as_ref(),
                    query,
                )? {
                    return Ok(PreparedQueryOperation::immediate(result));
                }
                let query = self.prepare_query(
                    query,
                    current_catalog,
                    current_database,
                    connector_context,
                )?;
                self.complete_distributed_read(
                    &query,
                    current_catalog,
                    current_database,
                    query_options,
                    connector_planning_context,
                    context.execution(),
                    context,
                    scope,
                )
            }
            _ => Err(FrontendQueryCompilerError::Engine(
                "query compiler only supports SELECT and EXPLAIN statements".to_string(),
            )),
        }
    }

    /// The owners in this process that can answer what completing a statement
    /// asks, and the lane their synchronous calls are admitted through.
    fn completion_fact_owners(
        &self,
    ) -> crate::query_execution::completion_facts::CompletionFactOwners {
        crate::query_execution::completion_facts::CompletionFactOwners::new(
            Arc::new(query_catalog_service_snapshot(&self.query)),
            self.query.catalog_application().cloned(),
            Arc::clone(self.query.typed_connector_control()),
            Arc::clone(self.query.unified_statistics()),
            self.mv_candidate_reader.clone(),
            Arc::clone(&self.mv_storage_observation),
            self.connector_blocking_io.clone(),
        )
    }

    /// The parallelism this statement's plan may be instantiated at.
    ///
    /// Scheduling picks one count per fragment and never exceeds the live
    /// backend count, so that count is the honest upper bound -- the domain
    /// states what the scheduler may choose today rather than a target of its
    /// own. Nothing requires a power of two: the scheduler picks arbitrary
    /// counts in that range, and declaring otherwise would make the plan
    /// demand a shape its own scheduler does not produce.
    fn pipeline_dop_domain(context: &RequestContext) -> PipelineDopDomain {
        let live =
            u32::try_from(context.execution().topology().targets().len()).unwrap_or(u32::MAX);
        PipelineDopDomain {
            min: 1,
            max: live.max(1),
            requires_power_of_two: false,
        }
    }

    /// How much one scan may return in a batch.
    ///
    /// This is a ceiling, not a target. The session may lower the row count;
    /// nothing in this deployment lowers the byte count, so it stays at the
    /// contract's own maximum, which states "unconstrained" rather than a
    /// number someone chose.
    fn scan_read_budget(query_options: Option<&QueryOptions>) -> ScanReadBudget {
        let max_batch_rows = query_options
            .map(|options| options.as_proto().batch_size)
            .and_then(|rows| u64::try_from(rows).ok())
            .filter(|rows| *rows > 0 && *rows <= MAX_SCAN_BATCH_ROWS)
            .unwrap_or(MAX_SCAN_BATCH_ROWS);
        ScanReadBudget {
            max_batch_rows,
            max_batch_bytes: MAX_SCAN_BATCH_BYTES,
        }
    }

    /// Compile one read into a completed plan and print it.
    ///
    /// EXPLAIN asks about the plan, so the plan is completed exactly as it is
    /// for execution -- the same driver, the same facts, the same version --
    /// and then printed rather than run. Nothing is executed, and the
    /// capabilities the completion takes are released with it.
    #[allow(clippy::too_many_arguments)]
    fn complete_distributed_explain(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_planning_context: novarocks_spi::connector::ConnectorPlanningContext,
        execution: &QueryExecutionContext,
        context: &RequestContext,
        scope: &novarocks_workload_control::WorkScope,
        level: ExplainLevel,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
        use novarocks_query_application::preparation::FinalPlanCompletionDriver;

        let connector_context = connector_planning_context.request();
        let bindings = Arc::new(
            crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
                .expect("query table binding scope allocation must not fail"),
        );
        let facts = crate::query_execution::completion_facts::frontend_fact_source(
            self.completion_fact_owners(),
            crate::query_execution::completion_facts::StatementFactScope::new(
                Arc::clone(&bindings),
                connector_context.clone(),
                current_catalog,
            ),
            crate::query_execution::compiler::typed_connector_session()
                .map_err(FrontendQueryCompilerError::Engine)?,
        );
        let request = novarocks_sql::compiler::SqlFinalPlanCompileRequest::new(
            crate::query_execution::physical_encoding::mint_plan_version(),
            SqlStatementInput::parsed_query(Box::new(query.clone())),
            SqlCompileIntent::Explain {
                level,
                analyze: false,
            },
            SqlSessionContext {
                current_catalog: current_catalog.map(str::to_string),
                current_database: current_database.to_string(),
                optimizer_settings: execution.optimizer_settings().clone(),
            },
            SqlPlanningEnvironment::Distributed,
            novarocks_sql::compiler::SqlFunctionCatalog::snapshot(self.functions.as_ref()),
            crate::query_execution::constant_eval::constant_evaluator(),
            SqlCompileControl::new(
                execution.deadline(),
                sql_cancellation_observation(execution.cancellation().clone()),
            ),
            Self::pipeline_dop_domain(context),
            Self::scan_read_budget(query_options.as_ref()),
            novarocks_sql::compiler::DEFAULT_COMPLETION_LIMITS,
        );
        let completed = self
            .connector_blocking_io
            .runtime()
            .block_on(FinalPlanCompletionDriver::new(Arc::new(facts)).complete(request, scope))
            .map_err(|failure| match failure.error() {
                novarocks_query_application::preparation::FinalPlanCompletionError::Analyze {
                    error,
                } => FrontendQueryCompilerError::Analyze(error.clone()),
                error => FrontendQueryCompilerError::Engine(error.to_string()),
            })?;
        let lines = completed
            .candidate()
            .render_explain_lines(novarocks_sql::compiler::ExplainRenderBudget::default())
            .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?;
        Ok(PreparedQueryOperation::explain_lines(lines)
            .map_err(FrontendQueryCompilerError::Engine)?)
    }

    /// Compile one plain read into a completed plan, and hand the execution
    /// it describes to the owner that runs attempts of it.
    ///
    /// The compiler asks for what it needs and this process answers: a
    /// relation, a statistic, a materialized view, or a provider read. Every
    /// answer reaches exactly one owner, and the ones that may call a remote
    /// catalog are admitted through the bounded lane rather than run here.
    ///
    /// Completion is asynchronous because answering can wait; preparation is
    /// not, and runs on its own threads rather than the runtime's, so it is
    /// awaited here. That keeps this statement's threading exactly as it was
    /// -- the sealed path reached the same catalogs synchronously from the
    /// same threads -- and leaves making preparation itself asynchronous as
    /// its own change.
    #[allow(clippy::too_many_arguments)]
    fn complete_distributed_read(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_planning_context: novarocks_spi::connector::ConnectorPlanningContext,
        execution: &QueryExecutionContext,
        context: &RequestContext,
        scope: &novarocks_workload_control::WorkScope,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
        use novarocks_query_application::preparation::FinalPlanCompletionDriver;

        let connector_context = connector_planning_context.request();
        self.query
            .query_execution()
            .reserve_logical_query()
            .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?;
        let bindings = Arc::new(
            crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
                .expect("query table binding scope allocation must not fail"),
        );
        let facts = crate::query_execution::completion_facts::frontend_fact_source(
            self.completion_fact_owners(),
            crate::query_execution::completion_facts::StatementFactScope::new(
                Arc::clone(&bindings),
                connector_context.clone(),
                current_catalog,
            ),
            crate::query_execution::compiler::typed_connector_session()
                .map_err(FrontendQueryCompilerError::Engine)?,
        );
        let version = crate::query_execution::physical_encoding::mint_plan_version();
        let request = novarocks_sql::compiler::SqlFinalPlanCompileRequest::new(
            version,
            SqlStatementInput::parsed_query(Box::new(query.clone())),
            SqlCompileIntent::Query,
            SqlSessionContext {
                current_catalog: current_catalog.map(str::to_string),
                current_database: current_database.to_string(),
                optimizer_settings: execution.optimizer_settings().clone(),
            },
            SqlPlanningEnvironment::Distributed,
            novarocks_sql::compiler::SqlFunctionCatalog::snapshot(self.functions.as_ref()),
            crate::query_execution::constant_eval::constant_evaluator(),
            SqlCompileControl::new(
                execution.deadline(),
                sql_cancellation_observation(execution.cancellation().clone()),
            ),
            Self::pipeline_dop_domain(context),
            Self::scan_read_budget(query_options.as_ref()),
            novarocks_sql::compiler::DEFAULT_COMPLETION_LIMITS,
        );
        let completed = self
            .connector_blocking_io
            .runtime()
            .block_on(FinalPlanCompletionDriver::new(Arc::new(facts)).complete(request, scope))
            .map_err(|failure| match failure.error() {
                // A statement the analyzer rejected reaches the client as the
                // analyzer stated it -- code, phase and place in the text --
                // exactly as it does when the sealed path compiles it.
                novarocks_query_application::preparation::FinalPlanCompletionError::Analyze {
                    error,
                } => FrontendQueryCompilerError::Analyze(error.clone()),
                error => FrontendQueryCompilerError::Engine(error.to_string()),
            })?;
        // What this statement delivers is a property of the plan, read before
        // the plan is consumed by encoding.
        let output = novarocks_query_application::preparation::OutputContract::from_completed_plan(
            novarocks_query_application::api::QueryExecutionKind::Read,
            completed.candidate().plan(),
        )
        .map_err(FrontendQueryCompilerError::Engine)?;
        let encoded = crate::query_execution::physical_encoding::encode_completed_plan(
            completed,
            self.functions.as_ref(),
            None,
        )
        .map_err(FrontendQueryCompilerError::Engine)?;
        let template = encoded.into_attempt_template(version);
        let description =
            novarocks_query_application::preparation::FrozenExecutionDescription::for_completed_plan(
                novarocks_query_application::api::QueryExecutionKind::Read,
                version,
                template
                    .attempt_scheduling_facts()
                    .map_err(FrontendQueryCompilerError::Engine)?
                    .fragments
                    .iter()
                    .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan))
                    .collect(),
                output,
                novarocks_query_application::coordination::ExecutionEffect::None,
                novarocks_query_application::coordination::RecoveryMode::NoRecovery,
                Vec::new(),
                novarocks_query_application::preparation::FrozenCostEstimate::unknown(
                    novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
                ),
                novarocks_query_application::preparation::ExecutionResourceRequirements::unknown(
                    novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
                ),
            )
            .map_err(FrontendQueryCompilerError::Engine)?;
        Ok(PreparedQueryOperation::LogicalRead(
            crate::query_execution::completion::PreparedLogicalRead::new(
                description,
                template,
                Arc::new(
                    crate::query_execution::contract::ResolvedQueryOptions::from_upstream(
                        query_options,
                    ),
                ),
            ),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_distributed_query(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_planning_context: novarocks_spi::connector::ConnectorPlanningContext,
        execution: &QueryExecutionContext,
        intent: SqlCompileIntent,
        allow_mv_rewrite_candidates: bool,
        completion_intent: PostCompileIntent,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
        let connector_context = connector_planning_context.request();
        let query_application_read = matches!(&completion_intent, PostCompileIntent::Result);
        let logical_reservation = if query_application_read {
            // The Query Application mints the runtime logical execution only
            // after this frozen description has been handed off. Reserve a
            // diagnostics-only query identity here so preparation events keep
            // their statement-to-logical correlation without creating an
            // attempt or retaining a legacy coordinator owner.
            self.query
                .query_execution()
                .reserve_logical_query()
                .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?;
            None
        } else {
            Some(
                self.query
                    .query_execution()
                    .reserve_logical_query()
                    .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?,
            )
        };
        let catalog_service = query_catalog_service_snapshot(&self.query);
        let bindings = Arc::new(
            QueryTableBindingStore::try_new()
                .expect("query table binding scope allocation must not fail"),
        );
        let materializer = build_catalog_service_provider_with_bindings_and_query_local_overlays(
            current_catalog,
            &catalog_service,
            self.query.connector_control().as_ref(),
            connector_context.clone(),
            Arc::clone(&bindings),
            Vec::new(),
            self.query.catalog_application().map(Arc::as_ref),
        );
        let mv_definitions = if allow_mv_rewrite_candidates {
            Some(freeze_query_mv_rewrite_definition_index(
                &self.query,
                &self.mv_candidate_reader,
                self.mv_storage_observation.as_ref(),
            )?)
        } else {
            None
        };
        let analyze_request = self.analyze_request(
            query,
            current_catalog,
            current_database,
            execution,
            &materializer,
            mv_definitions.as_ref(),
            intent.clone(),
        )?;
        let analyzed = crate::preparation_diagnostics::observe_result(
            "compile",
            "sql_analyze",
            "not-applicable",
            None,
            || SqlCompiler::analyze(analyze_request),
        )
        .map_err(FrontendQueryCompilerError::from_compile)?
        .into_pending()
        .map_err(FrontendQueryCompilerError::from_compile)?;
        reject_quarantined_mv_targets(bindings.as_ref(), self.mv_readiness.as_ref())?;
        let statistics = query_statistics_snapshot(&self.query, &materializer, connector_context)?;
        // Analysis (including optional candidate target materialization) and
        // statistics have now admitted every table this statement may use.
        // Freeze the exact receipt set before optimizer selection so the
        // selected action can only resolve pre-rewrite bindings from this
        // immutable query scope.
        materializer
            .query_table_bindings()
            .seal_for_topology_replan();
        let distributed_plan = crate::preparation_diagnostics::observe_result(
            "compile",
            "sql_optimize",
            "not-applicable",
            None,
            || {
                SqlCompiler::optimize(SqlOptimizeRequest::new(
                    analyzed,
                    &statistics,
                    SqlCompileControl::new(
                        execution.deadline(),
                        sql_cancellation_observation(execution.cancellation().clone()),
                    ),
                ))
            },
        )
        .map_err(FrontendQueryCompilerError::from_compile)?
        .into_distributed_query()
        .map_err(FrontendQueryCompilerError::from_compile)?;
        let retry_completion = RetryCompletionTemplate::from_first_round(&completion_intent);
        // Seal here rather than inside preparation, so this round keeps the
        // sealed plan. A pre-ready topology retry rebinds that exact seal
        // instead of returning to the analyzer and the optimizer, which is
        // what stops a second attempt from getting a different plan shape out
        // of drifted statistics.
        let (distributed_plan, sql_cost) = distributed_plan.into_parts();
        let sealed_plan =
            novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(distributed_plan);
        let (assembly, completion) = crate::preparation_diagnostics::observe_result(
            "compile",
            "prepare_distributed_description",
            "not-applicable",
            None,
            || {
                crate::query_execution::post_compile::prepare_sealed_logical_execution(
                    &sealed_plan,
                    sql_cost,
                    &self.query,
                    Some(bindings.as_ref()),
                    connector_context,
                    query_options.clone(),
                    execution,
                    completion_intent,
                )
            },
        )?;
        // Semantic admission is complete before native request construction.
        // Every round of this statement reuses these exact bindings; they are
        // sealed here and a retry rebinds them rather than re-materializing.
        let native_bundle = encode_native_fragment_bundle(assembly.encoding().encoding_view())?;
        if query_application_read {
            let logical_read = assembly.finish_logical_read(native_bundle)?;
            drop(materializer);
            return Ok(PreparedQueryOperation::LogicalRead(logical_read));
        }
        let request = assembly.finish(native_bundle)?;
        let logical_execution = request.restartable_read();
        drop(materializer);
        let logical_reservation = logical_reservation
            .expect("legacy distributed completion must reserve its old coordinator logical query");
        let mut operation =
            PreparedQueryDistributedOperation::new(request, completion, logical_reservation);
        if let Some(logical_execution) = logical_execution {
            operation =
                operation.with_attempt_factory(Box::new(FrontendDistributedAttemptFactory {
                    logical_execution,
                    statement: StatementAdmissionContext::new(
                        current_catalog.map(str::to_string),
                        current_database.to_string(),
                        execution.role(),
                        execution.deadline(),
                        execution.cancellation().clone(),
                        execution.optimizer_settings().clone(),
                    ),
                    completion: retry_completion,
                    effect_tracker: StatementEffectTracker::read_only(),
                }));
        }
        Ok(PreparedQueryOperation::Distributed(operation))
    }

    #[allow(clippy::too_many_arguments)]
    fn analyze_request<'a>(
        &'a self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &QueryExecutionContext,
        materializer: &'a dyn novarocks_sql::compiler::SqlCatalogSnapshot,
        mv_definitions: Option<&'a novarocks_sql::compiler::MvRewriteDefinitionIndex>,
        intent: SqlCompileIntent,
    ) -> Result<SqlAnalyzeRequest<'a>, String> {
        Ok(SqlAnalyzeRequest::new(
            SqlStatementInput::parsed_query(Box::new(query.clone())),
            intent,
            SqlSessionContext {
                current_catalog: current_catalog.map(str::to_string),
                current_database: current_database.to_string(),
                optimizer_settings: execution.optimizer_settings().clone(),
            },
            SqlPlanningEnvironment::Distributed,
            materializer,
            self.functions.as_ref(),
            crate::query_execution::constant_eval::constant_evaluator(),
            mv_definitions,
            SqlCompileControl::new(
                execution.deadline(),
                sql_cancellation_observation(execution.cancellation().clone()),
            ),
        ))
    }

    fn prepare_query(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Query, FrontendQueryCompilerError> {
        let mut prepared = self.prepare_explain_query(
            query,
            current_catalog,
            current_database,
            connector_context,
        )?;
        novarocks_query_application::system_catalog_rewrite::rewrite_query(
            self.system_tables.facts_port().as_ref(),
            self.system_tables.system_catalog().as_ref(),
            connector_context,
            &mut prepared,
        )?;
        Ok(prepared)
    }

    fn prepare_explain_query(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Query, FrontendQueryCompilerError> {
        let mut prepared = query.clone();
        self.view.view_service().rewrite_query(
            &crate::view::engine::FrontendViewEngine::new(self.view.clone()),
            &mut prepared,
            ViewRequestContext {
                current_catalog,
                current_database,
                connector_context: Some(connector_context),
            },
        )?;
        if has_time_travel_refs(&prepared) {
            rewrite_time_travel_refs(
                &self.query,
                current_catalog,
                current_database,
                &mut prepared,
                connector_context,
            )?;
        }
        Ok(prepared)
    }

    fn prepare_explain_analyze(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_planning_context: &novarocks_spi::connector::ConnectorPlanningContext,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
        let connector_context = connector_planning_context.request();
        let query = self.prepare_explain_query(
            query,
            current_catalog,
            current_database,
            connector_context,
        )?;
        let planning_start = std::time::Instant::now();
        self.prepare_distributed_query(
            &query,
            current_catalog,
            current_database,
            Some(query_options_for_explain_analyze(query_options)),
            connector_planning_context.clone(),
            execution,
            SqlCompileIntent::Explain {
                level: ExplainLevel::Analyze,
                analyze: true,
            },
            true,
            PostCompileIntent::Profile {
                planning_elapsed: planning_start.elapsed(),
                execution_started_at: std::time::Instant::now(),
            },
        )
    }
}

fn query_options_for_explain_analyze(query_options: Option<QueryOptions>) -> QueryOptions {
    let mut raw = query_options
        .as_ref()
        .map(|options| *options.as_proto())
        .unwrap_or_default();
    raw.enable_profile = true;
    QueryOptions::parse(raw).expect("enabling query profiling does not invalidate query options")
}

pub(crate) fn explain_mode(explain: &ExplainQuery) -> (ExplainLevel, bool) {
    let level = match explain.format {
        ExplainFormat::Default => ExplainLevel::Normal,
        ExplainFormat::Verbose => ExplainLevel::Verbose,
        ExplainFormat::Costs => ExplainLevel::Costs,
        ExplainFormat::Logical => ExplainLevel::Normal,
        ExplainFormat::Analyze => ExplainLevel::Analyze,
        ExplainFormat::Contract => ExplainLevel::Contract,
    };
    (
        level,
        explain.logical || matches!(explain.format, ExplainFormat::Logical),
    )
}
