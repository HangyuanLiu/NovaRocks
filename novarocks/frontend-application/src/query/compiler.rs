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
use crate::connector::connector_planning_context_for_query_on_runtime;
use crate::mv::domain::readiness::{MvCandidateReader, MvReadinessPort};
use crate::query_execution::compiler::query_catalog_service_snapshot;
use crate::query_execution::completion::PreparedQueryOperation;
use crate::query_execution::kernels::{
    QueryPreparationKernel, SystemTableQueryKernel, ViewExecutionKernel,
};
use crate::query_execution::planning::sql_compile_control_from_execution;
use crate::query_execution::planning::time_travel::{
    TimeTravelRewriteError, has_time_travel_refs, rewrite_time_travel_refs,
};
use novarocks_parser::ast::{ExplainFormat, ExplainQuery, Query, Statement};
use novarocks_physical_plan::{
    MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, ScanReadBudget,
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::{QueryExecutionContext, RequestContext};
use novarocks_query_application::view::ViewRequestContext;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_sql::analyze_error::AnalyzeError;
use novarocks_sql::compiler::{
    ExplainLevel, SqlCompileError, SqlCompileIntent, SqlPlanningEnvironment, SqlSessionContext,
    SqlStatementInput,
};

/// Application intent is supplied by the caller, never inferred from a
/// physical plan's column count. Wire installation remains a separate cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrontendQueryPurpose {
    ClientRows,
    LocalRows,
    ScalarValue,
}

impl FrontendQueryPurpose {
    pub(crate) fn client_query(query: &Query) -> Self {
        if information_schema::is_local_materialized_views_query(query) {
            Self::LocalRows
        } else {
            Self::ClientRows
        }
    }

    pub(crate) fn window_class(self) -> novarocks_workload_control::ResultWindowClass {
        use novarocks_workload_control::ResultWindowClass;
        match self {
            Self::ClientRows => ResultWindowClass::Client,
            Self::LocalRows => ResultWindowClass::Local,
            Self::ScalarValue => ResultWindowClass::Internal,
        }
    }

    fn validate(self, statement: &Statement) -> Result<(), FrontendQueryCompilerError> {
        let valid = match (self, statement) {
            (Self::ClientRows, Statement::Query(query)) => {
                !information_schema::is_local_materialized_views_query(query)
            }
            (Self::LocalRows, Statement::Query(query)) => {
                information_schema::is_local_materialized_views_query(query)
            }
            (Self::ScalarValue, Statement::Query(_)) => true,
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(FrontendQueryCompilerError::Engine(
                "query preparation purpose does not match the admitted statement".to_owned(),
            ))
        }
    }
}

/// Preserves SQL analyze-domain facts until the session still has the original
/// SQL source required to render a user location.
#[derive(Debug)]
pub(crate) enum FrontendQueryCompilerError {
    Application(novarocks_query_application::session_error::QueryServiceError),
    Engine(String),
    Analyze(AnalyzeError),
    Source(novarocks_failure_contract::SharedFailureCause),
    Control(novarocks_type_contract::CompileControlError),
}

impl FrontendQueryCompilerError {
    fn from_compile(error: SqlCompileError) -> Self {
        match error {
            SqlCompileError::Analyze(error) => match error.control_error() {
                Some(control) => Self::Control(control),
                None => Self::Analyze(error),
            },
            SqlCompileError::Cancelled => {
                Self::Control(novarocks_type_contract::CompileControlError::Cancelled)
            }
            SqlCompileError::DeadlineExceeded => {
                Self::Control(novarocks_type_contract::CompileControlError::DeadlineExceeded)
            }
            SqlCompileError::ResourceExhausted => {
                Self::Control(novarocks_type_contract::CompileControlError::ResourceExhausted)
            }
            SqlCompileError::CompilationSource(source) => Self::Source(source),
            error => Self::Engine(error.to_string()),
        }
    }
    fn from_encode(error: novarocks_plan_codec::PhysicalEncodeError) -> Self {
        match error {
            novarocks_plan_codec::PhysicalEncodeError::Control(error) => Self::Control(error),
            other => Self::Source(novarocks_failure_contract::SharedFailureCause::new(other)),
        }
    }
    fn from_completion(
        error: &novarocks_query_application::preparation::FinalPlanCompletionError,
    ) -> Self {
        use novarocks_query_application::preparation::FinalPlanCompletionError as E;
        match error {
            E::Cancelled { .. } => {
                Self::Control(novarocks_type_contract::CompileControlError::Cancelled)
            }
            E::DeadlineExceeded => {
                Self::Control(novarocks_type_contract::CompileControlError::DeadlineExceeded)
            }
            E::ResourceExhausted => {
                Self::Control(novarocks_type_contract::CompileControlError::ResourceExhausted)
            }
            E::Source { source } => Self::Source(source.clone()),
            E::Analyze { error } => match error.control_error() {
                Some(control) => Self::Control(control),
                None => Self::Analyze(error.clone()),
            },
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

#[derive(Clone)]
pub(crate) struct FrontendQueryCompiler {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
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

mod async_explain;
mod async_read;
pub(crate) use async_explain::{PreparedAsyncExplain, PreparedAsyncProfile};

impl FrontendQueryCompiler {
    pub(crate) fn validate_original_read_runtime(
        &self,
    ) -> Result<(), novarocks_workload_control::WorkError> {
        self.connector_blocking_io.validate_original_read_runtime()
    }
    pub(crate) fn connector_runtime(&self) -> &tokio::runtime::Handle {
        self.connector_blocking_io.runtime()
    }

    pub(crate) fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        query: QueryPreparationKernel,
        view: ViewExecutionKernel,
        system_tables: SystemTableQueryKernel,
        mv_readiness: Arc<MvReadinessPort>,
        mv_candidate_reader: MvCandidateReader,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        connector_blocking_io: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        constant_policy: novarocks_functions::ConstantPolicy,
    ) -> Self {
        Self {
            functions,
            constant_policy,
            query,
            view,
            system_tables,
            mv_readiness,
            mv_candidate_reader,
            mv_storage_observation,
            connector_blocking_io,
        }
    }

    pub(crate) const fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy
    }

    pub(crate) fn prepare_statement(
        &self,
        statement: &Statement,
        purpose: FrontendQueryPurpose,
        context: &RequestContext,
        query_options: Option<QueryOptions>,
        scope: &novarocks_workload_control::WorkScope,
        #[cfg(novarocks_dev_controls)] rewrite_statement: Option<
            &crate::mv::test_controls::MvRewriteStatementTarget,
        >,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
        purpose.validate(statement)?;
        let connector_planning_context = connector_planning_context_for_query_on_runtime(
            self.connector_blocking_io.runtime(),
            query_options.as_ref(),
            context.execution().cancellation().clone(),
        )?;
        let connector_context = connector_planning_context.request();
        let current_catalog = context.session().current_catalog();
        let current_database = context.session().current_database();

        match statement {
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
                    context.execution().sql_semantics(),
                )?;
                self.complete_distributed_read(
                    purpose,
                    &query,
                    current_catalog,
                    current_database,
                    query_options,
                    connector_planning_context,
                    context.execution(),
                    context,
                    scope,
                    #[cfg(novarocks_dev_controls)]
                    rewrite_statement,
                )
            }
            _ => Err(FrontendQueryCompilerError::Engine(
                "synchronous query preparation only accepts SELECT statements".to_string(),
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

    /// Freeze the same per-instance driver width that request construction
    /// will resolve from these query options. Backend count governs placement.
    fn pipeline_dop_domain(
        options: Option<&QueryOptions>,
    ) -> Result<PipelineDopDomain, FrontendQueryCompilerError> {
        crate::query_execution::contract::completed_plan_dop_domain(options)
            .map_err(FrontendQueryCompilerError::Engine)
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
    #[expect(
        clippy::too_many_arguments,
        reason = "The completion path receives distinct frozen read facts and live owner ports; bundling them would duplicate the operation contract."
    )]
    fn complete_distributed_read(
        &self,
        purpose: FrontendQueryPurpose,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_planning_context: novarocks_spi::connector::ConnectorPlanningContext,
        execution: &QueryExecutionContext,
        _context: &RequestContext,
        scope: &novarocks_workload_control::WorkScope,
        #[cfg(novarocks_dev_controls)] rewrite_statement: Option<
            &crate::mv::test_controls::MvRewriteStatementTarget,
        >,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
        use novarocks_query_application::preparation::FinalPlanCompletionDriver;

        let connector_context = connector_planning_context.request();
        let bindings = Arc::new(
            crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
                .expect("query table binding scope allocation must not fail"),
        );
        let facts = crate::query_execution::completion_facts::frontend_fact_source(
            self.completion_fact_owners(),
            crate::query_execution::completion_facts::StatementFactScope::binding_only(
                Arc::clone(&bindings),
                connector_context.clone(),
                current_catalog,
                scope,
                execution.result_capacity().cloned().ok_or_else(|| {
                    FrontendQueryCompilerError::Engine(
                        "completion facts require the admitted result capacity".to_owned(),
                    )
                })?,
            )
            .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?,
            crate::query_execution::compiler::typed_connector_session()
                .map_err(FrontendQueryCompilerError::Engine)?,
        );
        let version = crate::query_execution::physical_encoding::mint_plan_version();
        let request = novarocks_sql::compiler::SqlFinalPlanCompileRequest::new(
            version,
            SqlStatementInput::parsed_query(Box::new(query.clone())),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: execution.sql_semantics().clone(),
                current_catalog: current_catalog.map(str::to_string),
                current_database: current_database.to_string(),
                optimizer_settings: execution.optimizer_settings().clone(),
            },
            SqlPlanningEnvironment::Distributed,
            novarocks_sql::compiler::SqlFunctionCatalog::snapshot(self.functions.as_ref()),
            crate::query_execution::constant_eval::constant_evaluator(),
            self.constant_policy(),
            sql_compile_control_from_execution(execution),
            Self::pipeline_dop_domain(query_options.as_ref())?,
            Self::scan_read_budget(query_options.as_ref()),
            novarocks_sql::compiler::DEFAULT_COMPLETION_LIMITS,
        );
        let encoding_control = request.control().clone();
        let completed = self
            .connector_blocking_io
            .runtime()
            .block_on(FinalPlanCompletionDriver::new(Arc::new(facts)).complete(request, scope))
            .map_err(|failure| FrontendQueryCompilerError::from_completion(failure.error()))?;
        // Resolve application purpose before the plan is shared with Native
        // projection. Presentation uses one FE offset, never the BE clock.
        let root_output = match purpose {
            FrontendQueryPurpose::ClientRows => {
                novarocks_result_contract::FrozenRootOutput::ClientRows(
                    novarocks_sql::compiler::client_render_schema_from_port(
                        completed.candidate().original_public_result_port(),
                        chrono::Local::now().offset().local_minus_utc(),
                    )
                    .map_err(FrontendQueryCompilerError::Engine)?,
                )
            }
            FrontendQueryPurpose::ScalarValue => scalar_root_output_from_public_port(
                completed.candidate().original_public_result_port(),
            )?,
            FrontendQueryPurpose::LocalRows => {
                return Err(FrontendQueryCompilerError::Engine(
                    "distributed read received a different application output purpose".into(),
                ));
            }
        };
        let completed = completed
            .freeze_root_output(root_output)
            .map_err(|(error, _returned)| FrontendQueryCompilerError::Engine(error.to_string()))?;
        // What this statement delivers is a property of the frozen plan, read
        // before the plan is consumed by encoding.
        let output =
            novarocks_query_application::preparation::OutputContract::from_completed_candidate(
                novarocks_query_application::api::QueryExecutionKind::Read,
                completed.candidate(),
            )
            .map_err(FrontendQueryCompilerError::Engine)?;
        #[cfg(novarocks_dev_controls)]
        let selected_mv_rewrite = completed
            .candidate()
            .plan()
            .annotations()
            .iter()
            .any(|annotation| annotation.key.as_ref() == "sql.mv_rewrite_provenance");
        let encoded = crate::query_execution::physical_encoding::encode_completed_plan(
            completed,
            self.query.static_plan_carrier(),
            self.constant_policy(),
            None,
            &encoding_control,
        )
        .map_err(FrontendQueryCompilerError::from_encode)?;
        let (template, candidate) = encoded.into_attempt_template_with_candidate(version);
        let description =
            novarocks_query_application::preparation::FrozenExecutionDescription::for_completed_plan(
                novarocks_query_application::api::QueryExecutionKind::Read,
                candidate,
                template
                    .attempt_scheduling_facts()
                    .map_err(FrontendQueryCompilerError::Engine)?
                    .fragments
                    .iter()
                    .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan))
                    .collect(),
                output,
                novarocks_coordination_contract::ExecutionEffect::None,
                // A sealed, effect-free read may replace its attempt before
                // output visibility without compiling a different plan.
                novarocks_coordination_contract::RecoveryMode::RestartAttemptBeforeVisibility,
                Vec::new(),
                novarocks_query_application::preparation::FrozenCostEstimate::unknown(
                    novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
                ),
                novarocks_query_application::preparation::ExecutionResourceRequirements::unknown(
                    novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
                ),
            )
            .map_err(FrontendQueryCompilerError::Engine)?;
        #[cfg(novarocks_dev_controls)]
        if let Some(controls) = self.mv_readiness.test_controls() {
            controls
                .rewrite(
                    current_catalog,
                    current_database,
                    query,
                    selected_mv_rewrite,
                    rewrite_statement,
                    execution.cancellation(),
                )
                .map_err(FrontendQueryCompilerError::Engine)?;
        }
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

    fn prepare_query(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        sql_semantics: &novarocks_sql::sql_mode::SqlSemanticSettings,
    ) -> Result<Query, FrontendQueryCompilerError> {
        let mut prepared = self.prepare_query_views_and_time_travel(
            query,
            current_catalog,
            current_database,
            connector_context,
            sql_semantics,
        )?;
        novarocks_query_application::system_catalog_rewrite::rewrite_query(
            self.system_tables.facts_port().as_ref(),
            self.system_tables.system_catalog().as_ref(),
            connector_context,
            &mut prepared,
        )?;
        Ok(prepared)
    }

    fn prepare_query_views_and_time_travel(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        sql_semantics: &novarocks_sql::sql_mode::SqlSemanticSettings,
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
            sql_semantics,
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
}

fn query_options_for_explain_analyze(query_options: Option<QueryOptions>) -> QueryOptions {
    let mut raw = query_options
        .as_ref()
        .map(|options| *options.as_proto())
        .unwrap_or_default();
    raw.enable_profile = true;
    QueryOptions::from_proto(raw)
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

fn scalar_root_output_from_public_port(
    port: Option<&novarocks_physical_plan::ResultPort>,
) -> Result<novarocks_result_contract::FrozenRootOutput, FrontendQueryCompilerError> {
    novarocks_sql::compiler::validate_public_result_logical_identities(port)
        .map_err(FrontendQueryCompilerError::Engine)?;
    Ok(novarocks_result_contract::FrozenRootOutput::ScalarValue(
        port.and_then(|port| port.scalar_schema.clone())
            .ok_or_else(|| {
                FrontendQueryCompilerError::Engine(
                    "completed scalar query has no frozen SQL scalar schema".into(),
                )
            })?,
    ))
}

#[cfg(test)]
mod compile_control_tests {
    use super::*;
    use novarocks_type_contract::CompileControlError;

    fn service(
        error: FrontendQueryCompilerError,
    ) -> novarocks_query_application::session_error::QueryServiceError {
        match super::super::governed_compiler_failure(error, "SELECT value") {
            super::super::GovernedPreparationError::Service(error)
            | super::super::GovernedPreparationError::Cancelled(error) => error,
        }
    }

    #[test]
    fn original_repository_failure_keeps_its_domain_without_compile_provenance() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use novarocks_query_application::session_error::QueryServiceError;
        let original =
            SharedFailureCause::new(novarocks_state_store_api::StateStoreOriginalUnsupported);
        let expected = original.failure_fact();
        let error = QueryServiceError::from_failure_cause(original.clone());
        let governed = service(FrontendQueryCompilerError::Application(error.clone()));
        let routed =
            super::super::routed_execution_service_error(super::super::routed_compiler_failure(
                FrontendQueryCompilerError::Application(error),
                "SELECT * FROM information_schema.materialized_views",
            ));
        for actual in [governed, routed] {
            assert_eq!(actual.failure_cause(), Some(&original));
            assert_eq!(actual.execution_failure().unwrap().fact(), expected);
            assert!(actual.compile_control_error().is_none());
        }
    }

    #[test]
    fn compilation_source_keeps_original_resource_fact_through_the_real_session_consumer() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_query_application::execution_error::ExecutionFailureSite;
        let domain = novarocks_functions::ConstantError::arrow_validation(
            arrow::error::ArrowError::MemoryError("provider-local allocation detail".into()),
        );
        let expected = domain.failure_fact();
        let sql_error = SqlCompileError::from(domain);
        let SqlCompileError::CompilationSource(source) = &sql_error else {
            panic!("the constant author must retain its source")
        };
        let original = source.clone();
        let error = FrontendQueryCompilerError::from_compile(sql_error);
        assert!(
            matches!(&error, FrontendQueryCompilerError::Source(actual) if actual == &original)
        );
        let routed = super::super::routed_compiler_failure(
            FrontendQueryCompilerError::from_compile(SqlCompileError::CompilationSource(
                original.clone(),
            )),
            "EXPLAIN SELECT value",
        );
        let super::super::RoutedExecutionError::Control(control) = routed else {
            panic!("the EXPLAIN consumer must retain its nominal source")
        };
        assert_eq!(control, CompileControlError::Source(original.clone()));
        let routed_service =
            novarocks_query_application::session_error::QueryServiceError::from_compile_control(
                control,
            );
        assert_eq!(routed_service.execution_failure().unwrap().fact(), expected);
        assert_eq!(
            routed_service
                .execution_failure()
                .unwrap()
                .descriptor()
                .errno,
            1041
        );
        let service = service(error);
        let input = service.execution_failure().unwrap();
        assert_eq!(input.fact(), expected);
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(
            (input.descriptor().errno, input.descriptor().sqlstate),
            (1041, "HY000")
        );
        assert_eq!(
            service.compile_control_error(),
            Some(CompileControlError::Source(original))
        );
        assert_eq!(
            service.user_error().unwrap().phase(),
            novarocks_user_error::ErrorPhase::Execution
        );
        assert_eq!(service.user_error().unwrap().location(), None);
        assert!(!service.message().contains("provider-local"));
    }

    #[test]
    fn actual_constant_limit_keeps_its_source_through_the_completion_session_consumer() {
        use arrow::datatypes::{DataType, Field};
        use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
        use novarocks_query_application::execution_error::ExecutionFailureSite;
        use novarocks_query_application::preparation::FinalPlanCompletionError;
        let mut policy = novarocks_plan_codec::physical_package_v2::FROZEN_CONSTANT_POLICY;
        policy.max_rows = 0;
        let cause = novarocks_functions::ConstantValue::from_i64(
            Arc::new(Field::new("value", DataType::Int64, false)),
            novarocks_functions::FunctionValueType::new(DataType::Int64, false),
            1,
            policy,
            novarocks_type_contract::CompilePhase::Validate,
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap_err();
        assert!(matches!(
            cause,
            novarocks_functions::ConstantError::Limit(_)
        ));
        let expected = cause.failure_fact();
        assert_eq!(expected.class(), FailureClass::Resource);
        assert_eq!(expected.reason().domain(), domain::CONSTANT.get());
        assert_eq!(expected.reason().code(), 2);
        let compile = SqlCompileError::from(cause);
        let SqlCompileError::CompilationSource(original) = &compile else {
            panic!("the constant author must retain its source");
        };
        let original = original.clone();
        let completion = FinalPlanCompletionError::Source {
            source: original.clone(),
        };
        for error in [
            FrontendQueryCompilerError::from_compile(compile),
            FrontendQueryCompilerError::from_completion(&completion),
        ] {
            assert!(
                matches!(&error, FrontendQueryCompilerError::Source(source) if source == &original)
            );
            let routed = service(error);
            let input = routed.execution_failure().unwrap();
            assert_eq!(input.fact(), expected);
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (1041, "HY000")
            );
            assert_eq!(
                routed.compile_control_error(),
                Some(CompileControlError::Source(original.clone()))
            );
            assert_eq!(
                routed.user_error().unwrap().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
            assert_eq!(routed.user_error().unwrap().location(), None);
        }
    }

    #[test]
    fn real_checked_decimal_overflow_keeps_1690_through_compiler_and_session() {
        use novarocks_failure_contract::FailureProjection;
        let domain = novarocks_functions::legacy_decimal::pow10_i128(39).unwrap_err();
        let expected = domain.failure_fact();
        let domain = novarocks_functions::ConstantError::from_domain(domain);
        let sql_error = SqlCompileError::from(domain);
        let service = service(FrontendQueryCompilerError::from_compile(sql_error));
        let input = service.execution_failure().unwrap();
        assert_eq!(input.fact(), expected);
        assert_eq!(
            (input.descriptor().errno, input.descriptor().sqlstate),
            (1690, "22003")
        );
        let Some(CompileControlError::Source(source)) = service.compile_control_error() else {
            panic!("the original source must remain available locally")
        };
        assert!(
            source
                .downcast_ref::<novarocks_functions::ConstantError>()
                .is_some()
        );
        assert_eq!(
            service.user_error().unwrap().phase(),
            novarocks_user_error::ErrorPhase::Execution
        );
        assert_eq!(service.user_error().unwrap().location(), None);
    }

    #[test]
    fn ordinary_compile_strings_and_analyze_locations_keep_their_original_routes() {
        let service = service(FrontendQueryCompilerError::from_compile(
            SqlCompileError::Compilation("numeric overflow resource exhausted".into()),
        ));
        assert!(service.execution_failure().is_none());
        assert!(service.user_error().is_none());

        let sql = "SELECT missing";
        let error =
            AnalyzeError::unknown_column("unknown column", novarocks_parser::Span::new(7, 14));
        let expected = error.to_user_error(Some(sql));
        let super::super::GovernedPreparationError::Service(service) =
            super::super::governed_compiler_failure(
                FrontendQueryCompilerError::from_compile(SqlCompileError::Analyze(error)),
                sql,
            )
        else {
            panic!("analysis is not a cancellation observation")
        };
        assert_eq!(service.user_error(), Some(&expected));
        assert!(service.execution_failure().is_none());
        assert!(service.user_error().unwrap().location().is_some());
    }

    #[test]
    fn encode_capability_refusal_preserves_the_client_engine_code() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
                "CTE multicast".into(),
            ),
            novarocks_plan_codec::PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
        ] {
            let expected = original.failure_fact();
            let FrontendQueryCompilerError::Source(source) =
                FrontendQueryCompilerError::from_encode(original)
            else {
                panic!("typed encode author must survive")
            };
            assert_eq!(source.failure_fact(), expected);
            assert!(
                source
                    .downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>()
                    .is_some()
            );
            assert_eq!(
                expected.class(),
                match source
                    .downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>()
                    .unwrap()
                {
                    novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(_) =>
                        FailureClass::Unsupported,
                    novarocks_plan_codec::PhysicalEncodeError::Invalid(_) =>
                        FailureClass::Invariant,
                    other => panic!("unexpected author: {other}"),
                }
            );
            let super::super::GovernedPreparationError::Service(service) =
                super::super::governed_compiler_failure(
                    FrontendQueryCompilerError::Source(source),
                    "SELECT 1",
                )
            else {
                panic!("a source error is final")
            };
            assert_eq!(service.execution_failure().unwrap().fact(), expected);
            let (code, message) = match expected.class() {
                FailureClass::Unsupported => (
                    "UnsupportedCompiledShape",
                    "[UnsupportedCompiledShape] pure compilation failed in its source domain",
                ),
                FailureClass::Invariant => (
                    "query.execution.invariant",
                    "[query.execution.invariant] pure compilation failed in its source domain",
                ),
                other => panic!("unexpected author class: {other:?}"),
            };
            assert_eq!(service.user_error().unwrap().code().as_str(), code);
            assert_eq!(service.message(), message);
        }
    }

    #[test]
    fn query_compile_encode_and_analyze_preserve_control_category() {
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let errors = [
                FrontendQueryCompilerError::from_compile(control.clone().into()),
                FrontendQueryCompilerError::from_encode(
                    novarocks_plan_codec::PhysicalEncodeError::Control(control.clone()),
                ),
                FrontendQueryCompilerError::from_compile(SqlCompileError::Analyze(
                    AnalyzeError::control(control.clone()),
                )),
            ];
            for error in errors {
                assert!(
                    matches!(error, FrontendQueryCompilerError::Control(actual) if actual == control)
                );
            }
            use novarocks_failure_contract::{FailureClass, FailureProjection};
            let original = novarocks_plan_codec::PhysicalEncodeError::Invalid(control.to_string());
            let expected = original.failure_fact();
            let FrontendQueryCompilerError::Source(source) =
                FrontendQueryCompilerError::from_encode(original)
            else {
                panic!("the original Invalid author must survive")
            };
            assert_eq!(source.failure_fact(), expected);
            assert_eq!(expected.class(), FailureClass::Invariant);
            assert_ne!(expected, control.failure_fact());
            assert!(matches!(
                source.downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>(),
                Some(novarocks_plan_codec::PhysicalEncodeError::Invalid(detail))
                    if detail == &control.to_string()
            ));
            let service = service(FrontendQueryCompilerError::Source(source));
            assert_eq!(service.execution_failure().unwrap().fact(), expected);
            assert_ne!(
                service.kind(),
                novarocks_query_application::session_error::QueryServiceErrorKind::Interrupted
            );
            assert_ne!(
                service.kind(),
                novarocks_query_application::session_error::QueryServiceErrorKind::Timeout
            );
        }
    }
    #[test]
    fn comparison_source_survives_freeze_encode_and_the_actual_session_consumer() {
        use crate::query_execution::package_freeze::PackageFreezeError;
        use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
        use novarocks_functions::ComparisonPrepareError;
        use novarocks_query_application::execution_error::ExecutionFailureSite;
        use novarocks_sql::compiler::PackageSemanticsError;
        for (original, class, public_code) in [
            (
                ComparisonPrepareError::Unsupported,
                FailureClass::Unsupported,
                "query.execution.unsupported",
            ),
            (
                ComparisonPrepareError::TypeMismatch,
                FailureClass::Invariant,
                "query.execution.invariant",
            ),
        ] {
            let original_source = SharedFailureCause::new(original.clone());
            let expected = original_source.failure_fact();
            assert_eq!(expected.class(), class);
            let encoded = novarocks_plan_codec::PhysicalEncodeError::from(
                PackageFreezeError::Semantics(PackageSemanticsError::PrimitiveSource {
                    source: original_source.clone(),
                    cast: None,
                }),
            );
            let error = FrontendQueryCompilerError::from_encode(encoded);
            let FrontendQueryCompilerError::Source(retained) = &error else {
                panic!("the encoded source must remain typed");
            };
            let retained = retained.clone();
            let Some(novarocks_plan_codec::PhysicalEncodeError::Source(freeze_source)) =
                retained.downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>()
            else {
                panic!("retain the actual physical encode source");
            };
            let Some(PackageFreezeError::Semantics(PackageSemanticsError::PrimitiveSource {
                source: actual,
                cast: None,
            })) = freeze_source.downcast_ref::<PackageFreezeError>()
            else {
                panic!("retain the exact comparison source below its wrappers");
            };
            assert_eq!(actual, &original_source);
            assert_eq!(actual.detail(), original_source.detail());
            assert_eq!(
                actual.downcast_ref::<ComparisonPrepareError>(),
                Some(&original)
            );
            let service = service(error);
            let failure = service.execution_failure().unwrap();
            assert_eq!(failure.fact(), expected);
            assert_eq!(failure.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(failure.descriptor().descriptor.code.as_str(), public_code);
            assert_eq!(
                service.message(),
                format!("[{public_code}] pure compilation failed in its source domain")
            );
            assert_eq!(
                service.compile_control_error(),
                Some(CompileControlError::Source(retained))
            );
        }
    }
}

#[cfg(test)]
mod purpose_tests {
    use super::*;

    fn statement(sql: &str) -> Statement {
        novarocks_query_application::sql::parse_single_statement(sql).unwrap()
    }

    #[test]
    fn public_scalar_complete_uuid_is_named_before_the_original_missing_proof_error() {
        use novarocks_physical_plan::{
            ResultField, ResultPort, ResultValueDomain, ValueId, ValueType,
        };
        use novarocks_type_contract::ValueLogicalType;
        let ty = ValueType::try_with_logical_type(
            arrow::datatypes::DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap();
        let port = ResultPort {
            fragment: novarocks_physical_plan::FragmentId::new(1),
            output: novarocks_physical_plan::OutputPort {
                node: novarocks_physical_plan::NodeId::new(1),
                columns: vec![ValueId::new(1)].into(),
            },
            fields: vec![ResultField {
                domain: ResultValueDomain::Plain,
                name: "u".into(),
                alias: None,
                value: ValueId::new(1),
                ty,
            }]
            .into(),
            scalar_schema: None,
        };
        let error = scalar_root_output_from_public_port(Some(&port)).unwrap_err();
        assert!(
            matches!(error, FrontendQueryCompilerError::Engine(ref text) if text.contains("UUID") && text.contains("unsupported"))
        );
        let error = scalar_root_output_from_public_port(None).unwrap_err();
        assert!(
            matches!(error, FrontendQueryCompilerError::Engine(ref text) if text == "completed scalar query has no frozen SQL scalar schema")
        );
    }

    #[test]
    fn client_admission_uses_only_the_exact_local_immediate_shape() {
        for sql in [
            "SELECT * FROM information_schema.materialized_views",
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS WHERE IS_ACTIVE = true ORDER BY TABLE_NAME",
        ] {
            let parsed = statement(sql);
            let Statement::Query(query) = &parsed else {
                panic!("expected query")
            };
            assert_eq!(
                FrontendQueryPurpose::client_query(query),
                FrontendQueryPurpose::LocalRows,
                "{sql}"
            );
            assert!(FrontendQueryPurpose::LocalRows.validate(&parsed).is_ok());
            assert!(FrontendQueryPurpose::ClientRows.validate(&parsed).is_err());
        }
        for sql in [
            "SELECT 1",
            "SELECT TABLE_NAME FROM information_schema.tables",
            "SELECT * FROM catalog.information_schema.materialized_views",
            "SELECT * FROM information_schema.materialized_views m JOIN t ON m.TABLE_NAME = t.name",
            "SELECT * FROM information_schema.materialized_views UNION ALL SELECT * FROM information_schema.materialized_views",
            "SELECT * FROM (SELECT * FROM information_schema.materialized_views) m",
        ] {
            let parsed = statement(sql);
            let Statement::Query(query) = &parsed else {
                panic!("expected query")
            };
            assert_eq!(
                FrontendQueryPurpose::client_query(query),
                FrontendQueryPurpose::ClientRows,
                "{sql}"
            );
            assert!(FrontendQueryPurpose::ClientRows.validate(&parsed).is_ok());
            assert!(FrontendQueryPurpose::LocalRows.validate(&parsed).is_err());
        }
    }

    #[test]
    fn synchronous_read_purposes_reject_explain_and_do_not_infer_scalar_output() {
        let select = statement("SELECT 1");
        assert!(FrontendQueryPurpose::ClientRows.validate(&select).is_ok());
        assert!(FrontendQueryPurpose::ScalarValue.validate(&select).is_ok());
        for sql in ["EXPLAIN SELECT 1", "EXPLAIN ANALYZE SELECT 1"] {
            let explain = statement(sql);
            for purpose in [
                FrontendQueryPurpose::ClientRows,
                FrontendQueryPurpose::LocalRows,
                FrontendQueryPurpose::ScalarValue,
            ] {
                assert!(purpose.validate(&explain).is_err(), "{sql}: {purpose:?}");
            }
        }
        let local = statement("SELECT TABLE_NAME FROM information_schema.materialized_views");
        assert!(FrontendQueryPurpose::ScalarValue.validate(&local).is_ok());
    }
}
