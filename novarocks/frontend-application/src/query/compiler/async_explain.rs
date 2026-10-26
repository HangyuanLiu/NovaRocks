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

//! The real EXPLAIN path observes provider facts asynchronously and retains
//! the original pure SQL terminal and closed Profile computation creator.

use super::*;

pub(crate) enum PreparedAsyncExplain {
    Lines(Vec<String>),
    Profile(PreparedAsyncProfile),
}

pub(crate) struct PreparedAsyncProfile {
    pub(crate) computation:
        Option<crate::query_execution::logical_execution::PreparedLogicalComputation>,
    pub(crate) output: crate::query_execution::internal_consumer::profile::ProfileComputationOutput,
    pub(crate) completion: crate::query_execution::completion::PreparedQueryCompletion,
}

impl FrontendQueryCompiler {
    pub(crate) async fn prepare_explain_async(
        &self,
        explain: &ExplainQuery,
        context: &RequestContext,
        query_options: Option<QueryOptions>,
        scope: &novarocks_workload_control::WorkScope,
        provider_source: &crate::query_execution::completion_facts::FrontendCompletionCustody,
    ) -> Result<PreparedAsyncExplain, FrontendQueryCompilerError> {
        let connector_planning_context = connector_planning_context_for_query_on_runtime(
            self.connector_blocking_io.runtime(),
            query_options.as_ref(),
            context.execution().cancellation().clone(),
        )?;
        let execution = context.execution();
        let current_catalog = context.session().current_catalog();
        let current_database = context.session().current_database();
        let connector_context = connector_planning_context.request();
        let mut query = explain.query.as_ref().clone();
        self.view
            .view_service()
            .rewrite_query_async(
                &crate::view::engine::FrontendViewEngine::new(self.view.clone()),
                &mut query,
                ViewRequestContext {
                    current_catalog,
                    current_database,
                    connector_context: Some(connector_context),
                },
                execution.sql_semantics(),
            )
            .await?;
        if has_time_travel_refs(&query) {
            crate::query_execution::planning::time_travel::rewrite_time_travel_refs_async(
                &self.query,
                current_catalog,
                current_database,
                &mut query,
                connector_context,
            )
            .await?;
        }
        let (level, logical_only) = explain_mode(explain);
        if explain.format == ExplainFormat::Analyze {
            return self
                .complete_async_profile(
                    &query,
                    current_catalog,
                    current_database,
                    Some(query_options_for_explain_analyze(query_options)),
                    &connector_planning_context,
                    execution,
                    scope,
                    std::time::Instant::now(),
                    provider_source,
                )
                .await;
        }
        if logical_only {
            use novarocks_query_application::preparation::CatalogFactPort;
            let control = sql_compile_control_from_execution(execution);
            let logical = novarocks_sql::compiler::SqlLogicalExplainCatalog::try_new(
                query,
                SqlSessionContext {
                    sql_semantics: execution.sql_semantics().clone(),
                    current_catalog: current_catalog.map(str::to_string),
                    current_database: current_database.to_string(),
                    optimizer_settings: execution.optimizer_settings().clone(),
                },
                control,
            )
            .map_err(FrontendQueryCompilerError::from_compile)?;
            let bindings = Arc::new(
                QueryTableBindingStore::try_new().map_err(FrontendQueryCompilerError::Engine)?,
            );
            let scope = crate::query_execution::completion_facts::StatementFactScope::binding_only(
                bindings,
                connector_context.clone(),
                current_catalog,
                scope,
                execution.result_capacity().cloned().ok_or_else(|| {
                    FrontendQueryCompilerError::Engine(
                        "logical EXPLAIN requires the admitted result capacity".to_string(),
                    )
                })?,
            )
            .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?;
            let catalog = crate::query_execution::completion_facts::FrontendCatalogFacts::new(
                self.completion_fact_owners(),
                scope,
            );
            let facts = catalog
                .resolve_relations(logical.needs())
                .await
                .map_err(FrontendQueryCompilerError::Engine)?;
            return logical
                .finish(
                    &facts,
                    self.functions.as_ref(),
                    crate::query_execution::constant_eval::constant_evaluator(),
                    self.constant_policy(),
                    SqlPlanningEnvironment::Distributed,
                    level,
                )
                .map(PreparedAsyncExplain::Lines)
                .map_err(FrontendQueryCompilerError::from_compile);
        }
        self.complete_async_explain(
            &query,
            current_catalog,
            current_database,
            query_options,
            connector_planning_context,
            execution,
            scope,
            level,
            provider_source,
        )
        .await
    }

    async fn complete_async_explain(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_planning_context: novarocks_spi::connector::ConnectorPlanningContext,
        execution: &QueryExecutionContext,
        scope: &novarocks_workload_control::WorkScope,
        level: ExplainLevel,
        provider_source: &crate::query_execution::completion_facts::FrontendCompletionCustody,
    ) -> Result<PreparedAsyncExplain, FrontendQueryCompilerError> {
        let connector_context = connector_planning_context.request();
        let bindings = Arc::new(
            crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
                .expect("query table binding scope allocation must not fail"),
        );
        crate::query_execution::completion_facts::install_frontend_fact_source(
            provider_source,
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
        )
        .map_err(FrontendQueryCompilerError::Engine)?;
        let request = novarocks_sql::compiler::SqlFinalPlanCompileRequest::new(
            crate::query_execution::physical_encoding::mint_plan_version(),
            SqlStatementInput::parsed_query(Box::new(query.clone())),
            SqlCompileIntent::Explain {
                level,
                analyze: false,
            },
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
        let control = request.control().clone();
        let completed = provider_source
            .complete(request, scope)
            .await
            // The original typed health and entire Provider/result remain in
            // the action's source. This safe value is never published as a
            // domain verdict: QA checks local_health_failure before Ready.
            .map_err(|_| {
                FrontendQueryCompilerError::Engine(
                    "completion Provider actual exit is unproven".to_owned(),
                )
            })?
            .map_err(|failure| FrontendQueryCompilerError::from_completion(failure.error()))?;
        let lines = completed
            .candidate()
            .render_explain_lines(
                novarocks_sql::compiler::ExplainRenderBudget::default(),
                &control,
            )
            .map_err(|error| FrontendQueryCompilerError::from_completion(&error))?;
        Ok(PreparedAsyncExplain::Lines(lines))
    }

    async fn complete_async_profile(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_planning_context: &novarocks_spi::connector::ConnectorPlanningContext,
        execution: &QueryExecutionContext,
        scope: &novarocks_workload_control::WorkScope,
        planning_started_at: std::time::Instant,
        provider_source: &crate::query_execution::completion_facts::FrontendCompletionCustody,
    ) -> Result<PreparedAsyncExplain, FrontendQueryCompilerError> {
        let bindings = Arc::new(
            QueryTableBindingStore::try_new()
                .expect("query table binding scope allocation must not fail"),
        );
        crate::query_execution::completion_facts::install_frontend_fact_source(
            provider_source,
            self.completion_fact_owners(),
            crate::query_execution::completion_facts::StatementFactScope::binding_only(
                Arc::clone(&bindings),
                connector_planning_context.request().clone(),
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
        )
        .map_err(FrontendQueryCompilerError::Engine)?;
        let version = crate::query_execution::physical_encoding::mint_plan_version();
        let request = novarocks_sql::compiler::SqlFinalPlanCompileRequest::new(
            version,
            SqlStatementInput::parsed_query(Box::new(query.clone())),
            SqlCompileIntent::Explain {
                level: ExplainLevel::Analyze,
                analyze: true,
            },
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
        let completed = provider_source
            .complete(request, scope)
            .await
            // The original typed health and entire Provider/result remain in
            // the action's source. This safe value is never published as a
            // domain verdict: QA checks local_health_failure before Ready.
            .map_err(|_| {
                FrontendQueryCompilerError::Engine(
                    "completion Provider actual exit is unproven".to_owned(),
                )
            })?
            .map_err(|failure| FrontendQueryCompilerError::from_completion(failure.error()))?;
        let completed = completed
            .freeze_root_output(novarocks_result_contract::FrozenRootOutput::CountOnly)
            .map_err(|(error, _returned)| FrontendQueryCompilerError::Engine(error.to_string()))?;
        let plan = Arc::clone(completed.candidate().plan());
        let annotations: Arc<[novarocks_sql::compiler::SqlDisplayAnnotation]> =
            completed.candidate().display_annotations().to_vec().into();
        let output =
            novarocks_query_application::preparation::OutputContract::from_completed_candidate(
                novarocks_query_application::api::QueryExecutionKind::Read,
                completed.candidate(),
            )
            .map_err(FrontendQueryCompilerError::Engine)?;
        let encoded = crate::query_execution::physical_encoding::encode_completed_plan(
            completed,
            self.query.static_plan_carrier(),
            self.constant_policy(),
            None,
            &encoding_control,
        )
        .map_err(FrontendQueryCompilerError::from_encode)?;
        let (template, candidate) = encoded.into_attempt_template_with_candidate(version);
        let description = novarocks_query_application::preparation::FrozenExecutionDescription::for_completed_plan(
            novarocks_query_application::api::QueryExecutionKind::Read,
            candidate,
            template.attempt_scheduling_facts().map_err(FrontendQueryCompilerError::Engine)?
                .fragments.iter()
                .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan))
                .collect(),
            output,
            novarocks_coordination_contract::ExecutionEffect::None,
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
        let (computation, output) =
            crate::query_execution::internal_consumer::profile::prepare_profile_computation(
                description,
                template,
                Arc::new(
                    crate::query_execution::contract::ResolvedQueryOptions::from_upstream(
                        query_options,
                    ),
                ),
            )
            .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?;
        let completion =
            crate::query_execution::completion::PreparedQueryCompletion::completed_profile(
                plan,
                annotations,
                planning_started_at.elapsed(),
                std::time::Instant::now(),
                encoding_control,
            );
        Ok(PreparedAsyncExplain::Profile(PreparedAsyncProfile {
            computation: Some(computation),
            output,
            completion,
        }))
    }
}
