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

//! Async ordinary Read completion in the original statement source.
use super::*;
impl FrontendQueryCompiler {
    pub(crate) async fn prepare_read_async(
        &self,
        statement: &Statement,
        purpose: FrontendQueryPurpose,
        context: &RequestContext,
        query_options: Option<QueryOptions>,
        scope: &novarocks_workload_control::WorkScope,
        provider_source: &crate::query_execution::completion_facts::FrontendCompletionCustody,
        io: Arc<dyn novarocks_spi::connector::ConnectorIoAdmissionPort>,
        #[cfg(novarocks_dev_controls)] rewrite_statement: Option<
            &crate::mv::test_controls::MvRewriteStatementTarget,
        >,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
        purpose.validate(statement)?;
        let Statement::Query(original) = statement else {
            return Err(FrontendQueryCompilerError::Engine(
                "read preparation requires a query".into(),
            ));
        };
        if let Some(result) = information_schema::try_query_materialized_views(
            self.system_tables.mv_readiness().as_ref(),
            original,
        )? {
            return Ok(PreparedQueryOperation::immediate(result));
        }
        let planning = connector_planning_context_for_query_on_runtime(
            self.connector_blocking_io.runtime(),
            query_options.as_ref(),
            context.execution().cancellation().clone(),
        )?;
        let planning = novarocks_spi::connector::ConnectorPlanningContext::try_from_request(
            planning.request().clone().with_io_admission(io),
        )
        .map_err(|e| FrontendQueryCompilerError::Engine(e.to_string()))?;
        let connector_context = planning.request();
        let current_catalog = context.session().current_catalog();
        let current_database = context.session().current_database();
        let mut query = original.clone();
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
                context.execution().sql_semantics(),
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
        novarocks_query_application::system_catalog_rewrite::rewrite_query_async(
            self.system_tables.facts_port().as_ref(),
            self.system_tables.system_catalog().as_ref(),
            connector_context,
            &mut query,
        )
        .await?;
        self.complete_async_read(
            purpose,
            &query,
            current_catalog,
            current_database,
            query_options,
            planning,
            context.execution(),
            provider_source,
            scope,
            #[cfg(novarocks_dev_controls)]
            rewrite_statement,
        )
        .await
    }
    async fn complete_async_read(
        &self,
        purpose: FrontendQueryPurpose,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_planning_context: novarocks_spi::connector::ConnectorPlanningContext,
        execution: &QueryExecutionContext,
        provider_source: &crate::query_execution::completion_facts::FrontendCompletionCustody,
        scope: &novarocks_workload_control::WorkScope,
        #[cfg(novarocks_dev_controls)] rewrite_statement: Option<
            &crate::mv::test_controls::MvRewriteStatementTarget,
        >,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
        let connector_context = connector_planning_context.request();
        let bindings = Arc::new(
            crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
                .expect("query table binding scope allocation must not fail"),
        );
        crate::query_execution::completion_facts::install_frontend_fact_source(
            provider_source,
            self.completion_fact_owners(),
            crate::query_execution::completion_facts::StatementFactScope::governed_read_io(
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
        let completed = provider_source
            .complete(request, scope)
            .await
            .map_err(|_| {
                FrontendQueryCompilerError::Engine(
                    "completion Provider actual exit is unproven".into(),
                )
            })?
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
}
