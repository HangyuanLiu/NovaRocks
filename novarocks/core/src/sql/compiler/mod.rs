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

//! The statement-scoped SQL compiler boundary.
//!
//! This module deliberately owns only neutral compiler inputs and outputs.
//! Application admission, connector execution preparation, native encoding,
//! and query lifecycle orchestration remain outside this boundary.
// Design: ADR-0025 (docs/adr/ADR-0025-sql-compiler-explicit-input-boundary.md)
// Design: ADR-0036 (docs/adr/ADR-0036-sql-compiler-dependency-inversion.md)

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use crate::sql::explain::ExplainLevel;
use crate::sql::optimizer::options::SessionOptimizerSettings;

/// SQL's read-only observation of statement cancellation.
///
/// The application owns cancellation reasons and sources.  Compiler phases
/// only need this single immutable fact, so the SQL boundary never imports
/// query-lifecycle cancellation state.
pub(crate) trait SqlCancellationObservation: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

/// A narrow catalog capability available to one compiler request.
///
/// Concrete connector clients and registry mutation APIs are intentionally not
/// part of this contract. The provider is a query-scoped snapshot and owns the
/// one binding store shared by analysis, statistics, and scan preparation.
pub(crate) trait SqlCatalogSnapshot {
    fn planner_table_provider(&self) -> &dyn crate::sql::catalog::PlannerTableProvider;
}

pub(crate) struct SqlPlannerTableSnapshot<'a> {
    provider: &'a dyn crate::sql::catalog::PlannerTableProvider,
}

impl<'a> SqlPlannerTableSnapshot<'a> {
    pub(crate) fn new(provider: &'a dyn crate::sql::catalog::PlannerTableProvider) -> Self {
        Self { provider }
    }
}

impl SqlCatalogSnapshot for SqlPlannerTableSnapshot<'_> {
    fn planner_table_provider(&self) -> &dyn crate::sql::catalog::PlannerTableProvider {
        self.provider
    }
}

/// Query statistics facts collected during one compilation.  This is a SQL
/// value: it contains no provider, control host, or mutable cache handle.
pub(crate) struct SqlStatisticsPlan {
    pub(crate) snapshot: crate::sql::optimizer::stats_input::QueryStatsSnapshot,
    next_stats_ref: u32,
}

impl SqlStatisticsPlan {
    pub(crate) fn empty() -> Self {
        Self {
            snapshot: crate::sql::optimizer::stats_input::QueryStatsSnapshot::empty(),
            next_stats_ref: 0,
        }
    }

    pub(crate) fn add_stats(
        &mut self,
        label: impl Into<String>,
        stats: crate::sql::optimizer::stats_input::BaseTableStatistics,
    ) -> crate::sql::optimizer::stats_input::StatsRef {
        let stats_ref = crate::sql::optimizer::stats_input::StatsRef::new(self.next_stats_ref);
        self.next_stats_ref += 1;
        self.snapshot.insert(stats_ref, label, stats);
        stats_ref
    }

    pub(crate) fn set_next_stats_ref(&mut self, next_stats_ref: u32) {
        self.next_stats_ref = next_stats_ref;
    }
}

/// A narrow, exact statistics capability available to one request.  It reads
/// only bindings captured by the paired catalog snapshot; it cannot ask for a
/// newer connector generation.
pub(crate) trait SqlStatisticsSnapshot {
    fn collect_table_statistics(
        &self,
        database: &str,
        table: &crate::sql::planner::table::TableDef,
    ) -> (
        String,
        crate::sql::optimizer::stats_input::BaseTableStatistics,
    );
}

/// Required evidence for an incremental-MV rewrite.  This is data frozen by
/// application admission, not a callback that can re-enter application code
/// while the compiler is running.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvRewriteValidation {
    None,
    Aggregate,
    JoinAggregate,
    BranchUnionAggregate,
}

/// Application-frozen incremental-MV planning input.  The compiler owns the
/// rewrite pipeline and its validation; application provides only the exact
/// immutable refresh facts captured for this statement.
#[derive(Clone)]
pub(crate) struct SqlImvPlanningInput {
    pub(crate) snapshot: Arc<mv_rewrite::SqlImvRewriteSnapshot>,
    pub(crate) validation: SqlImvRewriteValidation,
}

impl SqlImvPlanningInput {
    pub(crate) fn new(
        snapshot: Arc<mv_rewrite::SqlImvRewriteSnapshot>,
        validation: SqlImvRewriteValidation,
    ) -> Self {
        Self {
            snapshot,
            validation,
        }
    }
}

/// Immutable SQL function semantics used by analysis and optimization.
///
/// The function implementation and its execution kernels are explicitly out
/// of scope for this compiler-facing contract.
pub(crate) trait SqlFunctionCatalog: Send + Sync {
    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<crate::sql::functions::ResolvedScalarFunction, crate::sql::functions::ResolveError>;

    fn volatility(&self, name: &str) -> crate::sql::functions::FunctionVolatility;
}

/// Statement material already owned by the SQL boundary.
#[derive(Clone, Debug)]
pub(crate) enum SqlStatementInput {
    Sql(String),
    ParsedQuery(Box<sqlparser::ast::Query>),
    /// A SQL-owned logical transformation that must re-enter the canonical
    /// optimizer kernel without reopening catalog resolution.  Application
    /// code uses this only after compiler-produced logical facts have been
    /// transformed by SQL-owned MV planning.
    LogicalPlan {
        plan: crate::sql::planner::logical::LogicalPlanNode,
        factory: crate::sql::column_id::ColumnRefFactory,
    },
}

/// The compiler result shape required by the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlCompileIntent {
    Query,
    Explain {
        level: ExplainLevel,
        analyze: bool,
    },
    AnalyzeOnly,
    LogicalOnly,
    IcebergWrite {
        root_distribution: RootDistributionRequirement,
    },
    ChangeStreamWrite,
}

/// A SQL-owned replacement for write-planning callbacks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RootDistributionRequirement {
    Any,
    ShuffleOutputOrdinal(usize),
    ShuffleOutputName(String),
}

/// SQL-relevant session state frozen at statement admission.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlSessionContext {
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) optimizer_settings: SessionOptimizerSettings,
}

/// Deployment facts consumed by planning without exposing topology objects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlPlanningEnvironment {
    Distributed { backend_count: NonZeroUsize },
    NotApplicable,
}

/// Read-only request control observed by compiler phases.
#[derive(Clone)]
pub(crate) struct SqlCompileControl {
    deadline: Option<Instant>,
    cancellation: Arc<dyn SqlCancellationObservation>,
}

struct SqlNeverCancelled;

impl SqlCancellationObservation for SqlNeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

impl SqlCompileControl {
    pub(crate) fn new(
        deadline: Option<Instant>,
        cancellation: Arc<dyn SqlCancellationObservation>,
    ) -> Self {
        Self {
            deadline,
            cancellation,
        }
    }

    /// Explicitly unbounded control for an already-admitted SQL logical
    /// transformation.  It is not an execution fallback: callers that have
    /// a request control must still pass its deadline and cancellation view.
    pub(crate) fn unbounded() -> Self {
        Self {
            deadline: None,
            cancellation: Arc::new(SqlNeverCancelled),
        }
    }

    pub(crate) fn check(&self) -> Result<(), SqlCompileError> {
        if self.cancellation.is_cancelled() {
            return Err(SqlCompileError::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(SqlCompileError::DeadlineExceeded);
        }
        Ok(())
    }

    fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

/// A logical request cannot resolve another table or function.  These values
/// exist only to keep the request shape uniform; any accidental use is a
/// compiler contract violation rather than an application fallback.
struct SqlLogicalInputCatalog;

impl SqlCatalogSnapshot for SqlLogicalInputCatalog {
    fn planner_table_provider(&self) -> &dyn crate::sql::catalog::PlannerTableProvider {
        panic!("logical SQL compiler input must not resolve catalog tables")
    }
}

struct SqlLogicalInputFunctions;

impl SqlFunctionCatalog for SqlLogicalInputFunctions {
    fn resolve_scalar_signature(
        &self,
        _name: &str,
        _arg_types: &[arrow::datatypes::DataType],
    ) -> Result<crate::sql::functions::ResolvedScalarFunction, crate::sql::functions::ResolveError>
    {
        panic!("logical SQL compiler input must not resolve functions")
    }

    fn volatility(&self, _name: &str) -> crate::sql::functions::FunctionVolatility {
        panic!("logical SQL compiler input must not resolve functions")
    }
}

static SQL_LOGICAL_INPUT_CATALOG: SqlLogicalInputCatalog = SqlLogicalInputCatalog;
static SQL_LOGICAL_INPUT_FUNCTIONS: SqlLogicalInputFunctions = SqlLogicalInputFunctions;

/// Conservative statistics source for SQL-owned logical transformations that
/// cannot admit a new catalog binding.  Missing evidence remains missing; it
/// is never guessed as an empty table.
pub(crate) struct SqlUnavailableStatisticsSnapshot;

impl SqlStatisticsSnapshot for SqlUnavailableStatisticsSnapshot {
    fn collect_table_statistics(
        &self,
        database: &str,
        table: &crate::sql::planner::table::TableDef,
    ) -> (
        String,
        crate::sql::optimizer::stats_input::BaseTableStatistics,
    ) {
        (
            format!("{database}.{}", table.name),
            crate::sql::optimizer::stats_input::BaseTableStatistics::missing(
                crate::sql::optimizer::stats_input::StatsMissingReason::ConnectorUnsupported(
                    "logical SQL compiler input has no additional statistics evidence".to_string(),
                ),
            ),
        )
    }
}

/// Complete immutable input consumed by the pure SQL compiler.
pub(crate) struct SqlCompileRequest<'a> {
    pub(crate) statement: SqlStatementInput,
    pub(crate) intent: SqlCompileIntent,
    pub(crate) session: SqlSessionContext,
    pub(crate) environment: SqlPlanningEnvironment,
    pub(crate) catalog: &'a dyn SqlCatalogSnapshot,
    pub(crate) statistics: &'a dyn SqlStatisticsSnapshot,
    pub(crate) functions: &'a dyn SqlFunctionCatalog,
    pub(crate) mv_rewrite: Option<&'a mv_rewrite::MvRewriteDefinitionIndex>,
    pub(crate) imv_rewrite: Option<&'a SqlImvPlanningInput>,
    pub(crate) control: SqlCompileControl,
}

impl<'a> SqlCompileRequest<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        statement: SqlStatementInput,
        intent: SqlCompileIntent,
        session: SqlSessionContext,
        environment: SqlPlanningEnvironment,
        catalog: &'a dyn SqlCatalogSnapshot,
        statistics: &'a dyn SqlStatisticsSnapshot,
        functions: &'a dyn SqlFunctionCatalog,
        mv_rewrite: Option<&'a mv_rewrite::MvRewriteDefinitionIndex>,
        control: SqlCompileControl,
    ) -> Self {
        Self {
            statement,
            intent,
            session,
            environment,
            catalog,
            statistics,
            functions,
            mv_rewrite,
            imv_rewrite: None,
            control,
        }
    }

    /// Build a request for an already SQL-owned logical plan.  It deliberately
    /// has no catalog, function, or MV-candidate callback: the input has
    /// already crossed analysis and the compiler may only optimize its frozen
    /// SQL facts.
    pub(crate) fn new_logical(
        plan: crate::sql::planner::logical::LogicalPlanNode,
        factory: crate::sql::column_id::ColumnRefFactory,
        intent: SqlCompileIntent,
        session: SqlSessionContext,
        environment: SqlPlanningEnvironment,
        statistics: &'a dyn SqlStatisticsSnapshot,
        control: SqlCompileControl,
    ) -> Self {
        Self {
            statement: SqlStatementInput::LogicalPlan { plan, factory },
            intent,
            session,
            environment,
            catalog: &SQL_LOGICAL_INPUT_CATALOG,
            statistics,
            functions: &SQL_LOGICAL_INPUT_FUNCTIONS,
            mv_rewrite: None,
            imv_rewrite: None,
            control,
        }
    }

    pub(crate) fn check_control(&self) -> Result<(), SqlCompileError> {
        self.control.check()
    }

    fn deadline(&self) -> Option<Instant> {
        self.control.deadline()
    }

    pub(crate) fn with_imv_rewrite(mut self, input: &'a SqlImvPlanningInput) -> Self {
        self.imv_rewrite = Some(input);
        self
    }
}

pub(crate) struct SqlAnalysisOutput {
    pub(crate) logical_plan: crate::sql::planner::logical::LogicalPlanNode,
    pub(crate) factory: crate::sql::column_id::ColumnRefFactory,
}

pub(crate) struct SqlOptimizedOutput {
    pub(crate) optimized_tree: crate::sql::optimizer::OptimizedOperatorNode,
    pub(crate) statistics: SqlStatisticsPlan,
    pub(crate) change_stream:
        crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
    pub(crate) mv_rewrite_diagnostics: Vec<mv_rewrite::SqlMvRewriteDiagnostic>,
}

pub(crate) struct SqlDistributedOutput {
    pub(crate) distributed_plan: crate::sql::planner::distributed::DistributedPlan,
    pub(crate) statistics: SqlStatisticsPlan,
    pub(crate) mv_rewrite_diagnostics: Vec<mv_rewrite::SqlMvRewriteDiagnostic>,
}

/// SQL-owned compiler facts. Native DTOs/bytes, lifecycle state and result
/// buffers are intentionally absent; application owns post-compile assembly.
pub(crate) enum SqlCompileOutput {
    Analysis(SqlAnalysisOutput),
    Logical(SqlAnalysisOutput),
    Optimized(SqlOptimizedOutput),
    ImmediateExplain(Vec<String>),
    Distributed(SqlDistributedOutput),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlCompileError {
    Cancelled,
    DeadlineExceeded,
    InvalidRequest(String),
    Compilation(String),
}

impl std::fmt::Display for SqlCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => f.write_str("SQL compilation was cancelled"),
            Self::DeadlineExceeded => f.write_str("SQL compilation deadline exceeded"),
            Self::InvalidRequest(error) | Self::Compilation(error) => f.write_str(error),
        }
    }
}

impl std::error::Error for SqlCompileError {}

/// The canonical parse/analyze/logical/optimize/physical/distributed kernel.
/// It accepts only immutable SQL facts and checks request control at every
/// externally visible phase boundary.
pub(crate) struct SqlCompiler;

impl SqlCompiler {
    pub(crate) fn compile(
        request: SqlCompileRequest<'_>,
    ) -> Result<SqlCompileOutput, SqlCompileError> {
        request.check_control()?;
        let (mut logical_plan, mut factory, logical_input) = match &request.statement {
            SqlStatementInput::LogicalPlan { plan, factory } => {
                (plan.clone(), factory.clone(), true)
            }
            _ => {
                let query = parse_query(&request.statement)?;
                let catalog = request.catalog.planner_table_provider();
                let (resolved, ctes, mut factory) =
                    crate::sql::analyzer::analyze_with_function_catalog(
                        &query,
                        catalog,
                        &request.session.current_database,
                        request.functions,
                    )
                    .map_err(SqlCompileError::Compilation)?;
                request.check_control()?;
                let logical_plan = crate::sql::planner::plan_query(resolved, ctes, &mut factory)
                    .map_err(SqlCompileError::Compilation)?;
                (logical_plan, factory, false)
            }
        };
        request.check_control()?;

        if matches!(request.intent, SqlCompileIntent::AnalyzeOnly) {
            return Ok(SqlCompileOutput::Analysis(SqlAnalysisOutput {
                logical_plan,
                factory,
            }));
        }
        if matches!(request.intent, SqlCompileIntent::LogicalOnly) && request.imv_rewrite.is_none()
        {
            return Ok(SqlCompileOutput::Logical(SqlAnalysisOutput {
                logical_plan,
                factory,
            }));
        }
        let mut settings = request.session.optimizer_settings.clone();
        if !matches!(request.intent, SqlCompileIntent::LogicalOnly)
            && !(logical_input
                && matches!(request.environment, SqlPlanningEnvironment::NotApplicable))
        {
            apply_planning_environment(&mut settings, request.environment)?;
        }
        let mut change_stream = logical_input
            .then(|| {
                crate::sql::planner::imv_rewrite::change_stream::build_change_stream_descriptor(
                    &logical_plan,
                )
            })
            .unwrap_or_default();
        if let Some(input) = request.imv_rewrite {
            if !matches!(
                request.intent,
                SqlCompileIntent::ChangeStreamWrite
                    | SqlCompileIntent::Explain { analyze: false, .. }
                    | SqlCompileIntent::LogicalOnly
            ) {
                return Err(SqlCompileError::InvalidRequest(
                    "incremental MV rewrite requires ChangeStreamWrite, LogicalOnly, or non-analyze EXPLAIN intent"
                        .to_string(),
                ));
            }
            let factory_cell = std::rc::Rc::new(std::cell::RefCell::new(factory));
            let outcome = crate::sql::planner::imv_rewrite::entrypoint::run_imv_rewrite(
                crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteInput {
                    plan: crate::sql::planner::imv_rewrite::entrypoint::normalize_imv_rewrite_root_project(logical_plan),
                    snapshot: Arc::clone(&input.snapshot),
                    disabled_rules: settings.disabled_rules.clone(),
                    deadline: request.deadline(),
                    column_ref_factory: std::rc::Rc::clone(&factory_cell),
                },
            )
            .map_err(|error| SqlCompileError::Compilation(format!("imv rewrite: {error}")))?;
            validate_imv_rewrite_outcome(input, &outcome)?;
            logical_plan = outcome.plan;
            change_stream = outcome.annotation.change_stream;
            factory = std::rc::Rc::try_unwrap(factory_cell)
                .map_err(|_| {
                    SqlCompileError::Compilation(
                        "IMV rewrite leaked ColumnRefFactory references".to_string(),
                    )
                })?
                .into_inner();
            request.check_control()?;
        }
        if matches!(request.intent, SqlCompileIntent::LogicalOnly) {
            return Ok(SqlCompileOutput::Logical(SqlAnalysisOutput {
                logical_plan,
                factory,
            }));
        }
        let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
        let mut optimizer_expr =
            crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
                &logical_plan,
                &mut scalar_arena,
            )
            .map_err(SqlCompileError::Compilation)?;
        let mut statistics = collect_statistics(request.statistics, &mut optimizer_expr);
        request.check_control()?;
        let mv_rewrite = request
            .mv_rewrite
            .map(|definitions| {
                let catalog = request.catalog.planner_table_provider();
                mv_rewrite::prepare_candidates(
                    definitions,
                    catalog,
                    &request.session.current_database,
                    &logical_plan,
                    &mut factory,
                    request.functions,
                    request.statistics,
                    &mut statistics,
                    &settings,
                )
            })
            .unwrap_or_else(|| mv_rewrite::SqlMvRewritePreparation {
                candidates: Vec::new(),
                diagnostics: Vec::new(),
            });
        let mv_rewrite::SqlMvRewritePreparation {
            candidates: mv_candidates,
            diagnostics: mv_rewrite_diagnostics,
        } = mv_rewrite;
        request.check_control()?;
        let root_distribution = match &request.intent {
            SqlCompileIntent::IcebergWrite { root_distribution } => {
                resolve_root_distribution_requirement(&logical_plan, root_distribution)?
            }
            _ => None,
        };
        let optimized_tree = match root_distribution {
            Some(root_distribution) => crate::sql::optimizer::optimize_with_root_distribution(
                optimizer_expr,
                scalar_arena,
                &statistics.snapshot,
                factory,
                root_distribution,
                &settings,
            ),
            None => crate::sql::optimizer::optimize(
                optimizer_expr,
                scalar_arena,
                &statistics.snapshot,
                factory,
                mv_candidates,
                &settings,
            ),
        }
        .map_err(SqlCompileError::Compilation)?;
        request.check_control()?;

        if let SqlCompileIntent::Explain {
            level,
            analyze: false,
        } = request.intent
        {
            let mut lines = Vec::new();
            if matches!(level, ExplainLevel::Costs) {
                lines.extend(statistics.snapshot.display_rows());
            }
            let physical = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .map_err(SqlCompileError::Compilation)?;
            let distributed = crate::sql::planner::pipeline::build_distributed_plan_with_settings(
                physical, &settings,
            )
            .map_err(SqlCompileError::Compilation)?;
            lines.extend(crate::sql::explain::distributed::explain_distributed_plan(
                &distributed,
                level,
            ));
            return Ok(SqlCompileOutput::ImmediateExplain(lines));
        }

        if matches!(
            request.intent,
            SqlCompileIntent::IcebergWrite { .. } | SqlCompileIntent::ChangeStreamWrite
        ) {
            return Ok(SqlCompileOutput::Optimized(SqlOptimizedOutput {
                optimized_tree,
                statistics,
                change_stream,
                mv_rewrite_diagnostics,
            }));
        }

        let physical = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
            .map_err(SqlCompileError::Compilation)?;
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan_with_settings(
            physical, &settings,
        )
        .map_err(SqlCompileError::Compilation)?;
        request.check_control()?;
        Ok(SqlCompileOutput::Distributed(SqlDistributedOutput {
            distributed_plan,
            statistics,
            mv_rewrite_diagnostics,
        }))
    }
}

fn parse_query(statement: &SqlStatementInput) -> Result<sqlparser::ast::Query, SqlCompileError> {
    let sql = match statement {
        SqlStatementInput::Sql(sql) => sql,
        SqlStatementInput::ParsedQuery(query) => return Ok((**query).clone()),
        SqlStatementInput::LogicalPlan { .. } => {
            return Err(SqlCompileError::InvalidRequest(
                "logical SQL compiler input must bypass parsing".to_string(),
            ));
        }
    };
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
        .map_err(SqlCompileError::Compilation)?;
    match crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|error| SqlCompileError::Compilation(error.to_string()))?
    {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        _ => Err(SqlCompileError::InvalidRequest(
            "SQL compiler requires a query statement after application preprocessing".to_string(),
        )),
    }
}

pub(crate) fn validate_imv_rewrite_outcome(
    input: &SqlImvPlanningInput,
    outcome: &crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteOutcome,
) -> Result<(), SqlCompileError> {
    let target = input.snapshot.target.fqn();
    let rule_changed = |rule_name: &str| {
        outcome.trace.events().iter().any(|event| {
            matches!(
                event,
                crate::sql::optimizer::rewrite::trace::RewriteTraceEvent::RuleChanged { rule, .. }
                    if *rule == rule_name
            )
        })
    };
    if input.validation == SqlImvRewriteValidation::JoinAggregate
        && !rule_changed("RewriteJoinDelta")
    {
        return Err(SqlCompileError::Compilation(format!(
            "iceberg join aggregate MV {target} incremental refresh rewrite did not apply RewriteJoinDelta"
        )));
    }
    if input.validation == SqlImvRewriteValidation::BranchUnionAggregate
        && !rule_changed("RewriteBranchUnion")
    {
        return Err(SqlCompileError::Compilation(format!(
            "iceberg branch UNION ALL aggregate MV {target} incremental refresh rewrite did not apply RewriteBranchUnion"
        )));
    }
    if input.validation != SqlImvRewriteValidation::None
        && input.validation != SqlImvRewriteValidation::BranchUnionAggregate
        && !rule_changed("RewriteAggregateState")
    {
        let label = match input.validation {
            SqlImvRewriteValidation::JoinAggregate => "join aggregate",
            _ => "aggregate",
        };
        return Err(SqlCompileError::Compilation(format!(
            "iceberg {label} MV {target} incremental refresh rewrite did not apply RewriteAggregateState"
        )));
    }
    if input.validation != SqlImvRewriteValidation::None
        && !outcome.annotation.change_stream.has_aggregate()
    {
        let label = match input.validation {
            SqlImvRewriteValidation::JoinAggregate => "join aggregate",
            SqlImvRewriteValidation::BranchUnionAggregate => "branch UNION ALL aggregate",
            _ => "aggregate",
        };
        return Err(SqlCompileError::Compilation(format!(
            "iceberg {label} MV {target} incremental refresh rewrite plan does not contain aggregate state change stream"
        )));
    }
    Ok(())
}

fn resolve_root_distribution_requirement(
    logical_plan: &crate::sql::planner::logical::LogicalPlanNode,
    requirement: &RootDistributionRequirement,
) -> Result<Option<crate::sql::optimizer::property::DistributionSpec>, SqlCompileError> {
    let output_columns = crate::sql::planner::plan_output_columns(logical_plan)
        .map_err(SqlCompileError::Compilation)?;
    let column = match requirement {
        RootDistributionRequirement::Any => return Ok(None),
        RootDistributionRequirement::ShuffleOutputOrdinal(index) => {
            output_columns.get(*index).ok_or_else(|| {
                SqlCompileError::InvalidRequest(format!(
                    "cannot derive Iceberg write root shuffle: output column index {index} out of range ({} columns)",
                    output_columns.len()
                ))
            })?
        }
        RootDistributionRequirement::ShuffleOutputName(name) => {
            let mut matches = output_columns.iter().filter(|column| column.name == *name);
            let column = matches.next().ok_or_else(|| {
                SqlCompileError::InvalidRequest(format!(
                    "cannot derive Iceberg write root shuffle: output column '{name}' not found"
                ))
            })?;
            if matches.next().is_some() {
                return Err(SqlCompileError::InvalidRequest(format!(
                    "cannot derive Iceberg write root shuffle: output column '{name}' is ambiguous"
                )));
            }
            column
        }
    };
    if column.column_id == crate::sql::column_id::ColumnId::UNSET {
        return Err(SqlCompileError::InvalidRequest(format!(
            "cannot derive Iceberg write root shuffle: output column '{}' has no ColumnId",
            column.name
        )));
    }
    Ok(Some(
        crate::sql::optimizer::property::DistributionSpec::shuffle_agg([column.column_id]),
    ))
}

fn collect_statistics(
    snapshot: &dyn SqlStatisticsSnapshot,
    expr: &mut crate::sql::optimizer::opt_expr::OptExpr,
) -> SqlStatisticsPlan {
    fn walk(
        snapshot: &dyn SqlStatisticsSnapshot,
        expr: &mut crate::sql::optimizer::opt_expr::OptExpr,
        plan: &mut SqlStatisticsPlan,
    ) {
        if let crate::sql::optimizer::operator::Operator::LogicalScan(scan) = &mut expr.op {
            let stats_ref = crate::sql::optimizer::stats_input::StatsRef::new(plan.next_stats_ref);
            plan.next_stats_ref += 1;
            scan.stats_ref = Some(stats_ref);
            let (label, stats) = snapshot.collect_table_statistics(&scan.database, &scan.table);
            plan.snapshot.insert(stats_ref, label, stats);
        }
        for child in &mut expr.children {
            walk(snapshot, child, plan);
        }
    }

    let mut plan = SqlStatisticsPlan::empty();
    walk(snapshot, expr, &mut plan);
    plan
}

fn apply_planning_environment(
    settings: &mut SessionOptimizerSettings,
    environment: SqlPlanningEnvironment,
) -> Result<(), SqlCompileError> {
    match environment {
        SqlPlanningEnvironment::Distributed { backend_count } => {
            settings.effective_backend_count = Some(backend_count.get() as f64);
            Ok(())
        }
        SqlPlanningEnvironment::NotApplicable => Err(SqlCompileError::InvalidRequest(
            "distributed SQL compilation requires a frozen non-zero backend count".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use super::*;

    struct Catalog;
    impl SqlCatalogSnapshot for Catalog {
        fn planner_table_provider(&self) -> &dyn crate::sql::catalog::PlannerTableProvider {
            panic!("control tests must not reach catalog resolution")
        }
    }
    struct Statistics;
    impl SqlStatisticsSnapshot for Statistics {
        fn collect_table_statistics(
            &self,
            _database: &str,
            _table: &crate::sql::planner::table::TableDef,
        ) -> (
            String,
            crate::sql::optimizer::stats_input::BaseTableStatistics,
        ) {
            panic!("control tests must not reach statistics collection")
        }
    }
    struct Functions;
    impl SqlFunctionCatalog for Functions {
        fn resolve_scalar_signature(
            &self,
            _name: &str,
            _arg_types: &[arrow::datatypes::DataType],
        ) -> Result<
            crate::sql::functions::ResolvedScalarFunction,
            crate::sql::functions::ResolveError,
        > {
            Err(crate::sql::functions::ResolveError::UnknownFunction)
        }

        fn volatility(&self, _name: &str) -> crate::sql::functions::FunctionVolatility {
            crate::sql::functions::FunctionVolatility::Immutable
        }
    }

    static CATALOG: Catalog = Catalog;
    static STATISTICS: Statistics = Statistics;
    static FUNCTIONS: Functions = Functions;

    #[derive(Default)]
    struct Cancellation(AtomicBool);

    impl Cancellation {
        fn request(&self) {
            self.0.store(true, Ordering::Release);
        }
    }

    impl SqlCancellationObservation for Cancellation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::Acquire)
        }
    }

    fn control(deadline: Option<Instant>, cancellation: &Arc<Cancellation>) -> SqlCompileControl {
        SqlCompileControl::new(
            deadline,
            Arc::clone(cancellation) as Arc<dyn SqlCancellationObservation>,
        )
    }

    fn request(control: SqlCompileControl) -> SqlCompileRequest<'static> {
        SqlCompileRequest::new(
            SqlStatementInput::Sql("select 1".to_string()),
            SqlCompileIntent::Query,
            SqlSessionContext {
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(3).unwrap(),
            },
            &CATALOG,
            &STATISTICS,
            &FUNCTIONS,
            None,
            control,
        )
    }

    #[test]
    fn sqlx2_request_keeps_sql_owned_cancellation_observation() {
        let cancellation = Arc::new(Cancellation::default());
        let request = request(control(None, &cancellation));
        assert_eq!(request.check_control(), Ok(()));
        assert!(matches!(
            request.environment,
            SqlPlanningEnvironment::Distributed { backend_count } if backend_count.get() == 3
        ));
    }

    #[test]
    fn sqlx2_request_rejects_cancelled_control_before_compilation() {
        let cancellation = Arc::new(Cancellation::default());
        cancellation.request();
        assert!(matches!(
            SqlCompiler::compile(request(control(None, &cancellation))),
            Err(SqlCompileError::Cancelled)
        ));
    }

    #[test]
    fn sqlx2_request_rejects_expired_deadline_before_compilation() {
        let cancellation = Arc::new(Cancellation::default());
        let deadline = Instant::now() - Duration::from_millis(1);
        assert!(matches!(
            SqlCompiler::compile(request(control(Some(deadline), &cancellation))),
            Err(SqlCompileError::DeadlineExceeded)
        ));
    }

    #[test]
    fn sqlx2_request_models_metadata_planning_without_a_fake_backend() {
        let cancellation = Arc::new(Cancellation::default());
        let mut request = request(control(None, &cancellation));
        request.environment = SqlPlanningEnvironment::NotApplicable;
        assert_eq!(request.check_control(), Ok(()));
    }

    #[test]
    fn sqlx1_kernel_compiles_a_query_without_application_state() {
        let catalog = crate::sql::catalog::local::PlannerMemoryCatalog::default();
        let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let cancellation = Arc::new(Cancellation::default());
        let request = SqlCompileRequest::new(
            SqlStatementInput::Sql("select 1".to_string()),
            SqlCompileIntent::Query,
            SqlSessionContext {
                current_catalog: None,
                current_database: "default".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(3).expect("non-zero fixture topology"),
            },
            &catalog_snapshot,
            &STATISTICS,
            crate::sql::functions::builtin_sql_function_catalog(),
            None,
            control(None, &cancellation),
        );

        assert!(matches!(
            SqlCompiler::compile(request),
            Ok(SqlCompileOutput::Distributed(_))
        ));
    }

    #[test]
    fn sqlx2_kernel_compiles_a_query_from_sql_owned_inputs() {
        let catalog = crate::sql::catalog::local::PlannerMemoryCatalog::default();
        let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let cancellation = Arc::new(Cancellation::default());
        let request = SqlCompileRequest::new(
            SqlStatementInput::Sql("select 1".to_string()),
            SqlCompileIntent::Query,
            SqlSessionContext {
                current_catalog: None,
                current_database: "default".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(3).expect("non-zero fixture topology"),
            },
            &catalog_snapshot,
            &STATISTICS,
            crate::sql::functions::builtin_sql_function_catalog(),
            None,
            control(None, &cancellation),
        );

        assert!(matches!(
            SqlCompiler::compile(request),
            Ok(SqlCompileOutput::Distributed(_))
        ));
    }

    #[test]
    fn parsed_query_input_preserves_complex_types_and_escapes_without_sql_round_trip() {
        let statement =
            crate::sql::parser::parse_sql_raw(r"SELECT CAST('{}' AS MAP<STRING, INT>), 'e\\f'")
                .expect("query fixture must parse");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query fixture");
        };

        assert_eq!(
            parse_query(&SqlStatementInput::ParsedQuery(query.clone())),
            Ok(*query)
        );
    }

    #[test]
    fn sqlx1_kernel_write_root_requirement_is_validated_in_sql() {
        let catalog = crate::sql::catalog::local::PlannerMemoryCatalog::default();
        let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let cancellation = Arc::new(Cancellation::default());
        let request = SqlCompileRequest::new(
            SqlStatementInput::Sql("select 1 as payload".to_string()),
            SqlCompileIntent::IcebergWrite {
                root_distribution: RootDistributionRequirement::ShuffleOutputName(
                    "missing".to_string(),
                ),
            },
            SqlSessionContext {
                current_catalog: None,
                current_database: "default".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(3).expect("non-zero fixture topology"),
            },
            &catalog_snapshot,
            &STATISTICS,
            crate::sql::functions::builtin_sql_function_catalog(),
            None,
            control(None, &cancellation),
        );
        assert!(matches!(
            SqlCompiler::compile(request),
            Err(SqlCompileError::InvalidRequest(error)) if error.contains("output column 'missing' not found")
        ));
    }

    #[test]
    fn sqlx2_kernel_rejects_missing_aggregate_rewrite_evidence() {
        let input = SqlImvPlanningInput::new(
            crate::sql::compiler::mv_rewrite::test_incremental_snapshot(),
            SqlImvRewriteValidation::Aggregate,
        );
        let outcome = crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteOutcome {
            plan: crate::sql::planner::logical::LogicalPlanNode::new(
                crate::sql::planner::logical::LogicalPlanKind::Values(
                    crate::sql::planner::payload::PlanValuesNode {
                        rows: Vec::new(),
                        columns: Vec::new(),
                    },
                ),
                Vec::new(),
                None,
            ),
            trace: crate::sql::optimizer::rewrite::trace::RewriteTrace::default(),
            annotation: crate::sql::planner::imv_rewrite::annotation::ImvPlanAnnotation::default(),
        };

        assert!(matches!(
            validate_imv_rewrite_outcome(&input, &outcome),
            Err(SqlCompileError::Compilation(error)) if error.contains("RewriteAggregateState")
        ));
    }
}
pub(crate) mod mv_rewrite;
