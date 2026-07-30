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

//! Frontend-owned SQL session admission and routing boundary.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use novarocks::common::app_config::ClusterRole;
use novarocks::engine::{PreparedQueryOperation, StandaloneNovaRocks, StatementResult};
use novarocks::query_execution::backend::BackendTopologyService;
use novarocks::query_execution::cancellation::QueryCancellationReason;
use novarocks::query_execution::control::{
    QueryCancelOutcome, QueryControlService, QuerySessionLease, SessionIdentity, SessionToken,
    StatementFinishOutcome,
};
use novarocks::query_execution::request_context::{
    RequestAdmission, RequestContext, SessionOptimizerSettings,
};
use novarocks::query_execution::service::QueryExecutionService;
use novarocks::query_execution::session::{
    QueryServiceError, QueryServiceErrorKind, QuerySession, QuerySessionFactory,
    QuerySessionOpenRequest, SessionExecutionSettings,
};
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_catalog::memory::DEFAULT_DATABASE;
use tokio::task;

const DEFAULT_CATALOG: &str = "default_catalog";

/// Design: ADR-0012 (docs/adr/ADR-0012-frontend-query-session-router.md)
#[derive(Clone)]
pub struct FrontendQueryService {
    engine: StandaloneNovaRocks,
    query_control: QueryControlService,
    query_execution: QueryExecutionService,
    role: ClusterRole,
    topology: BackendTopologyService,
}

impl FrontendQueryService {
    pub fn new(
        engine: StandaloneNovaRocks,
        query_control: QueryControlService,
        query_execution: QueryExecutionService,
        role: ClusterRole,
        topology: BackendTopologyService,
    ) -> Self {
        Self {
            engine,
            query_control,
            query_execution,
            role,
            topology,
        }
    }
}

impl QuerySessionFactory for FrontendQueryService {
    fn open_session(
        &self,
        request: QuerySessionOpenRequest,
    ) -> Result<Arc<dyn QuerySession>, QueryServiceError> {
        let lease = self
            .query_control
            .register_session(SessionIdentity::new(
                request.connection_id(),
                request.principal().to_string(),
            ))
            .map_err(|error| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    format!("register frontend query session failed: {error:?}"),
                )
            })?;
        Ok(Arc::new(FrontendQuerySession {
            service: self.clone(),
            lease: Mutex::new(Some(lease)),
            state: Mutex::new(FrontendSessionState::default()),
        }))
    }

    fn cancel_all(&self, reason: QueryCancellationReason) {
        self.query_control.cancel_all(reason);
    }
}

#[derive(Clone)]
struct FrontendSessionState {
    current_catalog: Option<String>,
    current_database: String,
    execution_settings: SessionExecutionSettings,
    optimizer_settings: SessionOptimizerSettings,
    user_variables: BTreeMap<String, String>,
}

impl Default for FrontendSessionState {
    fn default() -> Self {
        Self {
            current_catalog: None,
            current_database: DEFAULT_DATABASE.to_string(),
            execution_settings: SessionExecutionSettings::default(),
            optimizer_settings: SessionOptimizerSettings::default(),
            user_variables: BTreeMap::new(),
        }
    }
}

struct FrontendQuerySession {
    service: FrontendQueryService,
    lease: Mutex<Option<QuerySessionLease>>,
    state: Mutex<FrontendSessionState>,
}

impl FrontendQuerySession {
    fn token(&self) -> Result<SessionToken, QueryServiceError> {
        self.lease
            .lock()
            .map_err(|_| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    "session lease lock poisoned",
                )
            })?
            .as_ref()
            .map(QuerySessionLease::token)
            .ok_or_else(|| {
                QueryServiceError::new(
                    QueryServiceErrorKind::NoSuchSession,
                    "query session is closed",
                )
            })
    }

    async fn execute_statement(
        &self,
        statement: &str,
    ) -> Result<StatementResult, QueryServiceError> {
        let trimmed = statement.trim().trim_end_matches(';').trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            return Ok(StatementResult::Ok);
        }
        if let Some(schema) = parse_use_database(trimmed) {
            self.init_database(schema).await?;
            return Ok(StatementResult::Ok);
        }
        if let Some(connection_id) = parse_kill_query(trimmed)? {
            let requester = self.token()?;
            return match self
                .service
                .query_control
                .kill_query(requester, connection_id)
            {
                QueryCancelOutcome::Requested | QueryCancelOutcome::AlreadyRequested(_) => {
                    Ok(StatementResult::Ok)
                }
                QueryCancelOutcome::NoActiveStatement => Err(QueryServiceError::new(
                    QueryServiceErrorKind::NoSuchSession,
                    format!("connection {connection_id} has no active query"),
                )),
                QueryCancelOutcome::UnknownSession => Err(QueryServiceError::new(
                    QueryServiceErrorKind::NoSuchSession,
                    format!("unknown connection {connection_id}"),
                )),
                QueryCancelOutcome::PermissionDenied => Err(QueryServiceError::new(
                    QueryServiceErrorKind::PermissionDenied,
                    "permission denied to kill query owned by another principal",
                )),
            };
        }
        if self.apply_session_set(trimmed).await? {
            return Ok(StatementResult::Ok);
        }
        self.execute_admitted(trimmed.to_string()).await
    }

    async fn apply_session_set(&self, sql: &str) -> Result<bool, QueryServiceError> {
        let lower = sql.to_ascii_lowercase();
        if !lower.starts_with("set ") {
            return Ok(false);
        }
        let assignment = sql[4..].trim();
        if let Some(catalog) = assignment
            .strip_prefix("CATALOG ")
            .or_else(|| assignment.strip_prefix("catalog "))
        {
            let catalog = resolve_catalog_name(&self.service.engine, catalog.trim())?;
            let mut state = self.state.lock().map_err(poisoned_state)?;
            state.current_catalog = catalog;
            return Ok(true);
        }
        let Some((raw_name, raw_value)) = assignment.split_once('=') else {
            return Ok(true);
        };
        if raw_name.trim().starts_with('@') && !raw_name.trim().starts_with("@@") {
            let mut state = self.state.lock().map_err(poisoned_state)?;
            state.user_variables.insert(
                raw_name.trim().to_ascii_lowercase(),
                raw_value.trim().to_string(),
            );
            return Ok(true);
        }
        let name = raw_name
            .trim()
            .trim_start_matches("@@")
            .to_ascii_lowercase();
        let value = raw_value.trim().trim_matches('\'').trim_matches('"');
        if name == "catalog" {
            let catalog = resolve_catalog_name(&self.service.engine, value)?;
            let mut state = self.state.lock().map_err(poisoned_state)?;
            state.current_catalog = catalog;
            if state.current_catalog.is_none()
                && !self
                    .service
                    .engine
                    .database_exists(&state.current_database)
                    .map_err(internal_error)?
            {
                state.current_database = DEFAULT_DATABASE.to_string();
            }
            return Ok(true);
        }
        let mut state = self.state.lock().map_err(poisoned_state)?;
        match name.as_str() {
            "query_timeout" => {
                let seconds = value.parse::<u64>().map_err(|_| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "invalid query_timeout",
                    )
                })?;
                state.execution_settings.set_query_timeout_secs(seconds);
            }
            "group_concat_max_len" => {
                let value = value.parse::<i64>().map_err(|_| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "invalid group_concat_max_len",
                    )
                })?;
                state.execution_settings.set_group_concat_max_len(value)?;
            }
            "pipeline_dop" => {
                let value = value.parse::<i32>().map_err(|_| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "invalid pipeline_dop",
                    )
                })?;
                state.execution_settings.set_pipeline_dop(value);
            }
            "runtime_filter_scan_wait_time" => {
                let value = value.parse::<i64>().map_err(|_| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "invalid runtime_filter_scan_wait_time",
                    )
                })?;
                state
                    .execution_settings
                    .set_runtime_filter_scan_wait_time_ms(value)?;
            }
            "global_runtime_filter_wait_timeout" => {
                let value = value.parse::<i32>().map_err(|_| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "invalid global_runtime_filter_wait_timeout",
                    )
                })?;
                state
                    .execution_settings
                    .set_runtime_filter_wait_timeout_ms(value)?;
            }
            "disable_optimizer_rules" | "cbo_disabled_rules" => {
                state.optimizer_settings.set_disabled_rules(
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|rule| !rule.is_empty())
                        .map(ToOwned::to_owned)
                        .collect(),
                );
            }
            "enable_eliminate_agg" => {
                state
                    .optimizer_settings
                    .set_enable_eliminate_agg(parse_bool(value)?);
            }
            "enable_ukfk_opt" => {
                state
                    .optimizer_settings
                    .set_enable_ukfk_opt(parse_bool(value)?);
            }
            "cbo_broadcast_backend_count" => {
                state
                    .optimizer_settings
                    .set_broadcast_backend_count(value.parse().map_err(|_| {
                        QueryServiceError::new(
                            QueryServiceErrorKind::InvalidValue,
                            "invalid cbo_broadcast_backend_count",
                        )
                    })?);
            }
            _ => {}
        }
        Ok(true)
    }

    async fn execute_admitted(&self, sql: String) -> Result<StatementResult, QueryServiceError> {
        let state = self.state.lock().map_err(poisoned_state)?.clone();
        let assignments = state
            .user_variables
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        let sql = novarocks::sql::substitute_user_variables(&sql, &assignments)
            .map_err(classify_engine_error)?;
        let token = self.token()?;
        let mut active = self
            .service
            .query_control
            .begin_statement(token)
            .map_err(|error| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    format!("begin statement failed: {error:?}"),
                )
            })?;
        let cancellation = active.cancellation().clone();
        let query_timeout_secs = state.execution_settings.query_timeout_secs();
        let deadline = match query_timeout_secs {
            Some(seconds) => Instant::now()
                .checked_add(Duration::from_secs(seconds))
                .ok_or_else(|| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::Internal,
                        "query deadline exceeds monotonic clock range",
                    )
                })?,
            None => Instant::now(),
        };
        let deadline = query_timeout_secs.map(|_| deadline);
        let topology = match self.service.topology.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let _ = active.finish();
                return Err(QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    error.to_string(),
                ));
            }
        };
        let context = RequestContext::admit(RequestAdmission::new(
            state.current_catalog,
            state.current_database,
            self.service.role,
            topology,
            deadline,
            cancellation.clone(),
            state.optimizer_settings,
        ));
        let compiler = self.service.engine.query_compiler();
        let command_executor = self.service.engine.command_executor();
        let query_execution = self.service.query_execution.clone();
        let mut query_options = state.execution_settings.query_options();
        query_options.apply_sql_hints(&sql);
        let is_query = is_query_statement(&sql);
        let worker = task::spawn_blocking(move || {
            let result = if is_query {
                compiler
                    .prepare(&sql, &context, Some(query_options))
                    .and_then(|operation| execute_prepared_query(operation, &query_execution))
            } else {
                command_executor.execute(&sql, &context, Some(query_options))
            };
            let completion = active.finish();
            (result, completion)
        });
        let result = if let Some(seconds) = query_timeout_secs {
            match tokio::time::timeout(Duration::from_secs(seconds), worker).await {
                Ok(result) => result.map_err(|error| internal_error(error.to_string()))?,
                Err(_) => {
                    self.cancel_current(QueryCancellationReason::DeadlineExceeded {
                        timeout_ms: seconds.saturating_mul(1_000),
                    });
                    return Err(QueryServiceError::new(
                        QueryServiceErrorKind::Timeout,
                        format!("query timed out after {} ms", seconds.saturating_mul(1_000)),
                    ));
                }
            }
        } else {
            worker
                .await
                .map_err(|error| internal_error(error.to_string()))?
        };
        let (result, completion) = result;
        match completion {
            StatementFinishOutcome::Cancelled(reason) => Err(cancellation_error(reason)),
            StatementFinishOutcome::Stale if cancellation.is_cancelled() => Err(
                cancellation_error(cancellation.reason().expect("cancelled view has a reason")),
            ),
            StatementFinishOutcome::Completed | StatementFinishOutcome::Stale => {
                result.map_err(classify_engine_error)
            }
        }
    }
}

fn execute_prepared_query(
    operation: PreparedQueryOperation,
    query_execution: &QueryExecutionService,
) -> Result<StatementResult, String> {
    match operation {
        PreparedQueryOperation::Immediate(operation) => Ok(operation.into_result()),
        PreparedQueryOperation::Distributed(operation) => {
            let (request, completion) = operation.into_parts();
            let outcome = query_execution
                .execute(request)
                .map_err(|error| error.to_string())?;
            completion.complete(outcome)
        }
    }
}

#[async_trait]
impl QuerySession for FrontendQuerySession {
    async fn init_database(&self, schema: &str) -> Result<(), QueryServiceError> {
        let current_catalog = self
            .state
            .lock()
            .map_err(poisoned_state)?
            .current_catalog
            .clone();
        let engine = self.service.engine.clone();
        let schema = schema.to_string();
        let context = task::spawn_blocking(move || {
            resolve_database_context(&engine, current_catalog.as_deref(), &schema)
        })
        .await
        .map_err(|error| internal_error(error.to_string()))??;
        let mut state = self.state.lock().map_err(poisoned_state)?;
        state.current_catalog = context.catalog;
        state.current_database = context.database;
        Ok(())
    }

    async fn execute_batch(&self, sql: &str) -> Result<StatementResult, QueryServiceError> {
        let statements = split_sql_statements(sql)?;
        // Match the standalone MySQL session contract: a batch returns its
        // most recent result set even when subsequent DDL/session statements
        // complete successfully. In particular, all-in-one routes through
        // this frontend session before reaching the Stage/Start lifecycle.
        let mut last_query_result = None;
        for statement in statements {
            match self.execute_statement(&statement).await? {
                StatementResult::Query(result) => last_query_result = Some(result),
                StatementResult::Ok => {}
            }
        }
        Ok(last_query_result
            .map(StatementResult::Query)
            .unwrap_or(StatementResult::Ok))
    }

    fn cancel_current(&self, reason: QueryCancellationReason) {
        let token = self
            .lease
            .lock()
            .ok()
            .and_then(|lease| lease.as_ref().map(QuerySessionLease::token));
        if let Some(token) = token {
            let _ = self
                .service
                .query_control
                .cancel_session_statement(token, reason);
        }
    }

    fn close(&self) {
        self.cancel_current(QueryCancellationReason::ClientDisconnected);
        if let Ok(mut lease) = self.lease.lock() {
            lease.take();
        }
    }
}

impl Drop for FrontendQuerySession {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Debug)]
struct DatabaseContext {
    catalog: Option<String>,
    database: String,
}

fn resolve_catalog_name(
    engine: &StandaloneNovaRocks,
    catalog: &str,
) -> Result<Option<String>, QueryServiceError> {
    let normalized = normalize_identifier(catalog).map_err(classify_engine_error)?;
    if normalized == DEFAULT_CATALOG {
        return Ok(None);
    }
    if engine
        .iceberg_catalog_exists(&normalized)
        .map_err(classify_engine_error)?
    {
        Ok(Some(normalized))
    } else {
        Err(QueryServiceError::new(
            QueryServiceErrorKind::BadDatabase,
            format!("unknown catalog `{catalog}`"),
        ))
    }
}

fn resolve_database_context(
    engine: &StandaloneNovaRocks,
    current_catalog: Option<&str>,
    schema: &str,
) -> Result<DatabaseContext, QueryServiceError> {
    let parts = schema
        .split('.')
        .map(|part| part.trim().trim_matches('`'))
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [database] => {
            let database = normalize_identifier(database).map_err(classify_engine_error)?;
            match current_catalog {
                Some(catalog)
                    if engine
                        .iceberg_namespace_exists(catalog, &database)
                        .map_err(classify_engine_error)? =>
                {
                    Ok(DatabaseContext {
                        catalog: Some(catalog.to_string()),
                        database,
                    })
                }
                Some(_) => Err(QueryServiceError::new(
                    QueryServiceErrorKind::BadDatabase,
                    format!("unknown database `{schema}`"),
                )),
                None if engine
                    .database_exists(&database)
                    .map_err(classify_engine_error)? =>
                {
                    Ok(DatabaseContext {
                        catalog: None,
                        database,
                    })
                }
                None => Err(QueryServiceError::new(
                    QueryServiceErrorKind::BadDatabase,
                    format!("unknown database `{schema}`"),
                )),
            }
        }
        [catalog, database] => {
            let catalog = resolve_catalog_name(engine, catalog)?;
            let database = normalize_identifier(database).map_err(classify_engine_error)?;
            match catalog {
                Some(catalog)
                    if engine
                        .iceberg_namespace_exists(&catalog, &database)
                        .map_err(classify_engine_error)? =>
                {
                    Ok(DatabaseContext {
                        catalog: Some(catalog),
                        database,
                    })
                }
                None if engine
                    .database_exists(&database)
                    .map_err(classify_engine_error)? =>
                {
                    Ok(DatabaseContext {
                        catalog: None,
                        database,
                    })
                }
                _ => Err(QueryServiceError::new(
                    QueryServiceErrorKind::BadDatabase,
                    format!("unknown database `{schema}`"),
                )),
            }
        }
        _ => Err(QueryServiceError::new(
            QueryServiceErrorKind::BadDatabase,
            format!("unknown database `{schema}`; expected `<database>` or `<catalog>.<database>`"),
        )),
    }
}

fn split_sql_statements(sql: &str) -> Result<Vec<String>, QueryServiceError> {
    let mut statements = Vec::new();
    let mut start = 0;
    let mut quote = None;
    for (index, ch) in sql.char_indices() {
        match quote {
            Some(delimiter) if ch == delimiter => quote = None,
            Some(_) => {}
            None if matches!(ch, '\'' | '"' | '`') => quote = Some(ch),
            None if ch == ';' => {
                let statement = sql[start..index].trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                start = index + ch.len_utf8();
            }
            None => {}
        }
    }
    if quote.is_some() {
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "unterminated quoted string in SQL batch",
        ));
    }
    let statement = sql[start..].trim();
    if !statement.is_empty() {
        statements.push(statement.to_string());
    }
    Ok(statements)
}

fn parse_use_database(sql: &str) -> Option<&str> {
    sql.strip_prefix("USE ")
        .or_else(|| sql.strip_prefix("use "))
        .map(str::trim)
        .filter(|schema| !schema.is_empty())
}

fn parse_kill_query(sql: &str) -> Result<Option<u32>, QueryServiceError> {
    let mut words = sql.split_whitespace();
    let Some(first) = words.next() else {
        return Ok(None);
    };
    if !first.eq_ignore_ascii_case("kill") {
        return Ok(None);
    }
    let second = words.next();
    let target = match second {
        Some(word) if word.eq_ignore_ascii_case("query") => words.next(),
        Some(word) => Some(word),
        None => None,
    };
    let target = target.ok_or_else(|| {
        QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "KILL requires a connection id",
        )
    })?;
    target.parse().map(Some).map_err(|_| {
        QueryServiceError::new(QueryServiceErrorKind::Parse, "invalid KILL connection id")
    })
}

fn parse_bool(value: &str) -> Result<bool, QueryServiceError> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "on" | "true" => Ok(true),
        "0" | "off" | "false" => Ok(false),
        _ => Err(QueryServiceError::new(
            QueryServiceErrorKind::InvalidValue,
            format!("invalid boolean value `{value}`"),
        )),
    }
}

fn is_query_statement(sql: &str) -> bool {
    let keyword = sql
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    matches!(keyword.as_str(), "select" | "with" | "explain")
}

fn poisoned_state<T>(_error: std::sync::PoisonError<T>) -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::Internal,
        "frontend query session state lock poisoned",
    )
}

fn internal_error(message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}

fn classify_engine_error(error: impl ToString) -> QueryServiceError {
    let message = error.to_string();
    let lower = message.to_ascii_lowercase();
    let kind = if lower.contains("unknown database") || lower.contains("unknown catalog") {
        QueryServiceErrorKind::BadDatabase
    } else if lower.contains("unsupported") {
        QueryServiceErrorKind::Unsupported
    } else if lower.contains("expected")
        || lower.contains("unterminated")
        || lower.contains("invalid")
    {
        QueryServiceErrorKind::Parse
    } else {
        QueryServiceErrorKind::Internal
    };
    QueryServiceError::new(kind, message)
}

fn cancellation_error(reason: QueryCancellationReason) -> QueryServiceError {
    let (kind, message) = match reason {
        QueryCancellationReason::DeadlineExceeded { timeout_ms } => (
            QueryServiceErrorKind::Timeout,
            format!("query timed out after {timeout_ms} ms"),
        ),
        QueryCancellationReason::ExplicitKill { .. } => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted".to_string(),
        ),
        QueryCancellationReason::ClientDisconnected => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the client disconnected".to_string(),
        ),
        QueryCancellationReason::ServerShutdown => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the server is shutting down".to_string(),
        ),
    };
    QueryServiceError::new(kind, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_split_preserves_quoted_semicolons_and_statement_order() {
        let statements = split_sql_statements("SET query_timeout = 1; SELECT ';'; SELECT 3")
            .expect("batch must parse");
        assert_eq!(
            statements,
            vec![
                "SET query_timeout = 1".to_string(),
                "SELECT ';'".to_string(),
                "SELECT 3".to_string(),
            ]
        );
    }

    #[test]
    fn batch_split_rejects_unterminated_quote() {
        let error = split_sql_statements("SELECT 'unterminated").expect_err("must reject");
        assert_eq!(error.kind(), QueryServiceErrorKind::Parse);
    }

    #[test]
    fn kill_query_parser_accepts_explicit_and_short_forms() {
        assert_eq!(parse_kill_query("KILL QUERY 17").unwrap(), Some(17));
        assert_eq!(parse_kill_query("kill 18").unwrap(), Some(18));
        assert_eq!(parse_kill_query("SELECT 1").unwrap(), None);
    }

    #[test]
    fn cancellation_errors_keep_timeout_distinct_from_interrupts() {
        assert_eq!(
            cancellation_error(QueryCancellationReason::DeadlineExceeded { timeout_ms: 25 }).kind(),
            QueryServiceErrorKind::Timeout
        );
        assert_eq!(
            cancellation_error(QueryCancellationReason::ClientDisconnected).kind(),
            QueryServiceErrorKind::Interrupted
        );
    }
}
