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

//! Query-application session state.

use std::collections::BTreeMap;
use std::fmt;

use novarocks_parser::ast::{self, Fold, Statement};
use novarocks_sql::compiler::SessionOptimizerSettings;
use novarocks_types::naming::DEFAULT_DATABASE;

/// Connection-local SQL state that is independent of a protocol adapter or
/// role-local runtime. Adapters may validate and apply mutations to this
/// state, but do not own a second session representation.
#[derive(Clone)]
pub struct SessionSqlState {
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub execution_settings: SessionExecutionSettings,
    pub optimizer_settings: SessionOptimizerSettings,
    pub user_variables: BTreeMap<String, String>,
}

impl Default for SessionSqlState {
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

impl SessionSqlState {
    /// Rewrites references to this session's user variables with their stored
    /// scalar SQL expressions before a role adapter prepares the statement.
    pub fn substitute_user_variables(&self, statement: Statement) -> Result<Statement, String> {
        if self.user_variables.is_empty() {
            return Ok(statement);
        }

        let mut values = BTreeMap::new();
        for (name, value) in &self.user_variables {
            let statements = novarocks_parser::parse(&format!("SELECT {value}"))
                .map_err(|error| format!("invalid session user variable {name}: {error}"))?;
            let [Statement::Query(query)] = statements.as_slice() else {
                return Err(format!("invalid session user variable {name}"));
            };
            let ast::SetExpr::Select(select) = query.body.as_ref() else {
                return Err(format!("invalid session user variable {name}"));
            };
            let [item] = select.projection.as_slice() else {
                return Err(format!("invalid session user variable {name}"));
            };
            let expression = match item {
                ast::SelectItem::UnnamedExpr(expression)
                | ast::SelectItem::ExprWithAlias {
                    expr: expression, ..
                } => expression.clone(),
                ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(format!("invalid session user variable {name}"));
                }
            };
            values.insert(name.to_ascii_lowercase(), expression);
        }

        struct Substituter {
            values: BTreeMap<String, ast::Expr>,
        }

        impl Fold for Substituter {
            fn fold_expr(&mut self, expression: ast::Expr) -> ast::Expr {
                if let ast::Expr::UserVariable(variable) = &expression
                    && let Some(value) = self.values.get(&variable.value.to_ascii_lowercase())
                {
                    return value.clone();
                }
                ast::fold_expr(self, expression)
            }
        }

        Ok(Substituter { values }.fold_statement(statement))
    }
}

/// Connection-local settings that SQL admission has validated.
///
/// This type deliberately contains no protocol DTO. MySQL and native role
/// adapters project this state into their own validated wire contracts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionExecutionSettings {
    query_timeout_secs: Option<u64>,
    group_concat_max_len: i64,
    pipeline_dop: Option<i32>,
    enable_parquet_reader_page_index: bool,
    enable_scan_datacache: bool,
    enable_populate_datacache: bool,
    runtime_filter_scan_wait_time_ms: Option<i64>,
    runtime_filter_wait_timeout_ms: Option<i32>,
}

impl Default for SessionExecutionSettings {
    fn default() -> Self {
        Self {
            query_timeout_secs: None,
            group_concat_max_len: 1024,
            pipeline_dop: None,
            enable_parquet_reader_page_index: false,
            enable_scan_datacache: false,
            enable_populate_datacache: false,
            runtime_filter_scan_wait_time_ms: None,
            runtime_filter_wait_timeout_ms: None,
        }
    }
}

impl SessionExecutionSettings {
    pub const fn query_timeout_secs(&self) -> Option<u64> {
        self.query_timeout_secs
    }

    pub fn set_query_timeout_secs(&mut self, seconds: u64) {
        self.query_timeout_secs = (seconds > 0).then_some(seconds);
    }

    pub const fn group_concat_max_len(&self) -> i64 {
        self.group_concat_max_len
    }

    /// Keep the session value verbatim; aggregate lowering clamps it to the
    /// supported minimum before execution.
    pub fn set_group_concat_max_len(&mut self, value: i64) {
        self.group_concat_max_len = value;
    }

    pub const fn pipeline_dop(&self) -> Option<i32> {
        self.pipeline_dop
    }

    pub fn set_pipeline_dop(&mut self, value: i32) {
        self.pipeline_dop = (value > 0).then_some(value);
    }

    pub const fn enable_parquet_reader_page_index(&self) -> bool {
        self.enable_parquet_reader_page_index
    }

    pub fn set_enable_parquet_reader_page_index(&mut self, enabled: bool) {
        self.enable_parquet_reader_page_index = enabled;
    }

    pub const fn enable_scan_datacache(&self) -> bool {
        self.enable_scan_datacache
    }

    pub fn set_enable_scan_datacache(&mut self, enabled: bool) {
        self.enable_scan_datacache = enabled;
    }

    pub const fn enable_populate_datacache(&self) -> bool {
        self.enable_populate_datacache
    }

    pub fn set_enable_populate_datacache(&mut self, enabled: bool) {
        self.enable_populate_datacache = enabled;
    }

    pub const fn runtime_filter_scan_wait_time_ms(&self) -> Option<i64> {
        self.runtime_filter_scan_wait_time_ms
    }

    pub fn set_runtime_filter_scan_wait_time_ms(
        &mut self,
        value: i64,
    ) -> Result<(), SessionSettingError> {
        if value < 0 {
            return Err(SessionSettingError::NegativeRuntimeFilterScanWaitTime);
        }
        self.runtime_filter_scan_wait_time_ms = Some(value);
        Ok(())
    }

    pub const fn runtime_filter_wait_timeout_ms(&self) -> Option<i32> {
        self.runtime_filter_wait_timeout_ms
    }

    pub fn set_runtime_filter_wait_timeout_ms(
        &mut self,
        value: i32,
    ) -> Result<(), SessionSettingError> {
        if value < 0 {
            return Err(SessionSettingError::NegativeRuntimeFilterWaitTimeout);
        }
        self.runtime_filter_wait_timeout_ms = Some(value);
        Ok(())
    }
}

/// A closed validation failure for one SQL-session setting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionSettingError {
    NegativeRuntimeFilterScanWaitTime,
    NegativeRuntimeFilterWaitTimeout,
}

impl fmt::Display for SessionSettingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeRuntimeFilterScanWaitTime => {
                formatter.write_str("runtime_filter_scan_wait_time must be non-negative")
            }
            Self::NegativeRuntimeFilterWaitTimeout => {
                formatter.write_str("global_runtime_filter_wait_timeout must be non-negative")
            }
        }
    }
}

impl std::error::Error for SessionSettingError {}

#[cfg(test)]
mod tests {
    use super::{SessionExecutionSettings, SessionSettingError, SessionSqlState};
    use novarocks_types::naming::DEFAULT_DATABASE;

    #[test]
    fn default_sql_session_state_is_neutral_and_empty() {
        let state = SessionSqlState::default();
        assert_eq!(state.current_catalog, None);
        assert_eq!(state.current_database, DEFAULT_DATABASE);
        assert!(state.user_variables.is_empty());
        assert_eq!(
            state.execution_settings,
            SessionExecutionSettings::default()
        );
    }

    #[test]
    fn preserves_session_values_before_wire_projection() {
        let mut settings = SessionExecutionSettings::default();
        settings.set_query_timeout_secs(17);
        settings.set_pipeline_dop(4);
        settings
            .set_runtime_filter_scan_wait_time_ms(0)
            .expect("zero is valid");
        settings.set_group_concat_max_len(-1);

        assert_eq!(settings.query_timeout_secs(), Some(17));
        assert_eq!(settings.pipeline_dop(), Some(4));
        assert_eq!(settings.runtime_filter_scan_wait_time_ms(), Some(0));
        assert_eq!(settings.group_concat_max_len(), -1);
    }

    #[test]
    fn preserves_boolean_switches() {
        let mut settings = SessionExecutionSettings::default();
        settings.set_enable_parquet_reader_page_index(true);
        settings.set_enable_scan_datacache(true);
        settings.set_enable_populate_datacache(true);

        assert!(settings.enable_parquet_reader_page_index());
        assert!(settings.enable_scan_datacache());
        assert!(settings.enable_populate_datacache());
    }

    #[test]
    fn rejects_negative_runtime_filter_settings() {
        let mut settings = SessionExecutionSettings::default();
        assert_eq!(
            settings.set_runtime_filter_scan_wait_time_ms(-1),
            Err(SessionSettingError::NegativeRuntimeFilterScanWaitTime)
        );
        assert_eq!(
            settings.set_runtime_filter_wait_timeout_ms(-1),
            Err(SessionSettingError::NegativeRuntimeFilterWaitTimeout)
        );
    }

    #[test]
    fn substitutes_user_variables_without_a_frontend_router() {
        let mut state = SessionSqlState::default();
        state
            .user_variables
            .insert("@limit".to_string(), "7".to_string());
        let statement = novarocks_parser::parse("SELECT @limit")
            .expect("parse query")
            .pop()
            .expect("one statement");

        let rendered = novarocks_parser::printer::print_statement(
            &state
                .substitute_user_variables(statement)
                .expect("substitute session variable"),
        );
        assert_eq!(rendered, "SELECT 7");
    }
}
