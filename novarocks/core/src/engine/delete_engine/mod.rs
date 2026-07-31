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

//! Transitional reverse port for frontend-owned DELETE application routing.

pub(crate) mod equality;
pub(crate) mod standard;

use std::sync::Arc;

use crate::engine::{StandaloneState, StatementResult};
use crate::query_execution::request_context::QueryExecutionContext;
use crate::runtime::query_options::QueryOptions;

/// DELETE statements recognized by the frontend command router.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteStatementKind {
    Predicate,
    Equality,
}

/// Recognize a standard SQL DELETE without executing it.
pub fn parse_delete_statement(sql: &str) -> Result<Option<sqlparser::ast::Delete>, String> {
    let sql = sql.trim_start();
    let keyword_end = sql
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_alphabetic()).then_some(index))
        .unwrap_or(sql.len());
    if !sql[..keyword_end].eq_ignore_ascii_case("delete") {
        return Ok(None);
    }
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Delete(delete) => Ok(Some(delete)),
        _ => Ok(None),
    }
}

/// Recognize the NovaRocks equality-delete ALTER TABLE extension.
pub fn parse_equality_delete_statement(sql: &str) -> Result<Option<()>, String> {
    if !crate::engine::statement::looks_like_add_equality_delete(sql) {
        return Ok(None);
    }
    crate::engine::statement::parse_add_equality_delete_sql(sql)?;
    Ok(Some(()))
}

/// One admitted frontend DELETE request. The raw SQL stays inside the narrow
/// reverse port so the frontend never handles core-private DELETE AST payloads.
pub struct ExecuteDeleteRequest<'a> {
    pub sql: &'a str,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
    pub kind: DeleteStatementKind,
}

/// One-to-one core capability used only by the frontend DML application owner.
// Design: ADR-0020 (docs/adr/ADR-0020-frontend-delete-application-owner.md)
pub trait DeleteEngine: Send + Sync {
    fn execute_delete(&self, request: ExecuteDeleteRequest<'_>) -> Result<(), String>;
}

impl DeleteEngine for Arc<StandaloneState> {
    fn execute_delete(&self, request: ExecuteDeleteRequest<'_>) -> Result<(), String> {
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        match request.kind {
            DeleteStatementKind::Predicate => {
                let delete = parse_delete_statement(request.sql)?.ok_or_else(|| {
                    "DELETE request did not contain a DELETE statement".to_string()
                })?;
                let statement =
                    crate::engine::statement::convert_sqlparser_delete_to_custom(&delete)?;
                match standard::execute_delete_statement(
                    self,
                    &statement,
                    request.current_catalog.as_deref(),
                    &request.current_database,
                    &request.execution,
                    &connector_context,
                )? {
                    StatementResult::Ok => Ok(()),
                    StatementResult::Query(_) => {
                        Err("DELETE unexpectedly produced query rows".to_string())
                    }
                }
            }
            DeleteStatementKind::Equality => {
                let statement =
                    crate::engine::statement::parse_add_equality_delete_sql(request.sql)?;
                match equality::execute_add_equality_delete_statement(
                    self,
                    &statement,
                    request.current_catalog.as_deref(),
                    &request.current_database,
                    &request.execution,
                    &connector_context,
                )? {
                    StatementResult::Ok => Ok(()),
                    StatementResult::Query(_) => {
                        Err("equality DELETE unexpectedly produced query rows".to_string())
                    }
                }
            }
        }
    }
}
