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

//! Query-application SQL admission that is independent of a product route.

use super::{
    SqlBatchCursor, parse_optional_single_statement, query_service_parse_error,
    strip_leading_line_comments,
};
use crate::engine_error::EngineError;
use crate::session_error::{QueryServiceError, QueryServiceErrorKind};
use novarocks_parser::ast::{self, Statement as ParsedStatement};
use novarocks_types::EngineErrorCode;
use novarocks_workload_control::WorkClass;

/// Return whether this admitted statement needs the provider-publication
/// deadline policy rather than only the session deadline.
pub fn requires_lake_publication_deadline(statement: &ParsedStatement) -> bool {
    match statement {
        ParsedStatement::Dml(_) | ParsedStatement::Table(_) | ParsedStatement::Iceberg(_) => true,
        ParsedStatement::Catalog(statement) => {
            !matches!(statement, ast::CatalogStatement::ShowCreateTable(_))
        }
        ParsedStatement::Maintenance(statement) => {
            !matches!(statement, ast::MaintenanceStatement::ShowOptimize(_))
        }
        ParsedStatement::MaterializedView(statement) => !matches!(
            statement,
            ast::MaterializedViewStatement::Show(_)
                | ast::MaterializedViewStatement::ExplainRefresh(_)
        ),
        ParsedStatement::View(statement) => !matches!(
            statement,
            ast::ViewStatement::Show(_) | ast::ViewStatement::ShowCreate(_)
        ),
        ParsedStatement::Statistics(statement) => matches!(
            statement,
            ast::StatisticsStatement::AnalyzeTable(_)
                | ast::StatisticsStatement::DropStats(_)
                | ast::StatisticsStatement::DropHistogram(_)
                | ast::StatisticsStatement::DropMultipleColumnsStats(_)
        ),
        ParsedStatement::ShowBackends(_) | ParsedStatement::ShowProcessList(_) => false,
        ParsedStatement::Session(_)
        | ParsedStatement::Query(_)
        | ParsedStatement::ExplainQuery(_) => false,
    }
}

/// Classify a statement that has already been routed to the typed command
/// path. Plain queries and session commands must use their dedicated routes.
pub fn typed_statement_work_class(statement: &ParsedStatement) -> WorkClass {
    match statement {
        ParsedStatement::Dml(_) | ParsedStatement::ExplainQuery(_) => WorkClass::Query,
        ParsedStatement::Session(_) | ParsedStatement::Query(_) => {
            unreachable!("session and plain query statements do not use the typed route")
        }
        _ => WorkClass::Management,
    }
}

/// Returns all executable SQL fragments from one COM_QUERY request. Empty
/// fragments and comments have no effect. Callers must gate use of more than
/// one fragment on the protocol capability negotiated for that connection.
pub fn negotiated_query_statements(sql: &str) -> Result<Vec<&str>, QueryServiceError> {
    let mut cursor = SqlBatchCursor::new(sql);
    let mut statements = Vec::new();
    while let Some(fragment) = cursor.next_fragment()? {
        let trimmed = strip_leading_line_comments(fragment.trim());
        if trimmed.is_empty() {
            continue;
        }
        let is_statement = admin_raise_engine_error(trimmed)?.is_some()
            || parse_optional_single_statement(trimmed)
                .map_err(|error| query_service_parse_error(error, trimmed))?
                .is_some();
        if !is_statement {
            continue;
        }
        statements.push(fragment);
    }
    Ok(statements)
}

/// Return the single executable SQL statement admitted without negotiated
/// multi-statement support. Empty fragments and comments have no effect.
pub fn unnegotiated_query_statement(sql: &str) -> Result<Option<&str>, QueryServiceError> {
    let mut statements = negotiated_query_statements(sql)?;
    if statements.len() > 1 {
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::Unsupported,
            "multiple SQL statements require negotiated MySQL multi-statement support",
        ));
    }
    Ok(statements.pop())
}

/// Parse the test-only SQL hook that forces a stable engine error.
pub fn admin_raise_engine_error(sql: &str) -> Result<Option<QueryServiceError>, QueryServiceError> {
    let parts = sql.split_whitespace().collect::<Vec<_>>();
    if !matches!(parts.as_slice(), [admin, raise, engine, error, _]
        if admin.eq_ignore_ascii_case("admin")
            && raise.eq_ignore_ascii_case("raise")
            && engine.eq_ignore_ascii_case("engine")
            && error.eq_ignore_ascii_case("error"))
    {
        return Ok(None);
    }
    let [_, _, _, _, raw_code] = parts.as_slice() else {
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "expected ADMIN RAISE ENGINE ERROR '<engine_error_code>'",
        ));
    };
    let raw_code = raw_code
        .strip_prefix('\'')
        .and_then(|inner| inner.strip_suffix('\''))
        .or_else(|| {
            raw_code
                .strip_prefix('"')
                .and_then(|inner| inner.strip_suffix('"'))
        })
        .ok_or_else(|| {
            QueryServiceError::new(
                QueryServiceErrorKind::Parse,
                "expected ADMIN RAISE ENGINE ERROR '<engine_error_code>'",
            )
        })?;
    let code = EngineErrorCode::parse(raw_code).ok_or_else(|| {
        QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            format!("unknown engine error code: {raw_code}"),
        )
    })?;
    let error = match code {
        EngineErrorCode::UnsupportedDistributedDmlShape => {
            EngineError::unsupported_distributed_dml_shape(
                "ADMIN RAISE ENGINE ERROR",
                "forced P8 SQL runner error-code smoke",
            )
        }
        EngineErrorCode::IcebergWriteDescriptorMismatch => {
            EngineError::iceberg_write_descriptor_mismatch("forced P8 SQL runner error-code smoke")
        }
        EngineErrorCode::UnsupportedPositionDeleteDescriptor => {
            EngineError::unsupported_position_delete_descriptor(
                "forced position-delete descriptor error-code smoke",
            )
        }
        EngineErrorCode::CommitKnownUncommitted => {
            EngineError::commit_known_uncommitted("forced P8 SQL runner error-code smoke")
        }
        EngineErrorCode::CommitUnknown => {
            EngineError::commit_unknown("forced P8 SQL runner error-code smoke")
        }
        EngineErrorCode::CommitKnownCommittedFinalizeFailed => {
            EngineError::commit_known_committed_finalize_failed(
                "forced P8 SQL runner error-code smoke",
            )
        }
        EngineErrorCode::ProtocolDecodeError => {
            EngineError::protocol_decode("forced P8 SQL runner error-code smoke")
        }
        _ => {
            return Err(QueryServiceError::new(
                QueryServiceErrorKind::Parse,
                format!("unsupported engine error code for ADMIN RAISE ENGINE ERROR: {raw_code}"),
            ));
        }
    };
    Ok(Some(QueryServiceError::new(
        QueryServiceErrorKind::Unsupported,
        error.to_bracketed_user_message(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parse_single_statement;

    #[test]
    fn unnegotiated_admission_ignores_empty_fragments_and_returns_one_statement() {
        assert_eq!(
            unnegotiated_query_statement("; /* leading comment */; SELECT ';'; -- done\n")
                .expect("admit"),
            Some(" SELECT ';'")
        );
        assert_eq!(
            unnegotiated_query_statement("; -- comment only\n; /* still empty */").expect("admit"),
            None
        );
    }

    #[test]
    fn unnegotiated_admission_rejects_multiple_executable_statements() {
        let error = unnegotiated_query_statement("SET query_timeout = 1; SELECT 1")
            .expect_err("must reject multiple statements");
        assert_eq!(error.kind(), QueryServiceErrorKind::Unsupported);
    }

    #[test]
    fn negotiated_admission_retains_each_executable_fragment_in_order() {
        assert_eq!(
            negotiated_query_statements("; SET query_timeout = 1; /* separator */ SELECT 1;"),
            Ok(vec![" SET query_timeout = 1", " /* separator */ SELECT 1"])
        );
    }

    #[test]
    fn admin_raise_engine_error_keeps_the_engine_code_visible() {
        let error =
            admin_raise_engine_error("ADMIN RAISE ENGINE ERROR 'UnsupportedDistributedDmlShape'")
                .expect("parse")
                .expect("admin error");
        assert!(error.message().contains("UnsupportedDistributedDmlShape"));
    }

    #[test]
    fn typed_statement_work_class_keeps_data_plane_work_distinct_from_management() {
        let explain = parse_single_statement("EXPLAIN SELECT 1").expect("parse explain");
        let dml = parse_single_statement("INSERT INTO target VALUES (1)").expect("parse DML");
        let management =
            parse_single_statement("CREATE DATABASE governed_management").expect("parse DDL");

        assert_eq!(typed_statement_work_class(&explain), WorkClass::Query);
        assert_eq!(typed_statement_work_class(&dml), WorkClass::Query);
        assert_eq!(
            typed_statement_work_class(&management),
            WorkClass::Management
        );
    }

    #[test]
    fn publication_deadline_applies_only_to_effect_capable_statement_shapes() {
        let query = parse_single_statement("SELECT 1").expect("parse query");
        let dml = parse_single_statement("INSERT INTO target VALUES (1)").expect("parse DML");

        assert!(!requires_lake_publication_deadline(&query));
        assert!(requires_lake_publication_deadline(&dml));
    }
}
