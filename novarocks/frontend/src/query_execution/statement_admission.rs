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

//! SQLP-3 parser admission seam.
//!
//! Production routing switches family by family in T7-T10. This module fixes
//! the bounded legacy frontier and the parser-error carrier before those cuts;
//! it does not create a second production command route.

use novarocks_parser::{ParserError, TokenKind, ast::Statement, lex, parse};
use novarocks_user_error::UserError;

/// The only SQLP-3 exclusions allowed to bypass command-parser admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatementAdmission {
    LegacyFrontier,
    Parsed,
}

/// Admits one statement through the bounded frontier or one parser invocation.
pub(crate) fn admit_statement(
    source: &str,
) -> Result<(StatementAdmission, Vec<Statement>), UserError> {
    if is_legacy_frontier(source) {
        return Ok((StatementAdmission::LegacyFrontier, Vec::new()));
    }
    parse(source)
        .map(|statements| (StatementAdmission::Parsed, statements))
        .map_err(|error| parser_user_error(source, error))
}

fn parser_user_error(source: &str, error: ParserError) -> UserError {
    error.to_user_error(source)
}

fn is_legacy_frontier(source: &str) -> bool {
    let words = significant_words(source);
    let first = words.first().map(String::as_str);
    match first {
        Some("SELECT" | "WITH" | "VALUES" | "SET" | "USE") => true,
        Some("KILL") => words.get(1).map(String::as_str) != Some("ANALYZE"),
        Some("EXPLAIN") => explain_targets_query(&words),
        Some("DROP" | "ALTER") => false,
        Some("SHOW") => false,
        _ => false,
    }
}

fn explain_targets_query(words: &[String]) -> bool {
    words
        .iter()
        .skip(1)
        .map(String::as_str)
        .find(|word| !matches!(*word, "ANALYZE" | "VERBOSE" | "COSTS" | "LOGICAL"))
        .is_some_and(|word| matches!(word, "SELECT" | "WITH" | "VALUES"))
}

fn significant_words(source: &str) -> Vec<String> {
    let Ok(tokens) = lex(source) else {
        return Vec::new();
    };
    tokens
        .iter()
        .filter(|token| matches!(token.kind, TokenKind::Ident | TokenKind::Keyword(_)))
        .map(|token| source[token.span.start()..token.span.end()].to_ascii_uppercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_frontier_is_bounded_to_unmigrated_families() {
        for source in [
            "SELECT 1",
            "WITH c AS (SELECT 1) SELECT * FROM c",
            "VALUES (1)",
            "SET query_timeout = 1",
            "KILL QUERY 1",
            "EXPLAIN VERBOSE SELECT 1",
            "EXPLAIN ANALYZE VALUES (1)",
        ] {
            assert_eq!(
                admit_statement(source)
                    .expect("legacy frontier must not parse")
                    .0,
                StatementAdmission::LegacyFrontier,
                "{source}"
            );
        }
    }

    #[test]
    fn sqlp5_ddl_dml_is_admitted_through_the_typed_parser() {
        for source in [
            "CREATE TABLE t (k INT)",
            "CREATE TEMPORARY TABLE t (k INT)",
            "INSERT INTO t VALUES (1)",
            "DELETE FROM t WHERE k = 1",
            "UPDATE t SET k = 2 WHERE k = 1",
            "MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN DELETE",
            "ALTER TABLE t ADD EQUALITY DELETE (k) VALUES (1)",
        ] {
            assert_eq!(
                admit_statement(source)
                    .expect("SQLP-5 statement must parse once through typed admission")
                    .0,
                StatementAdmission::Parsed,
                "{source}"
            );
        }
    }

    #[test]
    fn owned_command_uses_parser_once_and_preserves_typed_failure() {
        let (admission, statements) = admit_statement("SHOW BACKENDS")
            .expect("owned command should parse through the new parser");
        assert_eq!(admission, StatementAdmission::Parsed);
        assert_eq!(statements.len(), 1);

        let error = admit_statement("SHOW").expect_err("owned malformed command must fail");
        assert_eq!(error.code().as_str(), "sql.parse.unexpected_token");
        assert_eq!(error.location().unwrap().column(), 5);
    }

    #[test]
    fn in_scope_catalog_command_is_not_legacy() {
        assert!(!is_legacy_frontier("DROP TABLE t"));
    }

    #[test]
    fn in_scope_explain_refresh_is_not_legacy() {
        assert!(!is_legacy_frontier(
            "EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW mv"
        ));
    }

    #[test]
    fn materialized_view_family_is_admitted_after_its_owner_cut() {
        for source in [
            "CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH(k) BUCKETS 1 AS SELECT k FROM t",
            "DROP MATERIALIZED VIEW mv",
            "REFRESH MATERIALIZED VIEW mv",
            "SHOW MATERIALIZED VIEWS",
            "EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW mv",
        ] {
            assert_eq!(
                admit_statement(source).expect("MV command should parse").0,
                StatementAdmission::Parsed,
                "{source}"
            );
        }
    }

    #[test]
    fn view_family_is_admitted_and_malformed_view_does_not_fall_back() {
        for source in [
            "CREATE VIEW v AS SELECT 1",
            "DROP VIEW v",
            "SHOW VIEWS",
            "SHOW CREATE VIEW v",
        ] {
            assert_eq!(
                admit_statement(source)
                    .expect("View command should parse")
                    .0,
                StatementAdmission::Parsed,
                "{source}"
            );
        }

        let error = admit_statement("CREATE VIEW v AS")
            .expect_err("malformed typed View command must not reach the legacy frontier");
        assert_eq!(error.code().as_str(), "sql.parse.unexpected_token");
    }

    #[test]
    fn maintenance_family_is_admitted_after_its_owner_cut() {
        for source in [
            "CALL ice.system.rewrite_manifests(table => 'db.t')",
            "ALTER TABLE ice.db.t OPTIMIZE",
            "ALTER TABLE ice.db.t EXPIRE SNAPSHOTS RETAIN LAST 3",
            "SHOW ALTER TABLE OPTIMIZE",
        ] {
            assert_eq!(
                admit_statement(source)
                    .expect("maintenance command should parse")
                    .0,
                StatementAdmission::Parsed,
                "{source}"
            );
        }
    }

    #[test]
    fn iceberg_alter_table_family_is_admitted_after_its_owner_cut() {
        for source in [
            "ALTER TABLE ice.db.t ADD COLUMN c INT",
            "ALTER TABLE ice.db.t SET ('format' = 'parquet')",
            "ALTER TABLE ice.db.t SET TBLPROPERTIES ('format' = 'parquet')",
            "ALTER TABLE ice.db.t CREATE BRANCH dev",
            "ALTER TABLE ice.db.t ADD FILES FROM 's3://warehouse/staged'",
        ] {
            assert_eq!(
                admit_statement(source)
                    .expect("Iceberg command should parse")
                    .0,
                StatementAdmission::Parsed,
                "{source}"
            );
        }
    }

    #[test]
    fn kill_analyze_is_not_a_legacy_session_kill() {
        assert!(!is_legacy_frontier(
            "KILL ANALYZE 018f8c30-8a95-7b4e-b515-4da6f2aeb419"
        ));
        assert!(is_legacy_frontier("KILL QUERY 42"));
    }

    #[test]
    fn leading_comment_does_not_change_the_legacy_frontier() {
        assert!(is_legacy_frontier(
            "/*+ SET_VAR(query_timeout=1) */ SELECT 1"
        ));
    }
}
