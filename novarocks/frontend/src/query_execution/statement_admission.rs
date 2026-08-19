// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

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
    DeferredFamily,
    Parsed,
}

/// Admits one statement through the bounded frontier or one parser invocation.
pub(crate) fn admit_statement(
    source: &str,
) -> Result<(StatementAdmission, Vec<Statement>), UserError> {
    if is_legacy_frontier(source) {
        return Ok((StatementAdmission::LegacyFrontier, Vec::new()));
    }
    if is_deferred_family(source) {
        return Ok((StatementAdmission::DeferredFamily, Vec::new()));
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
        Some("SELECT" | "WITH" | "INSERT" | "DELETE" | "UPDATE" | "MERGE" | "SET" | "USE") => true,
        Some("KILL") => words.get(1).map(String::as_str) != Some("ANALYZE"),
        Some("EXPLAIN") => explain_targets_query(&words),
        Some("CREATE") => {
            words.get(1).map(String::as_str) == Some("TABLE")
                || (words.get(1).map(String::as_str) == Some("TEMPORARY")
                    && words.get(2).map(String::as_str) == Some("TABLE"))
                || (words.get(1).map(String::as_str) == Some("EXTERNAL")
                    && words.get(2).map(String::as_str) == Some("TABLE"))
                || words.get(1).map(String::as_str) == Some("VIEW")
                || (words.get(1).map(String::as_str) == Some("OR")
                    && words.get(2).map(String::as_str) == Some("REPLACE")
                    && words.get(3).map(String::as_str) == Some("VIEW"))
        }
        Some("DROP" | "ALTER") => matches!(words.get(1).map(String::as_str), Some("VIEW")),
        Some("SHOW") => {
            words.get(1).map(String::as_str) == Some("VIEWS")
                || (words.get(1).map(String::as_str) == Some("CREATE")
                    && words.get(2).map(String::as_str) == Some("VIEW"))
        }
        _ => false,
    }
}

/// Holds later SQLP-3 families on their existing route until their own atomic
/// owner cut. This is a deterministic head gate, never a parse-error fallback.
fn is_deferred_family(source: &str) -> bool {
    let words = significant_words(source);
    let first = words.first().map(String::as_str);
    match first {
        Some("CALL") => false,
        Some("ALTER") if words.get(1).map(String::as_str) == Some("TABLE") => {
            !is_admitted_iceberg_alter(&words) && !is_admitted_maintenance_alter(&words)
        }
        Some("CREATE" | "DROP" | "REFRESH") => {
            words.get(1).map(String::as_str) == Some("MATERIALIZED")
        }
        Some("SHOW") => {
            matches!(
                words.get(1).map(String::as_str),
                Some("MATERIALIZED")
                    | Some("ALTER")
                        if !matches!(
                            (words.get(2).map(String::as_str), words.get(3).map(String::as_str)),
                            (Some("TABLE"), Some("OPTIMIZE"))
                        )
            ) || (words.get(1).map(String::as_str) == Some("CREATE")
                && words.get(2).map(String::as_str) == Some("TABLE"))
        }
        Some("EXPLAIN") => words.iter().any(|word| word == "REFRESH"),
        _ => false,
    }
}

fn is_admitted_maintenance_alter(words: &[String]) -> bool {
    words.windows(2).any(|pair| {
        matches!(
            (pair[0].as_str(), pair[1].as_str()),
            ("REWRITE", "MANIFESTS") | ("EXPIRE", "SNAPSHOTS") | ("REMOVE", "ORPHAN")
        )
    }) || words.iter().any(|word| word == "OPTIMIZE")
}

/// The Iceberg owner cut is intentionally narrower than the `ALTER TABLE`
/// head: maintenance remains on its legacy route until T9. This deterministic
/// lexical gate chooses the already-owned structural forms before parsing;
/// malformed forms within that owned shape still surface parser errors.
fn is_admitted_iceberg_alter(words: &[String]) -> bool {
    words.windows(2).any(|pair| {
        matches!(
            (pair[0].as_str(), pair[1].as_str()),
            ("ADD", "COLUMN" | "PARTITION" | "FILES")
                | ("DROP", "COLUMN" | "PARTITION" | "BRANCH" | "TAG")
                | ("RENAME", "COLUMN")
                | ("MODIFY", "COLUMN")
                | ("ALTER", "COLUMN")
                | ("SET", "TBLPROPERTIES")
                | ("UNSET", "TBLPROPERTIES")
                | ("CREATE", "BRANCH" | "TAG")
        )
    }) || words.iter().skip(2).any(|word| word == "COMMENT")
}

fn explain_targets_query(words: &[String]) -> bool {
    words
        .iter()
        .skip(1)
        .map(String::as_str)
        .find(|word| !matches!(*word, "ANALYZE" | "VERBOSE" | "COSTS" | "LOGICAL"))
        .is_some_and(|word| matches!(word, "SELECT" | "WITH"))
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
            "CREATE TABLE t (k INT)",
            "CREATE TEMPORARY TABLE t (k INT)",
            "CREATE VIEW v AS SELECT 1",
            "CREATE OR REPLACE VIEW v AS SELECT 1",
            "INSERT INTO t VALUES (1)",
            "DROP VIEW v",
            "SHOW VIEWS",
            "SET query_timeout = 1",
            "KILL QUERY 1",
            "EXPLAIN VERBOSE SELECT 1",
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
    fn later_command_families_are_deferred_without_parsing() {
        for source in [
            "ALTER TABLE ice.db.t ADD EQUALITY DELETE (id) VALUES (1)",
            "CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH(k) BUCKETS 1 AS SELECT k FROM t",
            "SHOW CREATE TABLE ice.db.t",
        ] {
            assert_eq!(
                admit_statement(source)
                    .expect("later family must remain deferred")
                    .0,
                StatementAdmission::DeferredFamily,
                "{source}"
            );
        }
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
