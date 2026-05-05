//! Parser probe + parse for `ALTER TABLE <name> (CREATE|DROP) [OR REPLACE]
//! [IF [NOT] EXISTS] (BRANCH|TAG) <ident> [AS OF VERSION <int>]
//! [retention-clause-tokens]`.
//!
//! The retention clause is consumed by token until the statement terminator
//! and stashed verbatim in `ignored_options`; phase 1 emits a warning at
//! analyzer time and discards the contents.

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::{convert_object_name, peek_word_eq};
use crate::sql::parser::ast::{
    AlterIcebergRefAction, AlterIcebergRefStmt, SnapshotAnchor, Statement,
};

pub(crate) fn looks_like_alter_iceberg_ref(parser: &Parser<'_>) -> bool {
    if !parser.peek_keyword(Keyword::ALTER) {
        return false;
    }
    if !peek_word_eq(parser, 1, "TABLE") {
        return false;
    }
    // Walk forward past the table name to reach the action token. Worst case
    // table name is `cat.ns.tbl` = 5 tokens; cap the look-ahead at 10.
    for offset in 3..10 {
        if peek_word_eq(parser, offset, "CREATE") || peek_word_eq(parser, offset, "DROP") {
            // Confirm the next non-modifier word is BRANCH or TAG.
            for inner in (offset + 1)..(offset + 6) {
                let w = parser.peek_nth_token_ref(inner);
                let token = match &w.token {
                    Token::Word(w) => w.value.as_str(),
                    _ => return false,
                };
                if token.eq_ignore_ascii_case("BRANCH") || token.eq_ignore_ascii_case("TAG") {
                    return true;
                }
                if !["OR", "REPLACE", "IF", "NOT", "EXISTS"]
                    .iter()
                    .any(|s| token.eq_ignore_ascii_case(s))
                {
                    return false;
                }
            }
        }
    }
    false
}

pub(crate) fn parse_alter_iceberg_ref(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;
    let table = convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;

    let is_create = parser.parse_keyword(Keyword::CREATE);
    if !is_create {
        parser
            .expect_keyword(Keyword::DROP)
            .map_err(|e| e.to_string())?;
    }

    // OR REPLACE is always before the BRANCH/TAG kind word (CREATE OR REPLACE BRANCH …).
    let replace = is_create && parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);

    let kind_word = parser.next_token();
    let kind = match &kind_word.token {
        Token::Word(w) if w.value.eq_ignore_ascii_case("BRANCH") => "BRANCH",
        Token::Word(w) if w.value.eq_ignore_ascii_case("TAG") => "TAG",
        other => return Err(format!("expected BRANCH or TAG, got {other:?}")),
    };

    // IF [NOT] EXISTS comes after the kind keyword.
    let if_not_exists =
        is_create && parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let if_exists = !is_create && parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

    let name = parser
        .parse_identifier()
        .map_err(|e| format!("expected ref name: {e}"))?
        .value;

    if is_create {
        let anchor = if parser.parse_keywords(&[Keyword::AS, Keyword::OF, Keyword::VERSION]) {
            let n = parser
                .parse_literal_uint()
                .map_err(|e| format!("expected snapshot id integer: {e}"))?;
            SnapshotAnchor::SnapshotId(n as i64)
        } else {
            SnapshotAnchor::CurrentMain
        };

        // Capture remaining tokens (retention) verbatim until end-of-statement.
        let mut ignored_options = Vec::new();
        while !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
            ignored_options.push(parser.next_token().to_string());
        }

        let action = match kind {
            "BRANCH" => AlterIcebergRefAction::CreateBranch {
                name,
                anchor,
                if_not_exists,
                replace,
                ignored_options,
            },
            _ => AlterIcebergRefAction::CreateTag {
                name,
                anchor,
                if_not_exists,
                replace,
                ignored_options,
            },
        };
        Ok(Statement::AlterIcebergRef(AlterIcebergRefStmt {
            table,
            action,
        }))
    } else {
        let action = match kind {
            "BRANCH" => AlterIcebergRefAction::DropBranch { name, if_exists },
            _ => AlterIcebergRefAction::DropTag { name, if_exists },
        };
        Ok(Statement::AlterIcebergRef(AlterIcebergRefStmt {
            table,
            action,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::dialect::StarRocksDialect;

    fn parse(sql: &str) -> Result<Statement, String> {
        let dialect = StarRocksDialect;
        let mut p = Parser::new(&dialect)
            .try_with_sql(sql)
            .map_err(|e| e.to_string())?;
        parse_alter_iceberg_ref(&mut p)
    }

    #[test]
    fn create_branch_anchor() {
        let stmt = parse("ALTER TABLE c.s.t CREATE BRANCH dev AS OF VERSION 12345").unwrap();
        match stmt {
            Statement::AlterIcebergRef(s) => match s.action {
                AlterIcebergRefAction::CreateBranch { name, anchor, .. } => {
                    assert_eq!(name, "dev");
                    assert_eq!(anchor, SnapshotAnchor::SnapshotId(12345));
                }
                other => panic!("wrong action: {other:?}"),
            },
            _ => panic!("wrong stmt"),
        }
    }

    #[test]
    fn create_tag_no_anchor_uses_current_main() {
        let stmt = parse("ALTER TABLE t CREATE TAG v1").unwrap();
        match stmt {
            Statement::AlterIcebergRef(s) => match s.action {
                AlterIcebergRefAction::CreateTag { anchor, .. } => {
                    assert_eq!(anchor, SnapshotAnchor::CurrentMain);
                }
                _ => panic!("wrong"),
            },
            _ => panic!("wrong"),
        }
    }

    #[test]
    fn create_or_replace_branch() {
        let stmt = parse("ALTER TABLE t CREATE OR REPLACE BRANCH dev AS OF VERSION 1").unwrap();
        match stmt {
            Statement::AlterIcebergRef(s) => match s.action {
                AlterIcebergRefAction::CreateBranch { replace, .. } => assert!(replace),
                _ => panic!("wrong"),
            },
            _ => panic!("wrong"),
        }
    }

    #[test]
    fn drop_branch_if_exists() {
        let stmt = parse("ALTER TABLE t DROP BRANCH IF EXISTS dev").unwrap();
        match stmt {
            Statement::AlterIcebergRef(s) => match s.action {
                AlterIcebergRefAction::DropBranch { if_exists, name } => {
                    assert!(if_exists);
                    assert_eq!(name, "dev");
                }
                _ => panic!("wrong"),
            },
            _ => panic!("wrong"),
        }
    }

    #[test]
    fn retention_options_captured() {
        let stmt = parse(
            "ALTER TABLE t CREATE BRANCH dev AS OF VERSION 1 WITH SNAPSHOT RETENTION 5 SNAPSHOTS",
        )
        .unwrap();
        match stmt {
            Statement::AlterIcebergRef(s) => match s.action {
                AlterIcebergRefAction::CreateBranch {
                    ignored_options, ..
                } => {
                    assert!(!ignored_options.is_empty());
                }
                _ => panic!("wrong"),
            },
            _ => panic!("wrong"),
        }
    }

    #[test]
    fn probe_recognizes_create_branch() {
        let dialect = StarRocksDialect;
        let p = Parser::new(&dialect)
            .try_with_sql("ALTER TABLE t CREATE BRANCH dev")
            .unwrap();
        assert!(looks_like_alter_iceberg_ref(&p));
    }

    #[test]
    fn probe_rejects_alter_table_other() {
        let dialect = StarRocksDialect;
        let p = Parser::new(&dialect)
            .try_with_sql("ALTER TABLE t ADD COLUMN c INT")
            .unwrap();
        assert!(!looks_like_alter_iceberg_ref(&p));
    }
}
