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

//! Parser probes for standalone backend management statements.

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::peek_word_eq;
use crate::sql::parser::ast::{AddBackendStmt, DropBackendStmt, ShowBackendsStmt, Statement};

pub(crate) fn looks_like_add_backend(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::ADD) && peek_word_eq(parser, 1, "BACKEND")
}

pub(crate) fn parse_add_backend(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser
        .expect_keyword(Keyword::ADD)
        .map_err(|e| format!("ADD BACKEND: {e}"))?;
    expect_word(parser, "BACKEND")?;
    let addr = parse_backend_addr(parser, "ADD BACKEND")?;
    expect_statement_end(parser, "ADD BACKEND")?;
    Ok(Statement::AddBackend(AddBackendStmt { addr }))
}

pub(crate) fn looks_like_drop_backend(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::DROP) && peek_word_eq(parser, 1, "BACKEND")
}

pub(crate) fn parse_drop_backend(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser
        .expect_keyword(Keyword::DROP)
        .map_err(|e| format!("DROP BACKEND: {e}"))?;
    expect_word(parser, "BACKEND")?;
    let addr = parse_backend_addr(parser, "DROP BACKEND")?;
    let force = parser.parse_keyword(Keyword::FORCE);
    expect_statement_end(parser, "DROP BACKEND")?;
    Ok(Statement::DropBackend(DropBackendStmt { addr, force }))
}

pub(crate) fn looks_like_show_backends(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::SHOW) && peek_word_eq(parser, 1, "BACKENDS")
}

pub(crate) fn parse_show_backends(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|e| format!("SHOW BACKENDS: {e}"))?;
    expect_word(parser, "BACKENDS")?;
    expect_statement_end(parser, "SHOW BACKENDS")?;
    Ok(Statement::ShowBackends(ShowBackendsStmt))
}

fn parse_backend_addr(parser: &mut Parser<'_>, context: &str) -> Result<String, String> {
    parser
        .parse_literal_string()
        .map_err(|e| format!("{context} expects a quoted backend address: {e}"))
}

fn expect_word(parser: &mut Parser<'_>, word: &str) -> Result<(), String> {
    let token = parser.next_token();
    match token.token {
        Token::Word(token_word) if token_word.value.eq_ignore_ascii_case(word) => Ok(()),
        other => Err(format!("expected {word}, got {other}")),
    }
}

fn expect_statement_end(parser: &mut Parser<'_>, context: &str) -> Result<(), String> {
    if parser.consume_token(&Token::SemiColon) && parser.peek_token_ref().token == Token::SemiColon
    {
        return Err(format!("{context}: only one final semicolon is allowed"));
    }
    match parser.peek_token_ref().token {
        Token::EOF => Ok(()),
        ref other => Err(format!("{context}: unexpected trailing token {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::dialect::StarRocksDialect;

    fn parse_one(sql: &str) -> Result<Statement, String> {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(sql)
            .map_err(|e| e.to_string())?;
        if looks_like_show_backends(&parser) {
            return parse_show_backends(&mut parser);
        }
        if looks_like_drop_backend(&parser) {
            return parse_drop_backend(&mut parser);
        }
        if looks_like_add_backend(&parser) {
            return parse_add_backend(&mut parser);
        }
        Err("no backend statement".to_string())
    }

    #[test]
    fn probes_do_not_match_unrelated_statements() {
        let dialect = StarRocksDialect;
        let show_mv = Parser::new(&dialect)
            .try_with_sql("SHOW MATERIALIZED VIEWS")
            .expect("parse");
        assert!(!looks_like_show_backends(&show_mv));
        let drop_table = Parser::new(&dialect)
            .try_with_sql("DROP TABLE t")
            .expect("parse");
        assert!(!looks_like_drop_backend(&drop_table));
    }

    #[test]
    fn rejects_show_backends_filters() {
        let err = parse_one("SHOW BACKENDS LIKE '%'").expect_err("LIKE must be rejected");
        assert!(err.contains("unexpected trailing token"), "{err}");
    }
}
