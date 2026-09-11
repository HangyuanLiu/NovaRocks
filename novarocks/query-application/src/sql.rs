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

//! Query-application ownership of parser-admitted SQL statement shape.

use std::fmt;

use novarocks_parser::{ParserError, ast::Statement};

/// Connection-local execution settings after SQL/session validation and before
/// a role adapter projects them into a particular wire contract.
pub mod session;

/// The application boundary accepts one already-framed SQL statement.
///
/// Protocol adapters own batch framing and multi-result negotiation. Once a
/// fragment crosses that boundary, this function is the sole parser admission
/// point and rejects a fragment that contains more than one statement.
pub fn parse_single_statement(source: &str) -> Result<Statement, SqlStatementParseError> {
    parse_optional_single_statement(source)?
        .ok_or(SqlStatementParseError::ExpectedExactlyOne { actual: 0 })
}

/// Parses one protocol-framed SQL fragment, preserving a comment-only fragment
/// as the absence of a statement.
pub fn parse_optional_single_statement(
    source: &str,
) -> Result<Option<Statement>, SqlStatementParseError> {
    let statements = novarocks_parser::parse(source).map_err(SqlStatementParseError::Parser)?;
    match statements.as_slice() {
        [] => Ok(None),
        [statement] => Ok(Some(statement.clone())),
        _ => Err(SqlStatementParseError::ExpectedExactlyOne {
            actual: statements.len(),
        }),
    }
}

/// Query-application parser-admission failure.
#[derive(Debug)]
pub enum SqlStatementParseError {
    Parser(ParserError),
    ExpectedExactlyOne { actual: usize },
}

impl fmt::Display for SqlStatementParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parser(error) => error.fmt(formatter),
            Self::ExpectedExactlyOne { actual } => {
                write!(formatter, "expected exactly one statement, found {actual}")
            }
        }
    }
}

impl std::error::Error for SqlStatementParseError {}

#[cfg(test)]
mod tests {
    use super::{SqlStatementParseError, parse_optional_single_statement, parse_single_statement};

    #[test]
    fn accepts_one_parser_statement() {
        let statement = parse_single_statement("SELECT 1").expect("one statement");
        assert!(matches!(
            statement,
            novarocks_parser::ast::Statement::Query(_)
        ));
    }

    #[test]
    fn rejects_multiple_parser_statements() {
        assert!(matches!(
            parse_single_statement("SELECT 1; SELECT 2"),
            Err(SqlStatementParseError::ExpectedExactlyOne { actual: 2 })
        ));
    }

    #[test]
    fn retains_comment_only_fragment_as_absent() {
        assert_eq!(
            parse_optional_single_statement("/* comment */").expect("comment parses"),
            None
        );
    }
}
