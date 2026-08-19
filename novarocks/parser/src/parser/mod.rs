// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Recursive-descent parser components.
//!
//! The public statement `parse()` entry point is added by SQLP-1 T6 after the
//! expression and printer foundations have converged.

mod expr;
mod pratt;
mod show_backends;

use crate::{
    ParseError, ParserError, Span, Token, TokenKind,
    ast::Statement,
    lex,
    token::{Keyword, Symbol},
};

pub use expr::parse_expression;

/// `statements ::= { statement [ ";" ] } EOF`
///
/// Parses the currently-owned statement families. A statement head which is
/// syntactically recognizable but has not yet been adopted is a typed parser
/// outcome rather than a route miss or a text-classified fallback.
pub fn parse(source: &str) -> Result<Vec<Statement>, ParserError> {
    let tokens = lex(source)?;
    StatementParser::new(source, &tokens).parse_statements()
}

struct StatementParser<'source, 'tokens> {
    source: &'source str,
    tokens: &'tokens [Token],
    position: usize,
}

impl<'source, 'tokens> StatementParser<'source, 'tokens> {
    fn new(source: &'source str, tokens: &'tokens [Token]) -> Self {
        Self {
            source,
            tokens,
            position: 0,
        }
    }

    fn parse_statements(mut self) -> Result<Vec<Statement>, ParserError> {
        let mut statements = Vec::new();
        self.skip_trivia();
        while !self.is_end() {
            statements.push(self.parse_statement()?);
            self.skip_trivia();
            if self.current_is_symbol(Symbol::Semicolon) {
                self.advance();
                self.skip_trivia();
            } else if !self.is_end() {
                return Err(self.unexpected("';' or end of input").into());
            }
        }
        Ok(statements)
    }

    /// `statement ::= show-backends | recognized-unsupported-statement`
    fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.current().map(|token| &token.kind) {
            Some(TokenKind::Keyword(Keyword::Show)) => Ok(show_backends::parse(self)?),
            _ => Err(ParseError::UnsupportedStatement {
                statement: self.current_description(),
                span: self.current_span(),
            }
            .into()),
        }
    }

    pub(super) fn current(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    pub(super) fn current_span(&self) -> Span {
        self.current()
            .map(|token| token.span)
            .unwrap_or_else(|| Span::new(self.source.len(), self.source.len()))
    }

    pub(super) fn current_is_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.current().map(|token| &token.kind), Some(TokenKind::Keyword(found)) if *found == keyword)
    }

    pub(super) fn current_is_symbol(&self, symbol: Symbol) -> bool {
        matches!(self.current().map(|token| &token.kind), Some(TokenKind::Symbol(found)) if *found == symbol)
    }

    pub(super) fn advance(&mut self) {
        if self.current().is_some() {
            self.position += 1;
        }
    }

    pub(super) fn unexpected(&self, expected: &'static str) -> ParseError {
        ParseError::UnexpectedToken {
            expected,
            found: self.current_description(),
            span: self.current_span(),
        }
    }

    fn skip_trivia(&mut self) {
        while matches!(
            self.current().map(|token| &token.kind),
            Some(TokenKind::Trivia(_))
        ) {
            self.advance();
        }
    }

    fn is_end(&self) -> bool {
        self.current()
            .is_none_or(|token| token.kind == TokenKind::End)
    }

    fn current_description(&self) -> String {
        match self.current() {
            None
            | Some(Token {
                kind: TokenKind::End,
                ..
            }) => "EOF".to_owned(),
            Some(token) => format!("`{}`", &self.source[token.span.start()..token.span.end()]),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ParserError, Span,
        ast::{ShowBackends, Statement},
        lex,
        printer::print_statements,
    };

    use super::parse;

    #[test]
    fn show_backends_is_a_complete_round_trip_vertical_slice() {
        let source = " /*+ SET_VAR(x=1) */ show backends; SHOW BACKENDS ";
        let ast = parse(source).expect("SHOW BACKENDS should parse");
        assert_eq!(ast.len(), 2);
        assert_eq!(print_statements(&ast), "SHOW BACKENDS; SHOW BACKENDS");
        let re_parsed = parse(&print_statements(&ast)).expect("printed statements should parse");
        assert_eq!(statement_shapes(&re_parsed), statement_shapes(&ast));
    }

    #[test]
    fn malformed_owned_statement_returns_code_span_and_location() {
        let source = "SHOW BACKENDZ";
        let parser_error = parse(source).expect_err("misspelled SHOW family must fail");
        let ParserError::Parse(_) = parser_error else {
            panic!("expected parser error");
        };
        assert_eq!(
            parser_error.to_user_error(source).code().as_str(),
            "sql.parse.unexpected_token"
        );
        assert_eq!(
            parser_error
                .to_user_error(source)
                .location()
                .unwrap()
                .column(),
            6
        );
        assert_eq!(
            parser_error
                .to_user_error(source)
                .location()
                .unwrap()
                .end_column(),
            Some(14)
        );
    }

    #[test]
    fn unsupported_statement_is_typed_and_trailing_token_is_rejected() {
        let unsupported = parse("SELECT 1").expect_err("SELECT is not owned in SQLP-1");
        assert_eq!(
            unsupported.to_user_error("SELECT 1").code().as_str(),
            "sql.parse.unsupported_statement"
        );

        let trailing = parse("SHOW BACKENDS unexpected").expect_err("trailing token must fail");
        assert_eq!(
            trailing
                .to_user_error("SHOW BACKENDS unexpected")
                .code()
                .as_str(),
            "sql.parse.unexpected_token"
        );
        assert_eq!(
            parse("SHOW BACKENDS").unwrap(),
            vec![Statement::ShowBackends(ShowBackends {
                span: Span::new(0, 13),
            })]
        );
    }

    #[test]
    fn lex_errors_remain_parser_errors_at_the_public_boundary() {
        let error = parse("SHOW `unterminated").expect_err("lexing must fail");
        assert!(matches!(error, ParserError::Lex(_)));
        assert!(lex("SHOW `unterminated").is_err());
    }

    fn statement_shapes(statements: &[Statement]) -> Vec<&'static str> {
        statements
            .iter()
            .map(|statement| match statement {
                Statement::ShowBackends(_) => "SHOW BACKENDS",
                Statement::RawQuery(_) => "RAW QUERY",
            })
            .collect()
    }
}
