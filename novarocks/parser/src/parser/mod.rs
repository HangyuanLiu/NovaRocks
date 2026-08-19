// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Recursive-descent parser components.
//!
//! The public statement `parse()` entry point is added by SQLP-1 T6 after the
//! expression and printer foundations have converged.

mod backend;
mod catalog;
mod command;
mod expr;
mod iceberg;
mod maintenance;
mod materialized_view;
mod pratt;
mod show_backends;
mod statistics;

use crate::{
    ParseError, ParserError, Span, Token, TokenKind,
    ast::{Ident, Literal, LiteralKind, ObjectName, Statement},
    keyword_class, lex,
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

    /// Dispatches one owned command family without legacy fallthrough.
    ///
    /// Family parsers return `None` only when the input is not their family.
    /// Once a parser has recognized a family, every malformed form is an
    /// immediate typed error from that parser.
    fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        for parser in [
            backend::parse as FamilyParser,
            statistics::parse,
            catalog::parse,
            iceberg::parse,
            maintenance::parse,
            materialized_view::parse,
        ] {
            if let Some(statement) = parser(self)? {
                return Ok(statement);
            }
        }
        Err(ParseError::UnsupportedStatement {
            statement: self.current_description(),
            span: self.current_span(),
        }
        .into())
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

    pub(super) fn current_is_word(&self, word: &str) -> bool {
        self.current().is_some_and(|token| {
            matches!(token.kind, TokenKind::Ident | TokenKind::Keyword(_))
                && self.source[token.span.start()..token.span.end()].eq_ignore_ascii_case(word)
        })
    }

    pub(super) fn peek_word(&self, offset: usize, word: &str) -> bool {
        self.significant_token(offset).is_some_and(|token| {
            matches!(token.kind, TokenKind::Ident | TokenKind::Keyword(_))
                && self.source[token.span.start()..token.span.end()].eq_ignore_ascii_case(word)
        })
    }

    pub(super) fn consume_word(&mut self, word: &'static str) -> Result<Span, ParseError> {
        if !self.current_is_word(word) {
            return Err(self.unexpected(word));
        }
        let span = self.current_span();
        self.advance();
        self.skip_trivia();
        Ok(span)
    }

    pub(super) fn consume_symbol(&mut self, symbol: Symbol) -> Result<Span, ParseError> {
        if !self.current_is_symbol(symbol) {
            return Err(self.unexpected(symbol.sql()));
        }
        let span = self.current_span();
        self.advance();
        self.skip_trivia();
        Ok(span)
    }

    pub(super) fn consume_if_word(&mut self, word: &str) -> bool {
        if !self.current_is_word(word) {
            return false;
        }
        self.advance();
        self.skip_trivia();
        true
    }

    pub(super) fn consume_if_symbol(&mut self, symbol: Symbol) -> bool {
        if !self.current_is_symbol(symbol) {
            return false;
        }
        self.advance();
        self.skip_trivia();
        true
    }

    pub(super) fn parse_ident(&mut self) -> Result<Ident, ParseError> {
        let token = self
            .current()
            .ok_or_else(|| self.unexpected("identifier"))?;
        match token.kind {
            TokenKind::Ident | TokenKind::QuotedIdent => {
                let source = &self.source[token.span.start()..token.span.end()];
                let (value, quoted) = if matches!(token.kind, TokenKind::QuotedIdent) {
                    (source[1..source.len() - 1].replace("``", "`"), true)
                } else {
                    (source.to_owned(), false)
                };
                let ident = Ident {
                    value,
                    quoted,
                    span: token.span,
                };
                self.advance();
                self.skip_trivia();
                Ok(ident)
            }
            TokenKind::Keyword(keyword)
                if keyword_class(keyword) == crate::KeywordClass::NonReserved =>
            {
                let ident = Ident {
                    value: self.source[token.span.start()..token.span.end()].to_owned(),
                    quoted: false,
                    span: token.span,
                };
                self.advance();
                self.skip_trivia();
                Ok(ident)
            }
            _ => Err(self.unexpected("identifier")),
        }
    }

    pub(super) fn parse_object_name(&mut self) -> Result<ObjectName, ParseError> {
        let first = self.parse_ident()?;
        let start = first.span.start();
        let mut end = first.span.end();
        let mut parts = vec![first];
        while self.consume_if_symbol(Symbol::Dot) {
            let part = self.parse_ident()?;
            end = part.span.end();
            parts.push(part);
        }
        Ok(ObjectName {
            parts,
            span: Span::new(start, end),
        })
    }

    pub(super) fn parse_literal(&mut self) -> Result<Literal, ParseError> {
        let token = self.current().ok_or_else(|| self.unexpected("literal"))?;
        let span = token.span;
        let source = &self.source[span.start()..span.end()];
        let kind = match token.kind {
            TokenKind::Keyword(Keyword::Null) => LiteralKind::Null,
            TokenKind::Keyword(Keyword::True) => LiteralKind::Boolean(true),
            TokenKind::Keyword(Keyword::False) => LiteralKind::Boolean(false),
            TokenKind::Number => LiteralKind::Number(source.to_owned()),
            TokenKind::HexNumber => LiteralKind::HexString(source.to_owned()),
            TokenKind::String => LiteralKind::String(unquote_string(source)),
            _ => return Err(self.unexpected("literal")),
        };
        self.advance();
        self.skip_trivia();
        Ok(Literal { kind, span })
    }

    pub(super) fn current_offset(&self) -> usize {
        self.current_span().start()
    }

    pub(super) fn source_slice(&self, span: Span) -> &str {
        &self.source[span.start()..span.end()]
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

    pub(super) fn skip_trivia(&mut self) {
        while matches!(
            self.current().map(|token| &token.kind),
            Some(TokenKind::Trivia(_))
        ) {
            self.advance();
        }
    }

    fn significant_token(&self, offset: usize) -> Option<&Token> {
        self.tokens
            .iter()
            .skip(self.position)
            .filter(|token| !matches!(token.kind, TokenKind::Trivia(_)))
            .nth(offset)
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

type FamilyParser = for<'source, 'tokens> fn(
    &mut StatementParser<'source, 'tokens>,
) -> Result<Option<Statement>, ParseError>;

fn unquote_string(source: &str) -> String {
    let body = &source[1..source.len() - 1];
    let quote = source.as_bytes()[0] as char;
    let mut output = String::with_capacity(body.len());
    let mut characters = body.chars().peekable();
    while let Some(character) = characters.next() {
        if character == quote && characters.peek() == Some(&quote) {
            output.push(character);
            characters.next();
        } else if character == '\\' {
            output.push(characters.next().unwrap_or(character));
        } else {
            output.push(character);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use crate::{
        ParserError, Span,
        ast::{BackendStatement, ShowBackends, Statement},
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
            vec![Statement::Backend(BackendStatement::ShowBackends(
                ShowBackends {
                    span: Span::new(0, 13),
                },
            ))]
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
                Statement::Backend(BackendStatement::ShowBackends(_)) => "SHOW BACKENDS",
                Statement::Statistics(_) => "STATISTICS",
                Statement::Catalog(_) => "CATALOG",
                Statement::Iceberg(_) => "ICEBERG",
                Statement::Maintenance(_) => "MAINTENANCE",
                Statement::MaterializedView(_) => "MATERIALIZED VIEW",
                Statement::RawQuery(_) => "RAW QUERY",
            })
            .collect()
    }
}
