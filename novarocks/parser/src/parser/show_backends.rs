// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use crate::{
    ParseError,
    ast::{BackendStatement, ShowBackends, Statement},
    token::Keyword,
};

use super::StatementParser;

/// `show-backends ::= SHOW BACKENDS`
pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    parser.advance(); // SHOW
    skip_trivia(parser);
    if !parser.current_is_keyword(Keyword::Backends) {
        return Err(parser.unexpected("BACKENDS"));
    }
    let end = parser.current_span().end();
    parser.advance();
    Ok(Statement::Backend(BackendStatement::ShowBackends(
        ShowBackends {
            span: crate::Span::new(start, end),
        },
    )))
}

fn skip_trivia(parser: &mut StatementParser<'_, '_>) {
    while matches!(
        parser.current().map(|token| &token.kind),
        Some(crate::TokenKind::Trivia(_))
    ) {
        parser.advance();
    }
}
