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

//! Backend-membership statement grammar.

use crate::{
    ParseError, Span,
    ast::backend::{AddBackend, DropBackend},
    ast::{BackendStatement, LiteralKind, Statement},
};

use super::{StatementParser, show_backends};

pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("ADD") && parser.peek_word(1, "BACKEND") {
        return parse_add(parser).map(Some);
    }
    if parser.current_is_word("DROP") && parser.peek_word(1, "BACKEND") {
        return parse_drop(parser).map(Some);
    }
    if parser.current_is_word("SHOW") {
        if parser.peek_word(1, "BACKENDS") {
            return show_backends::parse(parser).map(Some);
        }
        // Preserve a parser-domain error for malformed spelling in the owned
        // SHOW BACKENDS command while leaving other SHOW families available.
        if ![
            "ANALYZE",
            "TABLE",
            "CREATE",
            "BASIC",
            "HISTOGRAM",
            "ALTER",
            "MATERIALIZED",
        ]
        .iter()
        .any(|word| parser.peek_word(1, word))
        {
            return show_backends::parse(parser).map(Some);
        }
    }
    Ok(None)
}

fn parse_add(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    parser.consume_word("ADD")?;
    parser.consume_word("BACKEND")?;
    let address = parse_address(parser)?;
    Ok(Statement::Backend(BackendStatement::AddBackend(
        AddBackend {
            span: Span::new(start, address.span.end()),
            address,
        },
    )))
}

fn parse_drop(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    parser.consume_word("DROP")?;
    parser.consume_word("BACKEND")?;
    let address = parse_address(parser)?;
    let force = parser.consume_if_word("FORCE");
    let end = if force {
        parser.current_offset()
    } else {
        address.span.end()
    };
    Ok(Statement::Backend(BackendStatement::DropBackend(
        DropBackend {
            address,
            force,
            span: Span::new(start, end),
        },
    )))
}

fn parse_address(parser: &mut StatementParser<'_, '_>) -> Result<crate::ast::Literal, ParseError> {
    let address = parser.parse_literal()?;
    if !matches!(address.kind, LiteralKind::String(_)) {
        return Err(ParseError::UnexpectedToken {
            expected: "quoted backend address",
            found: parser.source_slice(address.span).to_owned(),
            span: address.span,
        });
    }
    Ok(address)
}
