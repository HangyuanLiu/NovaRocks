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

//! View statement grammar.

use crate::{
    ParseError, Span,
    ast::{CreateView, DropView, ShowCreateView, ShowViews, Statement, ViewStatement},
    token::Symbol,
};

use super::StatementParser;

pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("CREATE")
        && (parser.peek_word(1, "VIEW")
            || (parser.peek_word(1, "OR")
                && parser.peek_word(2, "REPLACE")
                && parser.peek_word(3, "VIEW")))
    {
        return parse_create(parser).map(Some);
    }
    if parser.current_is_word("DROP") && parser.peek_word(1, "VIEW") {
        return parse_drop(parser).map(Some);
    }
    if parser.current_is_word("SHOW")
        && parser.peek_word(1, "CREATE")
        && parser.peek_word(2, "VIEW")
    {
        return parse_show_create(parser).map(Some);
    }
    if parser.current_is_word("SHOW")
        && (parser.peek_word(1, "VIEWS") || parser.peek_word(1, "VIEW"))
    {
        return parse_show(parser).map(Some);
    }
    Ok(None)
}

fn parse_create(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("CREATE")?.start();
    let or_replace = if parser.consume_if_word("OR") {
        parser.consume_word("REPLACE")?;
        true
    } else {
        false
    };
    parser.consume_word("VIEW")?;
    let if_not_exists = consume_if_not_exists(parser)?;
    if or_replace && if_not_exists {
        return Err(parser.unexpected("CREATE OR REPLACE VIEW without IF NOT EXISTS"));
    }
    let name = parser.parse_object_name()?;
    let columns = if parser.current_is_symbol(Symbol::LParen) {
        parse_columns(parser)?
    } else {
        Vec::new()
    };
    let comment = if parser.consume_if_word("COMMENT") {
        Some(parser.parse_literal()?)
    } else {
        None
    };
    parser.consume_word("AS")?;
    let query = parser.parse_raw_query_slice()?;
    let span = Span::new(start, query.span.end());
    Ok(Statement::View(ViewStatement::Create(CreateView {
        or_replace,
        if_not_exists,
        name,
        columns,
        comment,
        query,
        span,
    })))
}

fn parse_drop(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("DROP")?.start();
    parser.consume_word("VIEW")?;
    let if_exists = consume_if_exists(parser)?;
    let name = parser.parse_object_name()?;
    let span = Span::new(start, name.span.end());
    Ok(Statement::View(ViewStatement::Drop(DropView {
        if_exists,
        name,
        span,
    })))
}

fn parse_show(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("SHOW")?.start();
    parser.consume_word("VIEWS")?;
    let database = if parser.consume_if_word("FROM") || parser.consume_if_word("IN") {
        Some(parser.parse_object_name()?)
    } else {
        None
    };
    if parser.current_is_word("LIKE") || parser.current_is_word("WHERE") {
        return Err(parser.unexpected("unfiltered SHOW VIEWS"));
    }
    let span = Span::new(start, parser.current_offset());
    Ok(Statement::View(ViewStatement::Show(ShowViews {
        database,
        span,
    })))
}

fn parse_show_create(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("SHOW")?.start();
    parser.consume_word("CREATE")?;
    parser.consume_word("VIEW")?;
    let name = parser.parse_object_name()?;
    let span = Span::new(start, name.span.end());
    Ok(Statement::View(ViewStatement::ShowCreate(ShowCreateView {
        name,
        span,
    })))
}

fn parse_columns(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<crate::ast::Ident>, ParseError> {
    parser.consume_symbol(Symbol::LParen)?;
    let mut columns = Vec::new();
    loop {
        columns.push(parser.parse_ident()?);
        if parser.consume_if_symbol(Symbol::RParen) {
            break;
        }
        parser.consume_symbol(Symbol::Comma)?;
    }
    Ok(columns)
}

fn consume_if_not_exists(parser: &mut StatementParser<'_, '_>) -> Result<bool, ParseError> {
    if !parser.consume_if_word("IF") {
        return Ok(false);
    }
    parser.consume_word("NOT")?;
    parser.consume_word("EXISTS")?;
    Ok(true)
}

fn consume_if_exists(parser: &mut StatementParser<'_, '_>) -> Result<bool, ParseError> {
    if !parser.consume_if_word("IF") {
        return Ok(false);
    }
    parser.consume_word("EXISTS")?;
    Ok(true)
}
