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

//! Session-command grammar.

use crate::{
    ParseError, Span, Token, TokenKind,
    ast::{
        Ident, KillKind, KillStatement, SessionStatement, SetAssignment, SetScope, SetStatement,
        SetTarget, SetValue, SetWord, Statement, UseStatement, UserVariable,
    },
    token::{Keyword, Symbol},
};

use super::{StatementParser, pratt::PrattParser, query};

pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("SET") {
        return parse_set(parser).map(Some);
    }
    if parser.current_is_word("USE") {
        return parse_use(parser).map(Some);
    }
    // `KILL ANALYZE` is owned by the preceding statistics family. Preserve
    // that ownership even if this parser is ever reused in another order.
    if parser.current_is_word("KILL") && !parser.peek_word(1, "ANALYZE") {
        return parse_kill(parser).map(Some);
    }
    Ok(None)
}

fn parse_set(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("SET")?.start();
    let mut assignments = Vec::new();
    loop {
        assignments.push(parse_assignment(parser)?);
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    let end = assignments
        .last()
        .expect("SET has at least one assignment")
        .span
        .end();
    Ok(Statement::Session(SessionStatement::Set(SetStatement {
        assignments,
        span: Span::new(start, end),
    })))
}

fn parse_assignment(parser: &mut StatementParser<'_, '_>) -> Result<SetAssignment, ParseError> {
    let start = parser.current_span().start();
    let mut scope = parse_optional_scope(parser);
    let target = if parser.current_is_word("NAMES") {
        let target_span = parser.consume_word("NAMES")?;
        let mut words = vec![parse_set_word(parser)?];
        if parser.current_is_word("COLLATE") {
            let collate_span = parser.consume_word("COLLATE")?;
            words.push(SetWord::Ident(Ident {
                value: "COLLATE".to_owned(),
                quoted: false,
                quote_style: None,
                span: collate_span,
            }));
            words.push(parse_set_word(parser)?);
        }
        return Ok(SetAssignment {
            scope,
            target: SetTarget::Names { span: target_span },
            span: Span::new(
                start,
                words.last().expect("NAMES value is present").span().end(),
            ),
            value: SetValue::Words(words),
        });
    } else if parser.current_is_word("TRANSACTION") {
        let target_span = parser.consume_word("TRANSACTION")?;
        let words = parse_words_until_assignment_end(parser)?;
        return Ok(SetAssignment {
            scope,
            target: SetTarget::Transaction { span: target_span },
            span: Span::new(
                start,
                words
                    .last()
                    .expect("TRANSACTION value is present")
                    .span()
                    .end(),
            ),
            value: SetValue::Words(words),
        });
    } else if parser.current_is_word("CATALOG") {
        let target_span = parser.consume_word("CATALOG")?;
        let value = if parser.consume_if_symbol(Symbol::Eq) {
            parse_assignment_value(parser)?
        } else {
            let value = parser.parse_contextual_ident()?;
            SetValue::Words(vec![SetWord::Ident(value)])
        };
        return Ok(SetAssignment {
            scope,
            target: SetTarget::Catalog { span: target_span },
            span: Span::new(start, value.span().end()),
            value,
        });
    } else {
        parse_variable_target(parser, &mut scope)?
    };

    parser.consume_symbol(Symbol::Eq)?;
    let value = parse_assignment_value(parser)?;
    let end = value.span().end();
    Ok(SetAssignment {
        scope,
        target,
        value,
        span: Span::new(start, end),
    })
}

fn parse_optional_scope(parser: &mut StatementParser<'_, '_>) -> SetScope {
    if parser.consume_if_word("SESSION") {
        SetScope::Session
    } else if parser.consume_if_word("LOCAL") {
        SetScope::Local
    } else if parser.consume_if_word("GLOBAL") {
        SetScope::Global
    } else {
        SetScope::Default
    }
}

fn parse_variable_target(
    parser: &mut StatementParser<'_, '_>,
    scope: &mut SetScope,
) -> Result<SetTarget, ParseError> {
    if matches!(
        parser.current().map(|token| &token.kind),
        Some(TokenKind::UserVariable)
    ) {
        let variable_span = parser.current_span();
        let raw_variable = parser.source_slice(variable_span).to_owned();
        parser.advance();
        parser.skip_trivia();
        if !raw_variable.starts_with("@@") {
            return Ok(SetTarget::UserVariable(UserVariable {
                value: raw_variable,
                span: variable_span,
            }));
        }

        let mut name = raw_variable.trim_start_matches("@@").to_owned();
        let mut span = variable_span;
        if parser.consume_if_symbol(Symbol::Dot) {
            let suffix = parser.parse_contextual_ident()?;
            span = Span::new(variable_span.start(), suffix.span.end());
            let qualifier = name.to_ascii_lowercase();
            if matches!(*scope, SetScope::Default) {
                *scope = match qualifier.as_str() {
                    "session" => SetScope::Session,
                    "local" => SetScope::Local,
                    "global" => SetScope::Global,
                    _ => SetScope::Default,
                };
            }
            name = if matches!(qualifier.as_str(), "session" | "local" | "global") {
                suffix.value
            } else {
                format!("{name}.{}", suffix.value)
            };
        }
        return Ok(SetTarget::SystemVariable(Ident {
            value: name,
            quoted: false,
            quote_style: None,
            span,
        }));
    }

    Ok(SetTarget::SystemVariable(parser.parse_contextual_ident()?))
}

fn parse_assignment_value(parser: &mut StatementParser<'_, '_>) -> Result<SetValue, ParseError> {
    if parser.current_is_word("ON") || parser.current_is_word("OFF") {
        return parser
            .parse_contextual_ident()
            .map(|value| SetValue::Words(vec![SetWord::Ident(value)]));
    }
    if parenthesized_query_follows(parser) {
        parser.consume_symbol(Symbol::LParen)?;
        let query = query::parse_query(parser)?;
        parser.consume_symbol(Symbol::RParen)?;
        return Ok(SetValue::Query(Box::new(query)));
    }
    parse_expression_until_assignment_end(parser).map(SetValue::Expression)
}

fn parenthesized_query_follows(parser: &StatementParser<'_, '_>) -> bool {
    if !parser.current_is_symbol(Symbol::LParen) {
        return false;
    }
    parser
        .tokens
        .iter()
        .skip(parser.position + 1)
        .find(|token| !matches!(token.kind, TokenKind::Trivia(_)))
        .is_some_and(|token| {
            matches!(
                token.kind,
                TokenKind::Keyword(Keyword::Select | Keyword::Values | Keyword::With)
                    | TokenKind::Symbol(Symbol::LParen)
            )
        })
}

fn parse_expression_until_assignment_end(
    parser: &mut StatementParser<'_, '_>,
) -> Result<crate::ast::Expr, ParseError> {
    let begin = parser.position;
    let end = assignment_value_end(parser);
    if begin == end {
        return Err(parser.unexpected("expression"));
    }
    let mut tokens: Vec<Token> = parser.tokens[begin..end].to_vec();
    let boundary = parser
        .tokens
        .get(end)
        .map_or_else(|| parser.current_span(), |token| token.span);
    tokens.push(Token::new(TokenKind::End, boundary));
    let expression = PrattParser::new(parser.source, &tokens).parse()?;
    parser.position = end;
    parser.skip_trivia();
    Ok(expression)
}

fn assignment_value_end(parser: &StatementParser<'_, '_>) -> usize {
    let mut end = parser.position;
    let mut nesting = 0usize;
    while let Some(token) = parser.tokens.get(end) {
        match token.kind {
            TokenKind::End => break,
            TokenKind::Symbol(Symbol::LParen | Symbol::LBracket | Symbol::LBrace) => nesting += 1,
            TokenKind::Symbol(Symbol::RParen | Symbol::RBracket | Symbol::RBrace)
                if nesting > 0 =>
            {
                nesting -= 1;
            }
            TokenKind::Symbol(Symbol::Comma | Symbol::Semicolon) if nesting == 0 => break,
            _ => {}
        }
        end += 1;
    }
    end
}

fn parse_words_until_assignment_end(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<SetWord>, ParseError> {
    let mut words = Vec::new();
    while !parser.is_end()
        && !parser.current_is_symbol(Symbol::Comma)
        && !parser.current_is_symbol(Symbol::Semicolon)
    {
        words.push(parse_set_word(parser)?);
    }
    if words.is_empty() {
        return Err(parser.unexpected("transaction characteristics"));
    }
    Ok(words)
}

fn parse_set_word(parser: &mut StatementParser<'_, '_>) -> Result<SetWord, ParseError> {
    if matches!(
        parser.current().map(|token| &token.kind),
        Some(TokenKind::String)
    ) {
        return parser.parse_literal().map(SetWord::Literal);
    }
    parser.parse_contextual_ident().map(SetWord::Ident)
}

fn parse_use(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("USE")?.start();
    let first = parser.parse_contextual_ident()?;
    let (catalog, database) = if parser.consume_if_symbol(Symbol::Dot) {
        (Some(first), parser.parse_contextual_ident()?)
    } else {
        (None, first)
    };
    let end = database.span.end();
    Ok(Statement::Session(SessionStatement::Use(UseStatement {
        catalog,
        database,
        span: Span::new(start, end),
    })))
}

fn parse_kill(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("KILL")?.start();
    let kind = if parser.consume_if_word("QUERY") {
        KillKind::Query
    } else if parser.consume_if_word("CONNECTION") {
        KillKind::Connection
    } else {
        KillKind::Default
    };
    if !matches!(
        parser.current().map(|token| &token.kind),
        Some(TokenKind::Number)
    ) {
        return Err(parser.unexpected("connection id"));
    }
    let connection_id = parser.parse_literal()?;
    let end = connection_id.span.end();
    Ok(Statement::Session(SessionStatement::Kill(KillStatement {
        kind,
        connection_id,
        span: Span::new(start, end),
    })))
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{
            KillKind, SessionStatement, SetScope, SetTarget, SetValue, SetWord, Statement,
            StatisticsStatement,
        },
        parser,
        printer::print_statement,
    };

    #[test]
    fn parses_independently_scoped_set_assignments() {
        let statements = parser::parse(
            "SET SESSION query_timeout = 1, GLOBAL pipeline_dop = 2, LOCAL @answer = '42'",
        )
        .expect("SET assignments should parse");
        let Statement::Session(SessionStatement::Set(set)) = &statements[0] else {
            panic!("expected SET session statement");
        };
        assert_eq!(set.assignments.len(), 3);
        assert_eq!(set.assignments[0].scope, SetScope::Session);
        assert_eq!(set.assignments[1].scope, SetScope::Global);
        assert_eq!(set.assignments[2].scope, SetScope::Local);
        assert!(matches!(
            &set.assignments[2].target,
            SetTarget::UserVariable(_)
        ));
        assert!(matches!(&set.assignments[2].value, SetValue::Expression(_)));
    }

    #[test]
    fn parses_qualified_variable_scope_and_special_set_forms() {
        let statements = parser::parse(
            "SET @@session.query_timeout = 1, @@global.pipeline_dop = 2; \
             SET NAMES 'utf8' COLLATE 'utf8_general_ci'; \
             SET TRANSACTION ISOLATION LEVEL READ COMMITTED; \
             SET CATALOG warehouse; SET CATALOG = warehouse",
        )
        .expect("session forms should parse");
        let Statement::Session(SessionStatement::Set(set)) = &statements[0] else {
            panic!("expected SET statement");
        };
        assert_eq!(set.assignments[0].scope, SetScope::Session);
        assert_eq!(set.assignments[1].scope, SetScope::Global);
        assert!(matches!(
            &set.assignments[0].target,
            SetTarget::SystemVariable(_)
        ));
        for statement in &statements[1..] {
            assert!(matches!(
                statement,
                Statement::Session(SessionStatement::Set(_))
            ));
        }
        assert_eq!(
            print_statement(&statements[1]),
            "SET NAMES 'utf8' COLLATE 'utf8_general_ci'"
        );
    }

    #[test]
    fn parses_set_user_variable_query_use_and_all_kill_forms() {
        let statements = parser::parse(
            "SET @answer = (SELECT 42); USE `catalog`.`analytics`; KILL 1; KILL QUERY 2; KILL CONNECTION 3",
        )
        .expect("session statements should parse");
        let Statement::Session(SessionStatement::Set(set)) = &statements[0] else {
            panic!("expected SET statement");
        };
        assert!(matches!(&set.assignments[0].value, SetValue::Query(_)));
        let Statement::Session(SessionStatement::Use(use_statement)) = &statements[1] else {
            panic!("expected USE statement");
        };
        assert_eq!(
            use_statement
                .catalog
                .as_ref()
                .map(|catalog| catalog.value.as_str()),
            Some("catalog")
        );
        assert_eq!(use_statement.database.value, "analytics");
        assert_eq!(print_statement(&statements[1]), "USE `catalog`.`analytics`");
        for (statement, kind) in
            statements[2..]
                .iter()
                .zip([KillKind::Default, KillKind::Query, KillKind::Connection])
        {
            let Statement::Session(SessionStatement::Kill(kill)) = statement else {
                panic!("expected KILL statement");
            };
            assert_eq!(kill.kind, kind);
        }
    }

    #[test]
    fn does_not_take_kill_analyze_from_statistics() {
        let statements = parser::parse("KILL ANALYZE 17").expect("statistics command should parse");
        assert!(matches!(
            statements.as_slice(),
            [Statement::Statistics(StatisticsStatement::CancelAnalyze(_))]
        ));
    }

    #[test]
    fn session_commands_reject_trailing_tokens_with_parser_errors() {
        for source in [
            "USE default extra",
            "USE catalog.database.extra",
            "KILL QUERY 1 extra",
            "KILL QUERY '1'",
        ] {
            let error = parser::parse(source).expect_err("trailing token must fail");
            assert_eq!(
                error.to_user_error(source).code().as_str(),
                "sql.parse.unexpected_token"
            );
        }
    }

    #[test]
    fn parses_set_boolean_values_without_widening_expression_grammar() {
        let statements =
            parser::parse("SET disable_function_fold_constants = on, enable_eliminate_agg = off")
                .expect("SET boolean assignments should parse");
        let Statement::Session(SessionStatement::Set(set)) = &statements[0] else {
            panic!("expected SET statement");
        };
        assert!(matches!(
            set.assignments[0].value,
            SetValue::Words(ref words)
                if matches!(words.as_slice(), [SetWord::Ident(value)] if value.value.eq_ignore_ascii_case("on"))
        ));
        assert!(matches!(
            set.assignments[1].value,
            SetValue::Words(ref words)
                if matches!(words.as_slice(), [SetWord::Ident(value)] if value.value.eq_ignore_ascii_case("off"))
        ));
        assert_eq!(
            print_statement(&statements[0]),
            "SET disable_function_fold_constants = on, enable_eliminate_agg = off"
        );
        assert!(parser::parse("SELECT on").is_err());
    }

    #[test]
    fn contextual_session_words_remain_query_identifiers() {
        assert!(matches!(
            parser::parse("SELECT * FROM session"),
            Ok(statements) if matches!(statements.as_slice(), [Statement::Query(_)])
        ));
    }
}
