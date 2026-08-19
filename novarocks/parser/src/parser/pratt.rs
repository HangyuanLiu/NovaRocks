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

//! Data-driven Pratt parsing machinery for the initial expression grammar.

use crate::{
    Span, Symbol, Token, TokenKind,
    ast::{
        BetweenExpr, BinaryExpr, BinaryOperator, CaseExpr, CastExpr, CastKind, Expr, FunctionCall,
        FunctionQuantifier, Ident, InListExpr, IsPredicate, IsPredicateExpr, LikeExpr,
        LikeOperator, Literal, LiteralKind, NestedExpr, ObjectName, TypeName, TypeNameArgument,
        UnaryExpr, UnaryOperator, WindowFrame, WindowFrameBound, WindowFrameExclusion,
        WindowFrameUnits, WindowSpec,
    },
    error::ParseError,
    keyword_class,
    token::Keyword,
};

const OR_PRECEDENCE: u8 = 10;
const AND_PRECEDENCE: u8 = 20;
const NOT_PRECEDENCE: u8 = 30;
const COMPARISON_PRECEDENCE: u8 = 40;
const ADDITIVE_PRECEDENCE: u8 = 50;
const MULTIPLICATIVE_PRECEDENCE: u8 = 60;
const UNARY_ARITHMETIC_PRECEDENCE: u8 = 70;

#[derive(Clone, Copy)]
enum TokenPattern {
    Keyword(Keyword),
    Symbol(Symbol),
}

impl TokenPattern {
    fn matches(self, kind: &TokenKind) -> bool {
        match (self, kind) {
            (Self::Keyword(expected), TokenKind::Keyword(found)) => expected == *found,
            (Self::Symbol(expected), TokenKind::Symbol(found)) => expected == *found,
            _ => false,
        }
    }
}

#[derive(Clone, Copy)]
struct InfixBinding {
    token: TokenPattern,
    operator: BinaryOperator,
    precedence: u8,
}

#[derive(Clone, Copy)]
struct PrefixBinding {
    token: TokenPattern,
    operator: UnaryOperator,
    precedence: u8,
}

// SQLP-4 extends this table without changing the Pratt loop below.
const INFIX_BINDINGS: &[InfixBinding] = &[
    InfixBinding {
        token: TokenPattern::Keyword(Keyword::Or),
        operator: BinaryOperator::Or,
        precedence: OR_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Keyword(Keyword::And),
        operator: BinaryOperator::And,
        precedence: AND_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Eq),
        operator: BinaryOperator::Equal,
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::NullSafeEq),
        operator: BinaryOperator::NullSafeEqual,
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Neq),
        operator: BinaryOperator::NotEqual,
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Lt),
        operator: BinaryOperator::LessThan,
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Lte),
        operator: BinaryOperator::LessThanOrEqual,
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Gt),
        operator: BinaryOperator::GreaterThan,
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Gte),
        operator: BinaryOperator::GreaterThanOrEqual,
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Plus),
        operator: BinaryOperator::Add,
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Minus),
        operator: BinaryOperator::Subtract,
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Star),
        operator: BinaryOperator::Multiply,
        precedence: MULTIPLICATIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Slash),
        operator: BinaryOperator::Divide,
        precedence: MULTIPLICATIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Percent),
        operator: BinaryOperator::Modulo,
        precedence: MULTIPLICATIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::DoublePipe),
        operator: BinaryOperator::StringConcat,
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::ShiftLeft),
        operator: BinaryOperator::ShiftLeft,
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::ShiftRight),
        operator: BinaryOperator::ShiftRight,
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Ampersand),
        operator: BinaryOperator::BitwiseAnd,
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Caret),
        operator: BinaryOperator::BitwiseXor,
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Pipe),
        operator: BinaryOperator::BitwiseOr,
        precedence: ADDITIVE_PRECEDENCE,
    },
];

const PREFIX_BINDINGS: &[PrefixBinding] = &[
    PrefixBinding {
        token: TokenPattern::Keyword(Keyword::Not),
        operator: UnaryOperator::Not,
        precedence: NOT_PRECEDENCE,
    },
    PrefixBinding {
        token: TokenPattern::Symbol(Symbol::Plus),
        operator: UnaryOperator::Plus,
        precedence: UNARY_ARITHMETIC_PRECEDENCE,
    },
    PrefixBinding {
        token: TokenPattern::Symbol(Symbol::Minus),
        operator: UnaryOperator::Minus,
        precedence: UNARY_ARITHMETIC_PRECEDENCE,
    },
    PrefixBinding {
        token: TokenPattern::Symbol(Symbol::Tilde),
        operator: UnaryOperator::BitwiseNot,
        precedence: UNARY_ARITHMETIC_PRECEDENCE,
    },
];

fn infix_binding(kind: &TokenKind) -> Option<InfixBinding> {
    INFIX_BINDINGS
        .iter()
        .copied()
        .find(|binding| binding.token.matches(kind))
}

fn prefix_binding(kind: &TokenKind) -> Option<PrefixBinding> {
    PREFIX_BINDINGS
        .iter()
        .copied()
        .find(|binding| binding.token.matches(kind))
}

pub(super) struct PrattParser<'source, 'tokens> {
    source: &'source str,
    tokens: &'tokens [Token],
    position: usize,
}

impl<'source, 'tokens> PrattParser<'source, 'tokens> {
    pub(super) fn new(source: &'source str, tokens: &'tokens [Token]) -> Self {
        Self {
            source,
            tokens,
            position: 0,
        }
    }

    /// `expression ::= prefix-expression { infix-operator prefix-expression }`
    pub(super) fn parse(mut self) -> Result<Expr, ParseError> {
        let expression = self.parse_binding_power(0)?;
        self.skip_trivia();
        if self.is_end() {
            Ok(expression)
        } else {
            Err(self.unexpected("end of expression"))
        }
    }

    /// `expression ::= prefix-expression { infix-operator expression }`
    fn parse_binding_power(&mut self, minimum_precedence: u8) -> Result<Expr, ParseError> {
        let mut left = self.parse_prefix_expression()?;

        loop {
            self.skip_trivia();
            if minimum_precedence <= COMPARISON_PRECEDENCE && self.starts_comparison_special() {
                left = self.parse_comparison_special(left)?;
                continue;
            }
            let Some(binding) = self.current().and_then(|token| infix_binding(&token.kind)) else {
                break;
            };
            if binding.precedence < minimum_precedence {
                break;
            }

            self.advance();
            // Incrementing the minimum implements left associativity for every
            // current table entry. A right-associative future entry can carry a
            // distinct right binding power without changing this parser shape.
            let right = self.parse_binding_power(binding.precedence + 1)?;
            let span = Span::new(left.span().start(), right.span().end());
            left = Expr::Binary(BinaryExpr {
                left: Box::new(left),
                operator: binding.operator,
                right: Box::new(right),
                span,
            });
        }

        Ok(left)
    }

    fn starts_comparison_special(&self) -> bool {
        self.current_is_keyword(Keyword::Between)
            || self.current_is_keyword(Keyword::In)
            || self.current_is_keyword(Keyword::Like)
            || self.current_is_keyword(Keyword::Ilike)
            || self.current_is_keyword(Keyword::Rlike)
            || self.current_is_keyword(Keyword::Similar)
            || self.current_is_keyword(Keyword::Is)
            || (self.current_is_keyword(Keyword::Not)
                && (self.peek_keyword(1, Keyword::Between)
                    || self.peek_keyword(1, Keyword::In)
                    || self.peek_keyword(1, Keyword::Like)
                    || self.peek_keyword(1, Keyword::Ilike)
                    || self.peek_keyword(1, Keyword::Rlike)
                    || self.peek_keyword(1, Keyword::Similar)))
    }

    fn parse_comparison_special(&mut self, left: Expr) -> Result<Expr, ParseError> {
        let negated = if self.current_is_keyword(Keyword::Not) {
            let is_special = self.peek_keyword(1, Keyword::Between)
                || self.peek_keyword(1, Keyword::In)
                || self.peek_keyword(1, Keyword::Like)
                || self.peek_keyword(1, Keyword::Ilike)
                || self.peek_keyword(1, Keyword::Rlike)
                || self.peek_keyword(1, Keyword::Similar);
            if is_special {
                self.advance();
                self.skip_trivia();
                true
            } else {
                false
            }
        } else {
            false
        };

        if self.current_is_keyword(Keyword::Between) {
            self.advance();
            self.skip_trivia();
            let low = self.parse_binding_power(COMPARISON_PRECEDENCE + 1)?;
            if !self.current_is_keyword(Keyword::And) {
                return Err(self.unexpected("AND in BETWEEN expression"));
            }
            self.advance();
            self.skip_trivia();
            let high = self.parse_binding_power(COMPARISON_PRECEDENCE + 1)?;
            let span = Span::new(left.span().start(), high.span().end());
            return Ok(Expr::Between(BetweenExpr {
                expr: Box::new(left),
                negated,
                low: Box::new(low),
                high: Box::new(high),
                span,
            }));
        }
        if self.current_is_keyword(Keyword::In) {
            self.advance();
            self.skip_trivia();
            if !self.current_is_symbol(Symbol::LParen) {
                return Err(self.unexpected("'(' after IN"));
            }
            self.advance();
            self.skip_trivia();
            let mut list = Vec::new();
            if !self.current_is_symbol(Symbol::RParen) {
                loop {
                    list.push(self.parse_binding_power(0)?);
                    self.skip_trivia();
                    if !self.current_is_symbol(Symbol::Comma) {
                        break;
                    }
                    self.advance();
                    self.skip_trivia();
                }
            }
            if !self.current_is_symbol(Symbol::RParen) {
                return Err(self.unexpected("')' after IN list"));
            }
            let end = self.current_span().end();
            self.advance();
            let start = left.span().start();
            return Ok(Expr::InList(InListExpr {
                expr: Box::new(left),
                negated,
                list,
                span: Span::new(start, end),
            }));
        }
        let operator = if self.current_is_keyword(Keyword::Like) {
            Some(LikeOperator::Like)
        } else if self.current_is_keyword(Keyword::Ilike) {
            Some(LikeOperator::ILike)
        } else if self.current_is_keyword(Keyword::Rlike) {
            Some(LikeOperator::RLike)
        } else if self.current_is_keyword(Keyword::Similar) {
            Some(LikeOperator::SimilarTo)
        } else {
            None
        };
        if let Some(operator) = operator {
            self.advance();
            self.skip_trivia();
            if operator == LikeOperator::SimilarTo && self.current_is_keyword(Keyword::To) {
                self.advance();
                self.skip_trivia();
            }
            let pattern = self.parse_binding_power(COMPARISON_PRECEDENCE + 1)?;
            let escape = if self.current_is_keyword(Keyword::Escape) {
                self.advance();
                self.skip_trivia();
                Some(Box::new(
                    self.parse_binding_power(COMPARISON_PRECEDENCE + 1)?,
                ))
            } else {
                None
            };
            let end = escape
                .as_ref()
                .map_or_else(|| pattern.span().end(), |escape| escape.span().end());
            let start = left.span().start();
            return Ok(Expr::Like(LikeExpr {
                expr: Box::new(left),
                negated,
                operator,
                pattern: Box::new(pattern),
                escape,
                span: Span::new(start, end),
            }));
        }
        if self.current_is_keyword(Keyword::Is) {
            if negated {
                return Err(self.unexpected("BETWEEN, IN, or LIKE after NOT"));
            }
            self.advance();
            self.skip_trivia();
            let negated = if self.current_is_keyword(Keyword::Not) {
                self.advance();
                self.skip_trivia();
                true
            } else {
                false
            };
            if self.current_is_keyword(Keyword::Distinct) {
                self.advance();
                self.skip_trivia();
                if !self.current_is_keyword(Keyword::From) {
                    return Err(self.unexpected("FROM after IS DISTINCT"));
                }
                self.advance();
                self.skip_trivia();
                let right = self.parse_binding_power(COMPARISON_PRECEDENCE + 1)?;
                let operator = if negated {
                    BinaryOperator::IsNotDistinctFrom
                } else {
                    BinaryOperator::IsDistinctFrom
                };
                let span = Span::new(left.span().start(), right.span().end());
                return Ok(Expr::Binary(BinaryExpr {
                    left: Box::new(left),
                    operator,
                    right: Box::new(right),
                    span,
                }));
            }
            let predicate = match (negated, self.current().map(|token| &token.kind)) {
                (false, Some(TokenKind::Keyword(Keyword::Null))) => IsPredicate::Null,
                (true, Some(TokenKind::Keyword(Keyword::Null))) => IsPredicate::NotNull,
                (false, Some(TokenKind::Keyword(Keyword::True))) => IsPredicate::True,
                (true, Some(TokenKind::Keyword(Keyword::True))) => IsPredicate::NotTrue,
                (false, Some(TokenKind::Keyword(Keyword::False))) => IsPredicate::False,
                (true, Some(TokenKind::Keyword(Keyword::False))) => IsPredicate::NotFalse,
                (false, Some(TokenKind::Keyword(Keyword::Unknown))) => IsPredicate::Unknown,
                (true, Some(TokenKind::Keyword(Keyword::Unknown))) => IsPredicate::NotUnknown,
                _ => {
                    return Err(self.unexpected("NULL, TRUE, FALSE, UNKNOWN, or DISTINCT after IS"));
                }
            };
            let end = self.current_span().end();
            self.advance();
            let start = left.span().start();
            return Ok(Expr::IsPredicate(IsPredicateExpr {
                expr: Box::new(left),
                predicate,
                span: Span::new(start, end),
            }));
        }
        Err(self.unexpected("comparison special form"))
    }

    /// `prefix-expression ::= unary-operator expression | primary-expression`
    fn parse_prefix_expression(&mut self) -> Result<Expr, ParseError> {
        self.skip_trivia();
        if let Some(binding) = self.current().and_then(|token| prefix_binding(&token.kind)) {
            let start = self.current_span().start();
            self.advance();
            let expression = self.parse_binding_power(binding.precedence)?;
            let span = Span::new(start, expression.span().end());
            return Ok(Expr::Unary(UnaryExpr {
                operator: binding.operator,
                expression: Box::new(expression),
                span,
            }));
        }

        self.parse_primary_expression()
    }

    /// `primary-expression ::= literal | identifier [ function-call ] | "(" expression ")"`
    fn parse_primary_expression(&mut self) -> Result<Expr, ParseError> {
        self.skip_trivia();
        let token = self.current().cloned();
        match token {
            Some(Token {
                kind: TokenKind::Number,
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: LiteralKind::Number(self.token_text(span).to_owned()),
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::HexNumber,
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: LiteralKind::HexString(self.token_text(span).to_owned()),
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::String,
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: LiteralKind::String(self.string_value(span)),
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::Keyword(Keyword::Null),
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: LiteralKind::Null,
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::Keyword(Keyword::True),
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: LiteralKind::Boolean(true),
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::Keyword(Keyword::False),
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: LiteralKind::Boolean(false),
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::Keyword(Keyword::Case),
                ..
            }) => self.parse_case_expression(),
            Some(Token {
                kind: TokenKind::Keyword(Keyword::Cast),
                ..
            }) => self.parse_cast_expression(CastKind::Cast),
            Some(Token {
                kind: TokenKind::Keyword(Keyword::TryCast),
                ..
            }) => self.parse_cast_expression(CastKind::TryCast),
            Some(Token {
                kind: TokenKind::Ident | TokenKind::QuotedIdent,
                span,
            }) => self.parse_identifier_or_function_call(span),
            Some(Token {
                kind: TokenKind::Keyword(keyword),
                span,
            }) if keyword_class(keyword) == crate::KeywordClass::NonReserved => {
                self.parse_identifier_or_function_call(span)
            }
            Some(Token {
                kind: TokenKind::Symbol(Symbol::LParen),
                span,
            }) => self.parse_nested_expression(span),
            _ => Err(self.unexpected("expression")),
        }
    }

    fn parse_case_expression(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        let operand = if self.current_is_keyword(Keyword::When) {
            None
        } else {
            Some(Box::new(self.parse_binding_power(0)?))
        };
        let mut conditions = Vec::new();
        let mut results = Vec::new();
        while self.current_is_keyword(Keyword::When) {
            self.advance();
            self.skip_trivia();
            let condition = self.parse_binding_power(0)?;
            if !self.current_is_keyword(Keyword::Then) {
                return Err(self.unexpected("THEN in CASE expression"));
            }
            self.advance();
            self.skip_trivia();
            let result = self.parse_binding_power(0)?;
            conditions.push(condition);
            results.push(result);
        }
        if conditions.is_empty() {
            return Err(self.unexpected("WHEN in CASE expression"));
        }
        let else_result = if self.current_is_keyword(Keyword::Else) {
            self.advance();
            self.skip_trivia();
            Some(Box::new(self.parse_binding_power(0)?))
        } else {
            None
        };
        if !self.current_is_keyword(Keyword::End) {
            return Err(self.unexpected("END in CASE expression"));
        }
        let end = self.current_span().end();
        self.advance();
        Ok(Expr::Case(CaseExpr {
            operand,
            conditions,
            results,
            else_result,
            span: Span::new(start, end),
        }))
    }

    fn parse_cast_expression(&mut self, kind: CastKind) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after CAST"));
        }
        self.advance();
        self.skip_trivia();
        let expr = self.parse_binding_power(0)?;
        if !self.current_is_keyword(Keyword::As) {
            return Err(self.unexpected("AS in CAST expression"));
        }
        self.advance();
        self.skip_trivia();
        let data_type = self.parse_type_name()?;
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')' after CAST type"));
        }
        let end = self.current_span().end();
        self.advance();
        Ok(Expr::Cast(CastExpr {
            expr: Box::new(expr),
            data_type,
            kind,
            format: None,
            span: Span::new(start, end),
        }))
    }

    fn parse_type_name(&mut self) -> Result<TypeName, ParseError> {
        let start = self.current_span().start();
        let Some(token) = self.current().cloned() else {
            return Err(self.unexpected("type name"));
        };
        if !matches!(
            token.kind,
            TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
        ) {
            return Err(self.unexpected("type name"));
        }
        let mut parts = vec![self.parse_identifier(token.span)];
        while self.current_is_symbol(Symbol::Dot) {
            self.advance();
            self.skip_trivia();
            let Some(token) = self.current().cloned() else {
                return Err(self.unexpected("type name segment"));
            };
            if !matches!(
                token.kind,
                TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
            ) {
                return Err(self.unexpected("type name segment"));
            }
            parts.push(self.parse_identifier(token.span));
        }
        let name_end = parts.last().map_or(start, |part| part.span.end());
        let name = ObjectName {
            parts,
            span: Span::new(start, name_end),
        };
        let mut arguments = Vec::new();
        let mut end = name_end;
        if self.current_is_symbol(Symbol::LParen) {
            self.advance();
            self.skip_trivia();
            loop {
                let literal = match self.current().map(|token| &token.kind) {
                    Some(TokenKind::Number) => {
                        let span = self.current_span();
                        let value = self.token_text(span).to_owned();
                        self.advance();
                        self.skip_trivia();
                        Literal {
                            kind: LiteralKind::Number(value),
                            span,
                        }
                    }
                    _ => return Err(self.unexpected("type parameter")),
                };
                arguments.push(TypeNameArgument::Literal(literal));
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
            if !self.current_is_symbol(Symbol::RParen) {
                return Err(self.unexpected("')' after type parameters"));
            }
            end = self.current_span().end();
            self.advance();
            self.skip_trivia();
        }
        Ok(TypeName {
            name,
            arguments,
            span: Span::new(start, end),
        })
    }

    /// `identifier ::= IDENT | QUOTED_IDENT`
    fn parse_identifier(&mut self, span: Span) -> Ident {
        let quoted = matches!(
            self.current().map(|token| &token.kind),
            Some(TokenKind::QuotedIdent)
        );
        self.advance();
        Ident {
            value: if quoted {
                self.quoted_identifier_value(span)
            } else {
                self.token_text(span).to_owned()
            },
            quoted,
            span,
        }
    }

    /// `function-call ::= object-name "(" [ expression { "," expression } ] ")"`
    fn parse_identifier_or_function_call(&mut self, span: Span) -> Result<Expr, ParseError> {
        let first = self.parse_identifier(span);
        let mut end = first.span.end();
        let mut parts = vec![first];
        while self.current_is_symbol(Symbol::Dot) {
            self.advance();
            self.skip_trivia();
            let Some(token) = self.current().cloned() else {
                return Err(self.unexpected("identifier after '.'"));
            };
            if !matches!(
                token.kind,
                TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
            ) {
                return Err(self.unexpected("identifier after '.'"));
            }
            let ident = self.parse_identifier(token.span);
            end = ident.span.end();
            parts.push(ident);
        }
        let name = ObjectName {
            parts,
            span: Span::new(span.start(), end),
        };
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            return Ok(if name.parts.len() == 1 {
                Expr::Identifier(name.parts.into_iter().next().expect("one identifier"))
            } else {
                Expr::CompoundIdentifier(crate::ast::CompoundIdentifier {
                    span: name.span,
                    parts: name.parts,
                })
            });
        }

        self.advance();
        let mut arguments = Vec::new();
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::RParen) {
            loop {
                if self.current_is_symbol(Symbol::Star) {
                    let star = self.current_span();
                    self.advance();
                    arguments.push(Expr::Identifier(Ident {
                        value: "*".to_owned(),
                        quoted: false,
                        span: star,
                    }));
                } else {
                    arguments.push(self.parse_binding_power(0)?);
                }
                self.skip_trivia();
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
                if self.current_is_symbol(Symbol::RParen) || self.is_end() {
                    return Err(self.unexpected("expression after ','"));
                }
            }
        }

        self.skip_trivia();
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')'"));
        }
        let end = self.current_span().end();
        self.advance();
        self.skip_trivia();
        let over = if self.current_is_keyword(Keyword::Over) {
            self.advance();
            self.skip_trivia();
            if matches!(
                self.current().map(|token| &token.kind),
                Some(TokenKind::Ident | TokenKind::QuotedIdent)
            ) {
                let name = self.parse_identifier(self.current_span());
                Some(Box::new(WindowSpec {
                    span: name.span,
                    existing_window_name: Some(name),
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    window_frame: None,
                }))
            } else {
                Some(Box::new(self.parse_window_spec()?))
            }
        } else {
            None
        };
        let span_end = over.as_ref().map_or(end, |window| window.span.end());
        Ok(Expr::FunctionCall(FunctionCall {
            name,
            arguments,
            quantifier: FunctionQuantifier::None,
            order_by: Vec::new(),
            separator: None,
            filter: None,
            null_treatment: None,
            over,
            span: Span::new(span.start(), span_end),
        }))
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec, ParseError> {
        let start = self.current_span().start();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after OVER"));
        }
        self.advance();
        self.skip_trivia();
        let mut existing_window_name = None;
        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        let mut window_frame = None;
        if matches!(
            self.current().map(|token| &token.kind),
            Some(TokenKind::Ident | TokenKind::QuotedIdent)
        ) && !self.peek_keyword(1, Keyword::By)
        {
            existing_window_name = Some(self.parse_identifier(self.current_span()));
        }
        if self.current_is_keyword(Keyword::Partition) {
            self.advance();
            self.skip_trivia();
            if !self.current_is_keyword(Keyword::By) {
                return Err(self.unexpected("BY after PARTITION"));
            }
            self.advance();
            self.skip_trivia();
            loop {
                partition_by.push(self.parse_binding_power(0)?);
                self.skip_trivia();
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
        }
        if self.current_is_keyword(Keyword::Order) {
            self.advance();
            self.skip_trivia();
            if !self.current_is_keyword(Keyword::By) {
                return Err(self.unexpected("BY after ORDER"));
            }
            self.advance();
            self.skip_trivia();
            loop {
                let expr = self.parse_binding_power(0)?;
                let asc = if self.current_is_keyword(Keyword::Asc) {
                    self.advance();
                    self.skip_trivia();
                    Some(true)
                } else if self.current_is_keyword(Keyword::Desc) {
                    self.advance();
                    self.skip_trivia();
                    Some(false)
                } else {
                    None
                };
                let span = Span::new(expr.span().start(), self.current_span().start());
                order_by.push(crate::ast::OrderByExpr {
                    expr,
                    asc,
                    nulls_first: None,
                    span,
                });
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
        }
        if let Some(units) = self.parse_window_frame_units() {
            let frame_start = self.current_span().start();
            let (start_bound, end_bound) = if self.current_is_keyword(Keyword::Between) {
                self.advance();
                self.skip_trivia();
                let start_bound = self.parse_window_frame_bound()?;
                if !self.current_is_keyword(Keyword::And) {
                    return Err(self.unexpected("AND in window frame"));
                }
                self.advance();
                self.skip_trivia();
                (start_bound, Some(self.parse_window_frame_bound()?))
            } else {
                (self.parse_window_frame_bound()?, None)
            };
            let frame_end = end_bound
                .as_ref()
                .map_or_else(|| start_bound.span().end(), |bound| bound.span().end());
            window_frame = Some(WindowFrame {
                units,
                start_bound,
                end_bound,
                exclusion: WindowFrameExclusion::NoOthers,
                span: Span::new(frame_start, frame_end),
            });
        }
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')' after window specification"));
        }
        let end = self.current_span().end();
        self.advance();
        self.skip_trivia();
        Ok(WindowSpec {
            existing_window_name,
            partition_by,
            order_by,
            window_frame,
            span: Span::new(start, end),
        })
    }

    fn parse_window_frame_units(&mut self) -> Option<WindowFrameUnits> {
        let units = if self.current_is_keyword(Keyword::Rows) {
            WindowFrameUnits::Rows
        } else if self.current_is_keyword(Keyword::Range) {
            WindowFrameUnits::Range
        } else if self.current_is_keyword(Keyword::Groups) {
            WindowFrameUnits::Groups
        } else {
            return None;
        };
        self.advance();
        self.skip_trivia();
        Some(units)
    }

    fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound, ParseError> {
        let start = self.current_span().start();
        if self.current_is_keyword(Keyword::Current) {
            self.advance();
            self.skip_trivia();
            if !self.current_is_keyword(Keyword::Row) {
                return Err(self.unexpected("ROW after CURRENT"));
            }
            let end = self.current_span().end();
            self.advance();
            self.skip_trivia();
            return Ok(WindowFrameBound::CurrentRow(Span::new(start, end)));
        }
        if self.current_is_keyword(Keyword::Unbounded) {
            self.advance();
            self.skip_trivia();
            if self.current_is_keyword(Keyword::Preceding) {
                let end = self.current_span().end();
                self.advance();
                self.skip_trivia();
                return Ok(WindowFrameBound::Preceding(None, Span::new(start, end)));
            }
            if self.current_is_keyword(Keyword::Following) {
                let end = self.current_span().end();
                self.advance();
                self.skip_trivia();
                return Ok(WindowFrameBound::Following(None, Span::new(start, end)));
            }
            return Err(self.unexpected("PRECEDING or FOLLOWING after UNBOUNDED"));
        }
        let value = self.parse_binding_power(0)?;
        if self.current_is_keyword(Keyword::Preceding) {
            let end = self.current_span().end();
            self.advance();
            self.skip_trivia();
            return Ok(WindowFrameBound::Preceding(
                Some(value),
                Span::new(start, end),
            ));
        }
        if self.current_is_keyword(Keyword::Following) {
            let end = self.current_span().end();
            self.advance();
            self.skip_trivia();
            return Ok(WindowFrameBound::Following(
                Some(value),
                Span::new(start, end),
            ));
        }
        Err(self.unexpected("PRECEDING or FOLLOWING in window frame"))
    }

    /// `nested-expression ::= "(" expression ")"`
    fn parse_nested_expression(&mut self, opening_span: Span) -> Result<Expr, ParseError> {
        self.advance();
        let expression = self.parse_binding_power(0)?;
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')'"));
        }
        let end = self.current_span().end();
        self.advance();
        Ok(Expr::Nested(NestedExpr {
            expression: Box::new(expression),
            span: Span::new(opening_span.start(), end),
        }))
    }

    fn current(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn current_span(&self) -> Span {
        self.current()
            .map(|token| token.span)
            .unwrap_or_else(|| Span::new(self.source.len(), self.source.len()))
    }

    fn current_is_symbol(&self, symbol: Symbol) -> bool {
        matches!(self.current().map(|token| &token.kind), Some(TokenKind::Symbol(found)) if *found == symbol)
    }

    fn current_is_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.current().map(|token| &token.kind), Some(TokenKind::Keyword(found)) if *found == keyword)
    }

    fn peek_keyword(&self, offset: usize, keyword: Keyword) -> bool {
        self.tokens
            .iter()
            .skip(self.position)
            .filter(|token| !matches!(token.kind, TokenKind::Trivia(_)))
            .nth(offset)
            .is_some_and(
                |token| matches!(token.kind, TokenKind::Keyword(found) if found == keyword),
            )
    }

    fn is_end(&self) -> bool {
        self.current()
            .is_none_or(|token| token.kind == TokenKind::End)
    }

    fn advance(&mut self) {
        if self.current().is_some() {
            self.position += 1;
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

    fn unexpected(&self, expected: &'static str) -> ParseError {
        ParseError::UnexpectedToken {
            expected,
            found: self.current_description(),
            span: self.current_span(),
        }
    }

    fn current_description(&self) -> String {
        match self.current() {
            None
            | Some(Token {
                kind: TokenKind::End,
                ..
            }) => "EOF".to_owned(),
            Some(token) => format!("`{}`", self.token_text(token.span)),
        }
    }

    fn token_text(&self, span: Span) -> &str {
        self.source
            .get(span.start()..span.end())
            .unwrap_or("<invalid token span>")
    }

    fn quoted_identifier_value(&self, span: Span) -> String {
        let text = self.token_text(span);
        text.strip_prefix('`')
            .and_then(|text| text.strip_suffix('`'))
            .unwrap_or(text)
            .replace("``", "`")
    }

    fn string_value(&self, span: Span) -> String {
        let text = self.token_text(span);
        let Some(quote) = text.chars().next() else {
            return String::new();
        };
        let body = text
            .strip_prefix(quote)
            .and_then(|text| text.strip_suffix(quote))
            .unwrap_or(text);
        unescape_string(body, quote)
    }
}

/// Parses exactly one parenthesized window specification from a query-clause
/// token slice. Named `WINDOW name AS (...)` definitions and function `OVER
/// (...)` deliberately share the same syntax production.
pub(super) fn parse_window_spec(source: &str, tokens: &[Token]) -> Result<WindowSpec, ParseError> {
    let mut parser = PrattParser::new(source, tokens);
    let specification = parser.parse_window_spec()?;
    parser.skip_trivia();
    if parser.is_end() {
        Ok(specification)
    } else {
        Err(parser.unexpected("end of window specification"))
    }
}

fn unescape_string(text: &str, quote: char) -> String {
    let mut value = String::with_capacity(text.len());
    let mut characters = text.chars();
    while let Some(character) = characters.next() {
        if character == '\\' {
            if let Some(escaped) = characters.next() {
                value.push(escaped);
            } else {
                value.push(character);
            }
        } else if character == quote && characters.clone().next() == Some(quote) {
            value.push(quote);
            characters.next();
        } else {
            value.push(character);
        }
    }
    value
}
