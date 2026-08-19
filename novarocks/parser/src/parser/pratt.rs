// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.

//! Data-driven Pratt parsing machinery for the initial expression grammar.

use crate::{
    Span, Symbol, Token, TokenKind,
    ast::{
        BinaryExpr, BinaryOperator, Expr, FunctionCall, Ident, Literal, LiteralKind, NestedExpr,
        UnaryExpr, UnaryOperator,
    },
    error::ParseError,
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
                kind: TokenKind::Ident | TokenKind::QuotedIdent,
                span,
            }) => self.parse_identifier_or_function_call(span),
            Some(Token {
                kind: TokenKind::Symbol(Symbol::LParen),
                span,
            }) => self.parse_nested_expression(span),
            _ => Err(self.unexpected("expression")),
        }
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

    /// `function-call ::= identifier "(" [ expression { "," expression } ] ")"`
    fn parse_identifier_or_function_call(&mut self, span: Span) -> Result<Expr, ParseError> {
        let identifier = self.parse_identifier(span);
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            return Ok(Expr::Identifier(identifier));
        }

        self.advance();
        let mut arguments = Vec::new();
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::RParen) {
            loop {
                arguments.push(self.parse_binding_power(0)?);
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
        Ok(Expr::FunctionCall(FunctionCall {
            name: identifier,
            arguments,
            span: Span::new(span.start(), end),
        }))
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
