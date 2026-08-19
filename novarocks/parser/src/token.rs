// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use crate::Span;

/// A source token, including trivia, with its exact original byte range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

impl Token {
    pub const fn new(kind: TokenKind, span: Span) -> Self {
        Self { kind, span }
    }
}

/// The lexical category of a token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TokenKind {
    Keyword(Keyword),
    Ident,
    QuotedIdent,
    UserVariable,
    String,
    Number,
    HexNumber,
    Symbol(Symbol),
    Trivia(TriviaKind),
    End,
}

/// Structured source trivia preserved in the token stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TriviaKind {
    Whitespace,
    LineComment,
    BlockComment,
    HintComment,
}

/// Keywords initially needed by the parser foundation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Keyword {
    As,
    Backends,
    False,
    From,
    Show,
    True,
}

/// Punctuation and operators recognized by the foundation lexer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Symbol {
    Ampersand,
    Bang,
    Caret,
    Colon,
    Comma,
    Dot,
    Eq,
    Gt,
    Gte,
    LBrace,
    LBracket,
    LParen,
    Lte,
    Lt,
    Minus,
    Neq,
    Percent,
    Pipe,
    Plus,
    Question,
    RBrace,
    RBracket,
    RParen,
    Semicolon,
    Slash,
    Star,
    Tilde,
}
