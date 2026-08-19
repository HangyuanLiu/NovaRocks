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
    Add,
    Alter,
    Analyze,
    And,
    As,
    Async,
    Backend,
    Backends,
    Basic,
    Branch,
    Buckets,
    By,
    Call,
    Cancel,
    Catalog,
    Column,
    Columns,
    Comment,
    Costs,
    Create,
    Database,
    Default,
    Drop,
    Exists,
    Expire,
    Explain,
    External,
    False,
    Files,
    Force,
    From,
    Full,
    Histogram,
    If,
    Jobs,
    Kill,
    Manifests,
    Materialized,
    Meta,
    Not,
    Null,
    Or,
    Orphan,
    Partition,
    Properties,
    Refresh,
    Remove,
    Rewrite,
    Sample,
    Set,
    Show,
    Snapshots,
    Stats,
    Sync,
    Table,
    Tables,
    Tag,
    To,
    True,
    Truncate,
    Unset,
    Verbose,
    View,
    Views,
    With,
}

/// Punctuation and operators recognized by the foundation lexer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Symbol {
    Ampersand,
    Bang,
    Backslash,
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

impl Symbol {
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Ampersand => "&",
            Self::Bang => "!",
            Self::Backslash => "\\",
            Self::Caret => "^",
            Self::Colon => ":",
            Self::Comma => ",",
            Self::Dot => ".",
            Self::Eq => "=",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::LBrace => "{",
            Self::LBracket => "[",
            Self::LParen => "(",
            Self::Lte => "<=",
            Self::Lt => "<",
            Self::Minus => "-",
            Self::Neq => "!=",
            Self::Percent => "%",
            Self::Pipe => "|",
            Self::Plus => "+",
            Self::Question => "?",
            Self::RBrace => "}",
            Self::RBracket => "]",
            Self::RParen => ")",
            Self::Semicolon => ";",
            Self::Slash => "/",
            Self::Star => "*",
            Self::Tilde => "~",
        }
    }
}
