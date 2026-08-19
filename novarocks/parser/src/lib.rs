// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! The dependency-light owner for NovaRocks SQL source facts.

pub mod ast;

mod error;
mod keyword;
mod lexer;
mod messages;
mod span;
mod token;

pub use error::{ERROR_CODE_DESCRIPTORS, LexError, ParseError, ParserError, ValidateError};
pub use lexer::lex;
pub use span::{LineCol, Span};
pub use token::{Keyword, Symbol, Token, TokenKind, TriviaKind};
