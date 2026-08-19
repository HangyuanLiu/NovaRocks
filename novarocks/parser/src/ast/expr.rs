// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use crate::{
    Span,
    ast::{Ident, Literal},
};

/// A SQL expression with source spans retained at every syntax node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr {
    Identifier(Ident),
    Literal(Literal),
    FunctionCall(FunctionCall),
    Unary(UnaryExpr),
    Binary(BinaryExpr),
    Nested(NestedExpr),
}

impl Expr {
    pub const fn span(&self) -> Span {
        match self {
            Self::Identifier(ident) => ident.span,
            Self::Literal(literal) => literal.span,
            Self::FunctionCall(call) => call.span,
            Self::Unary(expression) => expression.span,
            Self::Binary(expression) => expression.span,
            Self::Nested(expression) => expression.span,
        }
    }
}

/// A function name and its argument expressions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionCall {
    pub name: Ident,
    pub arguments: Vec<Expr>,
    pub span: Span,
}

/// A prefix unary expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnaryExpr {
    pub operator: UnaryOperator,
    pub expression: Box<Expr>,
    pub span: Span,
}

/// A binary expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub operator: BinaryOperator,
    pub right: Box<Expr>,
    pub span: Span,
}

/// A parenthesized expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NestedExpr {
    pub expression: Box<Expr>,
    pub span: Span,
}

/// Prefix operators initially recognized by the Pratt parser.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOperator {
    Not,
    Plus,
    Minus,
}

/// Infix operators initially recognized by the Pratt parser.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOperator {
    Or,
    And,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Add,
    Subtract,
    Multiply,
    Divide,
}
