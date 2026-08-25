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

//! Session-command syntax nodes.

use crate::Span;

use super::{Expr, Ident, Literal, Query, UserVariable};

/// A statement that changes or controls one SQL session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionStatement {
    Set(SetStatement),
    Use(UseStatement),
    Kill(KillStatement),
    TransactionControl(TransactionControlStatement),
}

impl SessionStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::Set(statement) => statement.span,
            Self::Use(statement) => statement.span,
            Self::Kill(statement) => statement.span,
            Self::TransactionControl(statement) => statement.span,
        }
    }
}

/// A transaction-control statement recognized at the session boundary.
///
/// NovaRocks does not implement a cross-statement transaction.  These nodes
/// retain the syntactic intent so admission can reject it before any catalog
/// or data mutation is dispatched.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionControlStatement {
    pub kind: TransactionControlKind,
    pub span: Span,
}

/// The closed transaction-control forms owned by the parser.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionControlKind {
    Begin,
    StartTransaction,
    Commit,
    Rollback,
    Savepoint,
}

impl TransactionControlKind {
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Begin => "BEGIN",
            Self::StartTransaction => "START TRANSACTION",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
            Self::Savepoint => "SAVEPOINT",
        }
    }
}

/// A `SET` statement containing independently scoped assignments.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetStatement {
    pub assignments: Vec<SetAssignment>,
    pub span: Span,
}

/// One `SET` assignment. Scope belongs to the assignment and never inherits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetAssignment {
    pub scope: SetScope,
    pub target: SetTarget,
    pub value: SetValue,
    pub span: Span,
}

/// The syntactic scope attached to one `SET` assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetScope {
    Default,
    Session,
    Local,
    Global,
}

/// The target shape of one `SET` assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SetTarget {
    UserVariable(UserVariable),
    SystemVariable(Ident),
    Names { span: Span },
    Transaction { span: Span },
    Catalog { span: Span },
}

impl SetTarget {
    pub const fn span(&self) -> Span {
        match self {
            Self::UserVariable(variable) => variable.span,
            Self::SystemVariable(variable) => variable.span,
            Self::Names { span } | Self::Transaction { span } | Self::Catalog { span } => *span,
        }
    }
}

/// The value syntax accepted after a `SET` target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SetValue {
    Expression(Expr),
    Query(Box<Query>),
    Words(Vec<SetWord>),
}

/// One keyword, identifier, or literal in a special `SET` form.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SetWord {
    Ident(Ident),
    Literal(Literal),
}

impl SetWord {
    pub const fn span(&self) -> Span {
        match self {
            Self::Ident(word) => word.span,
            Self::Literal(word) => word.span,
        }
    }
}

impl SetValue {
    pub fn span(&self) -> Span {
        match self {
            Self::Expression(expression) => expression.span(),
            Self::Query(query) => query.span,
            Self::Words(words) => match words.first() {
                Some(first) => Span::new(
                    first.span().start(),
                    words.last().expect("non-empty").span().end(),
                ),
                None => Span::new(0, 0),
            },
        }
    }
}

/// `USE <database>` or `USE <catalog>.<database>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UseStatement {
    pub catalog: Option<Ident>,
    pub database: Ident,
    pub span: Span,
}

/// `KILL [QUERY | CONNECTION] <id>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KillStatement {
    pub kind: KillKind,
    pub connection_id: Literal,
    pub span: Span,
}

/// The optional target kind in `KILL` syntax.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KillKind {
    Default,
    Query,
    Connection,
}
