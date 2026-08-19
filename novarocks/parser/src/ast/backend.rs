// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Backend-membership syntax nodes.

use crate::Span;

use super::{Fold, Visit};

/// A backend-membership statement.
///
/// SQLP-3 starts with the SQLP-1 vertical slice and extends this enum with
/// `ADD BACKEND` and `DROP BACKEND` in the family-local grammar task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendStatement {
    ShowBackends(ShowBackends),
}

impl BackendStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::ShowBackends(statement) => statement.span,
        }
    }
}

/// `SHOW BACKENDS`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShowBackends {
    pub span: Span,
}

pub(crate) fn write_sql(statement: &BackendStatement, output: &mut String) {
    match statement {
        BackendStatement::ShowBackends(_) => output.push_str("SHOW BACKENDS"),
    }
}

pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &BackendStatement) {
    match statement {
        BackendStatement::ShowBackends(statement) => visitor.visit_show_backends(statement),
    }
}

pub(crate) fn fold<F: Fold + ?Sized>(
    folder: &mut F,
    statement: BackendStatement,
) -> BackendStatement {
    match statement {
        BackendStatement::ShowBackends(statement) => {
            BackendStatement::ShowBackends(folder.fold_show_backends(statement))
        }
    }
}
