// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.

//! Backend-membership syntax nodes.

use crate::{Span, ast::Literal, printer::print_literal};

use super::{Fold, Visit};

/// A backend-membership statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendStatement {
    AddBackend(AddBackend),
    DropBackend(DropBackend),
    ShowBackends(ShowBackends),
}

impl BackendStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::AddBackend(statement) => statement.span,
            Self::DropBackend(statement) => statement.span,
            Self::ShowBackends(statement) => statement.span,
        }
    }
}

/// `ADD BACKEND '<host>:<port>'`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddBackend {
    pub address: Literal,
    pub span: Span,
}

/// `DROP BACKEND '<host>:<port>' [FORCE]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropBackend {
    pub address: Literal,
    pub force: bool,
    pub span: Span,
}

/// `SHOW BACKENDS`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShowBackends {
    pub span: Span,
}

pub(crate) fn write_sql(statement: &BackendStatement, output: &mut String) {
    match statement {
        BackendStatement::AddBackend(statement) => {
            output.push_str("ADD BACKEND ");
            output.push_str(&print_literal(&statement.address));
        }
        BackendStatement::DropBackend(statement) => {
            output.push_str("DROP BACKEND ");
            output.push_str(&print_literal(&statement.address));
            if statement.force {
                output.push_str(" FORCE");
            }
        }
        BackendStatement::ShowBackends(_) => output.push_str("SHOW BACKENDS"),
    }
}

pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &BackendStatement) {
    match statement {
        BackendStatement::AddBackend(statement) => visitor.visit_literal(&statement.address),
        BackendStatement::DropBackend(statement) => visitor.visit_literal(&statement.address),
        BackendStatement::ShowBackends(statement) => visitor.visit_show_backends(statement),
    }
}

pub(crate) fn fold<F: Fold + ?Sized>(
    folder: &mut F,
    statement: BackendStatement,
) -> BackendStatement {
    match statement {
        BackendStatement::AddBackend(mut statement) => {
            statement.address = folder.fold_literal(statement.address);
            BackendStatement::AddBackend(statement)
        }
        BackendStatement::DropBackend(mut statement) => {
            statement.address = folder.fold_literal(statement.address);
            BackendStatement::DropBackend(statement)
        }
        BackendStatement::ShowBackends(statement) => {
            BackendStatement::ShowBackends(folder.fold_show_backends(statement))
        }
    }
}
