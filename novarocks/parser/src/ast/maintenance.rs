// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Procedure and table-maintenance syntax nodes.

use crate::Span;

use super::{Fold, Visit};

/// Maintenance-family statement carrier reserved for the SQLP-3 grammar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceStatement {
    pub span: Span,
}

impl MaintenanceStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

pub(crate) fn write_sql(_: &MaintenanceStatement, _: &mut String) {
    unreachable!("maintenance AST is not constructible before the SQLP-3 maintenance grammar task")
}

pub(crate) fn walk<V: Visit + ?Sized>(_: &mut V, _: &MaintenanceStatement) {}

pub(crate) fn fold<F: Fold + ?Sized>(
    _: &mut F,
    statement: MaintenanceStatement,
) -> MaintenanceStatement {
    statement
}
