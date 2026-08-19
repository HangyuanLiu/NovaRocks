// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Statistics-command syntax nodes.

use crate::Span;

use super::{Fold, Visit};

/// Statistics-family statement carrier reserved for the SQLP-3 grammar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsStatement {
    pub span: Span,
}

impl StatisticsStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

pub(crate) fn write_sql(_: &StatisticsStatement, _: &mut String) {
    unreachable!("statistics AST is not constructible before the SQLP-3 statistics grammar task")
}

pub(crate) fn walk<V: Visit + ?Sized>(_: &mut V, _: &StatisticsStatement) {}

pub(crate) fn fold<F: Fold + ?Sized>(
    _: &mut F,
    statement: StatisticsStatement,
) -> StatisticsStatement {
    statement
}
