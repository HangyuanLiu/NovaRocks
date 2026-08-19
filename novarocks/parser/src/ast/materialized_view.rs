// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Materialized-view syntax nodes.

use crate::Span;

use super::{Fold, Visit};

/// Materialized-view statement carrier reserved for the SQLP-3 grammar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedViewStatement {
    pub span: Span,
}

impl MaterializedViewStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

pub(crate) fn write_sql(_: &MaterializedViewStatement, _: &mut String) {
    unreachable!("materialized-view AST is not constructible before the SQLP-3 MV grammar task")
}

pub(crate) fn walk<V: Visit + ?Sized>(_: &mut V, _: &MaterializedViewStatement) {}

pub(crate) fn fold<F: Fold + ?Sized>(
    _: &mut F,
    statement: MaterializedViewStatement,
) -> MaterializedViewStatement {
    statement
}
