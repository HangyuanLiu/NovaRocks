// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Catalog and truncate syntax nodes.

use crate::Span;

use super::{Fold, Visit};

/// Catalog-family statement carrier reserved for the SQLP-3 catalog grammar.
///
/// No production parser constructs this seam-only node. T3 replaces it with
/// complete typed catalog and truncate variants without touching the shared
/// top-level statement dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogStatement {
    pub span: Span,
}

impl CatalogStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

pub(crate) fn write_sql(_: &CatalogStatement, _: &mut String) {
    unreachable!("catalog AST is not constructible before the SQLP-3 catalog grammar task")
}

pub(crate) fn walk<V: Visit + ?Sized>(_: &mut V, _: &CatalogStatement) {}

pub(crate) fn fold<F: Fold + ?Sized>(_: &mut F, statement: CatalogStatement) -> CatalogStatement {
    statement
}
