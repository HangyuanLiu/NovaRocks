// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Iceberg table DDL syntax nodes.

use crate::Span;

use super::{Fold, Visit};

/// Iceberg-family statement carrier reserved for the SQLP-3 Iceberg grammar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStatement {
    pub span: Span,
}

impl IcebergStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

pub(crate) fn write_sql(_: &IcebergStatement, _: &mut String) {
    unreachable!("Iceberg AST is not constructible before the SQLP-3 Iceberg grammar task")
}

pub(crate) fn walk<V: Visit + ?Sized>(_: &mut V, _: &IcebergStatement) {}

pub(crate) fn fold<F: Fold + ?Sized>(_: &mut F, statement: IcebergStatement) -> IcebergStatement {
    statement
}
