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

//! Table DDL syntax nodes owned by SQLP-5.

use crate::Span;

use super::{Fold, Ident, Literal, ObjectName, TypeName, Visit};

/// Table DDL statements adopted by SQLP-5.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TableStatement {
    Create(CreateTable),
}

impl TableStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::Create(statement) => statement.span,
        }
    }
}

/// `CREATE [TEMPORARY | EXTERNAL] TABLE ...` before an optional CTAS payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTable {
    pub temporary: bool,
    pub external: bool,
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub like: Option<ObjectName>,
    pub columns: Vec<ColumnDefinition>,
    pub key: Option<TableKey>,
    pub distribution: Option<TableDistribution>,
    pub partition: Option<TablePartition>,
    pub properties: Vec<TableProperty>,
    pub comment: Option<Literal>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnDefinition {
    pub name: Ident,
    pub data_type: TypeName,
    pub nullable: Option<bool>,
    pub aggregation: Option<Ident>,
    pub default: Option<Literal>,
    pub comment: Option<Literal>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableKey {
    pub kind: TableKeyKind,
    pub columns: Vec<Ident>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableKeyKind {
    Duplicate,
    Unique,
    Aggregate,
    Primary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableDistribution {
    pub columns: Vec<Ident>,
    pub buckets: Option<u64>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TablePartition {
    Transform(TablePartitionTransform),
    LegacyRange(LegacyRangePartition),
}

impl TablePartition {
    pub const fn span(&self) -> Span {
        match self {
            Self::Transform(value) => value.span,
            Self::LegacyRange(value) => value.span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TablePartitionTransform {
    pub expressions: Vec<PartitionTransform>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PartitionTransform {
    Identity {
        column: Ident,
        span: Span,
    },
    Year {
        column: Ident,
        span: Span,
    },
    Month {
        column: Ident,
        span: Span,
    },
    Day {
        column: Ident,
        span: Span,
    },
    Hour {
        column: Ident,
        span: Span,
    },
    Bucket {
        buckets: u64,
        column: Ident,
        span: Span,
    },
    Truncate {
        width: u64,
        column: Ident,
        span: Span,
    },
    Void {
        column: Ident,
        span: Span,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyRangePartition {
    pub columns: Vec<Ident>,
    pub definitions: Vec<LegacyRangePartitionDefinition>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyRangePartitionDefinition {
    pub name: Ident,
    pub values: String,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableProperty {
    pub key: Literal,
    pub value: Literal,
    pub span: Span,
}

pub(crate) fn write_sql(statement: &TableStatement, output: &mut String) {
    match statement {
        TableStatement::Create(table) => {
            output.push_str("CREATE ");
            if table.temporary {
                output.push_str("TEMPORARY ");
            }
            if table.external {
                output.push_str("EXTERNAL ");
            }
            output.push_str("TABLE ");
            if table.if_not_exists {
                output.push_str("IF NOT EXISTS ");
            }
            output.push_str(&crate::printer::print_object_name(&table.name));
        }
    }
}

pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &TableStatement) {
    let TableStatement::Create(table) = statement;
    visitor.visit_object_name(&table.name);
}

pub(crate) fn fold<F: Fold + ?Sized>(folder: &mut F, statement: TableStatement) -> TableStatement {
    match statement {
        TableStatement::Create(mut table) => {
            table.name = folder.fold_object_name(table.name);
            TableStatement::Create(table)
        }
    }
}
