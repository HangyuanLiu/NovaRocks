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

//! Row DML syntax nodes owned by SQLP-5.

use crate::Span;

use super::{
    Expr, Fold, Ident, ObjectName, Query, RawQuerySlice, TableAlias, TableStatement, Visit,
};

/// Row DML statements adopted by SQLP-5.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DmlStatement {
    CreateTableAsSelect(CreateTableAsSelect),
    Insert(Insert),
    Delete(Delete),
    Update(Update),
    Merge(Merge),
    AddEqualityDelete(AddEqualityDelete),
}

impl DmlStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::CreateTableAsSelect(statement) => statement.span,
            Self::Insert(statement) => statement.span,
            Self::Delete(statement) => statement.span,
            Self::Update(statement) => statement.span,
            Self::Merge(statement) => statement.span,
            Self::AddEqualityDelete(statement) => statement.span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableAsSelect {
    pub table: TableStatement,
    pub query: RawQuerySlice,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Insert {
    pub overwrite: bool,
    pub target: ObjectName,
    pub columns: Vec<Ident>,
    pub partitions: Option<InsertPartitions>,
    pub source: RawQuerySlice,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsertPartitions {
    pub entries: Vec<InsertPartitionEntry>,
    pub dynamic: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsertPartitionEntry {
    pub name: Ident,
    pub value: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delete {
    pub target: ObjectName,
    pub selection: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Update {
    pub target: ObjectName,
    pub alias: Option<TableAlias>,
    pub assignments: Vec<Assignment>,
    pub source: Option<MutationSource>,
    pub selection: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Assignment {
    pub target: ObjectName,
    pub value: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationSource {
    Table {
        name: ObjectName,
        alias: Option<TableAlias>,
        span: Span,
    },
    Query {
        query: Box<Query>,
        alias: Option<TableAlias>,
        span: Span,
    },
}

impl MutationSource {
    pub const fn span(&self) -> Span {
        match self {
            Self::Table { span, .. } | Self::Query { span, .. } => *span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Merge {
    pub target: ObjectName,
    pub target_alias: Option<TableAlias>,
    pub source: MutationSource,
    pub on: Expr,
    pub clauses: Vec<MergeClause>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeClause {
    Matched {
        predicate: Option<Expr>,
        action: MergeMatchedAction,
        span: Span,
    },
    NotMatched {
        predicate: Option<Expr>,
        action: MergeNotMatchedAction,
        span: Span,
    },
    NotMatchedBySource {
        predicate: Option<Expr>,
        action: MergeMatchedAction,
        span: Span,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeMatchedAction {
    Update {
        assignments: Vec<Assignment>,
        span: Span,
    },
    Delete {
        span: Span,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeNotMatchedAction {
    pub columns: Vec<Ident>,
    pub values: Vec<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddEqualityDelete {
    pub target: ObjectName,
    pub columns: Vec<Ident>,
    pub rows: Vec<Vec<Expr>>,
    pub span: Span,
}

pub(crate) fn write_sql(statement: &DmlStatement, output: &mut String) {
    let text = match statement {
        DmlStatement::CreateTableAsSelect(_) => "CREATE TABLE",
        DmlStatement::Insert(_) => "INSERT",
        DmlStatement::Delete(_) => "DELETE",
        DmlStatement::Update(_) => "UPDATE",
        DmlStatement::Merge(_) => "MERGE",
        DmlStatement::AddEqualityDelete(_) => "ALTER TABLE",
    };
    output.push_str(text);
}

pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &DmlStatement) {
    match statement {
        DmlStatement::CreateTableAsSelect(value) => visitor.visit_table_statement(&value.table),
        DmlStatement::Insert(value) => visitor.visit_object_name(&value.target),
        DmlStatement::Delete(value) => visitor.visit_object_name(&value.target),
        DmlStatement::Update(value) => visitor.visit_object_name(&value.target),
        DmlStatement::Merge(value) => visitor.visit_object_name(&value.target),
        DmlStatement::AddEqualityDelete(value) => visitor.visit_object_name(&value.target),
    }
}

pub(crate) fn fold<F: Fold + ?Sized>(folder: &mut F, statement: DmlStatement) -> DmlStatement {
    match statement {
        DmlStatement::CreateTableAsSelect(mut value) => {
            value.table = folder.fold_table_statement(value.table);
            DmlStatement::CreateTableAsSelect(value)
        }
        DmlStatement::Insert(mut value) => {
            value.target = folder.fold_object_name(value.target);
            DmlStatement::Insert(value)
        }
        DmlStatement::Delete(mut value) => {
            value.target = folder.fold_object_name(value.target);
            DmlStatement::Delete(value)
        }
        DmlStatement::Update(mut value) => {
            value.target = folder.fold_object_name(value.target);
            DmlStatement::Update(value)
        }
        DmlStatement::Merge(mut value) => {
            value.target = folder.fold_object_name(value.target);
            DmlStatement::Merge(value)
        }
        DmlStatement::AddEqualityDelete(mut value) => {
            value.target = folder.fold_object_name(value.target);
            DmlStatement::AddEqualityDelete(value)
        }
    }
}
