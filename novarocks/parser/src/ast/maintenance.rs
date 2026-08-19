// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Procedure and table-maintenance syntax nodes.

use crate::Span;

use super::{Fold, Ident, Literal, ObjectName, Visit};

/// A procedure or table-maintenance statement.
///
/// These nodes retain only SQL structure. Procedure support, argument
/// capability, catalog resolution, and job state remain domain concerns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceStatement {
    Call(CallStatement),
    Optimize(OptimizeTable),
    RewriteManifests(RewriteManifests),
    ExpireSnapshots(ExpireSnapshots),
    RemoveOrphanFiles(RemoveOrphanFiles),
    ShowOptimize(ShowAlterTableOptimize),
}

impl MaintenanceStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::Call(statement) => statement.span,
            Self::Optimize(statement) => statement.span,
            Self::RewriteManifests(statement) => statement.span,
            Self::ExpireSnapshots(statement) => statement.span,
            Self::RemoveOrphanFiles(statement) => statement.span,
            Self::ShowOptimize(statement) => statement.span,
        }
    }
}

/// `CALL <procedure>(...)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallStatement {
    pub procedure: ObjectName,
    pub arguments: Vec<ProcedureArgument>,
    pub argument_mode: ProcedureArgumentMode,
    pub span: Span,
}

/// The syntactic mode of a procedure argument list.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcedureArgumentMode {
    Empty,
    Positional,
    Named,
}

/// One positional or named procedure argument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureArgument {
    pub name: Option<Ident>,
    pub value: MaintenanceValue,
    pub span: Span,
}

/// A syntax-level value accepted by the maintenance command family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceValue {
    Literal(Literal),
    Timestamp { value: Literal, span: Span },
    Map(ProcedureMap),
}

impl MaintenanceValue {
    pub const fn span(&self) -> Span {
        match self {
            Self::Literal(value) => value.span,
            Self::Timestamp { span, .. } => *span,
            Self::Map(map) => map.span,
        }
    }
}

/// A `MAP(key, value, ...)` procedure value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureMap {
    pub entries: Vec<ProcedureMapEntry>,
    pub span: Span,
}

/// One key/value pair in a [`ProcedureMap`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureMapEntry {
    pub key: Literal,
    pub value: Literal,
    pub span: Span,
}

/// `ALTER TABLE <table> OPTIMIZE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeTable {
    pub table: ObjectName,
    pub span: Span,
}

/// `ALTER TABLE <table> REWRITE MANIFESTS`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RewriteManifests {
    pub table: ObjectName,
    pub span: Span,
}

/// `ALTER TABLE <table> EXPIRE SNAPSHOTS` with its syntax options.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpireSnapshots {
    pub table: ObjectName,
    pub options: Vec<ExpireSnapshotsOption>,
    pub span: Span,
}

/// A clause accepted after `EXPIRE SNAPSHOTS`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExpireSnapshotsOption {
    OlderThan { value: MaintenanceValue, span: Span },
    RetainLast { value: MaintenanceValue, span: Span },
}

impl ExpireSnapshotsOption {
    pub const fn span(&self) -> Span {
        match self {
            Self::OlderThan { span, .. } | Self::RetainLast { span, .. } => *span,
        }
    }
}

/// `ALTER TABLE <table> REMOVE ORPHAN FILES OLDER THAN <value>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveOrphanFiles {
    pub table: ObjectName,
    pub older_than: MaintenanceValue,
    pub span: Span,
}

/// `SHOW ALTER TABLE OPTIMIZE` and its optional presentation clauses.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowAlterTableOptimize {
    pub from: Option<ObjectName>,
    pub filter: Option<ShowOptimizeFilter>,
    pub order_by: Option<ShowOptimizeOrder>,
    pub limit: Option<Literal>,
    pub span: Span,
}

/// A simple equality filter in `SHOW ALTER TABLE OPTIMIZE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowOptimizeFilter {
    pub column: Ident,
    pub value: Literal,
    pub span: Span,
}

/// An `ORDER BY` item in `SHOW ALTER TABLE OPTIMIZE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowOptimizeOrder {
    pub column: Ident,
    pub direction: Option<SortDirection>,
    pub span: Span,
}

/// An explicit `ORDER BY` direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

pub(crate) fn write_sql(statement: &MaintenanceStatement, output: &mut String) {
    match statement {
        MaintenanceStatement::Call(statement) => write_call(statement, output),
        MaintenanceStatement::Optimize(statement) => {
            output.push_str("ALTER TABLE ");
            write_object_name(&statement.table, output);
            output.push_str(" OPTIMIZE");
        }
        MaintenanceStatement::RewriteManifests(statement) => {
            output.push_str("ALTER TABLE ");
            write_object_name(&statement.table, output);
            output.push_str(" REWRITE MANIFESTS");
        }
        MaintenanceStatement::ExpireSnapshots(statement) => {
            output.push_str("ALTER TABLE ");
            write_object_name(&statement.table, output);
            output.push_str(" EXPIRE SNAPSHOTS");
            for option in &statement.options {
                output.push(' ');
                match option {
                    ExpireSnapshotsOption::OlderThan { value, .. } => {
                        output.push_str("OLDER THAN ");
                        write_value(value, output);
                    }
                    ExpireSnapshotsOption::RetainLast { value, .. } => {
                        output.push_str("RETAIN LAST ");
                        write_value(value, output);
                    }
                }
            }
        }
        MaintenanceStatement::RemoveOrphanFiles(statement) => {
            output.push_str("ALTER TABLE ");
            write_object_name(&statement.table, output);
            output.push_str(" REMOVE ORPHAN FILES OLDER THAN ");
            write_value(&statement.older_than, output);
        }
        MaintenanceStatement::ShowOptimize(statement) => write_show_optimize(statement, output),
    }
}

pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &MaintenanceStatement) {
    match statement {
        MaintenanceStatement::Call(statement) => {
            visitor.visit_object_name(&statement.procedure);
            for argument in &statement.arguments {
                if let Some(name) = &argument.name {
                    visitor.visit_ident(name);
                }
                walk_value(visitor, &argument.value);
            }
        }
        MaintenanceStatement::Optimize(statement) => visitor.visit_object_name(&statement.table),
        MaintenanceStatement::RewriteManifests(statement) => {
            visitor.visit_object_name(&statement.table)
        }
        MaintenanceStatement::ExpireSnapshots(statement) => {
            visitor.visit_object_name(&statement.table);
            for option in &statement.options {
                match option {
                    ExpireSnapshotsOption::OlderThan { value, .. }
                    | ExpireSnapshotsOption::RetainLast { value, .. } => walk_value(visitor, value),
                }
            }
        }
        MaintenanceStatement::RemoveOrphanFiles(statement) => {
            visitor.visit_object_name(&statement.table);
            walk_value(visitor, &statement.older_than);
        }
        MaintenanceStatement::ShowOptimize(statement) => {
            if let Some(from) = &statement.from {
                visitor.visit_object_name(from);
            }
            if let Some(filter) = &statement.filter {
                visitor.visit_ident(&filter.column);
                visitor.visit_literal(&filter.value);
            }
            if let Some(order_by) = &statement.order_by {
                visitor.visit_ident(&order_by.column);
            }
            if let Some(limit) = &statement.limit {
                visitor.visit_literal(limit);
            }
        }
    }
}

pub(crate) fn fold<F: Fold + ?Sized>(
    folder: &mut F,
    statement: MaintenanceStatement,
) -> MaintenanceStatement {
    match statement {
        MaintenanceStatement::Call(statement) => MaintenanceStatement::Call(CallStatement {
            procedure: folder.fold_object_name(statement.procedure),
            arguments: statement
                .arguments
                .into_iter()
                .map(|argument| ProcedureArgument {
                    name: argument.name.map(|name| folder.fold_ident(name)),
                    value: fold_value(folder, argument.value),
                    span: argument.span,
                })
                .collect(),
            argument_mode: statement.argument_mode,
            span: statement.span,
        }),
        MaintenanceStatement::Optimize(statement) => {
            MaintenanceStatement::Optimize(OptimizeTable {
                table: folder.fold_object_name(statement.table),
                span: statement.span,
            })
        }
        MaintenanceStatement::RewriteManifests(statement) => {
            MaintenanceStatement::RewriteManifests(RewriteManifests {
                table: folder.fold_object_name(statement.table),
                span: statement.span,
            })
        }
        MaintenanceStatement::ExpireSnapshots(statement) => {
            MaintenanceStatement::ExpireSnapshots(ExpireSnapshots {
                table: folder.fold_object_name(statement.table),
                options: statement
                    .options
                    .into_iter()
                    .map(|option| match option {
                        ExpireSnapshotsOption::OlderThan { value, span } => {
                            ExpireSnapshotsOption::OlderThan {
                                value: fold_value(folder, value),
                                span,
                            }
                        }
                        ExpireSnapshotsOption::RetainLast { value, span } => {
                            ExpireSnapshotsOption::RetainLast {
                                value: fold_value(folder, value),
                                span,
                            }
                        }
                    })
                    .collect(),
                span: statement.span,
            })
        }
        MaintenanceStatement::RemoveOrphanFiles(statement) => {
            MaintenanceStatement::RemoveOrphanFiles(RemoveOrphanFiles {
                table: folder.fold_object_name(statement.table),
                older_than: fold_value(folder, statement.older_than),
                span: statement.span,
            })
        }
        MaintenanceStatement::ShowOptimize(statement) => {
            MaintenanceStatement::ShowOptimize(ShowAlterTableOptimize {
                from: statement.from.map(|name| folder.fold_object_name(name)),
                filter: statement.filter.map(|filter| ShowOptimizeFilter {
                    column: folder.fold_ident(filter.column),
                    value: folder.fold_literal(filter.value),
                    span: filter.span,
                }),
                order_by: statement.order_by.map(|order_by| ShowOptimizeOrder {
                    column: folder.fold_ident(order_by.column),
                    direction: order_by.direction,
                    span: order_by.span,
                }),
                limit: statement.limit.map(|limit| folder.fold_literal(limit)),
                span: statement.span,
            })
        }
    }
}

fn write_call(statement: &CallStatement, output: &mut String) {
    output.push_str("CALL ");
    write_object_name(&statement.procedure, output);
    output.push('(');
    for (index, argument) in statement.arguments.iter().enumerate() {
        if index != 0 {
            output.push_str(", ");
        }
        if let Some(name) = &argument.name {
            write_ident(name, output);
            output.push_str(" => ");
        }
        write_value(&argument.value, output);
    }
    output.push(')');
}

fn write_show_optimize(statement: &ShowAlterTableOptimize, output: &mut String) {
    output.push_str("SHOW ALTER TABLE OPTIMIZE");
    if let Some(from) = &statement.from {
        output.push_str(" FROM ");
        write_object_name(from, output);
    }
    if let Some(filter) = &statement.filter {
        output.push_str(" WHERE ");
        write_ident(&filter.column, output);
        output.push_str(" = ");
        write_literal(&filter.value, output);
    }
    if let Some(order_by) = &statement.order_by {
        output.push_str(" ORDER BY ");
        write_ident(&order_by.column, output);
        if let Some(direction) = order_by.direction {
            output.push(' ');
            output.push_str(match direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            });
        }
    }
    if let Some(limit) = &statement.limit {
        output.push_str(" LIMIT ");
        write_literal(limit, output);
    }
}

fn write_value(value: &MaintenanceValue, output: &mut String) {
    match value {
        MaintenanceValue::Literal(value) => write_literal(value, output),
        MaintenanceValue::Timestamp { value, .. } => {
            output.push_str("TIMESTAMP ");
            write_literal(value, output);
        }
        MaintenanceValue::Map(map) => {
            output.push_str("MAP(");
            for (index, entry) in map.entries.iter().enumerate() {
                if index != 0 {
                    output.push_str(", ");
                }
                write_literal(&entry.key, output);
                output.push_str(", ");
                write_literal(&entry.value, output);
            }
            output.push(')');
        }
    }
}

fn write_object_name(name: &ObjectName, output: &mut String) {
    for (index, part) in name.parts.iter().enumerate() {
        if index != 0 {
            output.push('.');
        }
        write_ident(part, output);
    }
}

fn write_ident(ident: &Ident, output: &mut String) {
    if ident.quoted {
        output.push('`');
        output.push_str(&ident.value.replace('`', "``"));
        output.push('`');
    } else {
        output.push_str(&ident.value);
    }
}

fn write_literal(literal: &Literal, output: &mut String) {
    output.push_str(&crate::printer::print_literal(literal));
}

fn walk_value<V: Visit + ?Sized>(visitor: &mut V, value: &MaintenanceValue) {
    match value {
        MaintenanceValue::Literal(value) | MaintenanceValue::Timestamp { value, .. } => {
            visitor.visit_literal(value)
        }
        MaintenanceValue::Map(map) => {
            for entry in &map.entries {
                visitor.visit_literal(&entry.key);
                visitor.visit_literal(&entry.value);
            }
        }
    }
}

fn fold_value<F: Fold + ?Sized>(folder: &mut F, value: MaintenanceValue) -> MaintenanceValue {
    match value {
        MaintenanceValue::Literal(value) => MaintenanceValue::Literal(folder.fold_literal(value)),
        MaintenanceValue::Timestamp { value, span } => MaintenanceValue::Timestamp {
            value: folder.fold_literal(value),
            span,
        },
        MaintenanceValue::Map(map) => MaintenanceValue::Map(ProcedureMap {
            entries: map
                .entries
                .into_iter()
                .map(|entry| ProcedureMapEntry {
                    key: folder.fold_literal(entry.key),
                    value: folder.fold_literal(entry.value),
                    span: entry.span,
                })
                .collect(),
            span: map.span,
        }),
    }
}
