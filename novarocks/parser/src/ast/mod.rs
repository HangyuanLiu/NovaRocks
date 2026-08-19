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

//! Syntax-only SQL abstract syntax tree nodes.

pub(crate) mod backend;
pub(crate) mod catalog;
pub(crate) mod command;
mod expr;
pub(crate) mod iceberg;
pub(crate) mod maintenance;
pub(crate) mod materialized_view;
pub(crate) mod statistics;
pub(crate) mod view;
mod visit;

pub use backend::{AddBackend, BackendStatement, DropBackend, ShowBackends};
pub use catalog::{
    CatalogProperty, CatalogStatement, CreateCatalog, CreateDatabase, DropCatalog, DropDatabase,
    DropTable, ShowCreateTable, TruncateTable,
};
pub use command::{Property, PropertyKeyValue};
pub use expr::{
    BinaryExpr, BinaryOperator, Expr, FunctionCall, NestedExpr, UnaryExpr, UnaryOperator,
};
pub use iceberg::{
    AddFiles, AlterIcebergTable, ColumnPath, ColumnPosition, IcebergColumnAction,
    IcebergPartitionChange, IcebergPartitionField, IcebergPropertiesAction, IcebergReferenceAction,
    IcebergReferenceKind, IcebergSchemaChange, IcebergStatement, IcebergTableAction,
    RawReferenceOptions, ReferenceAnchor,
};
pub use maintenance::{
    CallStatement, ExpireSnapshots, ExpireSnapshotsOption, MaintenanceStatement, MaintenanceValue,
    OptimizeTable, ProcedureArgument, ProcedureArgumentMode, ProcedureMap, ProcedureMapEntry,
    RemoveOrphanFiles, RewriteManifests, ShowAlterTableOptimize, ShowOptimizeFilter,
    ShowOptimizeOrder, SortDirection,
};
pub use materialized_view::{
    AlterMaterializedView, CreateMaterializedView, DropMaterializedView,
    ExplainRefreshMaterializedView, MaterializedViewAlterAction, MaterializedViewDistribution,
    MaterializedViewExplainLevel, MaterializedViewPartitionArgument,
    MaterializedViewPartitionField, MaterializedViewProperty, MaterializedViewRefreshMode,
    MaterializedViewRefreshPolicy, MaterializedViewStatement, RefreshMaterializedView,
    ShowMaterializedViews,
};
pub use statistics::{
    AnalyzeMode, AnalyzeTable, CancelAnalyze, DropHistogram, DropMultipleColumnsStats, DropStats,
    ShowAnalyzeJobs, ShowBasicStatsMeta, ShowHistogramStatsMeta, ShowTableStats,
    StatisticsStatement,
};
pub use view::{CreateView, DropView, ShowCreateView, ShowViews, ViewStatement};
pub use visit::{
    Fold, Visit, fold_binary_expr, fold_expr, fold_function_call, fold_ident, fold_literal,
    fold_nested_expr, fold_object_name, fold_show_backends, fold_statement, fold_type_name,
    fold_unary_expr, fold_view_statement, walk_binary_expr, walk_expr, walk_function_call,
    walk_ident, walk_literal, walk_nested_expr, walk_object_name, walk_show_backends,
    walk_statement, walk_type_name, walk_unary_expr, walk_view_statement,
};

use crate::Span;

/// A top-level SQL statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Statement {
    Backend(BackendStatement),
    Statistics(StatisticsStatement),
    Catalog(CatalogStatement),
    Iceberg(IcebergStatement),
    Maintenance(MaintenanceStatement),
    MaterializedView(MaterializedViewStatement),
    View(ViewStatement),
    RawQuery(RawQuerySlice),
}

impl Statement {
    pub const fn span(&self) -> Span {
        match self {
            Self::Backend(statement) => statement.span(),
            Self::Statistics(statement) => statement.span(),
            Self::Catalog(statement) => statement.span(),
            Self::Iceberg(statement) => statement.span(),
            Self::Maintenance(statement) => statement.span(),
            Self::MaterializedView(statement) => statement.span(),
            Self::View(statement) => statement.span(),
            Self::RawQuery(query) => query.span,
        }
    }
}

/// An SQL identifier preserving its source spelling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Ident {
    pub value: String,
    pub quoted: bool,
    pub span: Span,
}

/// A qualified SQL object name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectName {
    pub parts: Vec<Ident>,
    pub span: Span,
}

/// A syntax-level type name, deliberately independent of execution data types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypeName {
    pub name: ObjectName,
    pub arguments: Vec<TypeNameArgument>,
    pub span: Span,
}

/// A type parameter retained without lowering it into an execution type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypeNameArgument {
    Type(TypeName),
    Literal(Literal),
    Field(StructField),
}

/// One named field in a `STRUCT<...>` type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructField {
    pub name: Ident,
    pub data_type: TypeName,
    pub span: Span,
}

/// A source literal and its syntactic category.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Literal {
    pub kind: LiteralKind,
    pub span: Span,
}

/// Literal categories initially required by the parser foundation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiteralKind {
    Null,
    Boolean(bool),
    Number(String),
    String(String),
    HexString(String),
}

/// An owned source slice used while embedded queries are not yet typed AST.
///
/// This transition node is retired by SQLP-6, which replaces it with a typed
/// `Query` node. Keeping the original text here lets the canonical printer
/// render it without receiving a separate source arena.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawQuerySlice {
    pub text: String,
    pub span: Span,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Fold, Visit};

    fn span(start: usize, end: usize) -> Span {
        Span::new(start, end)
    }

    #[test]
    fn every_seed_node_retains_its_source_span() {
        let name = ObjectName {
            parts: vec![Ident {
                value: "catalog".to_owned(),
                quoted: false,
                span: span(0, 7),
            }],
            span: span(0, 7),
        };
        let type_name = TypeName {
            name: name.clone(),
            arguments: Vec::new(),
            span: span(0, 7),
        };
        let literal = Literal {
            kind: LiteralKind::Number("42".to_owned()),
            span: span(8, 10),
        };
        let expression = Expr::Literal(literal.clone());
        let statement = Statement::Backend(BackendStatement::ShowBackends(ShowBackends {
            span: span(11, 24),
        }));
        let raw_query = RawQuerySlice {
            text: "SELECT 42".to_owned(),
            span: span(25, 34),
        };

        assert_eq!(name.span, span(0, 7));
        assert_eq!(type_name.span, span(0, 7));
        assert_eq!(literal.span, span(8, 10));
        assert_eq!(expression.span(), span(8, 10));
        assert_eq!(statement.span(), span(11, 24));
        assert_eq!(Statement::RawQuery(raw_query).span(), span(25, 34));
    }

    #[test]
    fn visitor_reaches_every_nested_node() {
        struct CountVisitor(usize);

        impl Visit for CountVisitor {
            fn visit_ident(&mut self, _: &Ident) {
                self.0 += 1;
            }

            fn visit_literal(&mut self, _: &Literal) {
                self.0 += 1;
            }
        }

        let expression = Expr::Binary(BinaryExpr {
            left: Box::new(Expr::Identifier(Ident {
                value: "a".to_owned(),
                quoted: false,
                span: span(0, 1),
            })),
            operator: BinaryOperator::Add,
            right: Box::new(Expr::FunctionCall(FunctionCall {
                name: Ident {
                    value: "abs".to_owned(),
                    quoted: false,
                    span: span(4, 7),
                },
                arguments: vec![Expr::Literal(Literal {
                    kind: LiteralKind::Number("1".to_owned()),
                    span: span(8, 9),
                })],
                span: span(4, 10),
            })),
            span: span(0, 10),
        });

        let mut visitor = CountVisitor(0);
        visitor.visit_expr(&expression);
        assert_eq!(visitor.0, 3);
    }

    #[test]
    fn fold_rebuilds_nested_expressions() {
        struct Rename;

        impl Fold for Rename {
            fn fold_ident(&mut self, mut ident: Ident) -> Ident {
                if ident.value == "old" {
                    ident.value = "new".to_owned();
                }
                ident
            }
        }

        let expression = Expr::Nested(NestedExpr {
            expression: Box::new(Expr::Identifier(Ident {
                value: "old".to_owned(),
                quoted: false,
                span: span(1, 4),
            })),
            span: span(0, 5),
        });

        let expression = Rename.fold_expr(expression);
        assert_eq!(
            expression,
            Expr::Nested(NestedExpr {
                expression: Box::new(Expr::Identifier(Ident {
                    value: "new".to_owned(),
                    quoted: false,
                    span: span(1, 4),
                })),
                span: span(0, 5),
            })
        );
    }
}
