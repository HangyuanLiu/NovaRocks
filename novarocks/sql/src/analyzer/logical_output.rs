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

//! SQL semantic output provenance for physically ambiguous scalar values.

use arrow::datatypes::{DataType, Field};
use novarocks_parser::{Span, ast};
use novarocks_types::logical::{LogicalType, field_with_logical_type};
use novarocks_types::schema::SqlType;

use super::{AnalyzerContext, scope::AnalyzerScope};
use crate::analysis::{ExprKind, TypedExpr};
use crate::analyze_error::AnalyzeError;

#[path = "logical_container.rs"]
mod container;

impl AnalyzerContext<'_> {
    pub(super) fn logical_output_type(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> Option<SqlType> {
        // The existing independent source witness also covers explicitly typed
        // empty/all-null JSON literals and catalog JSON-list columns. A physical
        // List<Utf8> or an output CAST alone does not establish this witness.
        if self.json_list_provenance(source, expression, scope)
            && matches!(&expression.data_type, DataType::List(item)
                if item.data_type() == &DataType::Utf8)
        {
            return Some(SqlType::Array(Box::new(SqlType::Json)));
        }
        // A public JSON CAST does not prove arbitrary string contents. Only
        // its already-proven Json operand preserves the existing domain. All
        // other explicit CASTs retain their current owner semantics, including
        // an explicit string CAST clearing ambiguous logical provenance.
        if let Some(ast::Expr::Cast(cast)) = source {
            let json_target = cast.data_type.name.parts.last().is_some_and(|part| {
                matches!(part.value.to_ascii_lowercase().as_str(), "json" | "jsonb")
            });
            return match (&expression.kind, json_target) {
                (ExprKind::Cast { expr: inner, .. }, true) => self
                    .logical_output_type(Some(&cast.expr), inner, scope)
                    .filter(|domain| *domain == SqlType::Json),
                _ => None,
            };
        }
        // Materialized implicit coercions preserve a domain only while both
        // carriers remain compatible with that already-established domain.
        if let ExprKind::Cast { expr: inner, .. } = &expression.kind {
            let domain = self.logical_output_type(source, inner, scope)?;
            return (logical_carrier_matches(&domain, &inner.data_type)
                && logical_carrier_matches(&domain, &expression.data_type))
            .then_some(domain);
        }
        let source_binding = match source {
            Some(ast::Expr::Identifier(ident)) => scope.resolve(None, &ident.value).ok(),
            Some(ast::Expr::CompoundIdentifier(parts)) if parts.parts.len() >= 2 => {
                let n = parts.parts.len();
                scope
                    .resolve(Some(&parts.parts[n - 2].value), &parts.parts[n - 1].value)
                    .ok()
            }
            _ => None,
        };
        if let Some((column_id, _, _)) = source_binding {
            return self
                .factory
                .borrow()
                .logical_type(column_id)
                .filter(|domain| logical_carrier_matches(domain, &expression.data_type));
        }
        let domain = match &expression.kind {
            ExprKind::ColumnRef { .. } => scope.logical_type_of_expr(expression),
            ExprKind::FunctionCall { binding, args, .. } => {
                if let Some(domain) = crate::functions::scalar_output_logical_type(binding) {
                    Some(domain)
                } else if binding.kind == novarocks_functions::FunctionKind::Scalar {
                    let source_args = match source {
                        Some(ast::Expr::FunctionCall(function)) => {
                            Some(function.arguments.as_slice())
                        }
                        _ => None,
                    };
                    let source_arg = |index: usize| source_args.and_then(|args| args.get(index));
                    match binding.function_id.as_str() {
                        "builtin.scalar/coalesce/v1" => self.merge_logical_values(
                            args.iter().enumerate().map(|(i, arg)| (source_arg(i), arg)),
                            scope,
                        ),
                        "builtin.scalar/ifnull/v1" | "builtin.scalar/nvl/v1" if args.len() == 2 => {
                            self.merge_logical_values(
                                args.iter().enumerate().map(|(i, arg)| (source_arg(i), arg)),
                                scope,
                            )
                        }
                        "builtin.scalar/if/v1" if args.len() == 3 => self.merge_logical_values(
                            args.iter()
                                .enumerate()
                                .skip(1)
                                .map(|(i, arg)| (source_arg(i), arg)),
                            scope,
                        ),
                        // The comparator can affect equality but never supplies
                        // the returned value. Its own logical domain is irrelevant.
                        "builtin.scalar/nullif/v1" if args.len() == 2 => {
                            self.logical_output_type(source_arg(0), &args[0], scope)
                        }
                        _ => self.container_output_type(source, expression, scope),
                    }
                } else {
                    None
                }
            }
            ExprKind::AggregateCall { resolved, args, .. } => {
                crate::functions::aggregate_output_logical_type(resolved).or_else(|| {
                    if resolved.kind == novarocks_functions::FunctionKind::Aggregate
                        && resolved.function_id.as_str() == "builtin.aggregate/any_value/v1"
                        && args.len() == 1
                    {
                        self.logical_output_type(
                            container::argument_source(source, 0),
                            &args[0],
                            scope,
                        )
                    } else {
                        self.container_output_type(source, expression, scope)
                    }
                })
            }
            ExprKind::WindowCall {
                aggregate_binding: Some(binding),
                args,
                ..
            } => crate::functions::aggregate_output_logical_type(binding).or_else(|| {
                if binding.kind == novarocks_functions::FunctionKind::Aggregate
                    && binding.function_id.as_str() == "builtin.aggregate/any_value/v1"
                    && args.len() == 1
                {
                    self.logical_output_type(container::argument_source(source, 0), &args[0], scope)
                } else {
                    self.container_output_type(source, expression, scope)
                }
            }),
            ExprKind::Case {
                when_then,
                else_expr,
                ..
            } => {
                let case = match source {
                    Some(ast::Expr::Case(case)) => Some(case),
                    _ => None,
                };
                self.merge_logical_values(
                    when_then
                        .iter()
                        .enumerate()
                        .map(|(i, (_, value))| (case.and_then(|case| case.results.get(i)), value))
                        .chain(else_expr.as_deref().map(|value| {
                            (case.and_then(|case| case.else_result.as_deref()), value)
                        })),
                    scope,
                )
            }
            ExprKind::Nested(inner) => self.logical_output_type(
                source.and_then(|source| match source {
                    ast::Expr::Nested(nested) => Some(nested.expression.as_ref()),
                    _ => None,
                }),
                inner,
                scope,
            ),
            _ => None,
        };
        domain.filter(|domain| logical_carrier_matches(domain, &expression.data_type))
    }

    fn merge_logical_values<'a>(
        &self,
        values: impl IntoIterator<Item = (Option<&'a ast::Expr>, &'a TypedExpr)>,
        scope: &AnalyzerScope,
    ) -> Option<SqlType> {
        let mut domain = None;
        for (source, value) in values {
            if is_null_value(source, value) {
                continue;
            }
            let current = self.logical_output_type(source, value, scope)?;
            match &domain {
                Some(previous) if *previous != current => return None,
                None => domain = Some(current),
                _ => {}
            }
        }
        domain
    }

    /// Value provenance is separate from a public CAST target's type spelling.
    /// Only catalog JSON, exact JSON producers, and their preserving operators
    /// establish this fact. Derived/CTE columns carry it by ColumnId.
    pub(super) fn json_list_provenance(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> bool {
        if let Some(ast::Expr::Cast(cast)) = source {
            let Some(ast::TypeNameArgument::Type(item)) = cast.data_type.arguments.first() else {
                return false;
            };
            let array_json = cast
                .data_type
                .name
                .parts
                .last()
                .is_some_and(|part| part.value.eq_ignore_ascii_case("array"))
                && item.name.parts.last().is_some_and(|part| {
                    matches!(part.value.to_ascii_lowercase().as_str(), "json" | "jsonb")
                });
            return array_json
                && matches!(&expression.kind,
                ExprKind::Cast { expr: inner, .. } if self.json_list_provenance(Some(&cast.expr), inner, scope));
        }
        let source_id = match source {
            Some(ast::Expr::Identifier(ident)) => scope.resolve(None, &ident.value).ok(),
            Some(ast::Expr::CompoundIdentifier(parts)) if parts.parts.len() >= 2 => {
                let n = parts.parts.len();
                scope
                    .resolve(Some(&parts.parts[n - 2].value), &parts.parts[n - 1].value)
                    .ok()
            }
            _ => None,
        };
        if let Some((column_id, _, _)) = source_id {
            return self.factory.borrow().has_json_list_provenance(column_id);
        }
        match &expression.kind {
            // Internal output adapters and materialized binding coercions do
            // not establish proof. The original source must establish it.
            ExprKind::Cast { expr: inner, .. } => self.json_list_provenance(source, inner, scope),
            ExprKind::ColumnRef { column_id, .. } => {
                self.factory.borrow().has_json_list_provenance(*column_id)
            }
            ExprKind::Nested(inner) => self.json_list_provenance(
                source.and_then(|source| match source {
                    ast::Expr::Nested(nested) => Some(nested.expression.as_ref()),
                    _ => None,
                }),
                inner,
                scope,
            ),
            ExprKind::FunctionCall { binding, args, .. }
                if binding.function_id.as_str() == "builtin.scalar/__array_literal/v1" =>
            {
                let Some(ast::Expr::Array(array)) = source else {
                    return false;
                };
                self.json_array_elements_provenance(array, args, scope)
            }
            ExprKind::FunctionCall { binding, args, .. }
                if binding.function_id.as_str() == "builtin.scalar/array_sortby/v1" =>
            {
                let Some(ast::Expr::FunctionCall(function)) = source else {
                    return false;
                };
                function
                    .arguments
                    .first()
                    .zip(args.first())
                    .is_some_and(|(source, value)| {
                        self.json_list_provenance(Some(source), value, scope)
                    })
            }
            ExprKind::AggregateCall { resolved, args, .. }
                if resolved.function_id.as_str() == "builtin.aggregate/array_agg/v1" =>
            {
                let Some(ast::Expr::FunctionCall(function)) = source else {
                    return false;
                };
                function
                    .arguments
                    .first()
                    .zip(args.first())
                    .is_some_and(|(source, value)| {
                        self.logical_output_type(Some(source), value, scope) == Some(SqlType::Json)
                    })
            }
            ExprKind::WindowCall {
                aggregate_binding: Some(binding),
                args,
                ..
            } if binding.function_id.as_str() == "builtin.aggregate/array_agg/v1" => {
                let Some(ast::Expr::FunctionCall(function)) = source else {
                    return false;
                };
                function
                    .arguments
                    .first()
                    .zip(args.first())
                    .is_some_and(|(source, value)| {
                        self.logical_output_type(Some(source), value, scope) == Some(SqlType::Json)
                    })
            }
            ExprKind::Case {
                when_then,
                else_expr,
                ..
            } => {
                let Some(ast::Expr::Case(case)) = source else {
                    return false;
                };
                let mut saw_json = false;
                for (source, value) in case
                    .results
                    .iter()
                    .zip(when_then.iter().map(|(_, value)| value))
                    .chain(case.else_result.as_deref().zip(else_expr.as_deref()))
                {
                    if matches!(
                        source,
                        ast::Expr::Literal(ast::Literal {
                            kind: ast::LiteralKind::Null,
                            ..
                        })
                    ) {
                        continue;
                    }
                    if !self.json_list_provenance(Some(source), value, scope) {
                        return false;
                    }
                    saw_json = true;
                }
                saw_json
            }
            _ => false,
        }
    }

    pub(super) fn json_array_elements_provenance(
        &self,
        array: &ast::ArrayExpr,
        args: &[TypedExpr],
        scope: &AnalyzerScope,
    ) -> bool {
        if array.element_type.as_ref().is_some_and(|target| {
            !target.name.parts.last().is_some_and(|part| {
                matches!(part.value.to_ascii_lowercase().as_str(), "json" | "jsonb")
            })
        }) {
            return false;
        }
        if array.elements.len() != args.len() {
            return false;
        }
        let mut saw_json = false;
        let all_json = array.elements.iter().zip(args).all(|(source, value)| {
            if matches!(
                source,
                ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::Null,
                    ..
                })
            ) {
                return true;
            }
            let json = self.logical_output_type(Some(source), value, scope) == Some(SqlType::Json);
            saw_json |= json;
            json
        });
        all_json && (saw_json || array.element_type.is_some())
    }

    /// Keep scalar/aggregate bindings exact. The ordinary output CAST adapts
    /// the List item's semantic field metadata. An explicitly typed empty
    /// JSON literal also materializes its Utf8 item carrier without any values.
    pub(super) fn adapt_json_list_output(
        &self,
        expression: TypedExpr,
        json_input: bool,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        if !json_input {
            return Ok(expression);
        }
        let binding = match &expression.kind {
            ExprKind::AggregateCall { resolved, .. } => Some(resolved),
            ExprKind::FunctionCall { binding, .. } => Some(binding),
            ExprKind::WindowCall {
                aggregate_binding, ..
            } => aggregate_binding.as_ref(),
            _ => None,
        };
        if !binding.is_some_and(|binding| {
            matches!(
                binding.function_id.as_str(),
                "builtin.aggregate/array_agg/v1"
                    | "builtin.scalar/__array_literal/v1"
                    | "builtin.scalar/array_sortby/v1"
            )
        }) {
            return Ok(expression);
        }
        let DataType::List(item) = &expression.data_type else {
            return Err(AnalyzeError::type_mismatch(
                "JSON list producer binding must declare a List result",
                span,
            ));
        };
        let empty_json_literal = matches!(&expression.kind,
            ExprKind::FunctionCall { binding, args, .. }
                if binding.function_id.as_str() == "builtin.scalar/__array_literal/v1" && args.is_empty());
        if item.data_type() != &DataType::Utf8
            && !(empty_json_literal && item.data_type() == &DataType::Null)
        {
            return Err(AnalyzeError::type_mismatch(
                "JSON list elements must use their declared Utf8 physical carrier",
                span,
            ));
        }
        let target = DataType::List(std::sync::Arc::new(field_with_logical_type(
            Field::new(item.name(), DataType::Utf8, item.is_nullable()),
            LogicalType::Json,
        )));
        let nullable = expression.nullable;
        Ok(TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(expression),
                target: target.clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            data_type: target,
            nullable,
        })
    }
}

// Compatibility checks consume a supplied logical authority; they never infer
// one from the physical carrier. Offset-width adaptations retain the same
// String/Binary/Json domain, while conversions to another family clear it.
fn logical_carrier_matches(domain: &SqlType, carrier: &DataType) -> bool {
    use arrow::datatypes::TimeUnit;
    match (domain, carrier) {
        (SqlType::String | SqlType::Json, DataType::Utf8 | DataType::LargeUtf8)
        | (
            SqlType::Binary
            | SqlType::Hll
            | SqlType::Bitmap
            | SqlType::Object
            | SqlType::Percentile,
            DataType::Binary | DataType::LargeBinary,
        )
        | (SqlType::Variant, DataType::LargeBinary)
        | (SqlType::TinyInt, DataType::Int8)
        | (SqlType::SmallInt, DataType::Int16)
        | (SqlType::Int, DataType::Int32)
        | (SqlType::BigInt, DataType::Int64)
        | (SqlType::LargeInt, DataType::FixedSizeBinary(16))
        | (SqlType::Float, DataType::Float32)
        | (SqlType::Double, DataType::Float64)
        | (SqlType::Boolean, DataType::Boolean)
        | (SqlType::Date, DataType::Date32)
        | (SqlType::Time, DataType::Time64(TimeUnit::Microsecond))
        | (SqlType::DateTime, DataType::Timestamp(TimeUnit::Microsecond, _))
        | (SqlType::DateTimeNs, DataType::Timestamp(TimeUnit::Nanosecond, _)) => true,
        (
            SqlType::Decimal { precision, scale },
            DataType::Decimal128(p, s) | DataType::Decimal256(p, s),
        ) => precision == p && scale == s,
        (
            SqlType::Array(item),
            DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _),
        ) => logical_carrier_matches(item, field.data_type()),
        (SqlType::Map(key, value), DataType::Map(entries, _)) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                logical_carrier_matches(key, fields[0].data_type())
                    && logical_carrier_matches(value, fields[1].data_type())
            }
            _ => false,
        },
        (SqlType::Struct(expected), DataType::Struct(actual)) => {
            expected.len() == actual.len()
                && expected.iter().zip(actual).all(|((name, domain), field)| {
                    name == field.name() && logical_carrier_matches(domain, field.data_type())
                })
        }
        _ => false,
    }
}
fn is_null_value(source: Option<&ast::Expr>, value: &TypedExpr) -> bool {
    if value.data_type == DataType::Null {
        return true;
    }
    match &value.kind {
        ExprKind::Literal(crate::analysis::LiteralValue::Null) => return true,
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) if is_null_value(None, expr) => {
            return true;
        }
        _ => {}
    }
    matches!(
        source,
        Some(ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        }))
    )
}

#[cfg(test)]
mod scalar_domain_tests {
    use super::*;
    use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
    use crate::column_id::ColumnRefFactory;
    use crate::planner::table::{SqlScanKind, TableDef};
    use novarocks_types::schema::ColumnDef;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::Arc;

    struct Catalog;
    fn declared(name: &str, data_type: DataType, logical_type: SqlType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            data_type,
            nullable: true,
            write_default: None,
            logical_type: Some(logical_type),
        }
    }
    impl PlannerTableProvider for Catalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<ResolvedAnalyzerTable, String> {
            let json_item = field_with_logical_type(
                Field::new("item", DataType::Utf8, true),
                LogicalType::Json,
            );
            let planner = TableDef {
                name: table.into(),
                columns: vec![
                    declared("j", DataType::Utf8, SqlType::Json),
                    declared("h", DataType::Binary, SqlType::Hll),
                    declared("b", DataType::Binary, SqlType::Bitmap),
                    declared("s", DataType::Utf8, SqlType::String),
                    declared("v", DataType::LargeBinary, SqlType::Variant),
                    declared("o", DataType::Binary, SqlType::Object),
                    declared("p", DataType::Binary, SqlType::Percentile),
                    declared(
                        "a",
                        DataType::List(Arc::new(json_item)),
                        SqlType::Array(Box::new(SqlType::Json)),
                    ),
                    declared(
                        "m",
                        DataType::Map(
                            Arc::new(Field::new(
                                "entries",
                                DataType::Struct(
                                    vec![
                                        Field::new("key", DataType::Utf8, true),
                                        Field::new("value", DataType::Binary, true),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        ),
                        SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::Hll)),
                    ),
                    declared(
                        "r",
                        DataType::Struct(
                            vec![
                                Field::new("json", DataType::Utf8, true),
                                Field::new("bitmap", DataType::Binary, true),
                            ]
                            .into(),
                        ),
                        SqlType::Struct(vec![
                            ("json".into(), SqlType::Json),
                            ("bitmap".into(), SqlType::Bitmap),
                        ]),
                    ),
                ],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source_for(
                    "ice",
                    database,
                    table,
                    SqlScanKind::ConnectorRead,
                ),
            };
            Ok(ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }
    }
    fn query(sql: &str) -> ast::Query {
        let mut statements = novarocks_parser::parse(sql).unwrap();
        let [ast::Statement::Query(query)] = statements.as_mut_slice() else {
            panic!("expected query")
        };
        query.clone()
    }
    fn output_domains(sql: &str) -> Vec<Option<SqlType>> {
        let (query, _, factory) = super::super::analyze(&query(sql), &Catalog, "db").unwrap();
        query
            .output_columns
            .iter()
            .map(|column| factory.logical_type(column.column_id))
            .collect()
    }
    fn context(factory: Rc<RefCell<ColumnRefFactory>>) -> AnalyzerContext<'static> {
        AnalyzerContext {
            catalog: &Catalog,
            current_database: "db",
            function_catalog: crate::functions::builtin_sql_function_catalog(),
            sql_semantics: Default::default(),
            factory,
            ctes: Default::default(),
            pending_ctes: Default::default(),
            next_subquery_id: Cell::new(0),
            next_lambda_slot_id: Cell::new(0),
            collected_subqueries: RefCell::new(Vec::new()),
            cte_registry: RefCell::new(Default::default()),
        }
    }

    #[test]
    fn m07_json_list_explicit_literal_target_keeps_its_source_decision() {
        assert_eq!(
            output_domains("select array<json>[],array<json>[null]"),
            vec![Some(SqlType::Array(Box::new(SqlType::Json))); 2],
        );
        assert_eq!(
            output_domains(
                "with q as (select array<json>[] as j) select array_sortby(j,[1]) from q"
            ),
            vec![Some(SqlType::Array(Box::new(SqlType::Json)))],
        );
        assert_eq!(
            output_domains("select array<varchar>[json_object('k',1)],array<json>['plain']"),
            vec![Some(SqlType::Array(Box::new(SqlType::String))); 2],
        );
        assert_eq!(
            output_domains("select cast(['plain'] as array<json>)"),
            vec![None],
        );
    }

    #[test]
    fn m07_json_list_missing_marker_requires_its_independent_source_witness() {
        for (marker, witness, accepted) in [
            (None, false, false),
            (None, true, true),
            (Some("json"), false, true),
            (Some("json"), true, true),
            (Some("unknown"), true, false),
            (Some("hll"), true, false),
        ] {
            let item = Field::new("item", DataType::Utf8, true);
            let item = match marker {
                Some(value) => item.with_metadata(
                    [(
                        novarocks_types::logical::NR_LOGICAL_TYPE_KEY.to_owned(),
                        value.to_owned(),
                    )]
                    .into(),
                ),
                None => item,
            };
            let carrier = DataType::List(Arc::new(item));
            let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
            let id = factory
                .borrow_mut()
                .create(None, "source".into(), carrier.clone(), true);
            factory
                .borrow_mut()
                .set_logical_type(id, Some(SqlType::Array(Box::new(SqlType::Json))));
            factory.borrow_mut().set_json_list_provenance(id, witness);
            let scope = AnalyzerScope::new(factory.clone());
            let context = context(factory);
            let source = TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: id,
                    qualifier: None,
                    column: "source".into(),
                },
                data_type: carrier,
                nullable: true,
            };
            if witness && !accepted {
                // A binding coercion must not hide the original conflicting
                // item marker behind an ordinary List<Utf8> argument.
                let target = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
                let coerced = TypedExpr {
                    kind: ExprKind::Cast {
                        expr: Box::new(source.clone()),
                        target: target.clone(),
                        decimal_overflow_policy:
                            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    },
                    data_type: target,
                    nullable: true,
                };
                let bound = super::super::resolve_expr::resolved_scalar_call_at(
                    context.function_catalog,
                    "row",
                    vec![coerced],
                    Span::new(0, 0),
                )
                .unwrap();
                assert!(
                    context
                        .adapt_bound_output_domains(bound, None, &scope, Span::new(0, 0))
                        .is_err()
                );
            }
            let bound = super::super::resolve_expr::resolved_scalar_call_at(
                context.function_catalog,
                "row",
                vec![source],
                Span::new(0, 0),
            )
            .unwrap();
            let result = context.adapt_bound_output_domains(bound, None, &scope, Span::new(0, 0));
            assert_eq!(
                result.is_ok(),
                accepted,
                "marker={marker:?} witness={witness}: {result:?}"
            );
            if accepted {
                let result = result.unwrap();
                let DataType::Struct(fields) = result.data_type else {
                    panic!("row result");
                };
                let DataType::List(item) = fields[0].data_type() else {
                    panic!("list result");
                };
                assert_eq!(
                    novarocks_types::logical::logical_type_of_field(item),
                    Some(LogicalType::Json)
                );
            }
        }
    }

    #[test]
    fn m07_scalar_catalog_domains_survive_projection_cte_and_union() {
        assert_eq!(
            output_domains("select j,h,b,s,v,a,m,r from t"),
            vec![
                Some(SqlType::Json),
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::String),
                Some(SqlType::Variant),
                Some(SqlType::Array(Box::new(SqlType::Json))),
                Some(SqlType::Map(
                    Box::new(SqlType::String),
                    Box::new(SqlType::Hll)
                )),
                Some(SqlType::Struct(vec![
                    ("json".into(), SqlType::Json),
                    ("bitmap".into(), SqlType::Bitmap),
                ])),
            ]
        );
        assert_eq!(
            output_domains("with q as (select h as x from t) select x from q"),
            vec![Some(SqlType::Hll)]
        );
        assert_eq!(
            output_domains("select h from t union all select h from t"),
            vec![Some(SqlType::Hll)]
        );
        assert_eq!(
            output_domains("select null union all select b from t"),
            vec![Some(SqlType::Bitmap)]
        );
        assert_eq!(
            output_domains("select h from t union all select b from t"),
            vec![None]
        );
    }

    #[test]
    fn m07_scalar_bound_conditionals_keep_only_homogeneous_returned_domains() {
        assert_eq!(
            output_domains(
                "select coalesce(j,null),ifnull(null,h),ifnull(b,null),if(true,h,null),case when true then h else null end from t"
            ),
            vec![
                Some(SqlType::Json),
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::Hll),
                Some(SqlType::Hll)
            ]
        );
        assert_eq!(
            output_domains(
                "select coalesce(v,v),ifnull(a,null),case when true then b else b end,coalesce(m,m),if(true,r,r) from t"
            ),
            vec![
                Some(SqlType::Variant),
                Some(SqlType::Array(Box::new(SqlType::Json))),
                Some(SqlType::Bitmap),
                Some(SqlType::Map(
                    Box::new(SqlType::String),
                    Box::new(SqlType::Hll)
                )),
                Some(SqlType::Struct(vec![
                    ("json".into(), SqlType::Json),
                    ("bitmap".into(), SqlType::Bitmap),
                ])),
            ]
        );
        assert_eq!(
            output_domains(
                "select coalesce(null,parse_json('null')),if(false,null,json_object('k',1)),case when true then null else parse_json('null') end"
            ),
            vec![Some(SqlType::Json); 3]
        );
        assert_eq!(
            output_domains("select coalesce(null,null),case when true then null else null end"),
            vec![None, None]
        );
    }

    #[test]
    fn m07_scalar_mixed_values_and_explicit_string_cast_never_guess_json_or_opaque() {
        assert_eq!(
            output_domains(
                "select coalesce(j,s),ifnull(j,'plain'),if(true,h,b),case when true then h else b end,cast(j as varchar),cast(h as varbinary),cast(s as json) from t"
            ),
            vec![None; 7]
        );
        assert_eq!(
            output_domains("select cast(j as json),coalesce(cast(j as json),null) from t"),
            vec![Some(SqlType::Json); 2]
        );
        // Explicit target spelling alone cannot make an arbitrary Utf8 a Json.
        assert_eq!(output_domains("select cast('plain' as json)"), vec![None]);
    }

    #[test]
    fn m07_scalar_nullif_comparator_does_not_supply_the_returned_domain() {
        assert_eq!(
            output_domains("select nullif(h,b),nullif(j,s),nullif(s,j),nullif(null,j) from t"),
            vec![
                Some(SqlType::Hll),
                Some(SqlType::Json),
                Some(SqlType::String),
                None
            ]
        );
    }

    #[test]
    fn m07_scalar_shadowed_spelling_cannot_use_builtin_value_preservation() {
        let (resolved, _, factory) =
            super::super::analyze(&query("select coalesce(j,null) from t"), &Catalog, "db")
                .unwrap();
        let crate::analysis::QueryBody::Select(select) = resolved.body else {
            panic!("expected select")
        };
        let mut expression = select.projection[0].expr.clone();
        let ExprKind::FunctionCall { binding, name, .. } = &mut expression.kind else {
            panic!("expected bound function")
        };
        assert_eq!(name, "coalesce");
        // Keep the actual selected argument/result signature and surface name,
        // but replace the producer identity as an external catalog would.
        let mut shadowed = binding.resolved().clone();
        shadowed.function_id =
            novarocks_functions::FunctionId::try_new("test.shadow/coalesce/v1").unwrap();
        *binding = shadowed.into();
        let factory = Rc::new(RefCell::new(factory));
        let scope = AnalyzerScope::new(factory.clone());
        assert_eq!(
            context(factory).logical_output_type(None, &expression, &scope),
            None
        );
    }

    #[test]
    fn m07_scalar_implicit_offset_adaptation_preserves_authority_but_family_change_clears_it() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let id = factory
            .borrow_mut()
            .create(None, "j".into(), DataType::Utf8, true);
        factory
            .borrow_mut()
            .set_logical_type(id, Some(SqlType::Json));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        let column = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: "j".into(),
            },
            data_type: DataType::Utf8,
            nullable: true,
        };
        let cast = |target: DataType| TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(column.clone()),
                target: target.clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            data_type: target,
            nullable: true,
        };
        assert_eq!(
            context.logical_output_type(None, &cast(DataType::LargeUtf8), &scope),
            Some(SqlType::Json)
        );
        assert_eq!(
            context.logical_output_type(None, &cast(DataType::Binary), &scope),
            None
        );
        assert_eq!(
            context.logical_output_type(None, &cast(DataType::Utf8), &scope),
            Some(SqlType::Json)
        );
    }

    fn output_expressions(sql: &str) -> Vec<TypedExpr> {
        let (query, _, _) = super::super::analyze(&query(sql), &Catalog, "db").unwrap();
        let crate::analysis::QueryBody::Select(select) = query.body else {
            panic!("expected select")
        };
        select
            .projection
            .into_iter()
            .map(|item| item.expr)
            .collect()
    }

    #[test]
    fn m07_scalar_exact_aggregate_and_window_producers_forward_domains() {
        assert_eq!(
            output_domains(
                "select bitmap_union(to_bitmap(1)),hll_union(hll_hash('x')),percentile_union(percentile_hash(1.0)),any_value(to_bitmap(1))"
            ),
            vec![
                Some(SqlType::Bitmap),
                Some(SqlType::Hll),
                Some(SqlType::Percentile),
                Some(SqlType::Bitmap)
            ]
        );
        assert_eq!(
            output_domains("select bitmap_union(to_bitmap(1)) over ()"),
            vec![Some(SqlType::Bitmap)]
        );
        assert_eq!(
            output_domains(
                "select array_agg(to_bitmap(1)),map_agg(cast(1 as bigint),hll_hash('x'))"
            ),
            vec![
                Some(SqlType::Array(Box::new(SqlType::Bitmap))),
                Some(SqlType::Map(
                    Box::new(SqlType::BigInt),
                    Box::new(SqlType::Hll)
                ))
            ]
        );
    }

    #[test]
    fn m07_scalar_nested_wrappers_and_selectors_keep_exact_value_origins() {
        assert_eq!(
            output_domains(
                "select element_at([to_bitmap(1),null],1),element_at(map(1,hll_hash('x')),1),element_at(map_values(map(1,percentile_hash(1.0))),1),named_struct('value',to_bitmap(1)).value"
            ),
            vec![
                Some(SqlType::Bitmap),
                Some(SqlType::Hll),
                Some(SqlType::Percentile),
                Some(SqlType::Bitmap)
            ]
        );
        assert_eq!(
            output_domains(
                "select element_at(array_slice([hll_hash('x')],1),1),element_at(array_sortby([to_bitmap(1)],[1]),1),element_at(map_from_arrays([cast(1 as bigint)],[percentile_hash(1.0)]),1)"
            ),
            vec![
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::Percentile)
            ]
        );
        assert_eq!(
            output_domains("select element_at([o],1),element_at([p],1),element_at([v],1) from t"),
            vec![
                Some(SqlType::Object),
                Some(SqlType::Percentile),
                Some(SqlType::Variant)
            ]
        );
    }

    #[test]
    fn m07_scalar_null_companion_retains_actual_null_and_independent_marker() {
        let expressions = output_expressions(
            "select row(null,to_bitmap(1)),named_struct('missing',null,'value',percentile_hash(1.0))",
        );
        for (expression, marker, names) in [
            (&expressions[0], LogicalType::Bitmap, ["col1", "col2"]),
            (
                &expressions[1],
                LogicalType::Percentile,
                ["missing", "value"],
            ),
        ] {
            let DataType::Struct(fields) = &expression.data_type else {
                panic!("expected struct")
            };
            assert_eq!(fields[0].data_type(), &DataType::Null);
            assert_eq!(fields[0].name(), names[0]);
            assert_eq!(fields[1].name(), names[1]);
            assert_eq!(
                novarocks_types::logical::logical_type_of_field(&fields[1]),
                Some(marker)
            );
        }
        // SqlType has no Null member. The exact actual fields carry this proof,
        // rather than a fabricated all-members SqlType or a plain opaque value.
        assert_eq!(output_domains("select row(null,to_bitmap(1))"), vec![None]);
        assert_eq!(
            output_domains(
                "select named_struct('missing',null,'value',percentile_hash(1.0)).value"
            ),
            vec![Some(SqlType::Percentile)]
        );
    }

    #[test]
    fn m07_scalar_wrapper_output_adapter_preserves_selected_overload_and_shape() {
        let expression = output_expressions("select [to_bitmap(1),null]").remove(0);
        let ExprKind::Cast { expr, target, .. } = &expression.kind else {
            panic!("expected explicit output adapter")
        };
        let ExprKind::FunctionCall { binding, .. } = &expr.kind else {
            panic!("expected original bound function")
        };
        assert_eq!(
            binding.function_id.as_str(),
            "builtin.scalar/__array_literal/v1"
        );
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("expected scalar result")
        };
        assert_eq!(result.data_type, expr.data_type);
        let (DataType::List(actual), DataType::List(adapted)) = (&expr.data_type, target) else {
            panic!("expected lists")
        };
        assert_eq!(actual.name(), adapted.name());
        assert_eq!(actual.is_nullable(), adapted.is_nullable());
        assert_eq!(actual.data_type(), adapted.data_type());
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(actual),
            None
        );
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(adapted),
            Some(LogicalType::Bitmap)
        );
    }

    #[test]
    fn m07_scalar_wrapper_mixed_domains_clear_and_missing_source_markers_refuse() {
        assert_eq!(
            output_domains(
                "select element_at([to_bitmap(1),hll_hash('x')],1),element_at([parse_json('{}'),'plain'],1)"
            ),
            vec![Some(SqlType::Binary), Some(SqlType::String)]
        );
        // These trusted catalog facts deliberately lack their declared nested
        // marker in Catalog. A wrapper must not launder them by adding a marker.
        for sql in ["select map_values(m) from t", "select row(r) from t"] {
            let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
            assert!(
                error.to_string().contains("logical identity differs"),
                "{error}"
            );
        }
        assert_eq!(
            output_domains("select cast(element_at([to_bitmap(1)],1) as varbinary)"),
            vec![None]
        );
    }

    #[test]
    fn m07_scalar_shadowed_wrapper_binding_cannot_project_child_domain() {
        let (resolved, _, factory) =
            super::super::analyze(&query("select [to_bitmap(1)]"), &Catalog, "db").unwrap();
        let crate::analysis::QueryBody::Select(select) = resolved.body else {
            panic!("expected select")
        };
        let ExprKind::Cast { expr, .. } = select.projection[0].expr.clone().kind else {
            panic!("expected adapter")
        };
        let mut original = *expr;
        let ExprKind::FunctionCall { binding, .. } = &mut original.kind else {
            panic!("expected original binding")
        };
        let mut shadowed = binding.resolved().clone();
        shadowed.function_id =
            novarocks_functions::FunctionId::try_new("test.shadow/__array_literal/v1").unwrap();
        *binding = shadowed.into();
        let factory = Rc::new(RefCell::new(factory));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        let adapted = context
            .adapt_bound_output_domains(original, None, &scope, Span::new(0, 0))
            .unwrap();
        let DataType::List(field) = &adapted.data_type else {
            panic!("expected list")
        };
        assert_eq!(novarocks_types::logical::logical_type_of_field(field), None);
        assert_eq!(context.logical_output_type(None, &adapted, &scope), None);
    }
}
