// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Immutable aggregate-state vocabulary used by SQL MV refresh planning.
//!
//! This deliberately models only the SQL shape required to construct the
//! first-refresh state projection.  Physical aggregate-state codecs and merge
//! execution remain application/runtime concerns.

use super::{AggregateFunctionKind, VisibleAggregateOutput};

pub(crate) const SQL_MV_ROW_ID_COLUMN: &str = "__row_id__";
pub(crate) const SQL_MV_AGG_STATE_PREFIX: &str = "__agg_state_";
pub(crate) const SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN: &str = "__agg_state___ivm_row_count";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlAggregateGroupKey {
    pub(crate) output_name: String,
    pub(crate) expr: sqlparser::ast::Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SqlAggregateInput {
    Star,
    Expr(Box<sqlparser::ast::Expr>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlAggregateCall {
    pub(crate) output_name: String,
    pub(crate) function: AggregateFunctionKind,
    pub(crate) input: SqlAggregateInput,
}

/// The SQL-only aggregate contract for one MV SELECT.  It is immutable by
/// convention: application adapters construct it from already-frozen facts,
/// and SQL planning only reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlAggregateCalls {
    pub(crate) group_keys: Vec<SqlAggregateGroupKey>,
    pub(crate) aggregates: Vec<SqlAggregateCall>,
    pub(crate) visible_outputs: Vec<VisibleAggregateOutput>,
}

impl SqlAggregateCalls {
    pub(crate) fn extract(query: &sqlparser::ast::Query) -> Result<Self, String> {
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            return Err("extract_aggregate_sql_calls: expected a plain SELECT body".to_string());
        };
        let group_by_exprs = match &select.group_by {
            sqlparser::ast::GroupByExpr::Expressions(exprs, modifiers) if !exprs.is_empty() => {
                if !modifiers.is_empty() {
                    return Err(
                        "incremental aggregate MV does not support GROUP BY modifiers".to_string(),
                    );
                }
                exprs
            }
            sqlparser::ast::GroupByExpr::Expressions(_, _) => {
                return Err("incremental aggregate MV requires a non-empty GROUP BY".to_string());
            }
            sqlparser::ast::GroupByExpr::All(_) => {
                return Err("incremental aggregate MV requires an explicit non-empty GROUP BY; GROUP BY ALL is unsupported".to_string());
            }
        };

        let mut group_keys = group_by_exprs
            .iter()
            .cloned()
            .map(|expr| SqlAggregateGroupKey {
                output_name: String::new(),
                expr,
            })
            .collect::<Vec<_>>();
        let mut aggregates = Vec::new();
        let mut visible_outputs = Vec::with_capacity(select.projection.len());
        let mut projected_group_keys = vec![false; group_keys.len()];

        for item in &select.projection {
            let (expr, output_name) = projection_expr_and_output_name(item)?;
            if let Some(group_key_index) = group_keys
                .iter()
                .position(|group_key| group_key.expr == *expr)
            {
                if group_keys[group_key_index].output_name.is_empty() {
                    group_keys[group_key_index].output_name = output_name;
                }
                projected_group_keys[group_key_index] = true;
                visible_outputs.push(VisibleAggregateOutput::GroupKey(group_key_index));
                continue;
            }
            let aggregate_index = aggregates.len();
            aggregates.push(classify_aggregate_call(expr, output_name)?);
            visible_outputs.push(VisibleAggregateOutput::Aggregate(aggregate_index));
        }
        if projected_group_keys.iter().any(|projected| !projected) {
            return Err(
                "incremental aggregate MV projection must include every GROUP BY key".to_string(),
            );
        }
        if aggregates.is_empty() {
            return Err(
                "incremental aggregate MV requires at least one aggregate output".to_string(),
            );
        }
        Ok(Self {
            group_keys,
            aggregates,
            visible_outputs,
        })
    }

    pub(crate) fn needs_retraction_count_state(&self) -> bool {
        !self.aggregates.iter().any(|aggregate| {
            aggregate.function == AggregateFunctionKind::Count
                && matches!(aggregate.input, SqlAggregateInput::Star)
        })
    }
}

pub(crate) fn state_column_name(output_name: &str) -> String {
    format!(
        "{SQL_MV_AGG_STATE_PREFIX}{}",
        sanitize_state_column_name(output_name)
    )
}

pub(crate) fn sanitize_state_column_name(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "agg".to_string()
    } else {
        sanitized
    }
}

/// Rewrite a validated aggregate SELECT into the state-shaped projection that
/// the distributed first-refresh write uses.  The SQL planner owns this text
/// shaping; runtime state codecs never enter this boundary.
pub(crate) fn rewrite_select_sql_for_state(
    select_sql: &str,
    calls: &SqlAggregateCalls,
) -> Result<String, String> {
    use sqlparser::ast::{SelectItem, SetExpr, Statement};

    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("rewrite_select_sql_for_state normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("rewrite_select_sql_for_state parse error: {e}"))?;
    let Statement::Query(query) = &mut stmt else {
        return Err("rewrite_select_sql_for_state: expected Query statement".to_string());
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err("rewrite_select_sql_for_state: expected SELECT body".to_string());
    };

    let mut projection = Vec::with_capacity(
        calls.visible_outputs.len()
            + calls.aggregates.len()
            + usize::from(calls.needs_retraction_count_state()),
    );
    for output in &calls.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(index) => {
                let key = calls.group_keys.get(*index).ok_or_else(|| {
                    format!("rewrite_select_sql_for_state: group key index {index} out of range")
                })?;
                projection.push(SelectItem::ExprWithAlias {
                    expr: key.expr.clone(),
                    alias: select_alias_ident(&key.output_name),
                });
            }
            VisibleAggregateOutput::Aggregate(index) => {
                let aggregate = calls.aggregates.get(*index).ok_or_else(|| {
                    format!("rewrite_select_sql_for_state: aggregate index {index} out of range")
                })?;
                projection.push(state_combinator_select_item(aggregate)?);
            }
        }
    }
    if calls.needs_retraction_count_state() {
        projection.push(count_star_select_item(
            SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN,
        ));
    }
    select.projection = projection;
    Ok(stmt.to_string())
}

fn projection_expr_and_output_name(
    item: &sqlparser::ast::SelectItem,
) -> Result<(&sqlparser::ast::Expr, String), String> {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(expr) => Ok((expr, expr.to_string())),
        sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
            Ok((expr, alias.value.clone()))
        }
        sqlparser::ast::SelectItem::QualifiedWildcard(_, _)
        | sqlparser::ast::SelectItem::Wildcard(_) => Err(
            "incremental aggregate MV projection can only contain expressions or aliases"
                .to_string(),
        ),
    }
}

fn classify_aggregate_call(
    expr: &sqlparser::ast::Expr,
    output_name: String,
) -> Result<SqlAggregateCall, String> {
    let sqlparser::ast::Expr::Function(function) = expr else {
        return Err(
            "incremental aggregate MV scalar projection must be a GROUP BY key or aggregate call"
                .to_string(),
        );
    };
    if function.name.0.len() != 1
        || function.uses_odbc_syntax
        || function.null_treatment.is_some()
        || function.over.is_some()
        || function.filter.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, sqlparser::ast::FunctionArguments::None)
    {
        return Err(aggregate_error());
    }
    let sqlparser::ast::FunctionArguments::List(args) = &function.args else {
        return Err(aggregate_error());
    };
    if !args.clauses.is_empty() {
        return Err(aggregate_error());
    }
    let name = function.name.to_string().to_ascii_lowercase();
    let (function, input) = match name.as_str() {
        "count"
            if matches!(
                args.duplicate_treatment,
                Some(sqlparser::ast::DuplicateTreatment::Distinct)
            ) =>
        {
            (
                AggregateFunctionKind::CountDistinct,
                single_expression_input(&args.args, "COUNT(DISTINCT)")?,
            )
        }
        "count" if args.duplicate_treatment.is_none() => count_input(&args.args)?,
        "count_distinct" | "multi_distinct_count" if args.duplicate_treatment.is_none() => (
            AggregateFunctionKind::CountDistinct,
            single_expression_input(&args.args, "COUNT(DISTINCT)")?,
        ),
        "approx_count_distinct" | "ndv" | "hll_ndv" if args.duplicate_treatment.is_none() => (
            AggregateFunctionKind::ApproxCountDistinct,
            single_expression_input(&args.args, "APPROX_COUNT_DISTINCT")?,
        ),
        "sum" if args.duplicate_treatment.is_none() => (
            AggregateFunctionKind::Sum,
            single_expression_input(&args.args, "SUM")?,
        ),
        "avg" if args.duplicate_treatment.is_none() => (
            AggregateFunctionKind::Avg,
            single_expression_input(&args.args, "AVG")?,
        ),
        "min" if args.duplicate_treatment.is_none() => (
            AggregateFunctionKind::Min,
            single_expression_input(&args.args, "MIN/MAX")?,
        ),
        "max" if args.duplicate_treatment.is_none() => (
            AggregateFunctionKind::Max,
            single_expression_input(&args.args, "MIN/MAX")?,
        ),
        "bool_or" | "boolor_agg" if args.duplicate_treatment.is_none() => (
            AggregateFunctionKind::BoolOr,
            single_expression_input(&args.args, "BOOL_OR/BOOL_AND")?,
        ),
        "bool_and" | "booland_agg" if args.duplicate_treatment.is_none() => (
            AggregateFunctionKind::BoolAnd,
            single_expression_input(&args.args, "BOOL_OR/BOOL_AND")?,
        ),
        _ => return Err(aggregate_error()),
    };
    Ok(SqlAggregateCall {
        output_name,
        function,
        input,
    })
}

fn count_input(
    args: &[sqlparser::ast::FunctionArg],
) -> Result<(AggregateFunctionKind, SqlAggregateInput), String> {
    let [arg] = args else {
        return Err(aggregate_error());
    };
    match simple_arg(arg)? {
        sqlparser::ast::FunctionArgExpr::Wildcard => {
            Ok((AggregateFunctionKind::Count, SqlAggregateInput::Star))
        }
        sqlparser::ast::FunctionArgExpr::Expr(expr) => Ok((
            AggregateFunctionKind::Count,
            SqlAggregateInput::Expr(Box::new(expr.clone())),
        )),
        sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_) => Err(aggregate_error()),
    }
}

fn single_expression_input(
    args: &[sqlparser::ast::FunctionArg],
    label: &str,
) -> Result<SqlAggregateInput, String> {
    let [arg] = args else {
        return Err(format!("{label} requires exactly one column expression"));
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_arg(arg)? else {
        return Err(format!("{label}(*) is not supported"));
    };
    Ok(SqlAggregateInput::Expr(Box::new(expr.clone())))
}

fn simple_arg(
    arg: &sqlparser::ast::FunctionArg,
) -> Result<&sqlparser::ast::FunctionArgExpr, String> {
    match arg {
        sqlparser::ast::FunctionArg::Unnamed(arg) => Ok(arg),
        sqlparser::ast::FunctionArg::Named { .. }
        | sqlparser::ast::FunctionArg::ExprNamed { .. } => Err(aggregate_error()),
    }
}

fn aggregate_error() -> String {
    "incremental aggregate MV query must be a single-table SELECT with non-empty GROUP BY and only supported aggregate outputs".to_string()
}

fn state_combinator_select_item(
    aggregate: &SqlAggregateCall,
) -> Result<sqlparser::ast::SelectItem, String> {
    let argument = match &aggregate.input {
        SqlAggregateInput::Star if aggregate.function == AggregateFunctionKind::Count => {
            sqlparser::ast::Expr::Value(
                sqlparser::ast::Value::Number("1".to_string(), false).into(),
            )
        }
        SqlAggregateInput::Star => {
            return Err(format!(
                "rewrite_select_sql_for_state: {} requires an expression input",
                aggregate_label(aggregate.function)
            ));
        }
        SqlAggregateInput::Expr(expr) => expr.as_ref().clone(),
    };
    aggregate_select_item(
        state_combinator_name(aggregate.function),
        argument,
        &state_column_name(&aggregate.output_name),
    )
}

fn aggregate_label(kind: AggregateFunctionKind) -> &'static str {
    match kind {
        AggregateFunctionKind::Count => "COUNT",
        AggregateFunctionKind::Sum => "SUM",
        AggregateFunctionKind::Avg => "AVG",
        AggregateFunctionKind::Min => "MIN",
        AggregateFunctionKind::Max => "MAX",
        AggregateFunctionKind::BoolOr => "BOOL_OR",
        AggregateFunctionKind::BoolAnd => "BOOL_AND",
        AggregateFunctionKind::CountDistinct => "COUNT_DISTINCT",
        AggregateFunctionKind::ApproxCountDistinct => "APPROX_COUNT_DISTINCT",
    }
}

fn state_combinator_name(kind: AggregateFunctionKind) -> &'static str {
    match kind {
        AggregateFunctionKind::Count => "count_state",
        AggregateFunctionKind::Sum => "sum_state",
        AggregateFunctionKind::Avg => "avg_state",
        AggregateFunctionKind::Min => "min_state",
        AggregateFunctionKind::Max => "max_state",
        AggregateFunctionKind::BoolOr => "bool_or_state",
        AggregateFunctionKind::BoolAnd => "bool_and_state",
        AggregateFunctionKind::CountDistinct => "count_distinct_state",
        AggregateFunctionKind::ApproxCountDistinct => "approx_count_distinct_state",
    }
}

fn select_alias_ident(alias: &str) -> sqlparser::ast::Ident {
    let mut chars = alias.chars();
    let plain = chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric());
    if plain {
        sqlparser::ast::Ident::new(alias)
    } else {
        sqlparser::ast::Ident::with_quote('`', alias)
    }
}

fn aggregate_select_item(
    function_name: &str,
    argument: sqlparser::ast::Expr,
    alias: &str,
) -> Result<sqlparser::ast::SelectItem, String> {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName, ObjectNamePart,
    };
    let function = Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(function_name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    };
    Ok(sqlparser::ast::SelectItem::ExprWithAlias {
        expr: sqlparser::ast::Expr::Function(function),
        alias: Ident::new(alias),
    })
}

fn count_star_select_item(alias: &str) -> sqlparser::ast::SelectItem {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName, ObjectNamePart,
    };
    let function = Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("COUNT"))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    };
    sqlparser::ast::SelectItem::ExprWithAlias {
        expr: sqlparser::ast::Expr::Function(function),
        alias: Ident::new(alias),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> sqlparser::ast::Query {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql).unwrap();
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized).unwrap();
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query");
        };
        *query
    }

    #[test]
    fn sqlx2_mv_aggregate_shape_is_sql_owned_and_rewrites_state() {
        let calls = SqlAggregateCalls::extract(&parse(
            "SELECT k, sum(v) AS total FROM ice.db.fact GROUP BY k",
        ))
        .unwrap();
        assert_eq!(calls.aggregates.len(), 1);
        assert_eq!(state_column_name("total"), "__agg_state_total");
        let sql = rewrite_select_sql_for_state(
            "SELECT k, sum(v) AS total FROM ice.db.fact GROUP BY k",
            &calls,
        )
        .unwrap();
        assert!(sql.contains("sum_state(v) AS __agg_state_total"), "{sql}");
        assert!(
            sql.contains("COUNT(*) AS __agg_state___ivm_row_count"),
            "{sql}"
        );
    }

    #[test]
    fn sqlx2_mv_aggregate_shape_count_star_omits_retraction_count() {
        let calls = SqlAggregateCalls::extract(&parse(
            "SELECT k, count(*) AS total FROM ice.db.fact GROUP BY k",
        ))
        .unwrap();
        assert!(!calls.needs_retraction_count_state());
    }
}
