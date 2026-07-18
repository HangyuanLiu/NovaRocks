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

use crate::engine::mv::agg_state::aggregate_sql_calls::AggregateSqlCalls;
use crate::engine::mv::agg_state::mv_agg_state::{
    AGG_RETRACTION_COUNT_STATE_COLUMN, aggregate_shape_needs_retraction_count_state,
    sanitize_state_column_name,
};
use crate::engine::mv::agg_state::mv_shape::{AggregateCallShape, AggregateInput};
use crate::exec::change_op::CHANGE_OP_COLUMN;
use crate::mv::model::{AggregateFunctionKind, VisibleAggregateOutput};
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    ObjectName, ObjectNamePart, SelectItem, SetExpr, Statement, Value,
};

pub(crate) fn rewrite_select_sql_for_signed_delta_state(
    select_sql: &str,
    calls: &AggregateSqlCalls,
) -> Result<String, String> {
    rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(select_sql, calls, None)
}

pub(crate) fn rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    change_op_qualifier: Option<&str>,
) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("rewrite_select_sql_for_signed_delta_state normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("rewrite_select_sql_for_signed_delta_state parse error: {e}"))?;

    let Statement::Query(query) = &mut stmt else {
        return Err(
            "rewrite_select_sql_for_signed_delta_state: expected Query statement".to_string(),
        );
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err("rewrite_select_sql_for_signed_delta_state: expected SELECT body".to_string());
    };

    let change_op = ChangeOpExpr::new(change_op_qualifier);
    select.projection = signed_delta_projection(calls, &change_op)?;

    Ok(stmt.to_string())
}

struct ChangeOpExpr {
    qualifier: Option<String>,
}

impl ChangeOpExpr {
    fn new(qualifier: Option<&str>) -> Self {
        Self {
            qualifier: qualifier.map(ToString::to_string),
        }
    }

    fn expr(&self) -> Expr {
        match &self.qualifier {
            Some(qualifier) => {
                Expr::CompoundIdentifier(vec![Ident::new(qualifier), Ident::new(CHANGE_OP_COLUMN)])
            }
            None => Expr::Identifier(Ident::new(CHANGE_OP_COLUMN)),
        }
    }
}

fn signed_delta_projection(
    calls: &AggregateSqlCalls,
    change_op: &ChangeOpExpr,
) -> Result<Vec<SelectItem>, String> {
    let mut projection = Vec::with_capacity(calls.visible_outputs.len() + calls.aggregates.len());
    for output in &calls.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let group_key = calls.group_keys.get(*group_key_index).ok_or_else(|| {
                    format!(
                        "rewrite_select_sql_for_signed_delta_state: group key index {group_key_index} out of range"
                    )
                })?;
                projection.push(SelectItem::ExprWithAlias {
                    expr: group_key.expr.clone(),
                    alias: select_alias_ident(&group_key.output_name),
                });
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let aggregate = calls.aggregates.get(*aggregate_index).ok_or_else(|| {
                    format!(
                        "rewrite_select_sql_for_signed_delta_state: aggregate index {aggregate_index} out of range"
                    )
                })?;
                push_signed_aggregate_state_projection(&mut projection, aggregate, change_op)?;
            }
        }
    }
    if aggregate_shape_needs_retraction_count_state(calls) {
        projection.push(make_aggregate_select_item(
            "SUM",
            change_op.expr(),
            AGG_RETRACTION_COUNT_STATE_COLUMN,
        ));
    }
    Ok(projection)
}

fn push_signed_aggregate_state_projection(
    projection: &mut Vec<SelectItem>,
    aggregate: &AggregateCallShape,
    change_op: &ChangeOpExpr,
) -> Result<(), String> {
    let func_name = combinator_name_for_kind(aggregate.function, true);
    let state_alias = aggregate_state_alias(&aggregate.output_name);
    let input = signed_state_input_expr(aggregate)?;
    projection.push(make_two_arg_aggregate_select_item(
        func_name,
        input,
        change_op.expr(),
        &state_alias,
    ));
    Ok(())
}

fn signed_state_input_expr(aggregate: &AggregateCallShape) -> Result<Expr, String> {
    match &aggregate.input {
        AggregateInput::Star => {
            if aggregate.function == AggregateFunctionKind::Count {
                Ok(Expr::Value(Value::Number("1".to_string(), false).into()))
            } else {
                Err(format!(
                    "rewrite_select_sql_for_signed_delta_state: {} requires an expression input",
                    aggregate_function_label(aggregate.function)
                ))
            }
        }
        AggregateInput::Expr(expr) => Ok(expr.as_ref().clone()),
    }
}

fn aggregate_state_alias(output_name: &str) -> String {
    let sanitized = sanitize_state_column_name(output_name);
    format!("__agg_state_{sanitized}")
}

fn aggregate_function_label(kind: AggregateFunctionKind) -> &'static str {
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

fn combinator_name_for_kind(kind: AggregateFunctionKind, signed: bool) -> &'static str {
    match (kind, signed) {
        (AggregateFunctionKind::Count, false) => "count_state",
        (AggregateFunctionKind::Count, true) => "count_state_signed",
        (AggregateFunctionKind::Sum, false) => "sum_state",
        (AggregateFunctionKind::Sum, true) => "sum_state_signed",
        (AggregateFunctionKind::Avg, false) => "avg_state",
        (AggregateFunctionKind::Avg, true) => "avg_state_signed",
        (AggregateFunctionKind::Min, false) => "min_state",
        (AggregateFunctionKind::Min, true) => "min_state_signed",
        (AggregateFunctionKind::Max, false) => "max_state",
        (AggregateFunctionKind::Max, true) => "max_state_signed",
        (AggregateFunctionKind::BoolOr, false) => "bool_or_state",
        (AggregateFunctionKind::BoolOr, true) => "bool_or_state_signed",
        (AggregateFunctionKind::BoolAnd, false) => "bool_and_state",
        (AggregateFunctionKind::BoolAnd, true) => "bool_and_state_signed",
        (AggregateFunctionKind::CountDistinct, false) => "count_distinct_state",
        (AggregateFunctionKind::CountDistinct, true) => "count_distinct_state_signed",
        (AggregateFunctionKind::ApproxCountDistinct, false) => "approx_count_distinct_state",
        (AggregateFunctionKind::ApproxCountDistinct, true) => "approx_count_distinct_state_signed",
    }
}

fn make_aggregate_select_item(func_name: &str, arg: Expr, alias: &str) -> SelectItem {
    let function = Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(func_name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    };
    SelectItem::ExprWithAlias {
        expr: Expr::Function(function),
        alias: select_alias_ident(alias),
    }
}

fn make_two_arg_aggregate_select_item(
    func_name: &str,
    arg1: Expr,
    arg2: Expr,
    alias: &str,
) -> SelectItem {
    let function = Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(func_name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(arg1)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(arg2)),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    };
    SelectItem::ExprWithAlias {
        expr: Expr::Function(function),
        alias: select_alias_ident(alias),
    }
}

fn select_alias_ident(alias: &str) -> Ident {
    if is_plain_identifier(alias) {
        Ident::new(alias)
    } else {
        Ident::with_quote('`', alias)
    }
}

fn is_plain_identifier(alias: &str) -> bool {
    let mut chars = alias.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}
