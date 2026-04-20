use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};
use sqlparser::ast as sqlast;

use crate::sql::analysis::JoinKind;

// ---------------------------------------------------------------------------
// SQL type -> Arrow type conversion
// ---------------------------------------------------------------------------

pub(super) fn sql_type_to_arrow(sql_type: &sqlast::DataType) -> Result<DataType, String> {
    match sql_type {
        sqlast::DataType::TinyInt(_) => Ok(DataType::Int8),
        sqlast::DataType::SmallInt(_) => Ok(DataType::Int16),
        sqlast::DataType::Int(_) | sqlast::DataType::Integer(_) => Ok(DataType::Int32),
        sqlast::DataType::BigInt(_) => Ok(DataType::Int64),
        sqlast::DataType::Float(_) => Ok(DataType::Float32),
        sqlast::DataType::Double(_) | sqlast::DataType::DoublePrecision => Ok(DataType::Float64),
        sqlast::DataType::Boolean => Ok(DataType::Boolean),
        sqlast::DataType::Varchar(_)
        | sqlast::DataType::CharVarying(_)
        | sqlast::DataType::Text => Ok(DataType::Utf8),
        sqlast::DataType::Char(_)
        | sqlast::DataType::Character(_)
        | sqlast::DataType::String(_) => Ok(DataType::Utf8),
        sqlast::DataType::Date => Ok(DataType::Date32),
        sqlast::DataType::Datetime(_) | sqlast::DataType::Timestamp(_, _) => Ok(
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        sqlast::DataType::Time(_, _) => {
            Ok(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond))
        }
        sqlast::DataType::Decimal(info)
        | sqlast::DataType::Dec(info)
        | sqlast::DataType::Numeric(info) => match info {
            sqlast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                Ok(DataType::Decimal128(*p as u8, *s as i8))
            }
            sqlast::ExactNumberInfo::Precision(p) => Ok(DataType::Decimal128(*p as u8, 0)),
            sqlast::ExactNumberInfo::None => Ok(DataType::Decimal128(38, 0)),
        },
        sqlast::DataType::Custom(name, _) => {
            let type_name = name.to_string().to_lowercase();
            match type_name.as_str() {
                "string" => Ok(DataType::Utf8),
                "largeint" => Ok(DataType::FixedSizeBinary(
                    crate::common::largeint::LARGEINT_BYTE_WIDTH,
                )),
                "json" | "jsonb" => Ok(DataType::Utf8),
                _ => Err(format!("unsupported SQL type: {name}")),
            }
        }
        sqlast::DataType::Array(elem_def) => {
            let inner = match elem_def {
                sqlast::ArrayElemTypeDef::AngleBracket(inner_type)
                | sqlast::ArrayElemTypeDef::SquareBracket(inner_type, _)
                | sqlast::ArrayElemTypeDef::Parenthesis(inner_type) => {
                    sql_type_to_arrow(inner_type)?
                }
                sqlast::ArrayElemTypeDef::None => {
                    return Err("ARRAY type requires an element type".to_string());
                }
            };
            Ok(DataType::List(Arc::new(Field::new("item", inner, true))))
        }
        sqlast::DataType::Map(key_type, value_type) => Ok(DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", sql_type_to_arrow(key_type)?, false)),
                    Arc::new(Field::new("value", sql_type_to_arrow(value_type)?, true)),
                ])),
                false,
            )),
            false,
        )),
        sqlast::DataType::Struct(fields, _) => {
            let out_fields: Vec<Arc<Field>> = fields
                .iter()
                .enumerate()
                .map(|(idx, field)| {
                    let name = field
                        .field_name
                        .as_ref()
                        .map(|ident| ident.value.clone())
                        .unwrap_or_else(|| format!("f{}", idx + 1));
                    Ok(Arc::new(Field::new(
                        name,
                        sql_type_to_arrow(&field.field_type)?,
                        true,
                    )))
                })
                .collect::<Result<_, String>>()?;
            Ok(DataType::Struct(Fields::from(out_fields)))
        }
        other => Err(format!("unsupported CAST target type: {other:?}")),
    }
}

// ---------------------------------------------------------------------------
// Expression display name
// ---------------------------------------------------------------------------

pub(super) fn expr_display_name(expr: &sqlast::Expr) -> String {
    match expr {
        // Strip outer parentheses: `(col)` → display name of `col`.
        // This matches how `SELECT distinct(col)` is parsed: DISTINCT is
        // the SELECT modifier and `(col)` is a Nested expression.
        sqlast::Expr::Nested(inner) => expr_display_name(inner),
        sqlast::Expr::CompoundIdentifier(parts) if parts.len() >= 2 => parts
            .last()
            .map(|i| i.value.clone())
            .unwrap_or_else(|| format!("{expr}")),
        sqlast::Expr::Identifier(ident) => ident.value.clone(),
        sqlast::Expr::Function(f) => format_function_display_name(f),
        // CAST: uppercase keyword, StarRocks-style type names (DECIMAL64/DECIMAL128),
        // wrap inner with parentheses if it's not a simple identifier or literal.
        sqlast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } if matches!(data_type, sqlast::DataType::Array(_))
            && matches!(inner.as_ref(), sqlast::Expr::Array(_)) =>
        {
            expr_display_name(inner)
        }
        sqlast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let inner_str = expr_display_name_with_parens(inner);
            let type_str = format_cast_type(data_type);
            format!("CAST({inner_str} AS {type_str})")
        }
        // Binary ops: wrap each operand with parentheses unless it's a simple
        // identifier or literal, matching StarRocks AST2StringVisitor behavior.
        sqlast::Expr::BinaryOp { left, op, right } => {
            let left_str = expr_display_name_with_parens(left);
            let right_str = expr_display_name_with_parens(right);
            format!("{left_str} {op} {right_str}")
        }
        // Expressions like SUBSTR, EXTRACT are rendered in uppercase by
        // sqlparser's Display. Lowercase leading keyword to match StarRocks FE.
        other => {
            let s = format!("{other}");
            // Lowercase leading keyword (up to the first '(') if present.
            if let Some(paren) = s.find('(') {
                let prefix = &s[..paren];
                // Only lowercase if the prefix is all-ASCII-alpha (a keyword).
                if !prefix.is_empty() && prefix.chars().all(|c| c.is_ascii_alphabetic()) {
                    format!("{}{}", prefix.to_lowercase(), &s[paren..])
                } else {
                    s
                }
            } else {
                s
            }
        }
    }
}

/// Wraps `expr_display_name(expr)` in parentheses unless the expression is
/// a simple identifier or literal — matching StarRocks `printWithParentheses`.
fn expr_display_name_with_parens(expr: &sqlast::Expr) -> String {
    match expr {
        sqlast::Expr::Identifier(_) | sqlast::Expr::CompoundIdentifier(_) => {
            expr_display_name(expr)
        }
        sqlast::Expr::Value(_) => expr_display_name(expr),
        sqlast::Expr::Nested(inner) => expr_display_name_with_parens(inner),
        _ => format!("({})", expr_display_name(expr)),
    }
}

/// Format a CAST target type using StarRocks-style names.
/// DECIMAL(p,s) is promoted to DECIMAL32/DECIMAL64/DECIMAL128 to match
/// the analyzed type name that StarRocks FE emits in column aliases.
fn format_cast_type(data_type: &sqlast::DataType) -> String {
    match data_type {
        sqlast::DataType::Decimal(info)
        | sqlast::DataType::Dec(info)
        | sqlast::DataType::Numeric(info) => match info {
            sqlast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                let kind = decimal_kind(*p);
                format!("{kind}({p},{s})")
            }
            sqlast::ExactNumberInfo::Precision(p) => {
                let kind = decimal_kind(*p);
                format!("{kind}({p},0)")
            }
            sqlast::ExactNumberInfo::None => "DECIMAL128(38,0)".to_string(),
        },
        other => format!("{other}"),
    }
}

fn decimal_kind(precision: u64) -> &'static str {
    if precision <= 9 {
        "DECIMAL32"
    } else if precision <= 18 {
        "DECIMAL64"
    } else {
        "DECIMAL128"
    }
}

fn canonical_display_function_name(name: &str) -> String {
    match name.to_lowercase().as_str() {
        "boolor_agg" => "bool_or".to_string(),
        "booland_agg" | "every" => "bool_and".to_string(),
        "approx_count_distinct_hll_sketch" => "ds_hll_count_distinct".to_string(),
        other => other.to_string(),
    }
}

fn format_function_display_name(function: &sqlast::Function) -> String {
    let mut out = format!(
        "{}{}{}",
        canonical_display_function_name(&function.name.to_string()),
        function.parameters,
        format_function_arguments(&function.args)
    );
    if !function.within_group.is_empty() {
        out.push_str(" WITHIN GROUP (ORDER BY ");
        out.push_str(
            &function
                .within_group
                .iter()
                .map(format_order_by_expr_display_name)
                .collect::<Vec<_>>()
                .join(", "),
        );
        out.push(')');
    }
    if let Some(filter_cond) = &function.filter {
        out.push_str(" FILTER (WHERE ");
        out.push_str(&expr_display_name(filter_cond));
        out.push(')');
    }
    if let Some(null_treatment) = &function.null_treatment {
        out.push(' ');
        out.push_str(&null_treatment.to_string());
    }
    if let Some(over) = &function.over {
        out.push_str(" OVER ");
        out.push_str(&over.to_string());
    }
    out
}

fn format_function_arguments(args: &sqlast::FunctionArguments) -> String {
    match args {
        sqlast::FunctionArguments::None => String::new(),
        sqlast::FunctionArguments::Subquery(query) => format!("({query})"),
        sqlast::FunctionArguments::List(list) => {
            format!("({})", format_function_argument_list(list))
        }
    }
}

fn format_function_argument_list(list: &sqlast::FunctionArgumentList) -> String {
    let mut out = String::new();
    if let Some(duplicate_treatment) = list.duplicate_treatment {
        out.push_str(&duplicate_treatment.to_string());
        out.push(' ');
    }
    out.push_str(
        &list
            .args
            .iter()
            .map(format_function_arg_display_name)
            .collect::<Vec<_>>()
            .join(", "),
    );
    if !list.clauses.is_empty() {
        if !list.args.is_empty() {
            out.push(' ');
        }
        out.push_str(
            &list
                .clauses
                .iter()
                .map(|clause| format_function_clause_display_name(clause, &list.args))
                .collect::<Vec<_>>()
                .join(" "),
        );
    }
    out
}

fn format_function_arg_display_name(arg: &sqlast::FunctionArg) -> String {
    match arg {
        sqlast::FunctionArg::Named {
            name,
            arg,
            operator,
        } => format!(
            "{name} {operator} {}",
            format_function_arg_expr_display_name(arg)
        ),
        sqlast::FunctionArg::ExprNamed {
            name,
            arg,
            operator,
        } => format!(
            "{} {operator} {}",
            expr_display_name(name),
            format_function_arg_expr_display_name(arg)
        ),
        sqlast::FunctionArg::Unnamed(arg) => format_function_arg_expr_display_name(arg),
    }
}

fn format_function_arg_expr_display_name(arg: &sqlast::FunctionArgExpr) -> String {
    match arg {
        sqlast::FunctionArgExpr::Expr(expr) => expr_display_name(expr),
        sqlast::FunctionArgExpr::QualifiedWildcard(prefix) => format!("{prefix}.*"),
        sqlast::FunctionArgExpr::Wildcard => "*".to_string(),
    }
}

fn format_function_clause_display_name(
    clause: &sqlast::FunctionArgumentClause,
    args: &[sqlast::FunctionArg],
) -> String {
    match clause {
        sqlast::FunctionArgumentClause::OrderBy(order_by) => format!(
            "ORDER BY {}",
            order_by
                .iter()
                .map(|item| format_function_order_by_expr_display_name(item, args))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        sqlast::FunctionArgumentClause::Limit(limit) => {
            format!("LIMIT {}", expr_display_name(limit))
        }
        _ => clause.to_string(),
    }
}

fn format_order_by_expr_display_name(order_by: &sqlast::OrderByExpr) -> String {
    let mut out = expr_display_name(&order_by.expr);
    let asc = order_by.options.asc.unwrap_or(true);
    out.push(' ');
    out.push_str(if asc { "ASC" } else { "DESC" });
    if let Some(nulls_first) = order_by.options.nulls_first
        && nulls_first != asc
    {
        out.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    if let Some(with_fill) = &order_by.with_fill {
        out.push(' ');
        out.push_str(&with_fill.to_string());
    }
    out
}

fn format_function_order_by_expr_display_name(
    order_by: &sqlast::OrderByExpr,
    args: &[sqlast::FunctionArg],
) -> String {
    let expr = match &order_by.expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::Number(n, false),
            ..
        }) => n
            .parse::<usize>()
            .ok()
            .and_then(|pos| args.get(pos.saturating_sub(1)))
            .map(format_function_arg_display_name)
            .unwrap_or_else(|| expr_display_name(&order_by.expr)),
        _ => expr_display_name(&order_by.expr),
    };

    let mut out = expr;
    let asc = order_by.options.asc.unwrap_or(true);
    out.push(' ');
    out.push_str(if asc { "ASC" } else { "DESC" });
    if let Some(nulls_first) = order_by.options.nulls_first
        && nulls_first != asc
    {
        out.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    if let Some(with_fill) = &order_by.with_fill {
        out.push(' ');
        out.push_str(&with_fill.to_string());
    }
    out
}

// ---------------------------------------------------------------------------
// JOIN operator parsing
// ---------------------------------------------------------------------------

pub(super) fn parse_join_operator(
    op: &sqlast::JoinOperator,
) -> Result<(JoinKind, Option<&sqlast::JoinConstraint>), String> {
    match op {
        sqlast::JoinOperator::Join(c) | sqlast::JoinOperator::Inner(c) => {
            Ok((JoinKind::Inner, Some(c)))
        }
        sqlast::JoinOperator::Left(c) | sqlast::JoinOperator::LeftOuter(c) => {
            Ok((JoinKind::LeftOuter, Some(c)))
        }
        sqlast::JoinOperator::Right(c) | sqlast::JoinOperator::RightOuter(c) => {
            Ok((JoinKind::RightOuter, Some(c)))
        }
        sqlast::JoinOperator::FullOuter(c) => Ok((JoinKind::FullOuter, Some(c))),
        sqlast::JoinOperator::CrossJoin(_) => Ok((JoinKind::Cross, None)),
        sqlast::JoinOperator::LeftSemi(c) => Ok((JoinKind::LeftSemi, Some(c))),
        sqlast::JoinOperator::RightSemi(c) => Ok((JoinKind::RightSemi, Some(c))),
        sqlast::JoinOperator::LeftAnti(c) => Ok((JoinKind::LeftAnti, Some(c))),
        sqlast::JoinOperator::RightAnti(c) => Ok((JoinKind::RightAnti, Some(c))),
        other => Err(format!("unsupported join type: {other:?}")),
    }
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET extraction
// ---------------------------------------------------------------------------

pub(super) fn extract_limit(query: &sqlast::Query) -> Result<Option<i64>, String> {
    match &query.limit_clause {
        Some(sqlast::LimitClause::LimitOffset {
            limit:
                Some(sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number(n, _),
                    ..
                })),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid LIMIT value: {e}")),
        Some(sqlast::LimitClause::LimitOffset { limit: None, .. }) => Ok(None),
        Some(sqlast::LimitClause::LimitOffset { .. }) => {
            Err("only constant LIMIT is supported".into())
        }
        Some(sqlast::LimitClause::OffsetCommaLimit {
            limit:
                sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number(n, _),
                    ..
                }),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid LIMIT value: {e}")),
        Some(sqlast::LimitClause::OffsetCommaLimit { .. }) => {
            Err("only constant LIMIT is supported".into())
        }
        None => Ok(None),
    }
}

pub(super) fn extract_offset(query: &sqlast::Query) -> Result<Option<i64>, String> {
    match &query.limit_clause {
        Some(sqlast::LimitClause::LimitOffset {
            offset:
                Some(sqlast::Offset {
                    value:
                        sqlast::Expr::Value(sqlast::ValueWithSpan {
                            value: sqlast::Value::Number(n, _),
                            ..
                        }),
                    ..
                }),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid OFFSET value: {e}")),
        Some(sqlast::LimitClause::LimitOffset { offset: None, .. }) => Ok(None),
        Some(sqlast::LimitClause::LimitOffset { .. }) => {
            Err("only constant OFFSET is supported".into())
        }
        Some(sqlast::LimitClause::OffsetCommaLimit {
            offset:
                sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number(n, _),
                    ..
                }),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid OFFSET value: {e}")),
        Some(sqlast::LimitClause::OffsetCommaLimit { .. }) => {
            Err("only constant OFFSET is supported".into())
        }
        None => Ok(None),
    }
}

/// Evaluate a constant integer expression (literals and simple arithmetic).
pub(super) fn eval_const_i64(expr: &sqlast::Expr) -> Result<i64, String> {
    match expr {
        sqlast::Expr::Value(v) => match &v.value {
            sqlast::Value::Number(n, _) => n
                .parse::<i64>()
                .map_err(|e| format!("cannot parse integer literal `{n}`: {e}")),
            _ => Err(format!("expected integer literal, got: {v}")),
        },
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => Ok(-eval_const_i64(inner)?),
        sqlast::Expr::BinaryOp { left, op, right } => {
            let l = eval_const_i64(left)?;
            let r = eval_const_i64(right)?;
            match op {
                sqlast::BinaryOperator::Plus => Ok(l + r),
                sqlast::BinaryOperator::Minus => Ok(l - r),
                sqlast::BinaryOperator::Multiply => Ok(l * r),
                sqlast::BinaryOperator::Divide if r != 0 => Ok(l / r),
                sqlast::BinaryOperator::Modulo if r != 0 => Ok(l % r),
                _ => Err(format!("unsupported operator in constant expression: {op}")),
            }
        }
        sqlast::Expr::Nested(inner) => eval_const_i64(inner),
        _ => Err(format!("expected constant integer expression, got: {expr}")),
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast as sqlast;

    use super::expr_display_name;
    use crate::sql::parser::dialect::StarRocksDialect;

    fn parse_select_expr(sql: &str) -> sqlast::Expr {
        let statements =
            sqlparser::parser::Parser::parse_sql(&StarRocksDialect, sql).expect("parse sql");
        let sqlast::Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        let sqlast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let sqlast::SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
            panic!("expected unnamed expr");
        };
        expr.clone()
    }

    #[test]
    fn expr_display_name_formats_distinct_function_args_recursively() {
        let expr = parse_select_expr("SELECT ARRAY_AGG(DISTINCT score > 0)");
        assert_eq!(expr_display_name(&expr), "array_agg(DISTINCT score > 0)");
    }

    #[test]
    fn expr_display_name_lowercases_nested_function_names() {
        let expr = parse_select_expr("SELECT array_min(ARRAY_UNIQUE_AGG(col_boolean))");
        assert_eq!(
            expr_display_name(&expr),
            "array_min(array_unique_agg(col_boolean))"
        );
    }
}
