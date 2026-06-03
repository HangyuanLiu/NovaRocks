//! Predicate selectivity estimation kernel.

use std::collections::HashMap;

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr, UnOp};
use crate::sql::optimizer::statistics::{
    ColumnStatistic, IN_PREDICATE_DEFAULT_FILTER, IS_NULL_FILTER, PREDICATE_UNKNOWN_FILTER,
};

/// Estimate selectivity of a predicate expression (0.0..1.0).
pub(crate) fn estimate_selectivity(
    expr: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    match &expr.kind {
        ExprKind::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                let l = estimate_selectivity(left, column_stats);
                let r = estimate_selectivity(right, column_stats);
                l * r
            }
            BinOp::Or => {
                let l = estimate_selectivity(left, column_stats);
                let r = estimate_selectivity(right, column_stats);
                l + r - l * r
            }
            BinOp::Eq | BinOp::EqForNull => estimate_eq_selectivity(left, right, column_stats),
            BinOp::Ne => 1.0 - estimate_eq_selectivity(left, right, column_stats),
            BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                estimate_range_selectivity(left, right, *op, column_stats)
            }
            _ => PREDICATE_UNKNOWN_FILTER,
        },
        ExprKind::IsNull { negated, expr } => {
            let col_name = extract_column_name(expr);
            let null_frac = col_name
                .and_then(|name| column_stats.get(&name.to_lowercase()))
                .map(|cs| {
                    if cs.nulls_fraction > 0.0 {
                        cs.nulls_fraction
                    } else {
                        IS_NULL_FILTER
                    }
                })
                .unwrap_or(IS_NULL_FILTER);
            if *negated { 1.0 - null_frac } else { null_frac }
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } => {
            let col_name = extract_column_name(expr);
            let ndv = col_name
                .and_then(|name| column_stats.get(&name.to_lowercase()))
                .map(|cs| cs.distinct_values_count.max(1.0))
                .unwrap_or(0.0);

            let sel = if ndv > 0.0 {
                (list.len() as f64 / ndv).min(1.0)
            } else {
                IN_PREDICATE_DEFAULT_FILTER
            };
            if *negated { 1.0 - sel } else { sel }
        }
        ExprKind::Between {
            negated,
            expr,
            low,
            high,
        } => {
            // a BETWEEN low AND high  ==  a >= low AND a <= high
            // The ge * le product uses the same independence model as the BinOp::And arm;
            // this slightly overestimates selectivity for narrow symmetric ranges.
            let ge = estimate_range_selectivity(expr, low, BinOp::Ge, column_stats);
            let le = estimate_range_selectivity(expr, high, BinOp::Le, column_stats);
            let sel = ge * le;
            if *negated { 1.0 - sel } else { sel }
        }
        ExprKind::Like { negated, .. } => {
            let sel = PREDICATE_UNKNOWN_FILTER;
            if *negated { 1.0 - sel } else { sel }
        }
        ExprKind::UnaryOp {
            op: UnOp::Not,
            expr,
        } => 1.0 - estimate_selectivity(expr, column_stats),
        ExprKind::IsTruthValue { negated, .. } => {
            // IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE
            let base = 0.5;
            if *negated { 1.0 - base } else { base }
        }
        ExprKind::Nested(inner) => estimate_selectivity(inner, column_stats),
        _ => PREDICATE_UNKNOWN_FILTER,
    }
}

fn estimate_eq_selectivity(
    left: &TypedExpr,
    right: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    // col = literal: use 1/ndv
    let col_name = extract_column_name(left).or_else(|| extract_column_name(right));

    if let Some(name) = col_name
        && let Some(cs) = column_stats.get(&name.to_lowercase())
        && cs.distinct_values_count > 1.0
    {
        return 1.0 / cs.distinct_values_count;
    }
    PREDICATE_UNKNOWN_FILTER
}

fn estimate_range_selectivity(
    left: &TypedExpr,
    right: &TypedExpr,
    op: BinOp,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    // Try to use min/max range if available.
    let col_name = extract_column_name(left);
    let literal_val = extract_literal_f64(right);

    if let (Some(name), Some(val)) = (col_name, literal_val)
        && let Some(cs) = column_stats.get(&name.to_lowercase())
    {
        let min = cs.min_value;
        let max = cs.max_value;
        if min.is_finite() && max.is_finite() && max > min {
            let range = max - min;
            return match op {
                BinOp::Lt => ((val - min) / range).clamp(0.01, 0.99),
                BinOp::Le => ((val - min + 1.0) / range).clamp(0.01, 0.99),
                BinOp::Gt => ((max - val) / range).clamp(0.01, 0.99),
                BinOp::Ge => ((max - val + 1.0) / range).clamp(0.01, 0.99),
                _ => 0.5,
            };
        }
    }
    0.5 // default for range predicates
}

fn extract_literal_f64(expr: &TypedExpr) -> Option<f64> {
    match &expr.kind {
        ExprKind::Literal(LiteralValue::Int(v)) => Some(*v as f64),
        ExprKind::Literal(LiteralValue::LargeInt(v)) => Some(*v as f64),
        ExprKind::Literal(LiteralValue::Float(v)) => Some(*v),
        ExprKind::Literal(LiteralValue::Decimal(s)) => s.parse::<f64>().ok(),
        ExprKind::Cast { expr, .. } => extract_literal_f64(expr),
        ExprKind::Nested(inner) => extract_literal_f64(inner),
        _ => None,
    }
}

/// Extract column name from a simple column reference expression.
pub(crate) fn extract_column_name(expr: &TypedExpr) -> Option<&str> {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => Some(column.as_str()),
        ExprKind::Cast { expr, .. } => extract_column_name(expr),
        ExprKind::Nested(inner) => extract_column_name(inner),
        _ => None,
    }
}
