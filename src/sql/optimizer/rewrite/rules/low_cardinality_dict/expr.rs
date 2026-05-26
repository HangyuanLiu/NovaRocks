//! Expression helpers used by the low-cardinality dictionary rewrite.

use arrow::datatypes::DataType;

use crate::engine::dictionary::model::DictionarySnapshot;
use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;

use super::context::DictScope;

/// Allowlist of deterministic single-string-argument functions that can be
/// represented as a query-local derived dictionary on top of a dict-encoded
/// source column. Used by Task 8 item 4 (derived dictionary expressions).
///
/// TODO(task-8-derived): wire this into the rewriter and codegen — Task 8
/// keeps the list here as the contract surface but does not yet allocate
/// query-local dict slots for derived expressions. See item 4 in the plan
/// (`docs/superpowers/plans/2026-05-26-low-cardinality-dictionary-rewrite.md`).
#[allow(dead_code)]
pub(crate) const DERIVED_DICT_FUNCTIONS: &[&str] = &["upper", "lower", "trim", "ltrim", "rtrim"];

/// Allowlist of aggregate function names whose argument may consume a dict
/// id slot directly (without a preceding Decode). For `min` / `max` the
/// rewriter additionally requires the snapshot to be order-preserving.
pub(crate) const DICT_AGG_FUNCTIONS: &[&str] = &[
    "count",
    "min",
    "max",
    "any_value",
    "array_agg",
    "approx_count_distinct",
];

/// True when two dictionary snapshots are compatible enough that a Join /
/// UNION ALL can safely compare and union their dict id columns directly,
/// without decoding either side first.
///
/// The check covers three fields:
///
/// * `owner.stable_key()` — same logical table.
/// * `version` — same on-disk encoding of the dictionary.
/// * `column_name` (case-insensitive) — same logical column.
///
/// `order_preserving` and `null_id` are intentionally NOT part of the key:
/// equi-join / UNION ALL semantics only need the encoding to agree, not the
/// ordering relation.
pub(crate) fn dict_keys_compatible(left: &DictionarySnapshot, right: &DictionarySnapshot) -> bool {
    left.owner.stable_key() == right.owner.stable_key()
        && left.version == right.version
        && left.column_name.eq_ignore_ascii_case(&right.column_name)
}

pub(crate) fn is_string_like(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

/// If `expr` is a `ColumnRef`, return the bare column name (no
/// qualifier).
///
/// `TODO(task-8)`: used once Task 8 starts rewriting non-trivial
/// expressions (function calls over dict columns); kept here so the
/// helper lives next to its siblings.
#[allow(dead_code)]
pub(crate) fn column_ref_name(expr: &TypedExpr) -> Option<&str> {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => Some(column.as_str()),
        _ => None,
    }
}

/// Rewrite a top-level column reference to point at the dict column,
/// when `scope` exposes a binding for that column. The synthesized
/// node carries `DataType::Int32` and preserves the source nullability.
/// Non-`ColumnRef` expressions and unknown columns are returned
/// unchanged.
pub(crate) fn rewrite_column_ref_with_scope(expr: &TypedExpr, scope: &DictScope) -> TypedExpr {
    if let ExprKind::ColumnRef {
        column, qualifier, ..
    } = &expr.kind
        && let Some(binding) = scope.get(column)
    {
        return TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: qualifier.clone(),
                column: binding.dict_column.clone(),
            },
            data_type: DataType::Int32,
            nullable: expr.nullable,
        };
    }
    expr.clone()
}

/// True when `expr` references (anywhere in its tree) a column that has
/// a dict mapping in `scope`. Used by the rewriter to decide whether a
/// Project item must keep its string source available (i.e. insert a
/// Decode boundary) or can be rewritten to the dict column.
///
/// `TODO(task-8)`: consumed by Task 8 when the rewriter inspects
/// project items and join predicates for derived dictionary usage.
#[allow(dead_code)]
pub(crate) fn expr_references_string_column(expr: &TypedExpr, scope: &DictScope) -> bool {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => scope.get(column).is_some(),
        ExprKind::Literal(_) | ExprKind::LambdaParamRef { .. } => false,
        ExprKind::BinaryOp { left, right, .. } => {
            expr_references_string_column(left, scope)
                || expr_references_string_column(right, scope)
        }
        ExprKind::UnaryOp { expr, .. } => expr_references_string_column(expr, scope),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            args.iter().any(|a| expr_references_string_column(a, scope))
        }
        ExprKind::LambdaFunction { body, .. } => expr_references_string_column(body, scope),
        ExprKind::Cast { expr, .. } | ExprKind::IsNull { expr, .. } => {
            expr_references_string_column(expr, scope)
        }
        ExprKind::InList { expr, list, .. } => {
            expr_references_string_column(expr, scope)
                || list.iter().any(|e| expr_references_string_column(e, scope))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            expr_references_string_column(expr, scope)
                || expr_references_string_column(low, scope)
                || expr_references_string_column(high, scope)
        }
        ExprKind::Like { expr, pattern, .. } => {
            expr_references_string_column(expr, scope)
                || expr_references_string_column(pattern, scope)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_deref()
                .is_some_and(|e| expr_references_string_column(e, scope))
                || when_then.iter().any(|(w, t)| {
                    expr_references_string_column(w, scope)
                        || expr_references_string_column(t, scope)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(|e| expr_references_string_column(e, scope))
        }
        ExprKind::IsTruthValue { expr, .. } | ExprKind::Nested(expr) => {
            expr_references_string_column(expr, scope)
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(|e| expr_references_string_column(e, scope))
                || partition_by
                    .iter()
                    .any(|e| expr_references_string_column(e, scope))
                || order_by
                    .iter()
                    .any(|s| expr_references_string_column(&s.expr, scope))
        }
        ExprKind::SubqueryPlaceholder { .. } => false,
        ExprKind::Lambda { body, .. } => expr_references_string_column(body, scope),
    }
}
