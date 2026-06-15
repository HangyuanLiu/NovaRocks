use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::split_and;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct RankingWindowPredicatePushdownRule;

impl LogicalRewriteRule for RankingWindowPredicatePushdownRule {
    fn name(&self) -> &'static str {
        "RankingWindowPredicatePushdown"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Filter(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        Ok(RewriteResult::Unchanged)
    }
}

/// Smallest finite upper bound K (>= 1) such that the conjunctive predicate can
/// only pass rows with rank_col <= K.  Returns None if no finite positive bound
/// exists (e.g., lower-bound-only predicates, K <= 0, or no reference to rank_col).
pub(crate) fn rank_upper_bound(predicate: &TypedExpr, rank_col: ColumnId) -> Option<usize> {
    let mut best: Option<i64> = None;
    for conj in split_and(predicate.clone()) {
        if let Some(k) = conjunct_upper_bound(&conj, rank_col) {
            best = Some(best.map_or(k, |b| b.min(k)));
        }
    }
    match best {
        Some(k) if k >= 1 => usize::try_from(k).ok(),
        _ => None,
    }
}

fn is_rank_col(e: &TypedExpr, rank_col: ColumnId) -> bool {
    matches!(&e.kind, ExprKind::ColumnRef { column_id, .. } if *column_id == rank_col)
}

fn int_lit(e: &TypedExpr) -> Option<i64> {
    match &e.kind {
        ExprKind::Literal(LiteralValue::Int(v)) => Some(*v),
        _ => None,
    }
}

fn conjunct_upper_bound(e: &TypedExpr, rank_col: ColumnId) -> Option<i64> {
    match &e.kind {
        ExprKind::BinaryOp { left, op, right } => {
            let (lit, col_on_left) = if is_rank_col(left, rank_col) {
                (int_lit(right)?, true)
            } else if is_rank_col(right, rank_col) {
                (int_lit(left)?, false)
            } else {
                return None;
            };
            match (op, col_on_left) {
                // rank_col <= lit  or  lit >= rank_col
                (BinOp::Le, true) | (BinOp::Ge, false) => Some(lit),
                // rank_col < lit  or  lit > rank_col
                (BinOp::Lt, true) | (BinOp::Gt, false) => Some(lit - 1),
                // rank_col = lit  or  lit = rank_col
                (BinOp::Eq, _) => Some(lit),
                _ => None,
            }
        }
        // BETWEEN low AND high: upper bound is `high`
        ExprKind::Between {
            expr,
            high,
            negated: false,
            ..
        } if is_rank_col(expr, rank_col) => int_lit(high),
        // IN (v1, v2, ...): upper bound is the max value in the list
        ExprKind::InList {
            expr,
            list,
            negated: false,
        } if is_rank_col(expr, rank_col) => list
            .iter()
            .map(int_lit)
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .max(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{RankingWindowPredicatePushdownRule, rank_upper_bound};
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn col(id: ColumnId) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: format!("rk_{}", id.0),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn int(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn binop(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn le(col: TypedExpr, v: i64) -> TypedExpr {
        binop(col, BinOp::Le, int(v))
    }

    fn lt(col: TypedExpr, v: i64) -> TypedExpr {
        binop(col, BinOp::Lt, int(v))
    }

    fn eq(col: TypedExpr, v: i64) -> TypedExpr {
        binop(col, BinOp::Eq, int(v))
    }

    fn ge(col: TypedExpr, v: i64) -> TypedExpr {
        binop(col, BinOp::Ge, int(v))
    }

    fn between(expr: TypedExpr, low_v: i64, high_v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Between {
                expr: Box::new(expr),
                low: Box::new(int(low_v)),
                high: Box::new(int(high_v)),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn in_list(expr: TypedExpr, values: &[i64]) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(expr),
                list: values.iter().map(|&v| int(v)).collect(),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: rule is recognized by the registry
    // -----------------------------------------------------------------------

    #[test]
    fn ranking_window_rule_is_known() {
        assert!(
            crate::sql::optimizer::rewrite::registry::is_known_rewrite_rule_name(
                "RankingWindowPredicatePushdown"
            )
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: rank_upper_bound extracts <=, <, =, BETWEEN, IN correctly
    //         and returns None for lower-bound-only / K<=0 / other column
    // -----------------------------------------------------------------------

    #[test]
    fn rank_upper_bound_extracts_le_lt_eq_between_in() {
        let rk = ColumnId::new_for_test(7);
        let other = ColumnId::new_for_test(99);

        // rk <= 5  -> Some(5)
        assert_eq!(rank_upper_bound(&le(col(rk), 5), rk), Some(5));

        // rk < 5   -> Some(4)
        assert_eq!(rank_upper_bound(&lt(col(rk), 5), rk), Some(4));

        // rk = 3   -> Some(3)
        assert_eq!(rank_upper_bound(&eq(col(rk), 3), rk), Some(3));

        // BETWEEN 2 AND 9  -> Some(9)
        assert_eq!(rank_upper_bound(&between(col(rk), 2, 9), rk), Some(9));

        // IN (1, 3, 5)  -> Some(5)
        assert_eq!(rank_upper_bound(&in_list(col(rk), &[1, 3, 5]), rk), Some(5));

        // rk >= 5  (lower bound only) -> None
        assert_eq!(rank_upper_bound(&ge(col(rk), 5), rk), None);

        // rk <= 0  (K <= 0) -> None
        assert_eq!(rank_upper_bound(&le(col(rk), 0), rk), None);

        // comparison on a DIFFERENT column -> None
        assert_eq!(rank_upper_bound(&le(col(other), 5), rk), None);
    }

    // Verify the rule struct itself is importable and constructable.
    #[test]
    fn ranking_window_predicate_pushdown_rule_is_constructable() {
        let _ = RankingWindowPredicatePushdownRule;
    }
}
