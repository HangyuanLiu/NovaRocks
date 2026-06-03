use std::collections::HashMap;

use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence};

use super::arith::damped_conjunction;
use super::ndv::get_expr_ndv;
use super::selectivity::{estimate_selectivity, extract_column_name};

#[derive(Default)]
pub(crate) struct JoinConditionEstimate {
    pub eq_key_ndvs: Vec<(f64, f64, Confidence)>,
    pub eq_key_pairs: Vec<(String, String)>,
    pub residual_selectivity: Option<(f64, Confidence)>,
}

pub(crate) fn estimate_join_condition(
    condition: Option<&TypedExpr>,
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
) -> JoinConditionEstimate {
    let Some(condition) = condition else {
        return JoinConditionEstimate::default();
    };

    let mut estimate = JoinConditionEstimate::default();
    let mut residuals = Vec::new();
    collect_join_conjuncts(
        condition,
        left_stats,
        right_stats,
        &mut estimate,
        &mut residuals,
    );

    if !residuals.is_empty() {
        let combined_stats = combined_column_statistics(left_stats, right_stats);
        let selectivities: Vec<_> = residuals
            .iter()
            .map(|expr| estimate_selectivity(expr, &combined_stats))
            .collect();
        estimate.residual_selectivity =
            Some((damped_conjunction(&selectivities), Confidence::Estimated));
    }

    estimate
}

fn collect_join_conjuncts<'a>(
    expr: &'a TypedExpr,
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
    estimate: &mut JoinConditionEstimate,
    residuals: &mut Vec<&'a TypedExpr>,
) {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_join_conjuncts(left, left_stats, right_stats, estimate, residuals);
            collect_join_conjuncts(right, left_stats, right_stats, estimate, residuals);
        }
        ExprKind::Nested(inner) => {
            collect_join_conjuncts(inner, left_stats, right_stats, estimate, residuals);
        }
        _ => {
            if !try_collect_equi_key(expr, left_stats, right_stats, estimate) {
                residuals.push(expr);
            }
        }
    }
}

fn try_collect_equi_key(
    expr: &TypedExpr,
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
    estimate: &mut JoinConditionEstimate,
) -> bool {
    let ExprKind::BinaryOp {
        left,
        op: BinOp::Eq | BinOp::EqForNull,
        right,
    } = &expr.kind
    else {
        return false;
    };

    let Some(left_name) = lower_column_name(left) else {
        return false;
    };
    let Some(right_name) = lower_column_name(right) else {
        return false;
    };

    let forward = left_stats.contains_key(&left_name) && right_stats.contains_key(&right_name);
    let reverse = left_stats.contains_key(&right_name) && right_stats.contains_key(&left_name);
    let (left_expr, right_expr, left_key, right_key) = match (forward, reverse) {
        (true, false) => (left.as_ref(), right.as_ref(), left_name, right_name),
        (false, true) => (right.as_ref(), left.as_ref(), right_name, left_name),
        (true, true) if left_name == right_name => {
            estimate.eq_key_ndvs.push((
                get_expr_ndv(left, left_stats),
                get_expr_ndv(right, right_stats),
                Confidence::Estimated,
            ));
            return true;
        }
        _ => return false,
    };

    estimate.eq_key_pairs.push((left_key, right_key));
    estimate.eq_key_ndvs.push((
        get_expr_ndv(left_expr, left_stats),
        get_expr_ndv(right_expr, right_stats),
        Confidence::Estimated,
    ));
    true
}

fn lower_column_name(expr: &TypedExpr) -> Option<String> {
    extract_column_name(expr).map(|name| name.to_lowercase())
}

fn combined_column_statistics(
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
) -> HashMap<String, ColumnStatistic> {
    let mut combined = left_stats.clone();
    combined.extend(right_stats.clone());
    combined
}
