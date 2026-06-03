use std::collections::HashMap;

use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
use crate::sql::optimizer::statistics::ColumnStatistic;

use super::selectivity::extract_column_name;

/// Get the NDV for an expression from column statistics.
pub(crate) fn get_expr_ndv(
    expr: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    // A column is only useful for cardinality if it carries a real NDV (> 1).
    // ColumnStatistic::unknown() (propagated for no-stats / managed-lake tables)
    // reports distinct_values_count = 1.0; treating that as a true NDV would make
    // get_join_key_ndv divide left*right by ~1 and explode joins to near
    // cross-products. Mirror the `> 1.0` guard estimate_eq_selectivity uses and
    // fall back to the default NDV for unknown/degenerate columns.
    if let Some(name) = extract_column_name(expr)
        && let Some(cs) = column_stats.get(&name.to_lowercase())
        && cs.distinct_values_count > 1.0
    {
        return cs.distinct_values_count;
    }
    10.0
}

/// For a join condition, extract the max NDV of join keys from both sides.
pub(crate) fn get_join_key_ndv(
    condition: &TypedExpr,
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    match &condition.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq | BinOp::EqForNull,
            right,
        } => {
            let left_ndv = get_expr_ndv(left, left_stats).max(get_expr_ndv(left, right_stats));
            let right_ndv = get_expr_ndv(right, left_stats).max(get_expr_ndv(right, right_stats));
            left_ndv.max(right_ndv).max(1.0)
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let l = get_join_key_ndv(left, left_stats, right_stats);
            let r = get_join_key_ndv(right, left_stats, right_stats);
            l.max(r)
        }
        _ => 1.0,
    }
}

/// A column's NDV can never exceed the number of surviving rows.
///
/// Invalid row counts or NDVs collapse to the conservative minimum NDV of 1.0.
pub(crate) fn cap_ndv_at_rows(ndv: f64, rows: f64) -> f64 {
    if !rows.is_finite() || rows <= 0.0 {
        return 1.0;
    }

    let ndv = if !ndv.is_finite() || ndv < 1.0 {
        1.0
    } else {
        ndv
    };
    ndv.min(rows).max(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::DataType;

    use crate::sql::analysis::{ExprKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::statistics::ColumnStatistic;

    fn col_ref(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    #[test]
    fn get_expr_ndv_ignores_unknown_ndv() {
        // OQ-3 propagates ColumnStatistic::unknown() (distinct_values_count = 1.0)
        // for no-stats / managed-lake tables. get_expr_ndv must treat that as
        // "no information" and return the 10.0 default, otherwise get_join_key_ndv
        // would divide left*right by ~1 and explode joins to near cross-products.
        let mut column_stats: HashMap<String, ColumnStatistic> = HashMap::new();
        column_stats.insert("unknown_col".to_string(), ColumnStatistic::unknown());
        assert_eq!(column_stats["unknown_col"].distinct_values_count, 1.0);
        let unknown_expr = col_ref("unknown_col");
        assert_eq!(get_expr_ndv(&unknown_expr, &column_stats), 10.0);

        // A degenerate ndv of exactly 1.0 (not via unknown()) is also ignored.
        column_stats.insert(
            "degenerate_col".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: 1.0,
                ..Default::default()
            },
        );
        let degenerate_expr = col_ref("degenerate_col");
        assert_eq!(get_expr_ndv(&degenerate_expr, &column_stats), 10.0);

        // A real NDV (> 1) is still used verbatim.
        column_stats.insert(
            "real_col".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: 50.0,
                ..Default::default()
            },
        );
        let real_expr = col_ref("real_col");
        assert_eq!(get_expr_ndv(&real_expr, &column_stats), 50.0);

        // An unknown column reference (absent from the map) also defaults.
        let missing_expr = col_ref("missing_col");
        assert_eq!(get_expr_ndv(&missing_expr, &column_stats), 10.0);
    }

    #[test]
    fn filter_ndv_capped_at_output_rows() {
        // NDV cannot exceed surviving rows.
        assert_eq!(cap_ndv_at_rows(1000.0, 50.0), 50.0);
        assert_eq!(cap_ndv_at_rows(30.0, 50.0), 30.0);
    }

    #[test]
    fn filter_ndv_cap_handles_invalid_inputs_conservatively() {
        assert_eq!(cap_ndv_at_rows(1000.0, 0.0), 1.0);
        assert_eq!(cap_ndv_at_rows(1000.0, f64::NAN), 1.0);
        assert_eq!(cap_ndv_at_rows(1000.0, f64::INFINITY), 1.0);
        assert_eq!(cap_ndv_at_rows(f64::NAN, 50.0), 1.0);
        assert_eq!(cap_ndv_at_rows(f64::INFINITY, 50.0), 1.0);
        assert_eq!(cap_ndv_at_rows(0.5, 50.0), 1.0);
    }
}
