//! Implementation rule: multi-phase DISTINCT aggregation.
//!
//! Matches a `LogicalAggregate` with at least one DISTINCT aggregate call,
//! where all DISTINCT calls share a single simple column as their argument.
//! Emits one alternative physical chain:
//!   - 3-phase (LOCAL -> DISTINCT_GLOBAL -> GLOBAL) when `group_by` is non-empty.
//!   - 4-phase (LOCAL -> DISTINCT_GLOBAL -> DISTINCT_LOCAL -> GLOBAL) when scalar.
//!
//! Mirrors StarRocks's `SplitAggregateRule` / `AggType.java` convention.

use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalAggregateOp, Operator};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::planner::plan::AggregateCall;

pub(crate) struct SplitDistinctAgg;

impl Rule for SplitDistinctAgg {
    fn name(&self) -> &str {
        "SplitDistinctAgg"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregate(a) if a.aggregates.iter().any(|c| c.distinct))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAggregate(agg) = &expr.op else {
            return vec![];
        };

        // Validate single-DISTINCT-column precondition.
        let distinct_col = match extract_single_distinct_col(&agg.aggregates) {
            Some(c) => c,
            None => return vec![], // multi-column DISTINCT, or multiple different DISTINCT cols
        };

        // Partition aggregates into DISTINCT-bearing (which are deduped away at LOCAL)
        // and non-DISTINCT (which flow as merge states through the phases).
        let non_distinct: Vec<AggregateCall> = agg
            .aggregates
            .iter()
            .filter(|c| !c.distinct)
            .cloned()
            .collect();

        if agg.group_by.is_empty() {
            apply_four_phase(expr, memo, agg, &distinct_col, &non_distinct)
        } else {
            apply_three_phase(expr, memo, agg, &distinct_col, &non_distinct)
        }
    }
}

/// Return the shared DISTINCT column if every DISTINCT aggregate takes exactly
/// one argument and all such arguments are the same simple `ColumnRef`.
/// Returns `None` for:
///   - no DISTINCT calls at all (shouldn't happen -- `matches` filters this)
///   - multi-arg DISTINCT (`count(distinct a, b)`)
///   - multiple distinct columns (`count(distinct a), count(distinct b)`)
///   - DISTINCT arg that is not a plain ColumnRef
fn extract_single_distinct_col(calls: &[AggregateCall]) -> Option<TypedExpr> {
    let mut distinct_calls = calls.iter().filter(|c| c.distinct);
    let first = distinct_calls.next()?;
    if first.args.len() != 1 {
        return None;
    }
    if !matches!(first.args[0].kind, ExprKind::ColumnRef { .. }) {
        return None;
    }
    for c in distinct_calls {
        if c.args.len() != 1 {
            return None;
        }
        if !typed_exprs_structurally_equal(&c.args[0], &first.args[0]) {
            return None;
        }
    }
    Some(first.args[0].clone())
}

fn typed_exprs_structurally_equal(a: &TypedExpr, b: &TypedExpr) -> bool {
    match (&a.kind, &b.kind) {
        (
            ExprKind::ColumnRef {
                qualifier: qa,
                column: ca,
            },
            ExprKind::ColumnRef {
                qualifier: qb,
                column: cb,
            },
        ) => qa == qb && ca == cb,
        _ => false,
    }
}

fn apply_three_phase(
    _expr: &MExpr,
    _memo: &mut Memo,
    _agg: &LogicalAggregateOp,
    _distinct_col: &TypedExpr,
    _non_distinct: &[AggregateCall],
) -> Vec<NewExpr> {
    // Implemented in Task 4.
    vec![]
}

fn apply_four_phase(
    _expr: &MExpr,
    _memo: &mut Memo,
    _agg: &LogicalAggregateOp,
    _distinct_col: &TypedExpr,
    _non_distinct: &[AggregateCall],
) -> Vec<NewExpr> {
    // Implemented in Task 5.
    vec![]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, TypedExpr};
    use crate::sql::optimizer::memo::Memo;
    use crate::sql::optimizer::operator::{LogicalAggregateOp, LogicalScanOp};
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan_group(memo: &mut Memo) -> usize {
        let m = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".into(),
                table: crate::sql::catalog::TableDef {
                    name: "t".into(),
                    columns: vec![],
                    storage: crate::sql::catalog::TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
            }),
            children: vec![],
        };
        memo.new_group(m)
    }

    fn count_distinct(arg_name: &str) -> AggregateCall {
        AggregateCall {
            name: "count".into(),
            args: vec![col(arg_name)],
            distinct: true,
            result_type: DataType::Int64,
            order_by: vec![],
        }
    }

    fn sum_non_distinct(arg_name: &str) -> AggregateCall {
        AggregateCall {
            name: "sum".into(),
            args: vec![col(arg_name)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
        }
    }

    #[test]
    fn matches_when_any_distinct() {
        let op = Operator::LogicalAggregate(LogicalAggregateOp {
            group_by: vec![],
            aggregates: vec![count_distinct("x"), sum_non_distinct("a")],
            output_columns: vec![],
        });
        assert!(SplitDistinctAgg.matches(&op));
    }

    #[test]
    fn does_not_match_when_no_distinct() {
        let op = Operator::LogicalAggregate(LogicalAggregateOp {
            group_by: vec![],
            aggregates: vec![sum_non_distinct("a")],
            output_columns: vec![],
        });
        assert!(!SplitDistinctAgg.matches(&op));
    }

    #[test]
    fn apply_skips_multi_arg_distinct() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let two_arg = AggregateCall {
            name: "count".into(),
            args: vec![col("a"), col("b")],
            distinct: true,
            result_type: DataType::Int64,
            order_by: vec![],
        };
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![],
                aggregates: vec![two_arg],
                output_columns: vec![],
            }),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn apply_skips_distinct_on_different_cols() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![],
                aggregates: vec![count_distinct("a"), count_distinct("b")],
                output_columns: vec![],
            }),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn extracts_distinct_col_for_same_col_multi_distinct() {
        // count(distinct x) + sum(distinct x) -- same col. Accepts both.
        let sum_distinct_x = AggregateCall {
            name: "sum".into(),
            args: vec![col("x")],
            distinct: true,
            result_type: DataType::Int64,
            order_by: vec![],
        };
        let col_out = extract_single_distinct_col(&[count_distinct("x"), sum_distinct_x]);
        assert!(col_out.is_some(), "expected Some for same-column multi-DISTINCT");
        let ExprKind::ColumnRef { column, .. } = &col_out.unwrap().kind else {
            panic!("expected ColumnRef");
        };
        assert_eq!(column, "x");
    }
}
