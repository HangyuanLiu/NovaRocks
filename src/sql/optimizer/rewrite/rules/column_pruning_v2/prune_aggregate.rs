//! PruneAggregateColumns — Phase 2 rule for Aggregate nodes.
//!
//! Output layout: `output_columns[0..group_by.len()]` are group-key outputs,
//! `output_columns[group_by.len()..]` are aggregate results in 1:1 order with
//! `aggregates`. Group-by output columns are always kept (they are semantically
//! required). Aggregate result columns are dropped when their output id is not
//! in `required_output_columns` (Gap 5).
//!
//! Unchanged when no aggregates are pruned (aggregates.len() same before/after).

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::*;

pub(crate) struct PruneAggregateColumns;

impl LogicalRewriteRule for PruneAggregateColumns {
    fn name(&self) -> &'static str {
        "PruneAggregateColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Aggregate(_))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Aggregate(mut node) = plan else {
            unreachable!()
        };

        // None means Phase 1 hasn't tagged this node — no-op.
        let Some(needed) = node.required_output_columns.clone() else {
            return Ok(RewriteResult::Unchanged);
        };

        let group_by_len = node.group_by.len();
        let original_agg_len = node.aggregates.len();

        // Group-key output_columns are always kept — semantically required.
        // Filter aggregate output_columns[group_by_len + i] and aggregates[i]
        // together based on whether the output id is in `needed`.
        let mut new_agg_output_columns = Vec::new();
        let mut new_aggregates = Vec::new();

        for (i, agg) in node.aggregates.into_iter().enumerate() {
            let out_col = &node.output_columns[group_by_len + i];
            if needed.contains(&out_col.column_id) {
                new_agg_output_columns.push(out_col.clone());
                new_aggregates.push(agg);
            }
        }

        // Unchanged check: if no aggregates were dropped, return Unchanged.
        if new_aggregates.len() == original_agg_len {
            return Ok(RewriteResult::Unchanged);
        }

        // Rebuild output_columns: group-key cols ++ surviving aggregate cols.
        let mut new_output_columns = node.output_columns[..group_by_len].to_vec();
        new_output_columns.extend(new_agg_output_columns);

        node.output_columns = new_output_columns;
        node.aggregates = new_aggregates;
        Ok(RewriteResult::Changed(LogicalPlan::Aggregate(node)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn col_ref_expr(id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn dummy_input() -> LogicalPlan {
        let table = TableDef {
            name: "t".to_string(),
            columns: vec![ColumnDef {
                name: "x".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        };
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table,
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(99),
                name: "x".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    #[test]
    fn prune_aggregate_drops_unneeded_aggregate_results() {
        // Aggregate[group_by=[k@1], sum(a)→201, avg(b)→202]
        // needed = {1, 201}  (group key + sum; avg not needed)
        // Expected: aggregates shrinks to [sum], output_columns = [k@1, sum@201]
        let id_k = ColumnId::new_for_test(1);
        let id_sum = ColumnId::new_for_test(201);
        let id_avg = ColumnId::new_for_test(202);
        let id_a = ColumnId::new_for_test(10);
        let id_b = ColumnId::new_for_test(20);

        let mut needed = HashSet::new();
        needed.insert(id_k);
        needed.insert(id_sum);

        let node = AggregateNode {
            input: Box::new(dummy_input()),
            group_by: vec![col_ref_expr(id_k, "k")],
            aggregates: vec![
                AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref_expr(id_a, "a")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                },
                AggregateCall {
                    name: "avg".to_string(),
                    args: vec![col_ref_expr(id_b, "b")],
                    distinct: false,
                    result_type: DataType::Float64,
                    order_by: vec![],
                },
            ],
            output_columns: vec![
                make_output_column(id_k, "k"),
                make_output_column(id_sum, "sum_a"),
                make_output_column(id_avg, "avg_b"),
            ],
            already_pushed: false,
            required_output_columns: Some(needed),
        };

        let plan = LogicalPlan::Aggregate(node);
        let rule = PruneAggregateColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let LogicalPlan::Aggregate(pruned) = changed else {
            panic!("expected Aggregate");
        };

        assert_eq!(pruned.aggregates.len(), 1);
        assert_eq!(pruned.aggregates[0].name, "sum");
        assert_eq!(pruned.output_columns.len(), 2);
        assert_eq!(pruned.output_columns[0].column_id, id_k);
        assert_eq!(pruned.output_columns[1].column_id, id_sum);
    }

    #[test]
    fn prune_aggregate_noop_when_required_output_columns_is_none() {
        let id_k = ColumnId::new_for_test(1);
        let id_sum = ColumnId::new_for_test(201);
        let id_a = ColumnId::new_for_test(10);

        let node = AggregateNode {
            input: Box::new(dummy_input()),
            group_by: vec![col_ref_expr(id_k, "k")],
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: vec![col_ref_expr(id_a, "a")],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![
                make_output_column(id_k, "k"),
                make_output_column(id_sum, "sum_a"),
            ],
            already_pushed: false,
            required_output_columns: None, // not tagged
        };

        let plan = LogicalPlan::Aggregate(node);
        let rule = PruneAggregateColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_aggregate_preserves_already_pushed_flag() {
        // Verify that already_pushed is preserved on the output node.
        let id_k = ColumnId::new_for_test(1);
        let id_sum = ColumnId::new_for_test(201);
        let id_avg = ColumnId::new_for_test(202);
        let id_a = ColumnId::new_for_test(10);
        let id_b = ColumnId::new_for_test(20);

        let mut needed = HashSet::new();
        needed.insert(id_k);
        needed.insert(id_sum);

        let node = AggregateNode {
            input: Box::new(dummy_input()),
            group_by: vec![col_ref_expr(id_k, "k")],
            aggregates: vec![
                AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref_expr(id_a, "a")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                },
                AggregateCall {
                    name: "avg".to_string(),
                    args: vec![col_ref_expr(id_b, "b")],
                    distinct: false,
                    result_type: DataType::Float64,
                    order_by: vec![],
                },
            ],
            output_columns: vec![
                make_output_column(id_k, "k"),
                make_output_column(id_sum, "sum_a"),
                make_output_column(id_avg, "avg_b"),
            ],
            already_pushed: true, // must be preserved
            required_output_columns: Some(needed),
        };

        let plan = LogicalPlan::Aggregate(node);
        let rule = PruneAggregateColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let LogicalPlan::Aggregate(pruned) = changed else {
            panic!("expected Aggregate");
        };

        assert!(
            pruned.already_pushed,
            "already_pushed flag must be preserved"
        );
    }
}
