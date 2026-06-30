//! PruneAggregateColumns — Phase 2 rule for Aggregate nodes.
//!
//! **Currently a no-op.**
//!
//! `LogicalAggregateOp.output_columns` starts with the group-by output prefix used by
//! the physical aggregate layout.  The aggregate function outputs themselves
//! must be identified from the output_column_id in each aggregate spec, not from
//! SELECT projection order or display names.
//!
//! Per-aggregate output pruning remains disabled in this rule until it can
//! preserve every required aggregate call by ColumnId.  Until then, this rule
//! returns `Unchanged` unconditionally.

use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneAggregateColumns;

impl LogicalRewriteRule for PruneAggregateColumns {
    fn name(&self) -> &'static str {
        "PruneAggregateColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Aggregate,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // No-op: see module-level doc comment.
        //
        // Per-aggregate output pruning (Gap 5) remains disabled until upper
        // projection refs and codegen binding use ScalarAggregateSpec output
        // column ids.
        let _ = expr; // suppress unused-variable warning
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        AggStage, LogicalAggregateOp, Operator, ScalarAggregateSpec, ScanOp,
    };
    use crate::sql::optimizer::opt_expr::OptExpr;
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
            is_internal: false,
        }
    }

    fn dummy_input() -> OptExpr {
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
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".to_string(),
            table,
            alias: None,
            stats_ref: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(99),
                name: "x".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    // -----------------------------------------------------------------------
    // Bug A regression: PruneAggregateColumns must be a no-op.
    //
    // Aggregate function outputs are identified by ScalarAggregateSpec output
    // column ids, not by SELECT projection order or display names. Until this
    // rule performs that ColumnId-aware pruning, it must always return Unchanged
    // regardless of what required_output_columns contains.
    // -----------------------------------------------------------------------

    /// Rule is a no-op even when needed contains only some aggregate output ids.
    #[test]
    fn prune_aggregate_is_noop_regardless_of_needed_set() {
        let id_y = ColumnId::new_for_test(1);
        let id_count_oc = ColumnId::new_for_test(301);
        let id_sum_oc = ColumnId::new_for_test(302);

        let mut needed = HashSet::new();
        needed.insert(id_count_oc); // only count needed, not sum

        let mut expr = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp {
                stage: AggStage::Single,
                group_by: vec![],
                aggregates: vec![
                    ScalarAggregateSpec {
                        output_column_id: id_count_oc,
                        name: "count".to_string(),
                        args: vec![],
                        distinct: false,
                        order_by: vec![],
                    },
                    ScalarAggregateSpec {
                        output_column_id: id_sum_oc,
                        name: "sum".to_string(),
                        args: vec![],
                        distinct: false,
                        order_by: vec![],
                    },
                ],
                output_columns: vec![
                    make_output_column(id_y, "y"),
                    make_output_column(id_count_oc, "count"),
                    make_output_column(id_sum_oc, "sum_x"),
                ],
                is_merge: vec![false, false],
                is_split: false,
            }),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let rule = PruneAggregateColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneAggregateColumns must be a no-op (Gap 5 not yet implemented); got {result:?}"
        );
    }

    /// Rule is also a no-op when required_output_columns is None (untagged).
    #[test]
    fn prune_aggregate_noop_when_required_output_columns_is_none() {
        let id_k = ColumnId::new_for_test(1);
        let id_sum = ColumnId::new_for_test(201);

        let expr = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp {
                stage: AggStage::Single,
                group_by: vec![],
                aggregates: vec![ScalarAggregateSpec {
                    output_column_id: id_sum,
                    name: "sum".to_string(),
                    args: vec![],
                    distinct: false,
                    order_by: vec![],
                }],
                output_columns: vec![
                    make_output_column(id_k, "k"),
                    make_output_column(id_sum, "sum_a"),
                ],
                is_merge: vec![false],
                is_split: false,
            }),
            vec![dummy_input()],
        );
        // required_output_columns = None (default)

        let rule = PruneAggregateColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }
}
