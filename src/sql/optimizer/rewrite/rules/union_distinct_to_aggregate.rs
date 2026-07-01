use crate::sql::optimizer::logical_props::make_column_ref_expr;
use crate::sql::optimizer::operator::{
    AggregateOutputLayout, LogicalAggregateOp, Operator, UnionOp,
};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct UnionDistinctToAggregate;

impl LogicalRewriteRule for UnionDistinctToAggregate {
    fn name(&self) -> &'static str {
        "UnionDistinctToAggregate"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::LogicalNormalize
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Union,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        matches!(&expr.op, Operator::LogicalUnion(op) if !op.all)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            children,
            required_output_columns,
        } = expr;
        let Operator::LogicalUnion(union) = op else {
            return Ok(RewriteResult::Unchanged);
        };
        if union.all {
            return Ok(RewriteResult::Unchanged);
        }

        let UnionOp {
            all: _,
            output_columns,
            child_output_columns,
        } = union;
        let group_by = {
            let arena = ctx.scalar_arena();
            let mut arena = arena.borrow_mut();
            output_columns
                .iter()
                .map(|column| make_column_ref_expr(&mut arena, column))
                .collect()
        };
        let aggregate = LogicalAggregateOp::single(
            group_by,
            vec![],
            AggregateOutputLayout::new(output_columns.clone(), vec![]),
            output_columns.clone(),
        );
        let union_all = OptExpr {
            op: Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns,
                child_output_columns,
            }),
            children,
            required_output_columns: None,
        };

        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalAggregate(aggregate),
            children: vec![union_all],
            required_output_columns,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::ExprKind;
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::OutputColumn;
    use crate::sql::optimizer::operator::ValuesOp;
    use crate::sql::optimizer::scalar::{ScalarArena, ScalarNode};
    use crate::sql::planner::optimizer_bridge::scalar::materialize;

    fn output_column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn values(columns: Vec<OutputColumn>) -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns,
        }))
    }

    fn union_distinct_plan() -> OptExpr {
        let out_k = output_column(10, "k", DataType::Int64, false);
        let out_s = output_column(11, "s", DataType::Utf8, true);
        let left_k = output_column(20, "k", DataType::Int64, false);
        let left_s = output_column(21, "s", DataType::Utf8, true);
        let right_k = output_column(30, "k", DataType::Int64, false);
        let right_s = output_column(31, "s", DataType::Utf8, true);
        OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: false,
                output_columns: vec![out_k, out_s],
                child_output_columns: vec![
                    vec![left_k.clone(), left_s.clone()],
                    vec![right_k.clone(), right_s.clone()],
                ],
            }),
            vec![values(vec![left_k, left_s]), values(vec![right_k, right_s])],
        )
    }

    fn union_all_plan() -> OptExpr {
        let out = output_column(10, "k", DataType::Int64, false);
        let left = output_column(20, "k", DataType::Int64, false);
        let right = output_column(30, "k", DataType::Int64, false);
        OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![out],
                child_output_columns: vec![vec![left.clone()], vec![right.clone()]],
            }),
            vec![values(vec![left]), values(vec![right])],
        )
    }

    fn apply_raw(plan: OptExpr) -> (RewriteResult, Rc<RefCell<ScalarArena>>) {
        let arena = Rc::new(RefCell::new(ScalarArena::new()));
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_scalar_arena(Rc::clone(&arena));
        let result = UnionDistinctToAggregate
            .apply(plan, &mut ctx)
            .expect("rewrite rule should not error");
        (result, arena)
    }

    fn apply_changed(plan: OptExpr) -> (OptExpr, Rc<RefCell<ScalarArena>>) {
        let (result, arena) = apply_raw(plan);
        let RewriteResult::Changed(rewritten) = result else {
            panic!("expected Changed result, got {result:?}");
        };
        (rewritten, arena)
    }

    fn column_ids(columns: &[OutputColumn]) -> Vec<u32> {
        columns.iter().map(|column| column.column_id.0).collect()
    }

    #[test]
    fn rewrites_union_distinct_to_aggregate_over_union_all() {
        let (rewritten, arena) = apply_changed(union_distinct_plan());

        let Operator::LogicalAggregate(agg) = &rewritten.op else {
            panic!("expected LogicalAggregate root, got {:?}", rewritten.op);
        };
        assert_eq!(agg.group_by.len(), 2);
        assert!(agg.aggregates.is_empty());
        assert_eq!(column_ids(&agg.output_columns), vec![10, 11]);
        assert_eq!(
            column_ids(&agg.output_layout.group_key_columns),
            vec![10, 11]
        );
        assert!(agg.output_layout.aggregate_columns.is_empty());

        let arena = arena.borrow();
        assert!(matches!(
            arena.node(agg.group_by[0]),
            ScalarNode::ColumnRef(column_id) if *column_id == ColumnId::new_for_test(10)
        ));
        assert_eq!(arena.data_type(agg.group_by[0]), &DataType::Int64);
        assert!(!arena.nullable(agg.group_by[0]));
        assert!(matches!(
            arena.node(agg.group_by[1]),
            ScalarNode::ColumnRef(column_id) if *column_id == ColumnId::new_for_test(11)
        ));
        assert_eq!(arena.data_type(agg.group_by[1]), &DataType::Utf8);
        assert!(arena.nullable(agg.group_by[1]));
        let materialized_group_by_names: Vec<_> = agg
            .group_by
            .iter()
            .map(|group_expr| {
                let expr = materialize(&arena, *group_expr);
                let ExprKind::ColumnRef { column, .. } = expr.kind else {
                    panic!("expected materialized aggregate group key to be ColumnRef");
                };
                column
            })
            .collect();
        assert_eq!(materialized_group_by_names, vec!["k", "s"]);

        assert_eq!(rewritten.children.len(), 1);
        let union_child = &rewritten.children[0];
        let Operator::LogicalUnion(union) = &union_child.op else {
            panic!("expected LogicalUnion child, got {:?}", union_child.op);
        };
        assert!(union.all);
        assert_eq!(column_ids(&union.output_columns), vec![10, 11]);
        assert_eq!(union.child_output_columns.len(), 2);
        assert_eq!(
            union
                .child_output_columns
                .iter()
                .map(|columns| column_ids(columns))
                .collect::<Vec<_>>(),
            vec![vec![20, 21], vec![30, 31]]
        );
        assert_eq!(union_child.children.len(), 2);
    }

    #[test]
    fn leaves_union_all_unchanged() {
        let (result, _arena) = apply_raw(union_all_plan());
        assert!(matches!(result, RewriteResult::Unchanged));
    }

    #[test]
    fn preserves_parent_required_outputs_on_aggregate_root() {
        let mut plan = union_distinct_plan();
        let mut required = HashSet::new();
        required.insert(ColumnId::new_for_test(10));
        plan.required_output_columns = Some(required.clone());

        let (rewritten, _arena) = apply_changed(plan);

        assert_eq!(rewritten.required_output_columns, Some(required));
        assert!(
            rewritten.children[0].required_output_columns.is_none(),
            "the later TagRequiredColumns stage owns child required-output stamping"
        );
    }
}
