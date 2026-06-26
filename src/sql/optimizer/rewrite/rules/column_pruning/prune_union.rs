//! PruneUnionColumns — Phase 2 rule for Union nodes.
//!
//! Filters `UnionOp.output_columns` to only those whose `column_id`
//! is in `required_output_columns`. Keeps at least one column to preserve
//! a valid output schema (Gap 4).
//!
//! The set-op node's `output_columns` and `child_output_columns` metadata must
//! be pruned by the same output positions. Branch inputs are NOT modified here
//! — the Phase-1 tagging pass has already tagged each branch with the
//! position-restricted required set, and the branches' own prune rules handle
//! their pruning independently.

use std::collections::HashSet;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::column_pruning::keep_at_least_one;

pub(crate) struct PruneUnionColumns;

impl LogicalRewriteRule for PruneUnionColumns {
    fn name(&self) -> &'static str {
        "PruneUnionColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Union,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            children,
            required_output_columns,
        } = expr;
        let Operator::LogicalUnion(mut node) = op else {
            unreachable!()
        };

        if !node.all {
            return Ok(RewriteResult::Unchanged);
        }

        // None means Phase 1 hasn't tagged this node — no-op.
        let Some(needed) = required_output_columns.clone() else {
            return Ok(RewriteResult::Unchanged);
        };

        let original_len = node.output_columns.len();

        // Determine which ids to keep.
        let filtered: HashSet<ColumnId> = node
            .output_columns
            .iter()
            .map(|c| c.column_id)
            .filter(|id| needed.contains(id))
            .collect();

        // Ensure at least one column survives.
        let fallback = node
            .output_columns
            .first()
            .map(|c| c.column_id)
            .unwrap_or(ColumnId::UNSET);
        let keep_ids = keep_at_least_one(filtered, fallback);

        let keep_positions: Vec<usize> = node
            .output_columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| keep_ids.contains(&column.column_id).then_some(idx))
            .collect();

        let new_output_columns: Vec<_> = keep_positions
            .iter()
            .map(|idx| node.output_columns[*idx].clone())
            .collect();

        if new_output_columns.len() == original_len {
            return Ok(RewriteResult::Unchanged);
        }

        if !node.child_output_columns.is_empty() {
            node.child_output_columns = node
                .child_output_columns
                .into_iter()
                .map(|columns| {
                    keep_positions
                        .iter()
                        .filter_map(|idx| columns.get(*idx).cloned())
                        .collect()
                })
                .collect();
        }
        node.output_columns = new_output_columns;
        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalUnion(node),
            children,
            required_output_columns,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{Operator, UnionOp, ValuesOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use arrow::datatypes::DataType;

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }
    }

    fn dummy_input() -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        }))
    }

    #[test]
    fn prune_union_filters_to_needed_subset() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut needed = HashSet::new();
        needed.insert(id_b);

        let mut expr = OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_b, "b"),
                    make_output_column(id_c, "c"),
                ],
                child_output_columns: vec![],
            }),
            vec![dummy_input(), dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let rule = PruneUnionColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalUnion(pruned) = &changed.op else {
            panic!("expected Union");
        };

        assert_eq!(pruned.output_columns.len(), 1);
        assert_eq!(pruned.output_columns[0].column_id, id_b);
        // inputs are untouched
        assert_eq!(changed.children.len(), 2);
    }

    #[test]
    fn prune_union_filters_child_output_columns_by_position() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);
        let left_a = ColumnId::new_for_test(11);
        let left_b = ColumnId::new_for_test(12);
        let left_c = ColumnId::new_for_test(13);
        let right_a = ColumnId::new_for_test(21);
        let right_b = ColumnId::new_for_test(22);
        let right_c = ColumnId::new_for_test(23);

        let mut needed = HashSet::new();
        needed.insert(id_a);
        needed.insert(id_c);

        let mut expr = OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_b, "b"),
                    make_output_column(id_c, "c"),
                ],
                child_output_columns: vec![
                    vec![
                        make_output_column(left_a, "left_a"),
                        make_output_column(left_b, "left_b"),
                        make_output_column(left_c, "left_c"),
                    ],
                    vec![
                        make_output_column(right_a, "right_a"),
                        make_output_column(right_b, "right_b"),
                        make_output_column(right_c, "right_c"),
                    ],
                ],
            }),
            vec![dummy_input(), dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let rule = PruneUnionColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalUnion(pruned) = &changed.op else {
            panic!("expected Union");
        };

        assert_eq!(
            pruned
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![id_a, id_c]
        );
        assert_eq!(
            pruned.child_output_columns[0]
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![left_a, left_c]
        );
        assert_eq!(
            pruned.child_output_columns[1]
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![right_a, right_c]
        );
    }

    #[test]
    fn prune_union_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);

        let expr = OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: false,
                output_columns: vec![make_output_column(id_a, "a")],
                child_output_columns: vec![],
            }),
            vec![dummy_input()],
        );
        // required_output_columns = None (default), also all=false

        let rule = PruneUnionColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_union_keeps_at_least_one_when_needed_empty() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);

        // needed is empty — must keep first column.
        let mut expr = OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![make_output_column(id_a, "a"), make_output_column(id_b, "b")],
                child_output_columns: vec![],
            }),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(HashSet::new());

        let rule = PruneUnionColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalUnion(pruned) = &changed.op else {
            panic!("expected Union");
        };

        assert_eq!(pruned.output_columns.len(), 1);
        assert_eq!(pruned.output_columns[0].column_id, id_a, "first col kept");
    }
}
