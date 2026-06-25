//! PruneDecodeColumns — Phase 2 rule for Decode nodes.
//!
//! ## Decode node mapping semantics
//!
//! `DecodeOp.output_columns[i].name` contains the **string column** name
//! (user-facing). The corresponding `DecodeMapping` has:
//!   - `dict_column`: name of the dict-encoded slot in the child's output
//!   - `string_column`: name exposed upward (matches output_columns[i].name)
//!
//! The Phase-1 tagging pass notes that the same `ColumnId` is used for both
//! the child dict-slot output and the Decode's string-column output, so
//! `needed` ColumnIds apply directly to `output_columns[i].column_id`.
//!
//! Pruning strategy:
//! 1. Filter `output_columns` to those whose `column_id ∈ needed`.
//! 2. Build the set of kept string-column names.
//! 3. Drop any `mappings` whose `string_column` is not in that set (the dict
//!    slot for that column is no longer needed).
//! 4. Keep at least one output column (first original).
//!
//! Unchanged when `output_columns.len()` is the same as before.

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

pub(crate) struct PruneDecodeColumns;

impl LogicalRewriteRule for PruneDecodeColumns {
    fn name(&self) -> &'static str {
        "PruneDecodeColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Decode,
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
        let Operator::LogicalDecode(mut node) = op else {
            unreachable!()
        };

        // None means Phase 1 hasn't tagged this node — no-op.
        let Some(needed) = required_output_columns.clone() else {
            return Ok(RewriteResult::Unchanged);
        };

        let original_len = node.output_columns.len();

        // Compute the filtered set of column ids to keep.
        let filtered_ids: HashSet<ColumnId> = node
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
        let keep_ids = keep_at_least_one(filtered_ids, fallback);

        // Filter output_columns.
        let new_output_columns: Vec<_> = node
            .output_columns
            .into_iter()
            .filter(|c| keep_ids.contains(&c.column_id))
            .collect();

        if new_output_columns.len() == original_len {
            return Ok(RewriteResult::Unchanged);
        }

        // Build set of kept string-column names for mapping filtering.
        let kept_string_names: HashSet<String> = new_output_columns
            .iter()
            .map(|c| c.name.to_lowercase())
            .collect();

        // Drop mappings whose string_column is no longer in any kept output column.
        let new_mappings: Vec<_> = node
            .mappings
            .into_iter()
            .filter(|m| kept_string_names.contains(&m.string_column.to_lowercase()))
            .collect();

        node.output_columns = new_output_columns;
        node.mappings = new_mappings;
        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalDecode(node),
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
    use crate::sql::optimizer::operator::{DecodeOp, Operator, ValuesOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::DecodeMapping;
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Utf8,
            nullable: true,
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
    fn prune_decode_drops_unneeded_output_columns_and_mappings() {
        // DecodeOp with 2 decoded columns (s1, s2) and 1 passthrough (a).
        // needed = {id_a, id_s1}; s2 and its mapping should be dropped.
        let id_a = ColumnId::new_for_test(1);
        let id_s1 = ColumnId::new_for_test(10);
        let id_s2 = ColumnId::new_for_test(20);

        let mut needed = HashSet::new();
        needed.insert(id_a);
        needed.insert(id_s1);

        let mut expr = OptExpr::new(
            Operator::LogicalDecode(DecodeOp {
                mappings: vec![
                    DecodeMapping {
                        source_column_id: id_s1,
                        output_column_id: id_s1,
                        dict_column: "__nr_dict_s1".to_string(),
                        string_column: "s1".to_string(),
                    },
                    DecodeMapping {
                        source_column_id: id_s2,
                        output_column_id: id_s2,
                        dict_column: "__nr_dict_s2".to_string(),
                        string_column: "s2".to_string(),
                    },
                ],
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_s1, "s1"),
                    make_output_column(id_s2, "s2"),
                ],
            }),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let rule = PruneDecodeColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalDecode(pruned) = &changed.op else {
            panic!("expected Decode");
        };

        // output_columns: a and s1 kept, s2 dropped.
        assert_eq!(pruned.output_columns.len(), 2);
        let col_ids: HashSet<ColumnId> =
            pruned.output_columns.iter().map(|c| c.column_id).collect();
        assert!(col_ids.contains(&id_a));
        assert!(col_ids.contains(&id_s1));
        assert!(!col_ids.contains(&id_s2));

        // mappings: only s1 mapping kept, s2 mapping dropped.
        assert_eq!(pruned.mappings.len(), 1);
        assert_eq!(pruned.mappings[0].string_column, "s1");
    }

    #[test]
    fn prune_decode_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let expr = OptExpr::new(
            Operator::LogicalDecode(DecodeOp {
                mappings: vec![],
                output_columns: vec![make_output_column(id_a, "a")],
            }),
            vec![dummy_input()],
        );
        // required_output_columns = None (default)

        let rule = PruneDecodeColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_decode_keeps_at_least_one_output_column() {
        // needed is empty — must keep at least the first output column.
        let id_a = ColumnId::new_for_test(1);
        let id_s = ColumnId::new_for_test(10);

        let mut expr = OptExpr::new(
            Operator::LogicalDecode(DecodeOp {
                mappings: vec![DecodeMapping {
                    source_column_id: id_s,
                    output_column_id: id_s,
                    dict_column: "__nr_dict_s".to_string(),
                    string_column: "s".to_string(),
                }],
                output_columns: vec![make_output_column(id_a, "a"), make_output_column(id_s, "s")],
            }),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(HashSet::new());

        let rule = PruneDecodeColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalDecode(pruned) = &changed.op else {
            panic!("expected Decode");
        };

        assert_eq!(
            pruned.output_columns.len(),
            1,
            "at least one column must survive"
        );
        // First original column is "a" (a passthrough, no mapping).
        assert_eq!(pruned.output_columns[0].column_id, id_a);
        // Since "a" has no mapping, all mappings should be gone.
        assert!(
            pruned.mappings.is_empty(),
            "no mapping for passthrough column a"
        );
    }
}
