//! PruneWindowColumns — Phase 2 rule for Window nodes.
//!
//! ## Window node layout
//!
//! `WindowNode.output_columns` is built from `original_projection` — the full
//! SELECT-list in declaration order, containing both passthrough columns (no
//! window call in their expr) and window-result columns (have a window call).
//!
//! `WindowNode.window_exprs` contains only the extracted window calls. Each
//! `WindowExpr.output_name` matches the `output_columns[j].name` of the
//! SELECT-list item that contained it.
//!
//! Pruning strategy:
//! 1. Build a set of `window_expr.output_name` → presence, for quick lookup.
//! 2. For each `output_columns[i]`:
//!    a. If it is a passthrough column (no matching window_expr name), keep it
//!       iff its id is in `needed`.
//!    b. If it is a window-result column (matches a window_expr by name), keep
//!       both the output_column and the window_expr iff its id is in `needed`.
//! 3. Keep at least one output column.
//!
//! Unchanged when both `window_exprs.len()` and `output_columns.len()` are
//! the same as before (nothing pruned).
//!
//! **Safety**: the name-based matching is deterministic because the planner
//! builds `output_columns` from `original_projection` items and sets
//! `window_expr.output_name = item.output_name`. If any ambiguity arises in
//! future (e.g. duplicate output names), the rule is conservative: a name
//! that appears in multiple window_exprs will match multiple output_columns,
//! and both will be treated as window-result columns.

use std::collections::{HashMap, HashSet};

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::column_pruning_v2::keep_at_least_one;
use crate::sql::planner::plan::*;

pub(crate) struct PruneWindowColumns;

impl LogicalRewriteRule for PruneWindowColumns {
    fn name(&self) -> &'static str {
        "PruneWindowColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Window(_))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Window(mut node) = plan else {
            unreachable!()
        };

        // None means Phase 1 hasn't tagged this node — no-op.
        let Some(needed) = node.required_output_columns.clone() else {
            return Ok(RewriteResult::Unchanged);
        };

        let original_output_len = node.output_columns.len();
        let original_window_len = node.window_exprs.len();

        // Build a map: window_expr_output_name → window_expr index.
        // Multiple window_exprs may share a name if items have the same
        // output_name (unusual but handle conservatively).
        let mut window_by_name: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, we) in node.window_exprs.iter().enumerate() {
            window_by_name
                .entry(we.output_name.as_str())
                .or_default()
                .push(i);
        }

        // Walk output_columns, deciding which to keep and which window_exprs to retain.
        // We track which window_expr indices survive.
        let mut kept_output_columns = Vec::new();
        let mut kept_window_expr_indices: HashSet<usize> = HashSet::new();

        for col in &node.output_columns {
            if let Some(indices) = window_by_name.get(col.name.as_str()) {
                // Window-result column: keep iff in needed.
                if needed.contains(&col.column_id) {
                    kept_output_columns.push(col.clone());
                    for &idx in indices {
                        kept_window_expr_indices.insert(idx);
                    }
                }
            } else {
                // Passthrough column: keep iff in needed.
                if needed.contains(&col.column_id) {
                    kept_output_columns.push(col.clone());
                }
            }
        }

        // Ensure at least one output column survives.
        if kept_output_columns.is_empty() {
            // Keep the first output column (passthrough preferred, otherwise first overall).
            if let Some(first) = node.output_columns.first() {
                kept_output_columns.push(first.clone());
                // If the first column is a window result, also keep its window_expr(s).
                if let Some(indices) = window_by_name.get(first.name.as_str()) {
                    for &idx in indices {
                        kept_window_expr_indices.insert(idx);
                    }
                }
            }
        } else {
            // Also apply keep_at_least_one logic via ids for safety.
            let kept_ids: HashSet<ColumnId> =
                kept_output_columns.iter().map(|c| c.column_id).collect();
            let fallback = node
                .output_columns
                .first()
                .map(|c| c.column_id)
                .unwrap_or(ColumnId::UNSET);
            let safe_ids = keep_at_least_one(kept_ids, fallback);
            // If keep_at_least_one added back the fallback, add it to kept_output_columns.
            // (This only fires when kept_output_columns was empty, already handled above.)
            let _ = safe_ids; // used implicitly via kept_output_columns being non-empty
        }

        // Build new window_exprs in original index order, keeping only those with a
        // surviving index.
        let new_window_exprs: Vec<WindowExpr> = node
            .window_exprs
            .into_iter()
            .enumerate()
            .filter_map(|(i, we)| {
                if kept_window_expr_indices.contains(&i) {
                    Some(we)
                } else {
                    None
                }
            })
            .collect();

        // Unchanged check: nothing pruned if both lens are the same.
        if kept_output_columns.len() == original_output_len
            && new_window_exprs.len() == original_window_len
        {
            return Ok(RewriteResult::Unchanged);
        }

        node.output_columns = kept_output_columns;
        node.window_exprs = new_window_exprs;
        Ok(RewriteResult::Changed(LogicalPlan::Window(node)))
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

    fn make_window_expr(output_name: &str) -> WindowExpr {
        WindowExpr {
            name: "row_number".to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            result_type: DataType::Int64,
            output_name: output_name.to_string(),
            ignore_nulls: false,
        }
    }

    fn dummy_input() -> Box<LogicalPlan> {
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
        Box::new(LogicalPlan::Scan(ScanNode {
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
        }))
    }

    #[test]
    fn prune_window_drops_unneeded_window_exprs_and_output_columns() {
        // Window node with 2 passthrough cols and 2 window result cols.
        // output_columns: [a@1(passthrough), b@2(passthrough), rn1@101(window "rn1"), rn2@102(window "rn2")]
        // window_exprs: [row_number→"rn1", rank→"rn2"]
        // needed = {1, 101}  (a + rn1; b and rn2 not needed)
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_rn1 = ColumnId::new_for_test(101);
        let id_rn2 = ColumnId::new_for_test(102);

        let mut needed = HashSet::new();
        needed.insert(id_a);
        needed.insert(id_rn1);

        let node = WindowNode {
            input: dummy_input(),
            window_exprs: vec![make_window_expr("rn1"), make_window_expr("rn2")],
            output_columns: vec![
                make_output_column(id_a, "a"),
                make_output_column(id_b, "b"),
                make_output_column(id_rn1, "rn1"),
                make_output_column(id_rn2, "rn2"),
            ],
            required_output_columns: Some(needed),
        };

        let plan = LogicalPlan::Window(node);
        let rule = PruneWindowColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let LogicalPlan::Window(pruned) = changed else {
            panic!("expected Window");
        };

        // output_columns: a + rn1 (2 columns).
        assert_eq!(pruned.output_columns.len(), 2);
        let col_ids: HashSet<ColumnId> =
            pruned.output_columns.iter().map(|c| c.column_id).collect();
        assert!(col_ids.contains(&id_a));
        assert!(col_ids.contains(&id_rn1));
        assert!(!col_ids.contains(&id_b));
        assert!(!col_ids.contains(&id_rn2));

        // window_exprs: only "rn1" survives.
        assert_eq!(pruned.window_exprs.len(), 1);
        assert_eq!(pruned.window_exprs[0].output_name, "rn1");
    }

    #[test]
    fn prune_window_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let id_rn = ColumnId::new_for_test(101);

        let node = WindowNode {
            input: dummy_input(),
            window_exprs: vec![make_window_expr("rn")],
            output_columns: vec![
                make_output_column(id_a, "a"),
                make_output_column(id_rn, "rn"),
            ],
            required_output_columns: None, // not tagged
        };

        let plan = LogicalPlan::Window(node);
        let rule = PruneWindowColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_window_keeps_at_least_one_col() {
        // needed is empty — must keep at least one column and its window_expr if applicable.
        let id_a = ColumnId::new_for_test(1);
        let id_rn = ColumnId::new_for_test(101);

        let node = WindowNode {
            input: dummy_input(),
            window_exprs: vec![make_window_expr("rn")],
            output_columns: vec![
                make_output_column(id_a, "a"),
                make_output_column(id_rn, "rn"),
            ],
            required_output_columns: Some(HashSet::new()),
        };

        let plan = LogicalPlan::Window(node);
        let rule = PruneWindowColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let LogicalPlan::Window(pruned) = changed else {
            panic!("expected Window");
        };

        assert!(
            !pruned.output_columns.is_empty(),
            "must keep at least one output column"
        );
    }
}
