//! PruneCTEConsumeColumns — Phase 2 rule for CTEConsume nodes.
//!
//! Filters `CTEConsumeNode.output_columns` to only those whose `column_id` is
//! in `required_output_columns`. Keeps at least one column (the first original).
//!
//! Unchanged when `output_columns.len()` is the same as before.

use std::collections::HashSet;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::column_pruning_v2::keep_at_least_one;
use crate::sql::planner::plan::*;

pub(crate) struct PruneCTEConsumeColumns;

impl LogicalRewriteRule for PruneCTEConsumeColumns {
    fn name(&self) -> &'static str {
        "PruneCTEConsumeColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::CTEConsume(_))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::CTEConsume(mut node) = plan else {
            unreachable!()
        };

        // None means Phase 1 hasn't tagged this node — no-op.
        let Some(needed) = node.required_output_columns.clone() else {
            return Ok(RewriteResult::Unchanged);
        };

        let original_len = node.output_columns.len();

        let filtered_ids: HashSet<ColumnId> = node
            .output_columns
            .iter()
            .map(|c| c.column_id)
            .filter(|id| needed.contains(id))
            .collect();

        let fallback = node
            .output_columns
            .first()
            .map(|c| c.column_id)
            .unwrap_or(ColumnId::UNSET);
        let keep_ids = keep_at_least_one(filtered_ids, fallback);

        let new_output_columns: Vec<_> = node
            .output_columns
            .into_iter()
            .filter(|c| keep_ids.contains(&c.column_id))
            .collect();

        if new_output_columns.len() == original_len {
            return Ok(RewriteResult::Unchanged);
        }

        node.output_columns = new_output_columns;
        Ok(RewriteResult::Changed(LogicalPlan::CTEConsume(node)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::CTEConsumeNode;
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    #[test]
    fn prune_cte_consume_filters_to_needed_subset() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut needed = HashSet::new();
        needed.insert(id_b);

        let node = CTEConsumeNode {
            cte_id: 1,
            alias: "cte1".to_string(),
            output_columns: vec![
                make_output_column(id_a, "a"),
                make_output_column(id_b, "b"),
                make_output_column(id_c, "c"),
            ],
            required_output_columns: Some(needed),
        };

        let plan = LogicalPlan::CTEConsume(node);
        let rule = PruneCTEConsumeColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let LogicalPlan::CTEConsume(pruned) = changed else {
            panic!("expected CTEConsume");
        };

        assert_eq!(pruned.output_columns.len(), 1);
        assert_eq!(pruned.output_columns[0].column_id, id_b);
    }

    #[test]
    fn prune_cte_consume_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let node = CTEConsumeNode {
            cte_id: 2,
            alias: "cte2".to_string(),
            output_columns: vec![make_output_column(id_a, "a")],
            required_output_columns: None, // not tagged
        };

        let plan = LogicalPlan::CTEConsume(node);
        let rule = PruneCTEConsumeColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_cte_consume_keeps_at_least_one_when_needed_empty() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);

        let node = CTEConsumeNode {
            cte_id: 3,
            alias: "cte3".to_string(),
            output_columns: vec![make_output_column(id_a, "a"), make_output_column(id_b, "b")],
            required_output_columns: Some(HashSet::new()),
        };

        let plan = LogicalPlan::CTEConsume(node);
        let rule = PruneCTEConsumeColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let LogicalPlan::CTEConsume(pruned) = changed else {
            panic!("expected CTEConsume");
        };

        assert_eq!(pruned.output_columns.len(), 1);
        assert_eq!(pruned.output_columns[0].column_id, id_a, "first col kept");
    }
}
