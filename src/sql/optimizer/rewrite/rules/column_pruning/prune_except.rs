//! PruneExceptColumns — Phase 2 rule for Except nodes.
//!
//! EXCEPT is a DISTINCT set operation: every output position participates in
//! row equality. Column pruning cannot remove set-key positions even when the
//! parent only needs row existence (for example `COUNT(*)` over the set).

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::*;

pub(crate) struct PruneExceptColumns;

impl LogicalRewriteRule for PruneExceptColumns {
    fn name(&self) -> &'static str {
        "PruneExceptColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlanNode, _ctx: &RewriteContext) -> bool {
        matches!(&plan.kind, LogicalPlanNodeKind::Except(_))
    }

    fn apply(
        &self,
        plan: LogicalPlanNode,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        let LogicalPlanNodeKind::Except(_) = plan.kind else {
            unreachable!()
        };

        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::*;
    use crate::sql::planner::plan::{LogicalExceptNode, LogicalPlanNodeKind, LogicalValuesNode};
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
            is_internal: false,
        }
    }

    fn dummy_input() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        )
    }

    #[test]
    fn prune_except_preserves_all_set_key_columns() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut needed = HashSet::new();
        needed.insert(id_b);

        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Except(LogicalExceptNode {
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_b, "b"),
                    make_output_column(id_c, "c"),
                ],
            }),
            vec![dummy_input(), dummy_input()],
            Some(needed),
        );

        let rule = PruneExceptColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "EXCEPT must retain every output column because all positions are part of the set key"
        );
    }

    #[test]
    fn prune_except_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);

        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Except(LogicalExceptNode {
                output_columns: vec![make_output_column(id_a, "a")],
            }),
            vec![dummy_input()],
            None, // not tagged
        );

        let rule = PruneExceptColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_except_keeps_all_columns_when_needed_empty() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);

        // needed is empty — must keep first column.
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Except(LogicalExceptNode {
                output_columns: vec![make_output_column(id_a, "a"), make_output_column(id_b, "b")],
            }),
            vec![dummy_input()],
            Some(HashSet::new()),
        );

        let rule = PruneExceptColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "EXCEPT cannot collapse the set key even when the parent only needs row existence"
        );
    }
}
