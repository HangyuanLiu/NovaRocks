//! PruneRepeatColumns — Phase 2 rule for Repeat (ROLLUP/CUBE/GROUPING SETS) nodes.
//!
//! This is a documented NO-OP. The Repeat node has keep-all-child semantics
//! assigned by the Phase-1 tagging pass: all input columns are needed because
//! the ROLLUP/CUBE/GROUPING SETS grouping logic references arbitrary subsets
//! of columns at runtime. No `output_columns` list is maintained on
//! `RepeatPlanNode` — pruning is not applicable here.
//!
//! Kept for architectural symmetry and to allow per-operator
//! `disable_optimizer_rules` control in the future.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct PruneRepeatColumns;

impl LogicalRewriteRule for PruneRepeatColumns {
    fn name(&self) -> &'static str {
        "PruneRepeatColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Repeat(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        // No-op: Repeat (ROLLUP/CUBE/GROUPING SETS) was assigned keep-all-child
        // semantics by the Phase-1 tagging pass. No output_columns list to prune.
        // Kept for architectural symmetry + per-operator disable_optimizer_rules
        // control.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::{RepeatPlanNode, ValuesNode};

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    #[test]
    fn prune_repeat_is_always_unchanged() {
        let node = RepeatPlanNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            repeat_column_ref_list: vec![],
            repeat_column_ref_ids: vec![],
            grouping_ids: vec![],
            all_rollup_columns: vec![],
            all_rollup_column_ids: vec![],
            grouping_key_aliases: vec![],
            grouping_fn_args: vec![],
            grouping_fn_arg_ids: vec![],
            grouping_fn_ids: vec![],
            required_output_columns: None,
        };

        let plan = LogicalPlan::Repeat(node);
        let rule = PruneRepeatColumns;

        // matches the right variant
        assert!(rule.matches(&plan, &ctx()));

        // apply always returns Unchanged
        let result = rule.apply(plan, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneRepeatColumns must always return Unchanged"
        );
    }
}
