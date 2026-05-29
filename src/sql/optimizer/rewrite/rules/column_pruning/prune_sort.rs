//! PruneSortColumns — Phase 2 rule for Sort nodes.
//!
//! This is a documented NO-OP. Sort has no own output metadata to prune;
//! it passes through its child's schema unchanged. Column needs were propagated
//! to the child by the Phase-1 tagging pass. The sort key expressions always
//! require their referenced columns, so there is nothing to drop at the
//! Sort level itself.
//!
//! Kept for architectural symmetry and to allow per-operator
//! `disable_optimizer_rules` control in the future.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct PruneSortColumns;

impl LogicalRewriteRule for PruneSortColumns {
    fn name(&self) -> &'static str {
        "PruneSortColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Sort(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        // No-op: Sort has no own output metadata to prune; column needs were
        // propagated to its child by the Phase-1 tagging pass. Kept for
        // architectural symmetry + per-operator disable_optimizer_rules control.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::{SortNode, ValuesNode};

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    #[test]
    fn prune_sort_is_always_unchanged() {
        let node = SortNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            items: vec![],
            analytic_partition_by: vec![],
            required_output_columns: None,
        };

        let plan = LogicalPlan::Sort(node);
        let rule = PruneSortColumns;

        // matches the right variant
        assert!(rule.matches(&plan, &ctx()));

        // apply always returns Unchanged
        let result = rule.apply(plan, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneSortColumns must always return Unchanged"
        );
    }
}
