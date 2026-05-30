//! PruneCTEAnchorColumns — Phase 2 rule for CTEAnchor nodes.
//!
//! This is a documented NO-OP. CTEAnchor is a scope wrapper that holds a CTE
//! producer subtree and the consumer query subtree. It has no own output
//! column metadata — it simply passes through the consumer's output. Column
//! needs were propagated into the producer and consumer children separately
//! by the Phase-1 tagging pass. Each child's own prune rules handle their
//! pruning independently.
//!
//! Kept for architectural symmetry and to allow per-operator
//! `disable_optimizer_rules` control in the future.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct PruneCTEAnchorColumns;

impl LogicalRewriteRule for PruneCTEAnchorColumns {
    fn name(&self) -> &'static str {
        "PruneCTEAnchorColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::CTEAnchor(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        // No-op: CTEAnchor is a scope wrapper with no own output metadata to
        // prune; column needs were propagated to the produce/consumer children
        // by the Phase-1 tagging pass. Kept for architectural symmetry +
        // per-operator disable_optimizer_rules control.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::{CTEAnchorNode, ValuesNode};

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    fn dummy_input() -> Box<LogicalPlan> {
        Box::new(LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        }))
    }

    #[test]
    fn prune_cte_anchor_is_always_unchanged() {
        let node = CTEAnchorNode {
            cte_id: 1u32,
            produce: dummy_input(),
            consumer: dummy_input(),
            required_output_columns: None,
        };

        let plan = LogicalPlan::CTEAnchor(node);
        let rule = PruneCTEAnchorColumns;

        // matches the right variant
        assert!(rule.matches(&plan, &ctx()));

        // apply always returns Unchanged
        let result = rule.apply(plan, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneCTEAnchorColumns must always return Unchanged"
        );
    }
}
