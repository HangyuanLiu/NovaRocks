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
use crate::sql::planner::plan::{LogicalPlanNode, LogicalPlanNodeKind};

pub(crate) struct PruneCTEAnchorColumns;

impl LogicalRewriteRule for PruneCTEAnchorColumns {
    fn name(&self) -> &'static str {
        "PruneCTEAnchorColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlanNode, _ctx: &RewriteContext) -> bool {
        matches!(&plan.kind, LogicalPlanNodeKind::CTEAnchor(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlanNode,
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
    use crate::sql::planner::plan::*;
    use crate::sql::planner::plan::{LogicalCTEAnchorNode, LogicalPlanNodeKind, LogicalValuesNode};

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
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
    fn prune_cte_anchor_is_always_unchanged() {
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: 1u32 }),
            vec![dummy_input(), dummy_input()],
            None,
        );

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
