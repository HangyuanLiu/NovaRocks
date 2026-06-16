//! PruneJoinColumns — Phase 2 rule for Join nodes.
//!
//! This is a documented NO-OP. Join has no own output metadata to prune;
//! its output is the concatenation of its two children's output schemas.
//! Column needs were propagated to the left and right children separately
//! by the Phase-1 tagging pass. Each child's own prune rule drops unused
//! columns from that child independently.
//!
//! Kept for architectural symmetry and to allow per-operator
//! `disable_optimizer_rules` control in the future.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::{LogicalPlanNode, LogicalPlanNodeKind};

pub(crate) struct PruneJoinColumns;

impl LogicalRewriteRule for PruneJoinColumns {
    fn name(&self) -> &'static str {
        "PruneJoinColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlanNode, _ctx: &RewriteContext) -> bool {
        matches!(&plan.kind, LogicalPlanNodeKind::Join(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlanNode,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        // No-op: Join has no own output metadata to prune; column needs were
        // propagated to its children by the Phase-1 tagging pass. Kept for
        // architectural symmetry + per-operator disable_optimizer_rules control.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::JoinKind;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::*;
    use crate::sql::planner::plan::{LogicalJoinNode, LogicalPlanNodeKind, LogicalValuesNode};

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
    fn prune_join_is_always_unchanged() {
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![dummy_input(), dummy_input()],
            None,
        );

        let rule = PruneJoinColumns;

        // matches the right variant
        assert!(rule.matches(&plan, &ctx()));

        // apply always returns Unchanged
        let result = rule.apply(plan, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneJoinColumns must always return Unchanged"
        );
    }
}
