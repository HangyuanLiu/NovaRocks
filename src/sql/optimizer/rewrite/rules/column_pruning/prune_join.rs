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
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct PruneJoinColumns;

impl LogicalRewriteRule for PruneJoinColumns {
    fn name(&self) -> &'static str {
        "PruneJoinColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Join(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlan,
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
    use crate::sql::planner::plan::{JoinNode, ValuesNode};

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
    fn prune_join_is_always_unchanged() {
        let node = JoinNode {
            left: dummy_input(),
            right: dummy_input(),
            join_type: JoinKind::Inner,
            condition: None,
            required_output_columns: None,
        };

        let plan = LogicalPlan::Join(node);
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
