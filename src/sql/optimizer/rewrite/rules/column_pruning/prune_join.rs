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

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneJoinColumns;

impl LogicalRewriteRule for PruneJoinColumns {
    fn name(&self) -> &'static str {
        "PruneJoinColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        matches!(&expr.op, Operator::LogicalJoin(_))
    }

    fn apply(
        &self,
        _expr: OptExpr,
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
    use crate::sql::optimizer::operator::{LogicalJoinOp, Operator, ValuesOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    fn dummy_input() -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        }))
    }

    #[test]
    fn prune_join_is_always_unchanged() {
        let expr = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![dummy_input(), dummy_input()],
        );

        let rule = PruneJoinColumns;

        // matches the right variant
        assert!(rule.matches(&expr, &ctx()));

        // apply always returns Unchanged
        let result = rule.apply(expr, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneJoinColumns must always return Unchanged"
        );
    }
}
