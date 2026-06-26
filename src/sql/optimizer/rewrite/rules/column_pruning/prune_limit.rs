//! PruneLimitColumns — Phase 2 rule for Limit nodes.
//!
//! This is a documented NO-OP. Limit has no own output metadata to prune;
//! it passes through its child's schema unchanged. Column needs were propagated
//! to the child by the Phase-1 tagging pass. The Limit node itself only carries
//! row-count semantics, not column metadata.
//!
//! Kept for architectural symmetry and to allow per-operator
//! `disable_optimizer_rules` control in the future.

use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneLimitColumns;

impl LogicalRewriteRule for PruneLimitColumns {
    fn name(&self) -> &'static str {
        "PruneLimitColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Limit,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, _expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // No-op: Limit has no own output metadata to prune; column needs were
        // propagated to its child by the Phase-1 tagging pass. Kept for
        // architectural symmetry + per-operator disable_optimizer_rules control.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::operator::{LimitOp, Operator, ValuesOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    #[test]
    fn prune_limit_is_always_unchanged() {
        let expr = OptExpr::new(
            Operator::LogicalLimit(LimitOp {
                limit: Some(10),
                offset: None,
            }),
            vec![OptExpr::leaf(Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }))],
        );
        let rule = PruneLimitColumns;

        // pattern gates the structural operator kind.
        assert!(
            crate::sql::optimizer::rewrite::tree_binder::bind_tree(&rule.pattern(), &expr)
                .is_some()
        );

        // apply always returns Unchanged
        let result = rule.apply(expr, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneLimitColumns must always return Unchanged"
        );
    }
}
