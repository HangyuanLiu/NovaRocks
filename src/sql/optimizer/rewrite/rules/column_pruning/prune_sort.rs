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

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneSortColumns;

impl LogicalRewriteRule for PruneSortColumns {
    fn name(&self) -> &'static str {
        "PruneSortColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        matches!(&expr.op, Operator::LogicalSort(_))
    }

    fn apply(
        &self,
        _expr: OptExpr,
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
    use crate::sql::optimizer::operator::{Operator, SortOp, ValuesOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    #[test]
    fn prune_sort_is_always_unchanged() {
        let expr = OptExpr::new(
            Operator::LogicalSort(SortOp {
                items: vec![],
                analytic_partition_exprs: vec![],
                partition_limit: None,
                topn_type: None,
            }),
            vec![OptExpr::leaf(Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }))],
        );
        let rule = PruneSortColumns;

        // matches the right variant
        assert!(rule.matches(&expr, &ctx()));

        // apply always returns Unchanged
        let result = rule.apply(expr, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneSortColumns must always return Unchanged"
        );
    }
}
