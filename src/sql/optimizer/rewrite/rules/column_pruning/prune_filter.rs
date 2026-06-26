//! PruneFilterColumns — Phase 2 rule for Filter nodes.
//!
//! This is a documented NO-OP. Filter has no own output metadata to prune;
//! it passes through its child's schema unchanged. Column needs were propagated
//! to the child by the Phase-1 tagging pass. The predicate expression always
//! requires all its referenced columns, so there is nothing to drop at the
//! Filter level itself.
//!
//! Kept for architectural symmetry and to allow per-operator
//! `disable_optimizer_rules` control in the future.

use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneFilterColumns;

impl LogicalRewriteRule for PruneFilterColumns {
    fn name(&self) -> &'static str {
        "PruneFilterColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Filter,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, _expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // No-op: Filter has no own output metadata to prune; column needs were
        // propagated to its child by the Phase-1 tagging pass. Kept for
        // architectural symmetry + per-operator disable_optimizer_rules control.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::LiteralValue;
    use crate::sql::optimizer::operator::{FilterOp, Operator, ValuesOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::optimizer::scalar::HashableLiteral;
    use crate::sql::optimizer::scalar::{ScalarArena, ScalarNode};
    use arrow::datatypes::DataType;

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
    fn prune_filter_is_always_unchanged() {
        let mut arena = ScalarArena::new();
        let pred_id = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(true))),
            DataType::Boolean,
            false,
        );

        let expr = OptExpr::new(
            Operator::LogicalFilter(FilterOp { predicate: pred_id }),
            vec![dummy_input()],
        );
        let rule = PruneFilterColumns;

        // pattern gates the structural operator kind.
        assert!(
            crate::sql::optimizer::rewrite::tree_binder::bind_tree(&rule.pattern(), &expr)
                .is_some()
        );

        // apply always returns Unchanged
        let result = rule.apply(expr, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneFilterColumns must always return Unchanged"
        );
    }
}
