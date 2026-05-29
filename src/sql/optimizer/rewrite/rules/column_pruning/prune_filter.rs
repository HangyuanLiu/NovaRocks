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

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct PruneFilterColumns;

impl LogicalRewriteRule for PruneFilterColumns {
    fn name(&self) -> &'static str {
        "PruneFilterColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Filter(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        // No-op: Filter has no own output metadata to prune; column needs were
        // propagated to its child by the Phase-1 tagging pass. Kept for
        // architectural symmetry + per-operator disable_optimizer_rules control.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::TypedExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::{FilterNode, ValuesNode};

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    fn bool_literal() -> TypedExpr {
        use crate::sql::analysis::ExprKind;
        use arrow::datatypes::DataType;
        TypedExpr {
            kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Bool(true)),
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    #[test]
    fn prune_filter_is_always_unchanged() {
        let node = FilterNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            predicate: bool_literal(),
            required_output_columns: None,
        };

        let plan = LogicalPlan::Filter(node);
        let rule = PruneFilterColumns;

        // matches the right variant
        assert!(rule.matches(&plan, &ctx()));

        // apply always returns Unchanged
        let result = rule.apply(plan, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneFilterColumns must always return Unchanged"
        );
    }
}
