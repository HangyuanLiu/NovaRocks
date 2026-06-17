use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RewriteTraversal {
    TopDown,
    BottomUp,
}

pub(crate) trait LogicalRewriteRule: Send + Sync {
    fn name(&self) -> &'static str;
    fn phase(&self) -> RewritePhase;
    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }
    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool;
    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String>;
}

/// Convenience trait for local `OptExpr -> OptExpr` rules.
///
/// The pipeline still consumes `LogicalRewriteRule`. This trait keeps simple
/// query rewrite rules focused on one-node matching and rewriting while the
/// new framework owns traversal, fixed-point iteration, disable handling, and
/// tracing.
pub(crate) trait PlanRewriteRule: Send + Sync {
    fn name(&self) -> &'static str;

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, expr: &OptExpr) -> bool;

    fn apply(&self, expr: OptExpr) -> Option<OptExpr>;
}

impl<T> LogicalRewriteRule for T
where
    T: PlanRewriteRule,
{
    fn name(&self) -> &'static str {
        PlanRewriteRule::name(self)
    }

    fn phase(&self) -> RewritePhase {
        PlanRewriteRule::phase(self)
    }

    fn traversal(&self) -> RewriteTraversal {
        PlanRewriteRule::traversal(self)
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        PlanRewriteRule::matches(self, expr)
    }

    fn apply(
        &self,
        expr: OptExpr,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        Ok(match PlanRewriteRule::apply(self, expr) {
            Some(rewritten) => RewriteResult::Changed(rewritten),
            None => RewriteResult::Unchanged,
        })
    }
}
