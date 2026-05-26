use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::planner::plan::LogicalPlan;

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
    fn matches(&self, plan: &LogicalPlan, ctx: &RewriteContext) -> bool;
    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String>;
}

/// Convenience trait for local `LogicalPlan -> LogicalPlan` rules.
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

    fn matches(&self, plan: &LogicalPlan) -> bool;

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan>;
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

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        PlanRewriteRule::matches(self, plan)
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        Ok(match PlanRewriteRule::apply(self, plan) {
            Some(rewritten) => RewriteResult::Changed(rewritten),
            None => RewriteResult::Unchanged,
        })
    }
}
