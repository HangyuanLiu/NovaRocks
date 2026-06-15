//! Terminal guard of the SubqueryRewrite stage: any Apply node still present
//! after the decorrelation rules means the subquery shape is unsupported.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::{LogicalApplyNode, LogicalPlanNode, LogicalPlanNodeKind};

pub(crate) struct ApplyException;

impl LogicalRewriteRule for ApplyException {
    fn name(&self) -> &'static str {
        "ApplyException"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlanNode, _ctx: &RewriteContext) -> bool {
        matches!(&plan.kind, LogicalPlanNodeKind::Apply(_))
    }

    fn apply(
        &self,
        plan: LogicalPlanNode,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        match &plan.kind {
            LogicalPlanNodeKind::Apply(node) => Err(apply_exception_message(node)),
            _ => Ok(RewriteResult::Unchanged),
        }
    }
}

pub(super) fn apply_exception_message(node: &LogicalApplyNode) -> String {
    format!(
        "subquery decorrelation failed: a residual Apply node (kind={:?}, correlated={}) \
         survived the SubqueryRewrite stage; this subquery shape is not yet supported",
        node.kind,
        !node.correlation_column_ids.is_empty()
    )
}
