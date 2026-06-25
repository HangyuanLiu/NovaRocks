//! Terminal guard of the SubqueryRewrite stage: any Apply node still present
//! after the decorrelation rules means the subquery shape is unsupported.

use crate::sql::optimizer::operator::{ApplyOp, Operator};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct ApplyException;

impl LogicalRewriteRule for ApplyException {
    fn name(&self) -> &'static str {
        "ApplyException"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Apply,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        match &expr.op {
            Operator::LogicalApply(op) => Err(apply_exception_message(op)),
            _ => Ok(RewriteResult::Unchanged),
        }
    }
}

pub(super) fn apply_exception_message(op: &ApplyOp) -> String {
    format!(
        "subquery decorrelation failed: a residual Apply node (kind={:?}, correlated={}) \
         survived the SubqueryRewrite stage; this subquery shape is not yet supported",
        op.kind,
        !op.correlation_column_ids.is_empty()
    )
}
