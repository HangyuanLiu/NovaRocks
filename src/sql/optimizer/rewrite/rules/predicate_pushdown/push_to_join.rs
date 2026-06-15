//! PushDownPredicateJoin rule wrapper.

use crate::sql::optimizer::rewrite::rule::PlanRewriteRule as RewriteRule;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::join_pushdown::{
    push_filter_predicates_through_join, push_join_condition_predicates,
};
use crate::sql::planner::plan::*;

pub(crate) struct PushDownPredicateJoin;

impl RewriteRule for PushDownPredicateJoin {
    fn name(&self) -> &'static str {
        "PushDownPredicateJoin"
    }

    fn matches(&self, plan: &LogicalPlanNode) -> bool {
        matches!(&plan.kind, LogicalPlanNodeKind::Filter(_))
            && matches!(&plan.unary_input().kind, LogicalPlanNodeKind::Join(_))
            || matches!(&plan.kind, LogicalPlanNodeKind::Join(join) if join.condition.is_some())
    }

    fn apply(&self, plan: LogicalPlanNode) -> Option<LogicalPlanNode> {
        let LogicalPlanNode {
            kind,
            mut children,
            required_output_columns,
        } = plan;
        match kind {
            LogicalPlanNodeKind::Filter(filter) => {
                if children.len() != 1 {
                    return None;
                }
                let join_plan = children.remove(0);
                let LogicalPlanNode {
                    kind,
                    mut children,
                    required_output_columns,
                } = join_plan;
                let LogicalPlanNodeKind::Join(join) = kind else {
                    return None;
                };
                if children.len() != 2 {
                    return None;
                }
                let right = children.remove(1);
                let left = children.remove(0);
                let (rewritten, changed) = push_filter_predicates_through_join(
                    filter.predicate,
                    join,
                    left,
                    right,
                    required_output_columns,
                );
                changed.then_some(rewritten)
            }
            LogicalPlanNodeKind::Join(join) => {
                if children.len() != 2 {
                    return None;
                }
                let right = children.remove(1);
                let left = children.remove(0);
                push_join_condition_predicates(join, left, right, required_output_columns)
            }
            _ => None,
        }
    }
}
