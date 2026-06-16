mod rule;

pub(crate) use rule::RankingWindowPredicatePushdownRule;

use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) fn ranking_window_predicate_pushdown_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(RankingWindowPredicatePushdownRule)]
}
