//! RBO rule registry.

use std::collections::HashMap;
use std::sync::Arc;

use super::rule::RewriteRule;
use crate::sql::statistics::TableStatistics;

pub(crate) mod column_pruning;
pub(crate) mod join_reorder;
pub(crate) mod predicate_pushdown;

pub(crate) fn column_pruning_rules() -> Vec<Box<dyn RewriteRule>> {
    vec![Box::new(column_pruning::PruneColumns)]
}

/// Structural RBO rules: predicate pushdown + column pruning.
/// These run both before and after join reorder to catch new opportunities.
pub(crate) fn structural_rbo_rules() -> Vec<Box<dyn RewriteRule>> {
    let mut all = Vec::new();
    all.extend(column_pruning_rules());
    all.extend(predicate_pushdown::predicate_pushdown_rules());
    all
}

/// Join reorder rule only. Called as a SEPARATE pass between two
/// structural_rbo_rules passes (the "push, reorder, push" pattern).
/// Do NOT mix with structural rules in a single fixed-point — pushdown
/// and reorder oscillate and either time out or produce column-scope errors.
pub(crate) fn join_reorder_rules(
    table_stats: &HashMap<String, TableStatistics>,
) -> Vec<Box<dyn RewriteRule>> {
    vec![Box::new(join_reorder::JoinReorderRule::new(
        Arc::new(table_stats.clone()),
    ))]
}

/// All RBO rules including join reorder. For registry test only;
/// production code calls structural_rbo_rules() and join_reorder_rules()
/// separately per the three-pass pattern.
pub(crate) fn all_rbo_rules(
    table_stats: &HashMap<String, TableStatistics>,
) -> Vec<Box<dyn RewriteRule>> {
    let mut all = structural_rbo_rules();
    all.extend(join_reorder_rules(table_stats));
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_contains_expected_rules() {
        let rules = all_rbo_rules(&HashMap::new());
        assert_eq!(rules.len(), 7);
        let mut names: Vec<&str> = rules.iter().map(|r| r.name()).collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "JoinReorder",
                "PruneColumns",
                "PushDownPredicateAggregate",
                "PushDownPredicateJoin",
                "PushDownPredicateProject",
                "PushDownPredicateScan",
                "PushSemiAntiRightOnlyCondition",
            ]
        );
    }
}
