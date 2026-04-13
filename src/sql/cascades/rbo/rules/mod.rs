//! RBO rule registry. Phases 2-5 of the unification spec land their
//! migrated rules here; Phase 1 ships an empty registry so the framework
//! is wired end-to-end with no behavior change.

use super::rule::RewriteRule;

pub(crate) mod column_pruning;

pub(crate) fn column_pruning_rules() -> Vec<Box<dyn RewriteRule>> {
    // Column pruning is fundamentally a top-down concern; expressed as a
    // single rule that recurses internally (documented exception to the
    // "rules don't recurse" convention — see column_pruning.rs module docs).
    vec![Box::new(column_pruning::PruneColumns)]
}

/// All RBO rules in canonical application order.
pub(crate) fn all_rbo_rules() -> Vec<Box<dyn RewriteRule>> {
    let mut all = Vec::new();
    all.extend(column_pruning_rules());
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_contains_prune_columns() {
        let rules = all_rbo_rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].name(), "PruneColumns");
    }
}
