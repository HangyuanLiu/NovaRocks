//! RBO rule registry. Phases 2-5 of the unification spec land their
//! migrated rules here; Phase 1 ships an empty registry so the framework
//! is wired end-to-end with no behavior change.

use super::rule::RewriteRule;

/// All RBO rules in canonical application order. Returns an empty vec
/// in Phase 1; Phase 2 (column_pruning) and Phase 3 (predicate_pushdown)
/// fill this in.
pub(crate) fn all_rbo_rules() -> Vec<Box<dyn RewriteRule>> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_empty_in_phase_1() {
        assert_eq!(all_rbo_rules().len(), 0);
    }
}
