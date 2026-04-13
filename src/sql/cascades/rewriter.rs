//! Legacy RBO rewrite pass. Being progressively migrated into cascades
//! RBO rules under `src/sql/cascades/rbo/rules/`. Each migrated rule is
//! deleted from both the legacy source and this call site; Phase 6
//! deletes this file entirely.

use std::collections::HashMap;

use crate::sql::plan::LogicalPlan;
use crate::sql::statistics::TableStatistics;

/// Apply remaining legacy RBO rewrites to the logical plan before Memo
/// insertion. Column pruning has been migrated to `PruneColumns` RBO rule
/// and no longer runs from here (Phase 2). Predicate pushdown and join
/// reorder will be migrated in Phase 3 and Phase 5 respectively.
pub(crate) fn rewrite(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> LogicalPlan {
    let plan = crate::sql::optimizer::predicate_pushdown::push_down_predicates(plan);
    let plan = crate::sql::optimizer::join_reorder::reorder_joins_cbo(plan, table_stats);
    // Second pushdown pass: after join reorder, newly formed joins may have
    // cross-side predicates that can now be pushed into join conditions
    // (e.g., OR factoring extracting common equi-joins at a lower level).
    let plan = crate::sql::optimizer::predicate_pushdown::push_down_predicates(plan);
    plan
}
