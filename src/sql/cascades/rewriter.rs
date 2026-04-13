//! Legacy RBO rewrite pass. Being progressively migrated into cascades
//! RBO rules under `src/sql/cascades/rbo/rules/`. After Phase 3, only
//! join reorder remains; Phase 5 migrates it into a cascades Rule and
//! Phase 6 deletes this file entirely.

use std::collections::HashMap;

use crate::sql::plan::LogicalPlan;
use crate::sql::statistics::TableStatistics;

/// Apply remaining legacy RBO rewrites to the logical plan. Column
/// pruning (Phase 2) and predicate pushdown (Phase 3) now run via the
/// RBO rule driver in `cascades::optimize`. Only join reorder remains
/// until Phase 5.
pub(crate) fn rewrite(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> LogicalPlan {
    crate::sql::optimizer::join_reorder::reorder_joins_cbo(plan, table_stats)
}
