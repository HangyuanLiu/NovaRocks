//! Phase 2 per-operator column pruning rules. Each reads its node's
//! `required_output_columns` (set by the Phase-1 TagRequiredColumns pass)
//! and prunes that node's own output metadata. None ⇒ no-op (keep all).
//!
//! **Not yet registered** in `column_pruning_rules()`. The old
//! `column_pruning::PruneColumns` single rule remains the active pruner.
//! Registration happens in a later task (C2/C3).

pub(crate) mod prune_aggregate;
pub(crate) mod prune_cte_anchor;
pub(crate) mod prune_cte_consume;
pub(crate) mod prune_cte_produce;
pub(crate) mod prune_decode;
pub(crate) mod prune_except;
pub(crate) mod prune_filter;
pub(crate) mod prune_intersect;
pub(crate) mod prune_join;
pub(crate) mod prune_limit;
pub(crate) mod prune_project;
pub(crate) mod prune_repeat;
pub(crate) mod prune_scan;
pub(crate) mod prune_sort;
pub(crate) mod prune_subquery_alias;
pub(crate) mod prune_table_function;
pub(crate) mod prune_union;
pub(crate) mod prune_window;

use std::collections::HashSet;

use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::optimizer::rewrite::context::RewriteContext;

/// When pruning would leave a Project with zero items, mint a placeholder
/// constant column so the operator still has a valid output. Mirrors
/// StarRocks' `Utils.findSmallestColumnRef` / `ConstantOperator.createTinyInt`
/// auto-fill behavior.
///
/// Returns `None` when no factory is available in context (rules that do
/// not have a factory set will fall back to "keep first original column"
/// instead of minting).
pub(crate) fn auto_fill_column_id(ctx: &mut RewriteContext) -> Option<ColumnId> {
    let factory = ctx.column_ref_factory()?;
    // We need a mutable borrow of the factory to call create().
    // Use RefCell::try_borrow_mut which won't panic in the common case.
    let id = factory
        .try_borrow_mut()
        .ok()
        .map(|mut f: std::cell::RefMut<ColumnRefFactory>| {
            f.create(
                None,
                "auto_fill".to_string(),
                arrow::datatypes::DataType::Int8,
                false,
            )
        })?;
    Some(id)
}

/// Keep at least one column from `output_columns` by returning the
/// first column's id when the filtered set would be empty.
///
/// For nodes that use `output_columns: Vec<OutputColumn>` (not Project items),
/// "keep first original" is simpler and safer than minting a fresh id.
pub(crate) fn keep_at_least_one(
    filtered: HashSet<ColumnId>,
    fallback_id: ColumnId,
) -> HashSet<ColumnId> {
    if filtered.is_empty() {
        let mut s = HashSet::new();
        s.insert(fallback_id);
        s
    } else {
        filtered
    }
}
