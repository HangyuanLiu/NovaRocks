//! Join reorder — DP / Greedy / LeftDeep / Heuristic algorithms.
//!
//! Moved from `src/sql/optimizer/{join_reorder,cardinality,cost}.rs` during
//! Phase 5 of the optimizer unification. Algorithm logic is unchanged;
//! only the module paths have been updated.

pub(crate) mod cardinality;
pub(crate) mod cost;
pub(crate) mod reorder;
pub(crate) mod rule;

// Re-export the main entry points for convenience.
pub(crate) use reorder::reorder_joins_cbo;
pub(crate) use rule::JoinReorderRule;
