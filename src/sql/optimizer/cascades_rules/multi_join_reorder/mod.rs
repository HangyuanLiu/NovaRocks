//! In-memo multi-candidate join reorder — enumeration cores (Phase 3).
//!
//! Pure enumeration over a flattened inner/cross join chain. Produces candidate
//! [`crate::sql::optimizer::memo::JoinTree`] orders (LeftDeep always; DP and
//! Greedy-TopK subject to caps) that a later one-shot pass materializes into the
//! memo via `stats::copy_in_join_tree`. Nothing here is wired into `optimize()`
//! yet (Phase 4/5).
#![allow(dead_code)] // Phase 3: enumeration cores are built and unit-tested here;
// they become live when the one-shot reorder pass is added in Phase 4.

mod algo;
mod flatten;

use crate::sql::analysis::TypedExpr;
use crate::sql::optimizer::memo::GroupId;
use crate::sql::optimizer::statistics::Statistics;

pub(crate) use algo::{ReorderCaps, enumerate_orders};
pub(crate) use flatten::flatten_join_chain;

/// A flattened inner/cross join chain: the leaf atoms (existing memo groups,
/// with their cached output statistics) plus the multi-relation predicates that
/// connect them, each tagged with the bitmask of atom indices it references.
///
/// `atoms`, `atom_stats`, and `atom_filters` are parallel: `atom_stats[i]` is
/// the (filtered) output statistics of the group `atoms[i]`, and
/// `atom_filters[i]` are the single-relation predicates that must be applied on
/// top of `atoms[i]` when the reorder pass materializes it (Phase 4). The
/// single-side selectivity is already reflected in `atom_stats[i]`.
pub(crate) struct MultiJoinGraph {
    pub(crate) atoms: Vec<GroupId>,
    pub(crate) atom_stats: Vec<Statistics>,
    pub(crate) atom_filters: Vec<Vec<TypedExpr>>,
    /// `(predicate, bitmask of atom indices it references)`. `u32` supports up
    /// to 32 atoms, matching the chain caps.
    pub(crate) predicates: Vec<(TypedExpr, u32)>,
}

impl MultiJoinGraph {
    pub(crate) fn atom_count(&self) -> usize {
        self.atoms.len()
    }
}
