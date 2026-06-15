//! In-memo multi-candidate join reorder — enumeration cores.
//!
//! Pure enumeration over a flattened inner/cross join chain. Produces candidate
//! [`crate::sql::optimizer::memo::JoinTree`] orders (LeftDeep always; DP and
//! Greedy-TopK subject to caps) that the one-shot [`pass`] materializes into the
//! memo via `stats::copy_in_join_tree`. The pass runs from `optimize()` right
//! after `derive_group_statistics` and is the only join-reorder mechanism (the
//! legacy RBO reorder was retired).

mod algo;
mod flatten;
mod pass;

use crate::sql::analysis::TypedExpr;
use crate::sql::optimizer::memo::GroupId;
use crate::sql::optimizer::statistics::Statistics;

pub(crate) use algo::{ReorderCaps, enumerate_orders};
pub(crate) use flatten::flatten_join_chain;
pub(crate) use pass::{ReorderOptions, run_multi_join_reorder};

/// A flattened inner/cross join chain: the leaf atoms (existing memo groups,
/// with their cached output statistics) plus the multi-relation predicates that
/// connect them, each tagged with the bitmask of atom indices it references.
///
/// `atoms` and `atom_stats` are parallel: `atom_stats[i]` is the output
/// statistics of the group `atoms[i]`. The flattener only accepts chains whose
/// extracted predicates are all multi-relation (it bails on single-side or
/// constant predicates), so every predicate here is a genuine join edge and
/// materialization never has to re-attach a single-relation filter.
pub(crate) struct MultiJoinGraph {
    pub(crate) atoms: Vec<GroupId>,
    pub(crate) atom_stats: Vec<Statistics>,
    /// `(predicate, bitmask of atom indices it references)`. `u32` supports up
    /// to 32 atoms, matching the chain caps.
    pub(crate) predicates: Vec<(TypedExpr, u32)>,
}

impl MultiJoinGraph {
    pub(crate) fn atom_count(&self) -> usize {
        self.atoms.len()
    }
}
