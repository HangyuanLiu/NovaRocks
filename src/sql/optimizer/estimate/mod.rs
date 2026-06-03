//! Pure-function statistics kernel: a single source of truth for saturating
//! arithmetic, join cardinality, predicate selectivity and NDV propagation.
//! Both the Cascades `stats` derivation and the join-reorder `cardinality`
//! walker delegate here so they never drift numerically.

pub(crate) mod arith;
pub(crate) mod cardinality;
pub(crate) mod selectivity;
// ndv added in later phases.
