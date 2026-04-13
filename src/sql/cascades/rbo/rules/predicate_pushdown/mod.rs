//! Predicate pushdown RBO rules. Each sub-module is a small bottom-up
//! rewrite matching a specific `LogicalFilter(X)` shape (or, for the
//! SEMI/ANTI case, a `LogicalJoin(...)` with an inner condition that can
//! be partly pushed into the right child).
//!
//! Unlike `PruneColumns` (documented exception — top-down, recurses
//! internally), these rules follow the convention: `apply` performs one
//! shape rewrite at this node only; the driver's bottom-up + fixed-point
//! walker handles traversal and repeated firing.
//!
//! Replaces the legacy `src/sql/optimizer/predicate_pushdown.rs` single
//! recursive function. The semantic target: every conjunct of every Filter
//! lands as close to the Scan as safely possible, respecting
//! SEMI/ANTI/OUTER null-preservation constraints.

use super::super::rule::RewriteRule;

/// Every predicate-pushdown rule in canonical application order.
pub(crate) fn predicate_pushdown_rules() -> Vec<Box<dyn RewriteRule>> {
    // Tasks 3–7 add each rule in sequence.
    Vec::new()
}
