//! Common-subexpression elimination (CSE v1): a post-CBO physical-tree pass that
//! detects repeated non-trivial scalar subexpressions within an operator's
//! expression set and materializes each as a Project output column computed once,
//! rewriting consumers to reference it by ColumnId.
//!
//! See docs/design/specs/2026-06-21-optimizer-cse-v1-design.md.

use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::options::OptimizerOptions;
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
use crate::sql::optimizer::scalar::ScalarArena;

/// Stable rule name for `SET disable_optimizer_rules`.
pub(crate) const CSE_RULE: &str = "CommonSubexpressionReuse";

/// Entry point: rewrite the physical tree in place. Gated by `CSE_RULE`.
pub(crate) fn rewrite(
    root: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
    options: &OptimizerOptions,
) {
    if !options.is_enabled(CSE_RULE) {
        return;
    }
    rewrite_node(root, scalars, factory);
}

/// Post-order walk. Per-operator drivers are added in later tasks.
fn rewrite_node(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    for child in &mut node.children {
        rewrite_node(child, scalars, factory);
    }
    // Per-operator rewrite dispatch added in Tasks 3-6.
    let _ = (scalars, factory, &node.op);
}
