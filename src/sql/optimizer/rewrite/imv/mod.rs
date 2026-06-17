//! IMV-specific logical rewrite substrate. See
//! docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md.
//!
//! PR-α lands the foundation: empty pipeline, single-tenant extension slot
//! wrapper, no-op end-to-end behavior. PR-β adds Delta/Version marker
//! operators on top of this module without changing the public entrypoint.

/// Convert an `OptExpr` to a `LogicalPlanNode` using the ScalarArena stored
/// in the given `RewriteContext`. Called from IMV rule `apply` implementations
/// that delegate to existing `LogicalPlanNode`-based helpers.
pub(crate) fn opt_expr_to_plan(
    expr: crate::sql::optimizer::opt_expr::OptExpr,
    ctx: &crate::sql::optimizer::rewrite::context::RewriteContext,
) -> crate::sql::planner::plan::LogicalPlanNode {
    let arena = ctx.scalar_arena();
    crate::sql::optimizer::convert::opt_expr_to_logical_plan(expr, &arena.borrow())
}

/// Convert a `LogicalPlanNode` to an `OptExpr` using the ScalarArena stored
/// in the given `RewriteContext`. Called from IMV rule `apply` implementations
/// to produce the return value expected by the pipeline.
pub(crate) fn plan_to_opt_expr(
    plan: crate::sql::planner::plan::LogicalPlanNode,
    ctx: &crate::sql::optimizer::rewrite::context::RewriteContext,
) -> crate::sql::optimizer::opt_expr::OptExpr {
    let arena = ctx.scalar_arena();
    crate::sql::optimizer::convert::logical_plan_to_opt_expr(&plan, &mut arena.borrow_mut())
}

/// Convenience wrapper: convert `OptExpr → LogicalPlanNode`, run the given
/// closure, then convert the result `LogicalPlanNode → OptExpr`. All three
/// steps use the same ScalarArena from the RewriteContext.
pub(crate) fn bridge_apply<F>(
    expr: crate::sql::optimizer::opt_expr::OptExpr,
    ctx: &crate::sql::optimizer::rewrite::context::RewriteContext,
    f: F,
) -> crate::sql::optimizer::opt_expr::OptExpr
where
    F: FnOnce(
        crate::sql::planner::plan::LogicalPlanNode,
    ) -> crate::sql::planner::plan::LogicalPlanNode,
{
    let plan = opt_expr_to_plan(expr, ctx);
    let out = f(plan);
    plan_to_opt_expr(out, ctx)
}

/// Intermediate result type used by closures passed to [`bridge_apply_result`].
/// Mirrors [`RewriteResult`] but holds a [`LogicalPlanNode`] in the `Changed`
/// variant so closures can work with plan-level types directly.
pub(crate) enum PlanRewriteResult {
    Unchanged,
    Changed(crate::sql::planner::plan::LogicalPlanNode),
    Rejected(crate::sql::optimizer::rewrite::result::RewriteDiagnostic),
}

/// Convenience wrapper: convert `OptExpr → LogicalPlanNode`, run the given
/// fallible closure, then convert the result `LogicalPlanNode → OptExpr`.
///
/// The closure returns [`PlanRewriteResult`] so it can work entirely with
/// `LogicalPlanNode` types. The wrapper converts `PlanRewriteResult::Changed`
/// back to `RewriteResult::Changed(OptExpr)`.
pub(crate) fn bridge_apply_result<F>(
    expr: crate::sql::optimizer::opt_expr::OptExpr,
    ctx: &crate::sql::optimizer::rewrite::context::RewriteContext,
    f: F,
) -> Result<crate::sql::optimizer::rewrite::result::RewriteResult, String>
where
    F: FnOnce(
        crate::sql::planner::plan::LogicalPlanNode,
        &crate::sql::optimizer::rewrite::context::RewriteContext,
    ) -> Result<PlanRewriteResult, String>,
{
    let plan = opt_expr_to_plan(expr, ctx);
    let result = f(plan, ctx)?;
    let arena = ctx.scalar_arena();
    let converted = match result {
        PlanRewriteResult::Changed(plan_out) => {
            let opt_out = crate::sql::optimizer::convert::logical_plan_to_opt_expr(
                &plan_out,
                &mut arena.borrow_mut(),
            );
            crate::sql::optimizer::rewrite::result::RewriteResult::Changed(opt_out)
        }
        PlanRewriteResult::Unchanged => crate::sql::optimizer::rewrite::result::RewriteResult::Unchanged,
        PlanRewriteResult::Rejected(diag) => crate::sql::optimizer::rewrite::result::RewriteResult::Rejected(diag),
    };
    Ok(converted)
}

pub(crate) mod action_column;
pub(crate) mod action_propagation;
pub(crate) mod aggregate_rewrite;
pub(crate) mod annotation;
pub(crate) mod apply_key;
pub(crate) mod branch_union;
pub(crate) mod delta_pushdown;
pub(crate) mod entrypoint;
pub(crate) mod join_delta;
pub(crate) mod join_delta_shape;
pub(crate) mod marker;
pub(crate) mod partition_derivation;
pub(crate) mod pipeline;
pub(crate) mod row_id_column;
pub(crate) mod scan_binding;
pub(crate) mod target_state;
pub(crate) mod union_delta;
