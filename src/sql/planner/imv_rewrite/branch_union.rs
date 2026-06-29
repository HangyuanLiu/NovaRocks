use arrow::datatypes::DataType;

use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN;
use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::column_alloc::allocate_imv_output_column;
use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::sql::planner::plan::{
    LogicalAggregateNode, LogicalImvDeltaNode, LogicalPlanNode, LogicalUnionNode, PlanNodeKind,
};

pub(crate) struct RewriteBranchUnionRule;

impl LogicalRewriteRule for RewriteBranchUnionRule {
    fn name(&self) -> &'static str {
        "RewriteBranchUnion"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        let PlanNodeKind::ImvDelta(delta) = &plan.kind else {
            return false;
        };
        if !delta.is_root {
            return false;
        }
        let input = plan.unary_input();
        matches!(
            &input.kind,
            PlanNodeKind::Union(union)
                if union.all
                    && input.children.iter().all(is_branch_union_aggregate_branch)
                    && !plan_contains_imv_marker(input)
        )
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result(expr, ctx, |plan, ctx| {
            let LogicalPlanNode {
                kind,
                mut children,
                required_output_columns: _,
            } = plan;
            let PlanNodeKind::ImvDelta(delta) = kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            if !delta.is_root {
                return Ok(PlanRewriteResult::Unchanged);
            }
            let action_column = delta.action_column;
            if children.len() != 1 {
                return Ok(PlanRewriteResult::Unchanged);
            }
            let union_plan = children.remove(0);
            let LogicalPlanNode {
                kind,
                children: inputs,
                required_output_columns,
            } = union_plan;
            let PlanNodeKind::Union(union) = kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            if !union.all {
                return Err("Iceberg IMV branch UNION rewrite supports UNION ALL only".to_string());
            }
            if inputs.len() < 2 {
                return Err(
                    "Iceberg IMV branch UNION rewrite requires at least two aggregate branches"
                        .to_string(),
                );
            }

            for branch in &inputs {
                if !is_branch_union_aggregate_branch(branch) {
                    return Err(format!(
                        "Iceberg IMV branch UNION rewrite supports only aggregate or Project-over-Aggregate branches, got {}",
                        plan_kind(branch)
                    ));
                }
            }

            let ext = ctx
                .extension::<ImvExtension>()
                .ok_or_else(|| {
                    "RewriteBranchUnion requires ImvExtension in RewriteContext".to_string()
                })?
                .clone();
            let output_columns = branch_union_aggregate_change_stream_output_columns(&ext, ctx)?;
            let mut rewritten_inputs = Vec::with_capacity(inputs.len());
            for (idx, branch) in inputs.into_iter().enumerate() {
                let branch_id = i32::try_from(idx)
                    .map_err(|_| "Iceberg IMV branch UNION branch index overflow".to_string())?;
                let branch_kind = plan_kind(&branch);
                let branch = extract_branch_union_aggregate_branch(branch).ok_or_else(|| {
                    format!(
                        "Iceberg IMV branch UNION rewrite supports only aggregate or Project-over-Aggregate branches, got {}",
                        branch_kind
                    )
                })?;
                // Tag the aggregate core as an independent, branch-scoped delta sub-problem.
                // The existing aggregate-state (and join/union-delta beneath it) rules
                // decompose it in later stages, reading branch_scope off this marker.
                // Each branch becomes its own root delta sub-problem: `is_root` is
                // per-sub-problem here, so the post-branch plan intentionally holds one
                // root delta per branch (not a single global root).
                let scope = crate::sql::catalog::BranchScope {
                    branch_id_column_name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
                    branch_id,
                };
                let aggregate = LogicalPlanNode::new(
                    PlanNodeKind::Aggregate(branch.aggregate),
                    vec![branch.aggregate_input],
                    branch.aggregate_required_output_columns,
                );
                let core = LogicalPlanNode::new(
                    PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                        is_root: true,
                        action_column,
                        branch_scope: Some(scope),
                    }),
                    vec![aggregate],
                    None,
                );
                rewritten_inputs.push(core);
            }

            Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                PlanNodeKind::Union(LogicalUnionNode {
                    all: true,
                    output_columns,
                }),
                rewritten_inputs,
                required_output_columns,
            )))
        })
    }
}

struct BranchUnionAggregateBranch {
    aggregate: LogicalAggregateNode,
    aggregate_input: LogicalPlanNode,
    aggregate_required_output_columns: Option<std::collections::HashSet<ColumnId>>,
}

fn is_branch_union_aggregate_branch(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        PlanNodeKind::Aggregate(_) => true,
        PlanNodeKind::Project(_) => {
            matches!(&plan.unary_input().kind, PlanNodeKind::Aggregate(_))
        }
        _ => false,
    }
}

fn extract_branch_union_aggregate_branch(
    branch: LogicalPlanNode,
) -> Option<BranchUnionAggregateBranch> {
    let LogicalPlanNode {
        kind,
        mut children,
        required_output_columns,
    } = branch;
    match kind {
        PlanNodeKind::Aggregate(aggregate) => Some(BranchUnionAggregateBranch {
            aggregate,
            aggregate_input: single_child(&mut children)?,
            aggregate_required_output_columns: required_output_columns,
        }),
        PlanNodeKind::Project(project) => {
            let _ = project;
            let aggregate_plan = single_child(&mut children)?;
            let LogicalPlanNode {
                kind,
                mut children,
                required_output_columns: aggregate_required_output_columns,
            } = aggregate_plan;
            let PlanNodeKind::Aggregate(aggregate) = kind else {
                return None;
            };
            Some(BranchUnionAggregateBranch {
                aggregate,
                aggregate_input: single_child(&mut children)?,
                aggregate_required_output_columns,
            })
        }
        _ => None,
    }
}

fn single_child(children: &mut Vec<LogicalPlanNode>) -> Option<LogicalPlanNode> {
    if children.len() == 1 {
        Some(children.remove(0))
    } else {
        None
    }
}

fn branch_union_aggregate_change_stream_output_columns(
    ext: &ImvExtension,
    ctx: &RewriteContext,
) -> Result<Vec<OutputColumn>, String> {
    let (_shape, layout) = ext.mv_ctx.aggregate_shape_and_layout_for_execution()?;
    let mut columns =
        Vec::with_capacity(1 + layout.visible_columns.len() + layout.state_columns.len() + 2);
    columns.push(allocate_imv_output_column(
        ctx,
        &layout.row_id_column.column.name,
        DataType::Utf8,
        false,
        true,
    )?);
    for column in &layout.visible_columns {
        columns.push(allocate_imv_output_column(
            ctx,
            &column.name,
            column.data_type.clone(),
            column.nullable,
            false,
        )?);
    }
    for column in &layout.state_columns {
        let data_type = match column.state_role {
            crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::Single => {
                DataType::Binary
            }
            crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::RetractionCount => {
                column.data_type.clone()
            }
        };
        columns.push(allocate_imv_output_column(
            ctx,
            &column.name,
            data_type,
            column.nullable,
            true,
        )?);
    }
    columns.push(allocate_imv_output_column(
        ctx,
        ICEBERG_MV_BRANCH_ID_COLUMN,
        DataType::Int32,
        false,
        true,
    )?);
    columns.push(allocate_imv_output_column(
        ctx,
        ImvActionColumn::NAME,
        DataType::Int8,
        false,
        true,
    )?);
    Ok(columns)
}

fn plan_kind(plan: &LogicalPlanNode) -> &'static str {
    match &plan.kind {
        PlanNodeKind::Scan(_) => "Scan",
        PlanNodeKind::Filter(_) => "Filter",
        PlanNodeKind::Project(_) => "Project",
        PlanNodeKind::Aggregate(_) => "Aggregate",
        PlanNodeKind::Join(_) => "Join",
        PlanNodeKind::Union(_) => "Union",
        _ => "Other",
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::plan::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::*;
    use crate::engine::mv::refresh_context::IcebergMvRewriteContext;
    use crate::engine::mv::refresh_context::tests_support::{
        make_mv_definition, make_pin, make_ref, make_schema_contract, make_target, parse_query,
    };
    use crate::meta::repository::mv_contract::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BranchIdColumnContract, BranchUnionContract,
    };
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::optimizer_bridge::plan::logical_plan_to_opt_expr;
    use crate::sql::planner::plan::{
        AggregateCall, LogicalAggregateNode, LogicalFilterNode, LogicalJoinNode, LogicalPlanNode,
        LogicalProjectNode, LogicalScanNode, LogicalUnionNode, PlanNodeKind,
    };

    #[test]
    fn rewrites_top_union_of_aggregates_into_branch_scoped_merges() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx();
        let plan = root_delta(LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            }),
            vec![
                aggregate_over(scan("t1", 1)),
                aggregate_over(scan("t2", 10)),
            ],
            None,
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(rewritten_expr) = rule.apply(expr, &mut ctx).expect("rewrite")
        else {
            panic!("expected Changed(Union)");
        };
        let arena = ctx.scalar_arena();
        let rewritten = crate::sql::planner::optimizer_bridge::plan::opt_expr_to_logical_plan(
            rewritten_expr,
            &arena.borrow(),
        );
        let PlanNodeKind::Union(_) = &rewritten.kind else {
            panic!("expected Changed(Union), got {rewritten:?}");
        };

        assert_eq!(rewritten.children.len(), 2);
        for (idx, branch) in rewritten.children.iter().enumerate() {
            assert_branch_scoped_delta(branch, idx as i32);
        }
    }

    #[test]
    fn rewrites_project_over_aggregate_branches_into_branch_scoped_merges() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx();
        let plan = root_delta(LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(30, "total")],
            }),
            vec![
                project_over_aggregate(scan("t1", 1)),
                project_over_aggregate(scan("t2", 10)),
            ],
            None,
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(rewritten_expr) = rule.apply(expr, &mut ctx).expect("rewrite")
        else {
            panic!("expected Changed(Union)");
        };
        let arena = ctx.scalar_arena();
        let rewritten = crate::sql::planner::optimizer_bridge::plan::opt_expr_to_logical_plan(
            rewritten_expr,
            &arena.borrow(),
        );
        let PlanNodeKind::Union(_) = &rewritten.kind else {
            panic!("expected Changed(Union), got {rewritten:?}");
        };

        assert_eq!(rewritten.children.len(), 2);
        for (idx, branch) in rewritten.children.iter().enumerate() {
            assert_branch_scoped_delta(branch, idx as i32);
        }
    }

    #[test]
    fn rejects_non_aggregate_branch() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx();
        let plan = root_delta(LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            }),
            vec![aggregate_over(scan("t1", 1)), scan("t2", 10)],
            None,
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("scan branch must fail");
        assert!(
            err.contains("supports only aggregate or Project-over-Aggregate branches"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn does_not_match_marked_union() {
        let rule = RewriteBranchUnionRule;
        let ctx = build_ctx();
        let plan = root_delta(LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            }),
            vec![
                LogicalPlanNode::new(
                    PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                        is_root: false,
                        action_column: None,
                        branch_scope: None,
                    }),
                    vec![aggregate_over(scan("t1", 1))],
                    None,
                ),
                aggregate_over(scan("t2", 10)),
            ],
            None,
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn does_not_match_projection_filter_union() {
        let rule = RewriteBranchUnionRule;
        let ctx = build_ctx();
        let plan = root_delta(LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(2, "amount")],
            }),
            vec![project_over_filter("t1", 1), project_over_filter("t2", 10)],
            None,
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn pipeline_branch_union_of_aggregates_final_shape_is_stable() {
        use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
        use crate::sql::planner::imv_rewrite::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        // build_ctx() registers ice.db.b as the only known base table; both
        // branches must reference that same table so scan binding succeeds.
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            }),
            vec![aggregate_over(scan("b", 1)), aggregate_over(scan("b", 10))],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        let out_expr = build_imv_pipeline()
            .rewrite(expr, &mut ctx)
            .expect("pipeline must succeed");
        let arena = ctx.scalar_arena();
        let out = crate::sql::planner::optimizer_bridge::plan::opt_expr_to_logical_plan(
            out_expr,
            &arena.borrow(),
        );

        // Top is a Union whose branches are branch-scoped aggregate
        // change-streams carrying __branch_id__ and __change_op, with no IMV
        // marker left anywhere.
        assert!(
            !plan_contains_imv_marker(&out),
            "no marker may survive validation"
        );
        let PlanNodeKind::Union(union) = &out.kind else {
            panic!("expected top Union, got {out:?}")
        };
        assert_eq!(out.children.len(), 2);
        assert!(
            union
                .output_columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case("__branch_id__")),
            "union output must expose __branch_id__"
        );
        for branch in &out.children {
            assert_aggregate_change_stream_branch(branch);
        }
    }

    fn assert_branch_scoped_delta(branch: &LogicalPlanNode, expected_branch_id: i32) {
        let PlanNodeKind::ImvDelta(delta) = &branch.kind else {
            panic!("branch core must be a delegated ImvDelta, got {branch:?}")
        };
        assert!(
            delta.is_root,
            "branch sub-problem delta must be a root delta"
        );
        assert_eq!(
            delta.branch_scope.as_ref().map(|s| s.branch_id),
            Some(expected_branch_id)
        );
        assert!(
            matches!(&branch.unary_input().kind, PlanNodeKind::Aggregate(_)),
            "delta must sit directly over the Aggregate core"
        );
    }

    fn assert_aggregate_change_stream_branch(branch: &LogicalPlanNode) {
        let output_names = aggregate_change_stream_output_names(branch);
        assert!(
            output_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case("__branch_id__")),
            "change-stream branch output must expose __branch_id__"
        );
        assert!(
            output_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(ImvActionColumn::NAME)),
            "change-stream branch output must expose __change_op"
        );
        assert!(
            contains_join_kind(branch, JoinKind::LeftOuter),
            "relational change-stream branch must merge delta and old target state once"
        );
        assert!(
            contains_join_kind(branch, JoinKind::Cross),
            "relational change-stream branch must expand DELETE/INSERT branches once"
        );
        assert!(
            contains_branch_marker_values(branch),
            "relational change-stream branch must generate a branch marker VALUES source"
        );
        assert!(
            project_filter_contains_state_all_zero(branch),
            "relational change-stream branch must guard INSERT output with state_all_zero"
        );
        assert!(
            contains_target_state_scan(branch),
            "change-stream branch must read old target state"
        );
        assert!(
            contains_signed_state_aggregate(branch),
            "change-stream branch must contain signed state aggregate"
        );
        assert!(
            !contains_aggregate_state_merge(branch),
            "relation cutover must not emit AggregateStateMerge"
        );
    }

    fn aggregate_change_stream_output_names(branch: &LogicalPlanNode) -> Vec<&str> {
        match &branch.kind {
            PlanNodeKind::Project(project) => project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect(),
            PlanNodeKind::Union(union) => union
                .output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect(),
            PlanNodeKind::CTEAnchor(_) => aggregate_change_stream_output_names(branch.child(1)),
            other => panic!("expected aggregate change-stream branch, got {other:?}"),
        }
    }

    fn contains_join_kind(plan: &LogicalPlanNode, join_type: JoinKind) -> bool {
        matches!(
            &plan.kind,
            PlanNodeKind::Join(join) if join.join_type == join_type
        ) || plan
            .children
            .iter()
            .any(|child| contains_join_kind(child, join_type))
    }

    fn contains_branch_marker_values(plan: &LogicalPlanNode) -> bool {
        matches!(&plan.kind, PlanNodeKind::Values(values)
            if values.columns.iter().any(|column| {
                column.name.eq_ignore_ascii_case("__imv_change_branch")
            })
        ) || plan.children.iter().any(contains_branch_marker_values)
    }

    fn project_filter_contains_state_all_zero(plan: &LogicalPlanNode) -> bool {
        let PlanNodeKind::Project(_) = &plan.kind else {
            return plan
                .children
                .iter()
                .any(project_filter_contains_state_all_zero);
        };
        let Some(filter_plan) = plan.children.first() else {
            return false;
        };
        let PlanNodeKind::Filter(filter) = &filter_plan.kind else {
            return false;
        };
        expr_contains_function(&filter.predicate, "state_all_zero")
    }

    fn expr_contains_function(expr: &TypedExpr, name: &str) -> bool {
        match &expr.kind {
            ExprKind::FunctionCall {
                name: func, args, ..
            }
            | ExprKind::AggregateCall {
                name: func, args, ..
            } => {
                func.eq_ignore_ascii_case(name)
                    || args.iter().any(|arg| expr_contains_function(arg, name))
            }
            ExprKind::BinaryOp { left, right, .. } => {
                expr_contains_function(left, name) || expr_contains_function(right, name)
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. } => expr_contains_function(expr, name),
            ExprKind::InList { expr, list, .. } => {
                expr_contains_function(expr, name)
                    || list.iter().any(|item| expr_contains_function(item, name))
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                expr_contains_function(expr, name)
                    || expr_contains_function(low, name)
                    || expr_contains_function(high, name)
            }
            ExprKind::Like { expr, pattern, .. } => {
                expr_contains_function(expr, name) || expr_contains_function(pattern, name)
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_deref()
                    .is_some_and(|expr| expr_contains_function(expr, name))
                    || when_then.iter().any(|(when_expr, then_expr)| {
                        expr_contains_function(when_expr, name)
                            || expr_contains_function(then_expr, name)
                    })
                    || else_expr
                        .as_deref()
                        .is_some_and(|expr| expr_contains_function(expr, name))
            }
            ExprKind::LambdaFunction { body, .. } => expr_contains_function(body, name),
            ExprKind::Nested(expr) | ExprKind::Lambda { body: expr, .. } => {
                expr_contains_function(expr, name)
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                args.iter().any(|arg| expr_contains_function(arg, name))
                    || partition_by
                        .iter()
                        .any(|expr| expr_contains_function(expr, name))
                    || order_by
                        .iter()
                        .any(|item| expr_contains_function(&item.expr, name))
            }
            ExprKind::ColumnRef { .. }
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::SubqueryPlaceholder { .. } => false,
        }
    }

    fn contains_target_state_scan(plan: &LogicalPlanNode) -> bool {
        matches!(
            &plan.kind,
            PlanNodeKind::Scan(LogicalScanNode {
                table: TableDef {
                    source: ScanSource::IcebergMvTargetState(_),
                    ..
                },
                ..
            })
        ) || plan.children.iter().any(contains_target_state_scan)
    }

    fn contains_signed_state_aggregate(plan: &LogicalPlanNode) -> bool {
        matches!(
            &plan.kind,
            PlanNodeKind::Aggregate(LogicalAggregateNode { aggregates, .. })
                if aggregates.iter().any(|call| call.name.ends_with("_state_signed"))
        ) || plan.children.iter().any(contains_signed_state_aggregate)
    }

    fn contains_aggregate_state_merge(plan: &LogicalPlanNode) -> bool {
        matches!(&plan.kind, PlanNodeKind::AggregateStateMerge(_))
            || plan.children.iter().any(contains_aggregate_state_merge)
    }

    fn single_state_column(type_signature: &str) -> AggregateStateColumnContract {
        AggregateStateColumnContract {
            column_name: "__agg_state_s".to_string(),
            target_field_id: 200,
            type_signature: type_signature.to_string(),
            nullable: true,
            role: AggregateStateRoleContract::Single,
        }
    }

    fn retraction_count_state_column() -> AggregateStateColumnContract {
        AggregateStateColumnContract {
            column_name: "__agg_state___ivm_row_count".to_string(),
            target_field_id: 201,
            type_signature: "long".to_string(),
            nullable: false,
            role: AggregateStateRoleContract::RetractionCount,
        }
    }

    fn build_ctx() -> RewriteContext {
        let mut mv_def = make_mv_definition();
        mv_def.select_sql =
            "SELECT region, sum(amount) AS s FROM ice.db.b GROUP BY region".to_string();
        mv_def.primary_key_columns = vec!["region".to_string()];
        let mut contract = make_schema_contract();
        contract.target.visible_columns[0].output_name = "region".to_string();
        contract.target.visible_columns[1].output_name = "s".to_string();
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.branch = Some(BranchUnionContract {
            branch_id_column: BranchIdColumnContract {
                column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN
                    .to_string(),
                target_field_id: 998,
            },
            branch_count: 2,
            inner_apply_key_source: ApplyKeySource::GroupRowId,
        });
        contract.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: vec![
                single_state_column("binary"),
                retraction_count_state_column(),
            ],
        });
        mv_def.schema_contract = Some(contract.clone());

        let target_schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "region",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        101,
                        "s",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        999,
                        "__row_id__",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::required(
                        998,
                        "__branch_id__",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        200,
                        "__agg_state_s",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                    Arc::new(NestedField::required(
                        201,
                        "__agg_state___ivm_row_count",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("build schema"),
        );
        let mv_ctx = Arc::new(
            IcebergMvRewriteContext::from_parts(
                make_target(),
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                Arc::new(mv_def),
                Arc::new(parse_query(
                    "SELECT region, sum(amount) AS s FROM ice.db.b GROUP BY region",
                )),
                Arc::from(vec![make_ref("ice", "db", "b")]),
                Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")])),
                Some(99),
                "uuid-tgt".to_string(),
                target_schema,
                Some(Arc::new(contract)),
            )
            .expect("aggregate rewrite context must build"),
        );

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_scalar_arena(std::rc::Rc::new(
            std::cell::RefCell::new(ScalarArena::new()),
        ));
        let factory = std::rc::Rc::new(std::cell::RefCell::new(ColumnRefFactory::new()));
        factory.borrow_mut().reserve_until(100);
        ctx.set_column_ref_factory(std::rc::Rc::clone(&factory));
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx,
            annotation: ImvPlanAnnotation::default(),
        });
        ctx
    }

    fn root_delta(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: None,
                branch_scope: None,
            }),
            vec![input],
            None,
        )
    }

    fn aggregate_over(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_expr(1, "region")],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_expr(2, "amount")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: ColumnId::UNSET,
                }],
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
                already_pushed: false,
            }),
            vec![input],
            None,
        )
    }

    fn project_over_aggregate(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    ProjectItem {
                        expr: col_expr(1, "region"),
                        output_name: "region".to_string(),
                        output_column_id: ColumnId::new_for_test(1),
                    },
                    ProjectItem {
                        expr: col_expr(3, "s"),
                        output_name: "total".to_string(),
                        output_column_id: ColumnId::new_for_test(30),
                    },
                ],
                output_qualifier: None,
            }),
            vec![aggregate_over(input)],
            None,
        )
    }

    fn scan(name: &str, first_id: u32) -> LogicalPlanNode {
        let columns = vec![column_def("region"), column_def("amount")];
        LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: name.to_string(),
                    columns,
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::IcebergDataFiles {
                        table: IcebergTableInfo {
                            catalog: "ice".to_string(),
                            namespace: "db".to_string(),
                            table: name.to_string(),
                            table_uuid: Some(format!("uuid-{name}")),
                            current_snapshot_id: Some(22),
                            schema_id: 7,
                            location: format!("file:///tmp/ice/db/{name}"),
                            schema: IcebergSchemaDef { fields: Vec::new() },
                            serialized_metadata: None,
                            serialized_metadata_rows: None,
                        },
                        files: Vec::new(),
                        cloud_properties: BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                columns: vec![
                    output_column(first_id, "region"),
                    output_column(first_id + 1, "amount"),
                ],
                predicates: Vec::new(),
                required_columns: None,
                dict_columns: Vec::new(),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn project_over_filter(name: &str, first_id: u32) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    ProjectItem {
                        expr: col_expr(first_id, "region"),
                        output_name: "region".to_string(),
                        output_column_id: ColumnId::new_for_test(first_id),
                    },
                    ProjectItem {
                        expr: col_expr(first_id + 1, "amount"),
                        output_name: "amount".to_string(),
                        output_column_id: ColumnId::new_for_test(first_id + 1),
                    },
                ],
                output_qualifier: None,
            }),
            vec![filter_over(scan(name, first_id), first_id, "region")],
            None,
        )
    }

    fn filter_over(input: LogicalPlanNode, column_id: u32, column: &str) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col_expr(column_id, column)),
                        op: BinOp::Ge,
                        right: Box::new(TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Int(0)),
                            data_type: DataType::Int32,
                            nullable: false,
                        }),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![input],
            None,
        )
    }

    fn column_def(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: name.eq_ignore_ascii_case("s"),
            is_internal: false,
        }
    }

    fn col_expr(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn join_of(left: LogicalPlanNode, right: LogicalPlanNode) -> LogicalPlanNode {
        join_of_on(left, right, 1, 10)
    }

    fn join_of_on(
        left: LogicalPlanNode,
        right: LogicalPlanNode,
        left_region_id: u32,
        right_region_id: u32,
    ) -> LogicalPlanNode {
        // An inner equi-join on caller-selected region column ids.
        let condition = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_expr(left_region_id, "region")),
                op: BinOp::Eq,
                right: Box::new(col_expr(right_region_id, "region")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            vec![left, right],
            None,
        )
    }

    fn assert_rule_changed(ctx: &RewriteContext, rule_name: &str) {
        use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;

        assert!(
            ctx.trace().events().iter().any(|event| {
                matches!(event, RewriteTraceEvent::RuleChanged { rule, .. } if *rule == rule_name)
            }),
            "{rule_name} must change the plan, trace: {:?}",
            ctx.trace().events()
        );
    }

    #[test]
    fn pipeline_aggregate_over_filtered_join_composes() {
        use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
        use crate::sql::planner::imv_rewrite::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        let join = join_of(scan("b", 1), scan("b", 10));
        let filtered = filter_over(join, 1, "region");
        let plan = aggregate_over(filtered);

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        let out_expr = build_imv_pipeline()
            .rewrite(expr, &mut ctx)
            .expect("aggregate over filtered join must compose");
        let arena = ctx.scalar_arena();
        let out = crate::sql::planner::optimizer_bridge::plan::opt_expr_to_logical_plan(
            out_expr,
            &arena.borrow(),
        );

        assert!(
            !plan_contains_imv_marker(&out),
            "no IMV marker may survive: {out:?}"
        );
        assert_rule_changed(&ctx, "RewriteJoinDelta");
    }

    #[test]
    fn pipeline_aggregate_over_nested_join_composes() {
        use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
        use crate::sql::planner::imv_rewrite::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        let inner = join_of(scan("b", 1), scan("b", 10));
        let outer = join_of_on(inner, scan("b", 20), 1, 20);
        let plan = aggregate_over(outer);

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        let out_expr = build_imv_pipeline()
            .rewrite(expr, &mut ctx)
            .expect("aggregate over nested join must compose");
        let arena = ctx.scalar_arena();
        let out = crate::sql::planner::optimizer_bridge::plan::opt_expr_to_logical_plan(
            out_expr,
            &arena.borrow(),
        );

        assert!(
            !plan_contains_imv_marker(&out),
            "no IMV marker may survive: {out:?}"
        );
        assert_rule_changed(&ctx, "RewriteJoinDelta");
    }

    #[test]
    fn pipeline_branch_union_of_project_over_aggregate_composes() {
        use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
        use crate::sql::planner::imv_rewrite::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        // project_over_aggregate outputs: region (id=1) and total (id=30).
        // Both branches reference the registered base "ice.db.b" so scan binding succeeds.
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(30, "total")],
            }),
            vec![
                project_over_aggregate(scan("b", 1)),
                project_over_aggregate(scan("b", 10)),
            ],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        let out_expr = build_imv_pipeline()
            .rewrite(expr, &mut ctx)
            .expect("branch union of Project-over-Aggregate must compose");
        let arena = ctx.scalar_arena();
        let out = crate::sql::planner::optimizer_bridge::plan::opt_expr_to_logical_plan(
            out_expr,
            &arena.borrow(),
        );
        assert!(
            !plan_contains_imv_marker(&out),
            "no marker may survive: each Project-over-Aggregate branch must fully decompose"
        );
        let PlanNodeKind::Union(union) = &out.kind else {
            panic!("expected top Union, got {out:?}")
        };
        assert_eq!(out.children.len(), 2);
        assert!(
            union
                .output_columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case("__branch_id__")),
            "union output must expose __branch_id__"
        );
        for branch in &out.children {
            assert_aggregate_change_stream_branch(branch);
        }
    }

    #[test]
    fn pipeline_branch_union_of_aggregate_over_join_composes() {
        use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
        use crate::sql::planner::imv_rewrite::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            }),
            vec![
                aggregate_over(join_of(scan("b", 1), scan("b", 10))),
                aggregate_over(join_of(scan("b", 20), scan("b", 30))),
            ],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        let out_expr = build_imv_pipeline()
            .rewrite(expr, &mut ctx)
            .expect("branch union of aggregate-over-join must compose");
        let arena = ctx.scalar_arena();
        let out = crate::sql::planner::optimizer_bridge::plan::opt_expr_to_logical_plan(
            out_expr,
            &arena.borrow(),
        );
        assert!(
            !plan_contains_imv_marker(&out),
            "no marker may survive: the inner joins must be delta-expanded and bound"
        );
    }
}
