//! Plan-time partition derivation: resolve the contract-level
//! `PartitionDerivationSpec` and record the outcome on `ImvPlanAnnotation`.
//!
//! P1 scope (umbrella spec §5.1 / D5): matches aggregate-state-merge shapes
//! only; the annotation is observability + P2 input — live pruning still
//! flows from plan-time manifest derivation, so this rule never changes the
//! plan and never fails the rewrite.

use crate::engine::mv::partition::resolve_partition_derivation_spec;
use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
use crate::sql::catalog::{ScanSource, TableDef};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::imv_rewrite::annotation::{
    ImvExtension, ImvPartitionAnnotation, ImvPlanAnnotation,
};
use crate::sql::planner::imv_rewrite::opt_expr_to_plan;
use crate::sql::planner::plan::{
    LogicalAggregateNode, LogicalPlanNode, LogicalProjectNode, LogicalScanNode, PlanNodeKind,
};

pub(crate) struct DerivePartitionSpecRule;

impl LogicalRewriteRule for DerivePartitionSpecRule {
    fn name(&self) -> &'static str {
        "DerivePartitionSpec"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        ctx.extension::<ImvExtension>()
            .is_some_and(|ext| ext.annotation.partition.is_none())
            && (matches!(
                &expr.op,
                crate::sql::optimizer::operator::Operator::LogicalAggregateStateMerge(_)
            ) || contains_aggregate_change_stream(&opt_expr_to_plan(expr.clone(), ctx)))
    }

    fn apply(&self, _expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or("DerivePartitionSpec requires ImvExtension")?
            .clone();

        let outcome = match resolve_partition_derivation_spec(&ext.mv_ctx.schema_contract) {
            Ok(None) => ImvPartitionAnnotation::Unpartitioned,
            Ok(Some(spec)) => ImvPartitionAnnotation::Derivable { specs: vec![spec] },
            Err(err) => ImvPartitionAnnotation::NotDerivable {
                reason: err.to_string(),
            },
        };

        tracing::info!(
            event = "iceberg_mv.partition_derivation",
            mv_id = ext.mv_ctx.mv_id,
            outcome = match &outcome {
                ImvPartitionAnnotation::Unpartitioned => "unpartitioned",
                ImvPartitionAnnotation::Derivable { .. } => "derivable",
                ImvPartitionAnnotation::NotDerivable { .. } => "not_derivable",
            },
            reason = outcome_reason(&outcome),
            "IMV partition derivation spec resolved"
        );

        ctx.set_extension::<ImvExtension>(ImvExtension {
            annotation: ImvPlanAnnotation {
                partition: Some(outcome),
            },
            ..ext
        });
        Ok(RewriteResult::Unchanged)
    }
}

fn outcome_reason(outcome: &ImvPartitionAnnotation) -> &str {
    match outcome {
        ImvPartitionAnnotation::NotDerivable { reason } => reason.as_str(),
        _ => "",
    }
}

fn contains_aggregate_change_stream(plan: &LogicalPlanNode) -> bool {
    contains_aggregate_change_stream_union(plan)
        || contains_relational_aggregate_change_stream(plan)
}

fn contains_aggregate_change_stream_union(plan: &LogicalPlanNode) -> bool {
    let matched = match &plan.kind {
        PlanNodeKind::Union(_) => {
            has_change_stream_union_output(plan)
                && contains_target_state_scan(plan)
                && contains_signed_state_aggregate(plan)
        }
        PlanNodeKind::CTEAnchor(_) if plan.children.len() == 2 => {
            has_change_stream_union_output(plan.child(1))
                && contains_target_state_scan(plan.child(0))
                && contains_signed_state_aggregate(plan.child(0))
        }
        _ => false,
    };
    matched
        || plan
            .children
            .iter()
            .any(contains_aggregate_change_stream_union)
}

fn contains_relational_aggregate_change_stream(plan: &LogicalPlanNode) -> bool {
    let matched = match &plan.kind {
        PlanNodeKind::Project(project) => {
            has_change_stream_project_output(project)
                && project_filter_contains_state_all_zero(plan)
                && contains_join_kind(plan, JoinKind::LeftOuter)
                && contains_join_kind(plan, JoinKind::Cross)
                && contains_branch_marker_values(plan)
                && contains_target_state_scan(plan)
                && contains_signed_state_aggregate(plan)
        }
        _ => false,
    };
    matched
        || plan
            .children
            .iter()
            .any(contains_relational_aggregate_change_stream)
}

fn has_change_stream_union_output(plan: &LogicalPlanNode) -> bool {
    matches!(
        &plan.kind,
        PlanNodeKind::Union(union)
            if union.output_columns.iter().any(|column| {
                column.name.eq_ignore_ascii_case(ImvActionColumn::NAME)
            })
    )
}

fn has_change_stream_project_output(project: &LogicalProjectNode) -> bool {
    project
        .items
        .iter()
        .any(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
}

fn project_filter_contains_state_all_zero(plan: &LogicalPlanNode) -> bool {
    let PlanNodeKind::Project(_) = &plan.kind else {
        return false;
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
