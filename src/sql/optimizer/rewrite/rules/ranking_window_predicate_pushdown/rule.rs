use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::sql::codegen::helpers::group_win_exprs_by_sig;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::split_and;
use crate::sql::planner::plan::{LogicalPlanNode, LogicalPlanNodeKind, LogicalSortNode};

pub(crate) struct RankingWindowPredicatePushdownRule;

/// Map a window function name to its `SortTopNType` variant.
/// Returns `None` for non-ranking functions (e.g. avg, sum, lead, lag).
fn ranking_topn_type(name: &str) -> Option<crate::exec::node::sort::SortTopNType> {
    use crate::exec::node::sort::SortTopNType::*;
    match name.to_ascii_lowercase().as_str() {
        "row_number" => Some(RowNumber),
        "rank" => Some(Rank),
        "dense_rank" => Some(DenseRank),
        _ => None,
    }
}

impl LogicalRewriteRule for RankingWindowPredicatePushdownRule {
    fn name(&self) -> &'static str {
        "RankingWindowPredicatePushdown"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlanNode, _ctx: &RewriteContext) -> bool {
        let LogicalPlanNodeKind::Filter(_) = &plan.kind else {
            return false;
        };
        let window_plan = match &plan.unary_input().kind {
            LogicalPlanNodeKind::Window(_) => plan.unary_input(),
            LogicalPlanNodeKind::Project(_) => match &plan.unary_input().unary_input().kind {
                LogicalPlanNodeKind::Window(_) => plan.unary_input().unary_input(),
                _ => return false,
            },
            _ => return false,
        };
        matches!(
            &window_plan.unary_input().kind,
            LogicalPlanNodeKind::Sort(sort) if !sort.analytic_partition_by.is_empty()
        )
    }

    fn apply(
        &self,
        plan: LogicalPlanNode,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        // --- Step 1: Destructure Filter -> optional Project -> Window -> Sort ---
        let LogicalPlanNodeKind::Filter(filter) = &plan.kind else {
            return Ok(RewriteResult::Unchanged);
        };

        let filter_input = plan.unary_input();
        let (project_plan_opt, project_opt, window_plan, window) = match &filter_input.kind {
            LogicalPlanNodeKind::Window(window) => (None, None, filter_input, window),
            LogicalPlanNodeKind::Project(project) => match &filter_input.unary_input().kind {
                LogicalPlanNodeKind::Window(window) => (
                    Some(filter_input),
                    Some(project),
                    filter_input.unary_input(),
                    window,
                ),
                _ => return Ok(RewriteResult::Unchanged),
            },
            _ => return Ok(RewriteResult::Unchanged),
        };

        let sort_plan = window_plan.unary_input();
        let LogicalPlanNodeKind::Sort(sort) = &sort_plan.kind else {
            return Ok(RewriteResult::Unchanged);
        };

        // --- Step 2: Idempotency guard ---
        if sort.partition_limit.is_some() {
            return Ok(RewriteResult::Unchanged);
        }

        // --- Step 3: All-ranking guard (CRITICAL correctness guard) ---
        // If any window expr is a full-partition aggregate (not a ranking function),
        // truncating the partition would corrupt its result.
        if window.window_exprs.is_empty()
            || window
                .window_exprs
                .iter()
                .any(|w| ranking_topn_type(&w.name).is_none())
        {
            return Ok(RewriteResult::Unchanged);
        }

        // --- Step 3b: Single-signature guard ---
        // All ranking window exprs must share ONE (partition_by, order_by, frame)
        // signature.  When two ranking fns have DIFFERENT ORDER BY, the analytic Sort
        // is keyed on window_exprs[0]'s order — setting partition_limit truncates
        // every partition by that first order, corrupting results for any window expr
        // with a different ORDER BY.  The safe case (same PARTITION+ORDER, e.g.
        // rank()+dense_rank() over the same spec) produces exactly one group.
        if group_win_exprs_by_sig(&window.window_exprs).len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }

        // --- Step 4: Non-empty partition guard ---
        if sort.analytic_partition_by.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }

        // --- Step 5: Find a ranking window expr with a finite upper bound ---
        // When a Project is present, we must map the filter's ColumnRef back through
        // the project to the window's output_column_id.  Only a BARE passthrough
        // (ProjectItem.expr == ColumnRef(w.output_column_id)) is allowed.
        let found = window.window_exprs.iter().find_map(|w_expr| {
            // Determine which ColumnId the filter predicate references for this ranking expr.
            let filter_col_id = if let Some(proj) = project_opt {
                // Walk the project items to find an item whose output_column_id matches
                // something the filter sees, and whose expr is a bare ColumnRef to
                // w_expr.output_column_id.
                proj.items.iter().find_map(|item| {
                    // Is this item a bare passthrough of w_expr.output_column_id?
                    if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind
                        && *column_id == w_expr.output_column_id
                    {
                        return Some(item.output_column_id);
                    }
                    None
                })?
            } else {
                // No project: the filter references the window output directly.
                w_expr.output_column_id
            };

            // Check whether the filter predicate provides a finite upper bound on filter_col_id.
            let k = rank_upper_bound(&filter.predicate, filter_col_id)?;
            Some((k, w_expr))
        });

        let Some((k, matched_w_expr)) = found else {
            return Ok(RewriteResult::Unchanged);
        };

        // --- Step 6: Rebuild the tree with partition_limit / topn_type on the Sort ---
        let topn_type = ranking_topn_type(&matched_w_expr.name).unwrap();

        // Clone and mutate the Sort.
        let new_sort = LogicalPlanNode::new(
            LogicalPlanNodeKind::Sort(LogicalSortNode {
                items: sort.items.clone(),
                analytic_partition_by: sort.analytic_partition_by.clone(),
                partition_limit: Some(k),
                topn_type: Some(topn_type),
            }),
            sort_plan.children.clone(),
            sort_plan.required_output_columns.clone(),
        );

        // Rebuild Window over the new Sort.
        let new_window = LogicalPlanNode::new(
            LogicalPlanNodeKind::Window(window.clone()),
            vec![new_sort],
            window_plan.required_output_columns.clone(),
        );

        // Rebuild Project (if present) over the new Window.
        let mid = if let Some(project_plan) = project_plan_opt {
            LogicalPlanNode::new(
                project_plan.kind.clone(),
                vec![new_window],
                project_plan.required_output_columns.clone(),
            )
        } else {
            new_window
        };

        // Rebuild Filter over mid.
        let new_filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(filter.clone()),
            vec![mid],
            plan.required_output_columns,
        );

        Ok(RewriteResult::Changed(new_filter))
    }
}

/// Smallest finite upper bound K (>= 1) such that the conjunctive predicate can
/// only pass rows with rank_col <= K.  Returns None if no finite positive bound
/// exists (e.g., lower-bound-only predicates, K <= 0, or no reference to rank_col).
pub(crate) fn rank_upper_bound(predicate: &TypedExpr, rank_col: ColumnId) -> Option<usize> {
    let mut best: Option<i64> = None;
    for conj in split_and(predicate.clone()) {
        if let Some(k) = conjunct_upper_bound(&conj, rank_col) {
            best = Some(best.map_or(k, |b| b.min(k)));
        }
    }
    match best {
        Some(k) if k >= 1 => usize::try_from(k).ok(),
        _ => None,
    }
}

fn is_rank_col(e: &TypedExpr, rank_col: ColumnId) -> bool {
    matches!(&e.kind, ExprKind::ColumnRef { column_id, .. } if *column_id == rank_col)
}

fn int_lit(e: &TypedExpr) -> Option<i64> {
    match &e.kind {
        ExprKind::Literal(LiteralValue::Int(v)) => Some(*v),
        _ => None,
    }
}

fn conjunct_upper_bound(e: &TypedExpr, rank_col: ColumnId) -> Option<i64> {
    match &e.kind {
        ExprKind::BinaryOp { left, op, right } => {
            let (lit, col_on_left) = if is_rank_col(left, rank_col) {
                (int_lit(right)?, true)
            } else if is_rank_col(right, rank_col) {
                (int_lit(left)?, false)
            } else {
                return None;
            };
            match (op, col_on_left) {
                // rank_col <= lit  or  lit >= rank_col
                (BinOp::Le, true) | (BinOp::Ge, false) => Some(lit),
                // rank_col < lit  or  lit > rank_col
                (BinOp::Lt, true) | (BinOp::Gt, false) => Some(lit - 1),
                // rank_col = lit  or  lit = rank_col
                (BinOp::Eq, _) => Some(lit),
                _ => None,
            }
        }
        // BETWEEN low AND high: upper bound is `high`
        ExprKind::Between {
            expr,
            high,
            negated: false,
            ..
        } if is_rank_col(expr, rank_col) => int_lit(high),
        // IN (v1, v2, ...): upper bound is the max value in the list
        ExprKind::InList {
            expr,
            list,
            negated: false,
        } if is_rank_col(expr, rank_col) => list
            .iter()
            .map(int_lit)
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .max(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{RankingWindowPredicatePushdownRule, rank_upper_bound};
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn col(id: ColumnId) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: format!("rk_{}", id.0),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn int(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn binop(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn le(col: TypedExpr, v: i64) -> TypedExpr {
        binop(col, BinOp::Le, int(v))
    }

    fn lt(col: TypedExpr, v: i64) -> TypedExpr {
        binop(col, BinOp::Lt, int(v))
    }

    fn eq(col: TypedExpr, v: i64) -> TypedExpr {
        binop(col, BinOp::Eq, int(v))
    }

    fn ge(col: TypedExpr, v: i64) -> TypedExpr {
        binop(col, BinOp::Ge, int(v))
    }

    fn between(expr: TypedExpr, low_v: i64, high_v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Between {
                expr: Box::new(expr),
                low: Box::new(int(low_v)),
                high: Box::new(int(high_v)),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn in_list(expr: TypedExpr, values: &[i64]) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(expr),
                list: values.iter().map(|&v| int(v)).collect(),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: rule is recognized by the registry
    // -----------------------------------------------------------------------

    #[test]
    fn ranking_window_rule_is_known() {
        assert!(
            crate::sql::optimizer::rewrite::registry::is_known_rewrite_rule_name(
                "RankingWindowPredicatePushdown"
            )
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: rank_upper_bound extracts <=, <, =, BETWEEN, IN correctly
    //         and returns None for lower-bound-only / K<=0 / other column
    // -----------------------------------------------------------------------

    #[test]
    fn rank_upper_bound_extracts_le_lt_eq_between_in() {
        let rk = ColumnId::new_for_test(7);
        let other = ColumnId::new_for_test(99);

        // rk <= 5  -> Some(5)
        assert_eq!(rank_upper_bound(&le(col(rk), 5), rk), Some(5));

        // rk < 5   -> Some(4)
        assert_eq!(rank_upper_bound(&lt(col(rk), 5), rk), Some(4));

        // rk = 3   -> Some(3)
        assert_eq!(rank_upper_bound(&eq(col(rk), 3), rk), Some(3));

        // BETWEEN 2 AND 9  -> Some(9)
        assert_eq!(rank_upper_bound(&between(col(rk), 2, 9), rk), Some(9));

        // IN (1, 3, 5)  -> Some(5)
        assert_eq!(rank_upper_bound(&in_list(col(rk), &[1, 3, 5]), rk), Some(5));

        // rk >= 5  (lower bound only) -> None
        assert_eq!(rank_upper_bound(&ge(col(rk), 5), rk), None);

        // rk <= 0  (K <= 0) -> None
        assert_eq!(rank_upper_bound(&le(col(rk), 0), rk), None);

        // comparison on a DIFFERENT column -> None
        assert_eq!(rank_upper_bound(&le(col(other), 5), rk), None);
    }

    // Verify the rule struct itself is importable and constructable.
    #[test]
    fn ranking_window_predicate_pushdown_rule_is_constructable() {
        let _ = RankingWindowPredicatePushdownRule;
    }

    // -----------------------------------------------------------------------
    // Helpers for integration tests (matches + apply)
    // -----------------------------------------------------------------------

    use crate::exec::node::sort::SortTopNType;
    use crate::sql::analysis::{OutputColumn, ProjectItem, SortItem};
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::planner::plan::{
        LogicalFilterNode, LogicalPlanNode, LogicalPlanNodeKind, LogicalProjectNode,
        LogicalSortNode, LogicalValuesNode, LogicalWindowNode, WindowExpr,
    };

    fn col_ref(id: ColumnId) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: format!("c_{}", id.0),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn output_col(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn empty_values() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        )
    }

    fn sort_item(e: TypedExpr) -> SortItem {
        SortItem {
            expr: e,
            asc: true,
            nulls_first: true,
        }
    }

    /// Build Sort(partition_by=[p_col], items=[p_col]) over an empty Values.
    fn make_sort(p_id: ColumnId) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Sort(LogicalSortNode {
                items: vec![sort_item(col_ref(p_id))],
                analytic_partition_by: vec![col_ref(p_id)],
                partition_limit: None,
                topn_type: None,
            }),
            vec![empty_values()],
            None,
        )
    }

    fn make_sort_with_limit(p_id: ColumnId, limit: usize) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Sort(LogicalSortNode {
                items: vec![sort_item(col_ref(p_id))],
                analytic_partition_by: vec![col_ref(p_id)],
                partition_limit: Some(limit),
                topn_type: Some(SortTopNType::Rank),
            }),
            vec![empty_values()],
            None,
        )
    }

    fn make_sort_no_partition(p_id: ColumnId) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Sort(LogicalSortNode {
                items: vec![sort_item(col_ref(p_id))],
                analytic_partition_by: vec![],
                partition_limit: None,
                topn_type: None,
            }),
            vec![empty_values()],
            None,
        )
    }

    fn make_window_expr(fn_name: &str, output_id: ColumnId, p_id: ColumnId) -> WindowExpr {
        WindowExpr {
            name: fn_name.to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![col_ref(p_id)],
            order_by: vec![sort_item(col_ref(p_id))],
            window_frame: None,
            result_type: DataType::Int64,
            output_name: fn_name.to_string(),
            output_column_id: output_id,
            ignore_nulls: false,
        }
    }

    fn window_over(
        input: LogicalPlanNode,
        window_exprs: Vec<WindowExpr>,
        output_columns: Vec<OutputColumn>,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Window(LogicalWindowNode {
                window_exprs,
                output_columns,
            }),
            vec![input],
            None,
        )
    }

    fn filter_over(input: LogicalPlanNode, predicate: TypedExpr) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode { predicate }),
            vec![input],
            None,
        )
    }

    fn project_over(input: LogicalPlanNode, items: Vec<ProjectItem>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Project(LogicalProjectNode {
                items,
                output_qualifier: None,
            }),
            vec![input],
            None,
        )
    }

    /// Build Filter(rk_col <= k) -> Window(fn_name, out=rk_id, partition=[p_id]) -> Sort -> Values
    fn make_filter_window_sort(
        fn_name: &str,
        rk_id: ColumnId,
        p_id: ColumnId,
        k: i64,
    ) -> LogicalPlanNode {
        let sort = make_sort(p_id);
        let window = window_over(
            sort,
            vec![make_window_expr(fn_name, rk_id, p_id)],
            vec![output_col(rk_id, fn_name)],
        );
        filter_over(window, binop(col_ref(rk_id), BinOp::Le, int(k)))
    }

    fn apply_rule(plan: LogicalPlanNode) -> RewriteResult {
        let rule = RankingWindowPredicatePushdownRule;
        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        rule.apply(plan, &mut ctx).unwrap()
    }

    fn extract_sort_from_changed(result: RewriteResult) -> LogicalSortNode {
        if let RewriteResult::Changed(plan) = result {
            let LogicalPlanNodeKind::Filter(_) = &plan.kind else {
                panic!("expected Changed(Filter(...)), got {:?}", plan);
            };
            let filter_input = plan.unary_input();
            let window_plan = match &filter_input.kind {
                LogicalPlanNodeKind::Window(_) => filter_input,
                LogicalPlanNodeKind::Project(_) => {
                    let window_plan = filter_input.unary_input();
                    if matches!(&window_plan.kind, LogicalPlanNodeKind::Window(_)) {
                        window_plan
                    } else {
                        panic!("expected Window under Project, got {:?}", window_plan);
                    }
                }
                _ => panic!(
                    "expected Window or Project under Filter, got {:?}",
                    filter_input
                ),
            };
            let sort_plan = window_plan.unary_input();
            let LogicalPlanNodeKind::Sort(sort) = &sort_plan.kind else {
                panic!("expected Sort under Window, got {:?}", sort_plan);
            };
            sort.clone()
        } else {
            panic!("expected Changed(Filter(...)), got {:?}", result);
        }
    }

    // -----------------------------------------------------------------------
    // Test 3: fires on rank() per group — sets partition_limit + topn_type
    // -----------------------------------------------------------------------

    #[test]
    fn fires_on_rank_per_group_sets_partition_limit() {
        let rk_id = ColumnId::new_for_test(1);
        let p_id = ColumnId::new_for_test(2);
        let plan = make_filter_window_sort("rank", rk_id, p_id, 2);

        let rule = RankingWindowPredicatePushdownRule;
        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        assert!(rule.matches(&plan, &ctx), "matches() must return true");

        let result = rule.apply(plan, &mut ctx).unwrap();
        let sort = extract_sort_from_changed(result);
        assert_eq!(sort.partition_limit, Some(2));
        assert_eq!(sort.topn_type, Some(SortTopNType::Rank));
    }

    // -----------------------------------------------------------------------
    // Test 4: fires for row_number and dense_rank too
    // -----------------------------------------------------------------------

    #[test]
    fn fires_for_row_number_and_dense_rank() {
        let rk_id = ColumnId::new_for_test(10);
        let p_id = ColumnId::new_for_test(11);

        // row_number
        let plan_rn = make_filter_window_sort("row_number", rk_id, p_id, 3);
        let sort_rn = extract_sort_from_changed(apply_rule(plan_rn));
        assert_eq!(sort_rn.partition_limit, Some(3));
        assert_eq!(sort_rn.topn_type, Some(SortTopNType::RowNumber));

        // dense_rank
        let plan_dr = make_filter_window_sort("dense_rank", rk_id, p_id, 5);
        let sort_dr = extract_sort_from_changed(apply_rule(plan_dr));
        assert_eq!(sort_dr.partition_limit, Some(5));
        assert_eq!(sort_dr.topn_type, Some(SortTopNType::DenseRank));
    }

    // -----------------------------------------------------------------------
    // Test 5: rejects when window has a non-ranking aggregate window expr
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_when_window_has_aggregate_over() {
        let rk_id = ColumnId::new_for_test(20);
        let p_id = ColumnId::new_for_test(21);
        let sort = make_sort(p_id);

        // Window has both rank() AND avg() — avg is not a ranking fn, so we must reject.
        let avg_id = ColumnId::new_for_test(22);
        let window = window_over(
            sort,
            vec![
                make_window_expr("rank", rk_id, p_id),
                make_window_expr("avg", avg_id, p_id),
            ],
            vec![output_col(rk_id, "rank"), output_col(avg_id, "avg")],
        );
        let plan = filter_over(window, binop(col_ref(rk_id), BinOp::Le, int(2)));

        assert!(matches!(apply_rule(plan), RewriteResult::Unchanged));
    }

    // -----------------------------------------------------------------------
    // Test 6: rejects when sort.analytic_partition_by is empty
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_empty_partition_by() {
        let rk_id = ColumnId::new_for_test(30);
        let p_id = ColumnId::new_for_test(31);
        let sort = make_sort_no_partition(p_id);
        let window = window_over(
            sort,
            vec![make_window_expr("rank", rk_id, p_id)],
            vec![output_col(rk_id, "rank")],
        );
        let plan = filter_over(window, binop(col_ref(rk_id), BinOp::Le, int(2)));

        let rule = RankingWindowPredicatePushdownRule;
        let ctx = RewriteContext::new(RewriteConsumer::Query);
        // matches() should return false because analytic_partition_by is empty
        assert!(!rule.matches(&plan, &ctx));
        assert!(matches!(apply_rule(plan), RewriteResult::Unchanged));
    }

    // -----------------------------------------------------------------------
    // Test 7: rejects when predicate has no finite upper bound (rk >= 2)
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_no_upper_bound() {
        let rk_id = ColumnId::new_for_test(40);
        let p_id = ColumnId::new_for_test(41);
        let sort = make_sort(p_id);
        let window = window_over(
            sort,
            vec![make_window_expr("rank", rk_id, p_id)],
            vec![output_col(rk_id, "rank")],
        );
        // Filter: rk >= 2 (lower bound only — no upper bound)
        let plan = filter_over(window, binop(col_ref(rk_id), BinOp::Ge, int(2)));

        assert!(matches!(apply_rule(plan), RewriteResult::Unchanged));
    }

    // -----------------------------------------------------------------------
    // Test 8: idempotent — sort already has partition_limit set
    // -----------------------------------------------------------------------

    #[test]
    fn idempotent_when_sort_already_has_partition_limit() {
        let rk_id = ColumnId::new_for_test(50);
        let p_id = ColumnId::new_for_test(51);
        let sort = make_sort_with_limit(p_id, 2);
        let window = window_over(
            sort,
            vec![make_window_expr("rank", rk_id, p_id)],
            vec![output_col(rk_id, "rank")],
        );
        let plan = filter_over(window, binop(col_ref(rk_id), BinOp::Le, int(2)));

        assert!(matches!(apply_rule(plan), RewriteResult::Unchanged));
    }

    // -----------------------------------------------------------------------
    // Test 9: sees through a bare passthrough Project
    // -----------------------------------------------------------------------

    #[test]
    fn sees_through_bare_passthrough_project() {
        let rk_id = ColumnId::new_for_test(60); // window output column id
        let proj_rk_id = ColumnId::new_for_test(61); // project output column id (projected rk)
        let p_id = ColumnId::new_for_test(62);

        let sort = make_sort(p_id);
        let window = window_over(
            sort,
            vec![make_window_expr("rank", rk_id, p_id)],
            vec![output_col(rk_id, "rank")],
        );

        // Project: proj_rk_id <- rk_id (identity/passthrough)
        let project = project_over(
            window,
            vec![ProjectItem {
                expr: col_ref(rk_id), // bare ColumnRef to window output
                output_name: "rk".to_string(),
                output_column_id: proj_rk_id,
            }],
        );

        // Filter references the projected column (proj_rk_id), not rk_id directly
        let plan = filter_over(project, binop(col_ref(proj_rk_id), BinOp::Le, int(3)));

        let rule = RankingWindowPredicatePushdownRule;
        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        assert!(
            rule.matches(&plan, &ctx),
            "matches() must fire on Filter->Project->Window->Sort"
        );

        let result = rule.apply(plan, &mut ctx).unwrap();
        let sort = extract_sort_from_changed(result);
        assert_eq!(sort.partition_limit, Some(3));
        assert_eq!(sort.topn_type, Some(SortTopNType::Rank));
    }

    // -----------------------------------------------------------------------
    // Test: rejects mixed ranking+aggregate window (tpc-ds q47/q57 shape)
    //
    // Window has rank() OVER w AND avg(x) OVER w.  The filter is on the rank
    // column, and the Sort has a non-empty analytic_partition_by — so matches()
    // fires — but apply() must return Unchanged because truncating the partition
    // would corrupt the avg result (Step 3 all-ranking guard).
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_mixed_ranking_and_aggregate_window() {
        let rk_id = ColumnId::new_for_test(80); // rank() output
        let avg_id = ColumnId::new_for_test(81); // avg() output
        let p_id = ColumnId::new_for_test(82);

        let sort = make_sort(p_id); // analytic_partition_by is non-empty
        let window = window_over(
            sort,
            vec![
                make_window_expr("rank", rk_id, p_id),
                make_window_expr("avg", avg_id, p_id),
            ],
            vec![output_col(rk_id, "rank"), output_col(avg_id, "avg")],
        );
        // Filter on the rank column only (rk_id <= 2).
        let plan = filter_over(window, binop(col_ref(rk_id), BinOp::Le, int(2)));

        let rule = RankingWindowPredicatePushdownRule;
        let ctx = RewriteContext::new(RewriteConsumer::Query);
        // matches() sees Filter -> Window -> Sort(non-empty partition) and fires.
        assert!(
            rule.matches(&plan, &ctx),
            "matches() should fire — the structural shape is valid"
        );
        // apply() must reject because avg is not a ranking function.
        assert!(
            matches!(apply_rule(plan), RewriteResult::Unchanged),
            "apply() must return Unchanged when window contains a non-ranking expr"
        );
    }

    // -----------------------------------------------------------------------
    // Test 10: rejects when the Project transforms the rank column (not bare)
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_when_project_transforms_rank_col() {
        let rk_id = ColumnId::new_for_test(70);
        let proj_rk_id = ColumnId::new_for_test(71);
        let p_id = ColumnId::new_for_test(72);

        let sort = make_sort(p_id);
        let window = window_over(
            sort,
            vec![make_window_expr("rank", rk_id, p_id)],
            vec![output_col(rk_id, "rank")],
        );

        // Project: proj_rk_id <- rk_id + 1 (NOT a bare passthrough)
        let project = project_over(
            window,
            vec![ProjectItem {
                expr: binop(col_ref(rk_id), BinOp::Add, int(1)),
                output_name: "rk_plus_one".to_string(),
                output_column_id: proj_rk_id,
            }],
        );

        // Filter references the projected column
        let plan = filter_over(project, binop(col_ref(proj_rk_id), BinOp::Le, int(3)));

        assert!(matches!(apply_rule(plan), RewriteResult::Unchanged));
    }

    // -----------------------------------------------------------------------
    // Test: rejects multiple ranking fns with DIFFERENT ORDER BY (C1 bug shape)
    //
    // Window has rank() ORDER BY a AND rank() ORDER BY b (same PARTITION BY p,
    // different ORDER BY).  group_win_exprs_by_sig returns 2 groups → Unchanged.
    // This is exactly the bug shape: the analytic Sort is keyed on exprs[0]'s
    // order, so setting partition_limit would corrupt exprs[1]'s result.
    // -----------------------------------------------------------------------

    fn make_window_expr_with_order(
        fn_name: &str,
        output_id: ColumnId,
        p_id: ColumnId,
        order_id: ColumnId,
    ) -> WindowExpr {
        WindowExpr {
            name: fn_name.to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![col_ref(p_id)],
            order_by: vec![sort_item(col_ref(order_id))],
            window_frame: None,
            result_type: DataType::Int64,
            output_name: fn_name.to_string(),
            output_column_id: output_id,
            ignore_nulls: false,
        }
    }

    #[test]
    fn rejects_multiple_ranking_signatures_different_order() {
        let rka_id = ColumnId::new_for_test(90); // rank() ORDER BY a
        let rkb_id = ColumnId::new_for_test(91); // rank() ORDER BY b
        let p_id = ColumnId::new_for_test(92);
        let a_id = ColumnId::new_for_test(93);
        let b_id = ColumnId::new_for_test(94);

        // Sort keyed on partition=[p_id], order=[a_id] (first window's order)
        let sort = LogicalPlanNode::new(
            LogicalPlanNodeKind::Sort(LogicalSortNode {
                items: vec![sort_item(col_ref(p_id)), sort_item(col_ref(a_id))],
                analytic_partition_by: vec![col_ref(p_id)],
                partition_limit: None,
                topn_type: None,
            }),
            vec![empty_values()],
            None,
        );

        // Window has TWO ranking exprs with different ORDER BY signatures.
        let window = window_over(
            sort,
            vec![
                make_window_expr_with_order("rank", rka_id, p_id, a_id),
                make_window_expr_with_order("rank", rkb_id, p_id, b_id),
            ],
            vec![output_col(rka_id, "rka"), output_col(rkb_id, "rkb")],
        );

        // Filter on the SECOND ranking expr's column (rkb <= 2) — the one that
        // would be corrupted if partition_limit were set on the first-order Sort.
        let plan = filter_over(window, binop(col_ref(rkb_id), BinOp::Le, int(2)));

        // matches() fires (structural shape is valid)
        let rule = RankingWindowPredicatePushdownRule;
        let ctx = RewriteContext::new(RewriteConsumer::Query);
        assert!(
            rule.matches(&plan, &ctx),
            "matches() should fire on this structural shape"
        );

        // apply() must return Unchanged — different ORDER BY signatures detected.
        assert!(
            matches!(apply_rule(plan), RewriteResult::Unchanged),
            "apply() must return Unchanged when ranking fns have different ORDER BY"
        );
    }

    // -----------------------------------------------------------------------
    // Test: fires when two ranking fns share the SAME (partition_by, order_by)
    //
    // rank() + dense_rank() over PARTITION p ORDER o → single signature group →
    // group_win_exprs_by_sig returns 1 group → rule fires and sets partition_limit.
    // -----------------------------------------------------------------------

    #[test]
    fn fires_for_same_signature_multi_fn() {
        let rk_id = ColumnId::new_for_test(100); // rank() output
        let drk_id = ColumnId::new_for_test(101); // dense_rank() output
        let p_id = ColumnId::new_for_test(102);
        let o_id = ColumnId::new_for_test(103);

        let sort = LogicalPlanNode::new(
            LogicalPlanNodeKind::Sort(LogicalSortNode {
                items: vec![sort_item(col_ref(p_id)), sort_item(col_ref(o_id))],
                analytic_partition_by: vec![col_ref(p_id)],
                partition_limit: None,
                topn_type: None,
            }),
            vec![empty_values()],
            None,
        );

        // Both exprs share PARTITION BY p ORDER BY o → same signature.
        let window = window_over(
            sort,
            vec![
                make_window_expr_with_order("rank", rk_id, p_id, o_id),
                make_window_expr_with_order("dense_rank", drk_id, p_id, o_id),
            ],
            vec![output_col(rk_id, "rk"), output_col(drk_id, "drk")],
        );

        // Filter on the rank column (rk <= 3).
        let plan = filter_over(window, binop(col_ref(rk_id), BinOp::Le, int(3)));

        // Rule must FIRE — single signature, both are ranking fns, non-empty partition.
        let sort_node = extract_sort_from_changed(apply_rule(plan));
        assert_eq!(
            sort_node.partition_limit,
            Some(3),
            "partition_limit must be set to 3 for same-signature rank+dense_rank"
        );
        assert_eq!(
            sort_node.topn_type,
            Some(SortTopNType::Rank),
            "topn_type must reflect the matched ranking function (rank)"
        );
    }
}
