//! `PushDownApplyFilter` — ports StarRocks `PushDownApplyFilterRule`.
//!
//! Matches a correlated scalar `Apply` whose inner is:
//!   `[Project?] Filter(corr_pred AND residual_pred)(inner_scan)`
//! where the inner is **NOT** an Aggregate (that case belongs to
//! `PushDownApplyAggFilter`). Rewrites the inner Filter: correlated EQ
//! conjuncts move onto `Apply.correlation_conjuncts`; residual conjuncts
//! stay as the inner Filter (or the Filter is removed if all conjuncts
//! were correlated). `need_check_max_rows` stays `true` — no aggregate
//! means `ScalarApplyToJoin`'s with-check branch must add the row guard.

use std::collections::HashSet;

use super::decorrelate_util::{all_binary_eq, orient_eq, partition_conjuncts};
use crate::sql::analysis::ExprKind;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::combine_and;
use crate::sql::planner::plan::{
    ApplyKind, LogicalApplyNode, LogicalFilterNode, LogicalPlanNode, LogicalPlanNodeKind,
    LogicalProjectNode,
};

pub(crate) struct PushDownApplyFilter;

impl LogicalRewriteRule for PushDownApplyFilter {
    fn name(&self) -> &'static str {
        "PushDownApplyFilter"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlanNode, _ctx: &RewriteContext) -> bool {
        let LogicalPlanNodeKind::Apply(a) = &plan.kind else {
            return false;
        };
        if a.kind != ApplyKind::Scalar {
            return false;
        }
        if a.correlation_column_ids.is_empty() {
            return false;
        }
        let corr_ids: HashSet<ColumnId> = a.correlation_column_ids.iter().copied().collect();
        inner_has_correlated_nonagg_filter(plan.right(), &corr_ids)
    }

    fn apply(
        &self,
        plan: LogicalPlanNode,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        let LogicalPlanNode {
            kind,
            mut children,
            required_output_columns,
        } = plan;
        let LogicalPlanNodeKind::Apply(a) = kind else {
            return Ok(RewriteResult::Unchanged);
        };
        if children.len() != 2 {
            return Ok(RewriteResult::Unchanged);
        }
        let right = children.remove(1);
        let left = children.remove(0);
        let corr_ids: HashSet<ColumnId> = a.correlation_column_ids.iter().copied().collect();

        // Peel the optional leading Project and extract the filter node.
        let peeled = peel_inner(&right, &corr_ids)
            .ok_or_else(|| "PushDownApplyFilter: inner shape mismatch".to_string())?;

        // Split the Filter predicate into (correlated, residual).
        let predicate = peeled.filter.predicate.clone();
        let (correlated, residual) = partition_conjuncts(predicate, &corr_ids);

        // If nothing to hoist, leave unchanged.
        if correlated.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }
        if !all_binary_eq(&correlated) {
            return Err(
                "non-EQ correlated predicate in correlated subquery is not supported".to_string(),
            );
        }

        // Require each correlated EQ conjunct's inner side to be a ColumnRef.
        // Non-column inner sides are out of M1b scope — fall back to Unchanged.
        for conj in &correlated {
            let Some((_, inner_side)) = orient_eq(conj, &corr_ids) else {
                return Ok(RewriteResult::Unchanged);
            };
            if !matches!(&inner_side.kind, ExprKind::ColumnRef { .. }) {
                return Ok(RewriteResult::Unchanged);
            }
        }

        // Rebuild the filter input: drop the Filter if all conjuncts were correlated,
        // or keep a Filter with only the residual conjuncts.
        let new_filter_input: LogicalPlanNode = if residual.is_empty() {
            peeled.filter_input.clone()
        } else {
            LogicalPlanNode::new(
                LogicalPlanNodeKind::Filter(LogicalFilterNode {
                    predicate: combine_and(residual),
                }),
                vec![peeled.filter_input.clone()],
                None,
            )
        };

        // Re-wrap in the leading Project if present (input is updated to the new filter input).
        let new_inner: LogicalPlanNode = if let Some(proj) = peeled.leading_project {
            LogicalPlanNode::new(
                LogicalPlanNodeKind::Project(LogicalProjectNode {
                    items: proj.items.clone(),
                    output_qualifier: proj.output_qualifier.clone(),
                }),
                vec![new_filter_input],
                None,
            )
        } else {
            new_filter_input
        };

        // Append the correlated EQ conjuncts to correlation_conjuncts.
        // need_check_max_rows stays true — ScalarApplyToJoin's with-check branch handles this.
        let mut new_correlation = a.correlation_conjuncts.clone();
        new_correlation.extend(correlated);

        Ok(RewriteResult::Changed(LogicalPlanNode::new(
            LogicalPlanNodeKind::Apply(LogicalApplyNode {
                correlation_conjuncts: new_correlation,
                // need_check_max_rows stays true: no aggregate → ScalarApplyToJoin
                // must add the count(1)/any_value/assert_true row-check.
                ..a
            }),
            vec![left, new_inner],
            required_output_columns,
        )))
    }
}

/// Returns true iff the given plan has the shape:
///   `[Project?] Filter{corr_pred}(inner)`
/// where the Filter is **not** underneath an Aggregate (non-agg case),
/// and the predicate has at least one conjunct referencing `corr_ids`.
///
/// If the inner is `Aggregate(Filter(...))` this returns false so that
/// `PushDownApplyAggFilter` can own it (mutual exclusion guarantee).
fn inner_has_correlated_nonagg_filter(
    plan: &LogicalPlanNode,
    corr_ids: &HashSet<ColumnId>,
) -> bool {
    let after_project = if matches!(&plan.kind, LogicalPlanNodeKind::Project(_)) {
        plan.unary_input()
    } else {
        plan
    };
    // The node after the optional project must be a Filter, NOT an Aggregate.
    // An Aggregate (possibly over a Filter) belongs to PushDownApplyAggFilter.
    match &after_project.kind {
        LogicalPlanNodeKind::Aggregate(_) => false,
        LogicalPlanNodeKind::Filter(f) => {
            // At least one conjunct must reference a corr_id.
            use crate::sql::optimizer::rewrite::rules::utils::{collect_column_id_refs, split_and};
            split_and(f.predicate.clone())
                .iter()
                .any(|c| !collect_column_id_refs(c).is_disjoint(corr_ids))
        }
        _ => false,
    }
}

struct PeeledFilter<'a> {
    leading_project: Option<&'a LogicalProjectNode>,
    filter: &'a LogicalFilterNode,
    filter_input: &'a LogicalPlanNode,
}

/// Destructures the inner plan into `[Project?] Filter(input)`.
/// Returns `None` if the shape doesn't match (non-agg Filter required).
fn peel_inner<'a>(
    plan: &'a LogicalPlanNode,
    corr_ids: &HashSet<ColumnId>,
) -> Option<PeeledFilter<'a>> {
    match &plan.kind {
        LogicalPlanNodeKind::Project(proj) => {
            let (filter, filter_input) = peel_corr_filter(plan.unary_input(), corr_ids)?;
            Some(PeeledFilter {
                leading_project: Some(proj),
                filter,
                filter_input,
            })
        }
        _ => {
            let (filter, filter_input) = peel_corr_filter(plan, corr_ids)?;
            Some(PeeledFilter {
                leading_project: None,
                filter,
                filter_input,
            })
        }
    }
}

fn peel_corr_filter<'a>(
    plan: &'a LogicalPlanNode,
    corr_ids: &HashSet<ColumnId>,
) -> Option<(&'a LogicalFilterNode, &'a LogicalPlanNode)> {
    // Only match a Filter that is NOT an Aggregate-over-Filter (that's PushDownApplyAggFilter).
    // A plain Filter node with a correlated predicate is what we want.
    let LogicalPlanNodeKind::Filter(f) = &plan.kind else {
        return None;
    };
    // Verify at least one conjunct references a corr_id.
    use crate::sql::optimizer::rewrite::rules::utils::{collect_column_id_refs, split_and};
    let has_corr = split_and(f.predicate.clone())
        .iter()
        .any(|c| !collect_column_id_refs(c).is_disjoint(corr_ids));
    if has_corr {
        Some((f, plan.unary_input()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::plan::*;
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::planner::plan::{
        AggregateCall, ApplyKind, LogicalAggregateNode, LogicalApplyNode, LogicalFilterNode,
        LogicalPlanNode, LogicalPlanNodeKind, LogicalScanNode, LogicalValuesNode,
    };

    // ---- Column ID constants -------------------------------------------------
    const T2_K: ColumnId = ColumnId(1); // t2.k  (inner correlation column)
    const T2_V2: ColumnId = ColumnId(2); // t2.v2 (inner value column)
    const OUTER_K: ColumnId = ColumnId(100); // t1.k as seen inside the subquery
    const APPLY_OUT: ColumnId = ColumnId(20); // the Apply's output column

    fn col_ref(id: ColumnId, name: &str, dt: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: dt,
            nullable: false,
        }
    }

    fn col_ref_nullable(id: ColumnId, name: &str, dt: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: dt,
            nullable: true,
        }
    }

    fn eq_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn gt_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Gt,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn make_t2_scan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "default".to_string(),
                table: TableDef {
                    name: "t2".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: T2_K,
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: T2_V2,
                        name: "v2".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
            }),
            vec![],
            None,
        )
    }

    /// Build the inner plan with a leading Project:
    ///   Project(v2) -> Filter(t2.k == OUTER AND v2 > 5)(Scan t2)
    fn inner_project_over_corr_filter_with_residual() -> LogicalPlanNode {
        let corr_pred = eq_expr(
            col_ref(T2_K, "k", DataType::Int64),
            col_ref(OUTER_K, "k", DataType::Int64),
        );
        let residual_pred = gt_expr(
            col_ref(T2_V2, "v2", DataType::Int64),
            TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(5)),
                data_type: DataType::Int64,
                nullable: false,
            },
        );
        let combined_pred = and_expr(corr_pred, residual_pred);

        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: combined_pred,
            }),
            vec![make_t2_scan()],
            None,
        );

        LogicalPlanNode::new(
            LogicalPlanNodeKind::Project(LogicalProjectNode {
                items: vec![ProjectItem {
                    expr: col_ref(T2_V2, "v2", DataType::Int64),
                    output_name: "v2".to_string(),
                    output_column_id: T2_V2,
                }],
                output_qualifier: None,
            }),
            vec![filter],
            None,
        )
    }

    fn correlated_nonagg_apply() -> LogicalPlanNode {
        let outer_values = LogicalPlanNode::new(
            LogicalPlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: OUTER_K,
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![],
            None,
        );

        LogicalPlanNode::new(
            LogicalPlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: T2_V2,
                correlation_column_ids: vec![OUTER_K],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![outer_values, inner_project_over_corr_filter_with_residual()],
            None,
        )
    }

    /// Core correctness test: the rule hoists the correlated EQ and keeps the residual.
    /// Input:  Apply{ right: Project(v2)(Filter(t2.k==OUTER AND v2>5)(Scan t2)),
    ///                correlation_column_ids={OUTER}, correlation_conjuncts=[],
    ///                need_check_max_rows=true }
    /// Output: Apply{ right: Project(v2)(Filter(v2>5)(Scan t2)),
    ///                correlation_conjuncts=[OUTER==t2.k],
    ///                need_check_max_rows=true (UNCHANGED) }
    #[test]
    fn push_down_apply_filter_hoists_correlated_eq_keeps_residual() {
        use crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs;

        let rule = PushDownApplyFilter;
        let plan = correlated_nonagg_apply();
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        assert!(
            rule.matches(&plan, &ctx),
            "rule must match a correlated non-agg Apply with a Filter"
        );

        let result = rule
            .apply(plan, &mut ctx)
            .expect("rule apply must not error");
        let new_plan = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let LogicalPlanNodeKind::Apply(new_apply) = &new_plan.kind else {
            panic!("expected Apply, got: {new_plan:?}");
        };

        // need_check_max_rows must stay TRUE (non-agg path, row-check required).
        assert!(
            new_apply.need_check_max_rows,
            "need_check_max_rows must remain true for non-agg correlated Apply"
        );

        // correlation_conjuncts must contain the OUTER == t2.k EQ.
        assert_eq!(
            new_apply.correlation_conjuncts.len(),
            1,
            "must have exactly 1 correlation conjunct"
        );
        let conj = &new_apply.correlation_conjuncts[0];
        let ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = &conj.kind
        else {
            panic!("correlation conjunct must be a BinaryOp(Eq), got: {conj:?}");
        };
        let all_ids: HashSet<ColumnId> = collect_column_id_refs(left)
            .union(&collect_column_id_refs(right))
            .copied()
            .collect();
        assert!(all_ids.contains(&OUTER_K), "must reference OUTER_K");
        assert!(all_ids.contains(&T2_K), "must reference T2_K");

        // The inner is Project(v2)(Filter(v2>5)(Scan t2)).
        let right_plan = new_plan.right();
        let LogicalPlanNodeKind::Project(proj) = &right_plan.kind else {
            panic!("right child must be Project, got: {:?}", right_plan);
        };
        // Project passes v2 through.
        assert_eq!(proj.items.len(), 1);
        assert_eq!(proj.items[0].output_column_id, T2_V2);

        // Project's input: Filter with residual only (v2 > 5).
        let residual_plan = right_plan.unary_input();
        let LogicalPlanNodeKind::Filter(residual_filter) = &residual_plan.kind else {
            panic!(
                "project input must be Filter (residual), got: {:?}",
                residual_plan
            );
        };
        // The residual filter predicate must NOT reference OUTER_K.
        let residual_ids = collect_column_id_refs(&residual_filter.predicate);
        assert!(
            !residual_ids.contains(&OUTER_K),
            "residual filter must not reference OUTER_K; got ids: {residual_ids:?}"
        );

        // Residual filter's input must be the Scan.
        assert!(
            matches!(
                &residual_plan.unary_input().kind,
                LogicalPlanNodeKind::Scan(_)
            ),
            "residual filter input must be Scan"
        );
    }

    /// Test: when the inner Filter has ONLY the correlated conjunct (no residual),
    /// the Filter is removed entirely and the Project sits directly over the Scan.
    #[test]
    fn push_down_apply_filter_removes_filter_when_no_residual() {
        // Filter: only t2.k == OUTER (no residual)
        let corr_pred = eq_expr(
            col_ref(T2_K, "k", DataType::Int64),
            col_ref(OUTER_K, "k", DataType::Int64),
        );

        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: corr_pred,
            }),
            vec![make_t2_scan()],
            None,
        );

        let inner = LogicalPlanNode::new(
            LogicalPlanNodeKind::Project(LogicalProjectNode {
                items: vec![ProjectItem {
                    expr: col_ref(T2_V2, "v2", DataType::Int64),
                    output_name: "v2".to_string(),
                    output_column_id: T2_V2,
                }],
                output_qualifier: None,
            }),
            vec![filter],
            None,
        );

        let apply = LogicalPlanNode::new(
            LogicalPlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: T2_V2,
                correlation_column_ids: vec![OUTER_K],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::Values(LogicalValuesNode {
                        rows: vec![],
                        columns: vec![OutputColumn {
                            column_id: OUTER_K,
                            name: "k".to_string(),
                            data_type: DataType::Int64,
                            nullable: false,
                            is_internal: false,
                        }],
                    }),
                    vec![],
                    None,
                ),
                inner,
            ],
            None,
        );

        let rule = PushDownApplyFilter;
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let result = rule
            .apply(apply, &mut ctx)
            .expect("rule apply must not error");
        let new_plan = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let LogicalPlanNodeKind::Apply(new_apply) = &new_plan.kind else {
            panic!("expected Apply");
        };

        // need_check_max_rows must stay true.
        assert!(new_apply.need_check_max_rows);
        // One correlation conjunct hoisted.
        assert_eq!(new_apply.correlation_conjuncts.len(), 1);

        // Inner: Project sits directly over the Scan (Filter removed).
        let right_plan = new_plan.right();
        let LogicalPlanNodeKind::Project(_) = &right_plan.kind else {
            panic!("right child must be Project");
        };
        assert!(
            matches!(&right_plan.unary_input().kind, LogicalPlanNodeKind::Scan(_)),
            "project input must be Scan when all filter conjuncts were correlated; got: {:?}",
            right_plan.unary_input()
        );
    }

    /// No-match test: an Apply whose inner is Aggregate(Filter(...)) must NOT match
    /// PushDownApplyFilter — that belongs to PushDownApplyAggFilter (mutual exclusion).
    #[test]
    fn push_down_apply_filter_no_match_aggregate_shape() {
        // Inner: Aggregate{group_by:[]}(Filter(t2.k==OUTER)(Scan t2))
        // This is the AGGREGATE shape — PushDownApplyAggFilter owns it.
        const MAX_RESULT: ColumnId = ColumnId(10);

        let corr_pred = eq_expr(
            col_ref(T2_K, "k", DataType::Int64),
            col_ref(OUTER_K, "k", DataType::Int64),
        );
        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: corr_pred,
            }),
            vec![make_t2_scan()],
            None,
        );
        let inner = LogicalPlanNode::new(
            LogicalPlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "max".to_string(),
                    args: vec![col_ref(T2_V2, "v2", DataType::Int64)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: MAX_RESULT,
                }],
                output_columns: vec![OutputColumn {
                    column_id: MAX_RESULT,
                    name: "max(v2)".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                }],
                already_pushed: false,
            }),
            vec![filter],
            None,
        );

        let apply = LogicalPlanNode::new(
            LogicalPlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: MAX_RESULT,
                correlation_column_ids: vec![OUTER_K],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::Values(LogicalValuesNode {
                        rows: vec![],
                        columns: vec![],
                    }),
                    vec![],
                    None,
                ),
                inner,
            ],
            None,
        );

        let rule = PushDownApplyFilter;
        let ctx = RewriteContext::for_query(Vec::<String>::new());

        // MUST NOT match: the aggregate shape belongs to PushDownApplyAggFilter.
        assert!(
            !rule.matches(&apply, &ctx),
            "PushDownApplyFilter must NOT match the aggregate-inner shape (mutual exclusion with PushDownApplyAggFilter)"
        );
    }

    /// No-match test: an uncorrelated Apply must not match.
    #[test]
    fn push_down_apply_filter_no_match_uncorrelated() {
        let rule = PushDownApplyFilter;
        let ctx = RewriteContext::for_query(Vec::<String>::new());

        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: APPLY_OUT,
                correlation_column_ids: vec![],
                // uncorrelated
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::Values(LogicalValuesNode {
                        rows: vec![],
                        columns: vec![],
                    }),
                    vec![],
                    None,
                ),
                LogicalPlanNode::new(
                    LogicalPlanNodeKind::Values(LogicalValuesNode {
                        rows: vec![],
                        columns: vec![],
                    }),
                    vec![],
                    None,
                ),
            ],
            None,
        );

        assert!(
            !rule.matches(&plan, &ctx),
            "must not match uncorrelated Apply"
        );
    }
}
