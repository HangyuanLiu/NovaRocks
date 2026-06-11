//! `ApplyToWindow` — ports StarRocks `ScalarApply2AnalyticRule` ("WinMagic").
//!
//! Rewrites `Filter( ... lhs op APPLY_OUT ... )` over a decorrelated correlated
//! scalar-aggregate `Apply` into a `Window` (analytic) over the OUTER relation,
//! discarding the subquery subtree. Runs BEFORE `ScalarApplyToJoin`; on any
//! precondition failure returns `Unchanged` so `ScalarApplyToJoin` produces the
//! M1 join form. Never errors (the join form is always a valid fallback).

use std::collections::HashSet;

use super::win_magic_util::{collect_scan_column_map, collect_table_ids, expr_phys_eq, TableIdentity};
use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::{collect_column_id_refs, split_and};
use crate::sql::planner::plan::{AggregateCall, AggregateNode, ApplyKind, ApplyNode, LogicalPlan};

const WHITELIST: &[&str] = &["count", "sum", "avg", "min", "max"];

pub(crate) struct ApplyToWindow;

/// Everything Task 3's transform needs, validated by `check_preconditions`.
#[allow(dead_code)]
pub(super) struct WinMagicMatch {
    /// All conjuncts of the matched WHERE Filter (already AND-split).
    pub outer_conjuncts: Vec<TypedExpr>,
    /// The single outer conjunct that references `APPLY_OUT`.
    pub subquery_conjunct: TypedExpr,
    /// Outer-side ColumnRef of each correlation conjunct — the window PARTITION BY keys.
    pub partition_by: Vec<TypedExpr>,
    /// The inner single aggregate call (name in WHITELIST, non-distinct).
    pub inner_agg: AggregateCall,
}

impl LogicalRewriteRule for ApplyToWindow {
    fn name(&self) -> &'static str {
        "ApplyToWindow"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        let LogicalPlan::Filter(f) = plan else { return false };
        let LogicalPlan::Apply(a) = f.input.as_ref() else { return false };
        a.kind == ApplyKind::Scalar
            && !a.need_check_max_rows
            && !a.correlation_conjuncts.is_empty()
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Filter(f) = &plan else { return Ok(RewriteResult::Unchanged) };
        let LogicalPlan::Apply(a) = f.input.as_ref() else { return Ok(RewriteResult::Unchanged) };
        let Some(_m) = check_preconditions(&f.predicate, a) else {
            return Ok(RewriteResult::Unchanged);
        };
        // Task 3 replaces this with the transform.
        Ok(RewriteResult::Unchanged)
    }
}

/// Port of StarRocks ScalarApply2AnalyticRule's check() family. Returns the
/// validated match data, or None if any precondition fails (-> caller Unchanged).
pub(super) fn check_preconditions(
    where_pred: &TypedExpr,
    a: &ApplyNode,
) -> Option<WinMagicMatch> {
    // (0) Inner: peel optional leading Project, require a vector Aggregate with a
    // single non-DISTINCT whitelisted aggregate.
    let agg = peel_to_aggregate(&a.right)?;
    if agg.aggregates.len() != 1 {
        return None;
    }
    let inner_agg = agg.aggregates[0].clone();
    if inner_agg.distinct {
        return None;
    }
    if !WHITELIST.contains(&inner_agg.name.as_str()) {
        return None;
    }

    // (1) No LIMIT and only whitelisted operators in either subtree.
    if !operator_whitelist_ok(&a.left, false) {
        return None;
    }
    if !operator_whitelist_ok(&a.right, true) {
        return None;
    }

    // (2) Table-set identity: outerTables == subqueryTables + exactly 1 extra;
    // no duplicate physical table on either side (rejects self-joins).
    let outer_tabs = collect_table_ids(&a.left);
    let sub_tabs = collect_table_ids(&a.right);
    let outer_set: HashSet<TableIdentity> = outer_tabs.iter().cloned().collect();
    let sub_set: HashSet<TableIdentity> = sub_tabs.iter().cloned().collect();
    if outer_tabs.len() != outer_set.len() || sub_tabs.len() != sub_set.len() {
        return None;
    }
    if outer_set.len() != sub_set.len() + 1 {
        return None;
    }
    if !sub_set.is_subset(&outer_set) {
        return None;
    }
    let extra: Vec<&TableIdentity> = outer_set.difference(&sub_set).collect();
    if extra.len() != 1 {
        return None;
    }
    let correlated_outer_table = extra[0].clone();

    // (3) Partition-by keys = outer side of each correlation conjunct. Verify each
    // outer side is a ColumnRef of `correlated_outer_table`.
    let corr_ids: HashSet<ColumnId> = a.correlation_column_ids.iter().copied().collect();
    let col_map = collect_scan_column_map(&a.left);
    let mut partition_by = Vec::new();
    for conj in &a.correlation_conjuncts {
        let (outer_side, _inner) = super::decorrelate_util::orient_eq(conj, &corr_ids)?;
        let ExprKind::ColumnRef { column_id, .. } = &outer_side.kind else {
            return None;
        };
        match col_map.get(column_id) {
            Some((tab, _)) if *tab == correlated_outer_table => {}
            _ => return None,
        }
        partition_by.push(outer_side.clone());
    }

    // (4) Predicate identity (StarRocks checkPredicate, 4 steps). Use a phys map
    // spanning BOTH subtrees so inner/outer instances unify.
    let full_map = {
        let mut m = collect_scan_column_map(&a.left);
        m.extend(collect_scan_column_map(&a.right));
        m
    };
    let mut outer_conjuncts = split_and(where_pred.clone());

    // 4a. Each correlation conjunct must have a phys-identical twin among outer conjuncts.
    let mut unmatched_corr = a.correlation_conjuncts.clone();
    unmatched_corr.retain(|cc| {
        if let Some(pos) = outer_conjuncts
            .iter()
            .position(|oc| expr_phys_eq(cc, oc, &full_map))
        {
            outer_conjuncts.remove(pos);
            false
        } else {
            true
        }
    });
    if !unmatched_corr.is_empty() {
        return None;
    }

    // 4b. Exactly the subquery-comparison conjunct references APPLY_OUT; remove it.
    let apply_out = a.output_column.column_id;
    let sub_pos = outer_conjuncts
        .iter()
        .position(|oc| collect_column_id_refs(oc).contains(&apply_out))?;
    let subquery_conjunct = outer_conjuncts.remove(sub_pos);
    if outer_conjuncts
        .iter()
        .any(|oc| collect_column_id_refs(oc).contains(&apply_out))
    {
        return None;
    }

    // 4c. Drop outer conjuncts that reference ONLY `correlated_outer_table`.
    outer_conjuncts.retain(|oc| {
        let refs = collect_column_id_refs(oc);
        let only_extra = !refs.is_empty()
            && refs.iter().all(|id| {
                matches!(col_map.get(id), Some((t, _)) if *t == correlated_outer_table)
            });
        !only_extra
    });

    // 4d. Remaining outer conjuncts must 1:1 phys-match the subquery's residual Filter conjuncts.
    let mut sub_residual = subquery_residual_conjuncts(&a.right);
    if outer_conjuncts.len() != sub_residual.len() {
        return None;
    }
    for oc in &outer_conjuncts {
        match sub_residual
            .iter()
            .position(|sc| expr_phys_eq(oc, sc, &full_map))
        {
            Some(pos) => {
                sub_residual.remove(pos);
            }
            None => return None,
        }
    }

    Some(WinMagicMatch {
        outer_conjuncts: split_and(where_pred.clone()),
        subquery_conjunct,
        partition_by,
        inner_agg,
    })
}

/// Peel optional leading Project and return the underlying AggregateNode, if any.
fn peel_to_aggregate(plan: &LogicalPlan) -> Option<&AggregateNode> {
    match plan {
        LogicalPlan::Aggregate(agg) => Some(agg),
        LogicalPlan::Project(p) => peel_to_aggregate(&p.input),
        _ => None,
    }
}

/// Walk `plan` and confirm it contains only whitelisted operators.
///
/// For `is_subquery = false` (outer subtree): allow Scan, Cross-only Join,
/// Filter, Project.
/// For `is_subquery = true` (inner/subquery subtree): additionally allow
/// Aggregate.
///
/// Any other node (Limit, Sort, Window, Union, Apply, …) returns `false`.
fn operator_whitelist_ok(plan: &LogicalPlan, is_subquery: bool) -> bool {
    match plan {
        LogicalPlan::Scan(_) => true,
        LogicalPlan::Filter(f) => operator_whitelist_ok(&f.input, is_subquery),
        LogicalPlan::Project(p) => operator_whitelist_ok(&p.input, is_subquery),
        LogicalPlan::Join(j) => {
            if j.join_type != crate::sql::analysis::JoinKind::Cross {
                return false;
            }
            operator_whitelist_ok(&j.left, is_subquery)
                && operator_whitelist_ok(&j.right, is_subquery)
        }
        LogicalPlan::Aggregate(agg) if is_subquery => {
            operator_whitelist_ok(&agg.input, is_subquery)
        }
        _ => false,
    }
}

/// Collect the residual (non-correlation) Filter conjuncts from the subquery's
/// aggregate input, if a Filter is present.
fn subquery_residual_conjuncts(apply_right: &LogicalPlan) -> Vec<TypedExpr> {
    // Peel optional leading Project, then the Aggregate.
    let agg = match apply_right {
        LogicalPlan::Aggregate(a) => a,
        LogicalPlan::Project(p) => match p.input.as_ref() {
            LogicalPlan::Aggregate(a) => a,
            _ => return vec![],
        },
        _ => return vec![],
    };
    // If the aggregate's input is a Filter, split its predicate into conjuncts.
    match agg.input.as_ref() {
        LogicalPlan::Filter(f) => split_and(f.predicate.clone()),
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, ApplyKind, ApplyNode, FilterNode, JoinNode, LimitNode,
        LogicalPlan, ScanNode,
    };

    // ---- Column ID constants -------------------------------------------------
    // Outer lineitem scan (table_id=1, first instance)
    const L_ORDERKEY: ColumnId = ColumnId(1);
    const L_PARTKEY: ColumnId = ColumnId(2);
    const L_QUANTITY: ColumnId = ColumnId(3);
    // part scan (table_id=2)
    const P_PARTKEY: ColumnId = ColumnId(10);
    const P_BRAND: ColumnId = ColumnId(11);
    // Inner lineitem scan (table_id=1, second instance — same physical table, different ColumnIds)
    const INNER_L_PARTKEY: ColumnId = ColumnId(20);
    const INNER_L_QUANTITY: ColumnId = ColumnId(21);
    // AVG result
    const AVG_RESULT: ColumnId = ColumnId(30);
    // Apply output
    const APPLY_OUT: ColumnId = ColumnId(50);

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

    fn lt_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Lt,
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

    fn str_lit(s: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(s.to_string())),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    /// Build `Scan(lineitem, table_id=1)` for the outer left side (first instance).
    fn make_outer_lineitem_scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "default".to_string(),
            table: TableDef {
                name: "lineitem".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 1,
                },
            },
            alias: None,
            columns: vec![
                OutputColumn {
                    column_id: L_ORDERKEY,
                    name: "l_orderkey".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: L_PARTKEY,
                    name: "l_partkey".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: L_QUANTITY,
                    name: "l_quantity".to_string(),
                    data_type: DataType::Float64,
                    nullable: false,
                    is_internal: false,
                },
            ],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    /// Build `Scan(part, table_id=2)`.
    fn make_part_scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "default".to_string(),
            table: TableDef {
                name: "part".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 2,
                },
            },
            alias: None,
            columns: vec![
                OutputColumn {
                    column_id: P_PARTKEY,
                    name: "p_partkey".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: P_BRAND,
                    name: "p_brand".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                },
            ],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    /// Build inner `Scan(lineitem, table_id=1)` — second instance with INNER_ ColumnIds.
    fn make_inner_lineitem_scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "default".to_string(),
            table: TableDef {
                name: "lineitem".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 1,
                },
            },
            alias: None,
            columns: vec![
                OutputColumn {
                    column_id: INNER_L_PARTKEY,
                    name: "l_partkey".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: INNER_L_QUANTITY,
                    name: "l_quantity".to_string(),
                    data_type: DataType::Float64,
                    nullable: false,
                    is_internal: false,
                },
            ],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    /// Build outer left plan: `CrossJoin(lineitem_scan, part_scan)`.
    fn make_outer_join() -> LogicalPlan {
        LogicalPlan::Join(JoinNode {
            left: Box::new(make_outer_lineitem_scan()),
            right: Box::new(make_part_scan()),
            join_type: JoinKind::Cross,
            condition: None,
            required_output_columns: None,
        })
    }

    /// Build inner aggregate: `Agg{group_by:[inner_l_partkey], avg(l_quantity)}(inner_scan)`.
    /// This is the post-PushDownApplyAggFilter shape.
    fn make_inner_avg_agg() -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(make_inner_lineitem_scan()),
            group_by: vec![col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64)],
            aggregates: vec![AggregateCall {
                name: "avg".to_string(),
                args: vec![col_ref(INNER_L_QUANTITY, "l_quantity", DataType::Float64)],
                distinct: false,
                result_type: DataType::Float64,
                order_by: vec![],
                output_column_id: AVG_RESULT,
            }],
            output_columns: vec![
                OutputColumn {
                    column_id: INNER_L_PARTKEY,
                    name: "l_partkey".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: AVG_RESULT,
                    name: "avg(l_quantity)".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            already_pushed: false,
            required_output_columns: None,
        })
    }

    /// Build the q17-shaped `Filter(Apply(...))` plan.
    ///
    /// WHERE predicate:
    ///   (part.p_partkey == lineitem.l_partkey)
    ///   AND (part.p_brand == 'x')
    ///   AND (lineitem.l_quantity < APPLY_OUT)
    ///
    /// Apply: left = CrossJoin(lineitem, part), right = avg_agg(inner_lineitem),
    ///        correlation_conjuncts = [part.p_partkey == inner.l_partkey],
    ///        need_check_max_rows = false.
    fn winmagic_filter_apply() -> LogicalPlan {
        // Correlation conjunct: part.p_partkey == inner.l_partkey
        let corr_conj = eq_expr(
            col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
            col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64),
        );

        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_outer_join()),
            right: Box::new(make_inner_avg_agg()),
            kind: ApplyKind::Scalar,
            subquery_expr: col_ref_nullable(APPLY_OUT, "avg_subq", DataType::Float64),
            output_column: OutputColumn {
                column_id: APPLY_OUT,
                name: "avg_subq".to_string(),
                data_type: DataType::Float64,
                nullable: true,
                is_internal: true,
            },
            inner_output_column_id: AVG_RESULT,
            correlation_column_ids: vec![P_PARTKEY],
            correlation_conjuncts: vec![corr_conj],
            residual_predicate: None,
            need_check_max_rows: false,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        // WHERE: (p_partkey == l_partkey) AND (p_brand == 'x') AND (l_quantity < APPLY_OUT)
        let pred = and_expr(
            and_expr(
                // corr twin: outer p_partkey == outer l_partkey
                eq_expr(
                    col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
                    col_ref(L_PARTKEY, "l_partkey", DataType::Int64),
                ),
                // extra: p_brand == 'x'  (references only correlated_outer_table=part)
                eq_expr(
                    col_ref(P_BRAND, "p_brand", DataType::Utf8),
                    str_lit("x"),
                ),
            ),
            // subquery comparison: l_quantity < APPLY_OUT
            lt_expr(
                col_ref(L_QUANTITY, "l_quantity", DataType::Float64),
                col_ref_nullable(APPLY_OUT, "avg_subq", DataType::Float64),
            ),
        );

        LogicalPlan::Filter(FilterNode {
            input: Box::new(apply),
            predicate: pred,
            required_output_columns: None,
        })
    }

    fn ctx() -> RewriteContext {
        RewriteContext::for_query(Vec::<String>::new())
    }

    // ---- matches() tests --------------------------------------------------------

    #[test]
    fn matches_returns_true_for_q17_shape() {
        let rule = ApplyToWindow;
        let plan = winmagic_filter_apply();
        assert!(rule.matches(&plan, &ctx()));
    }

    #[test]
    fn matches_returns_false_for_bare_apply() {
        let rule = ApplyToWindow;
        // Apply without Filter wrapper → should not match
        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_outer_join()),
            right: Box::new(make_inner_avg_agg()),
            kind: ApplyKind::Scalar,
            subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Float64),
            output_column: OutputColumn {
                column_id: APPLY_OUT,
                name: "subq".to_string(),
                data_type: DataType::Float64,
                nullable: true,
                is_internal: true,
            },
            inner_output_column_id: AVG_RESULT,
            correlation_column_ids: vec![P_PARTKEY],
            correlation_conjuncts: vec![eq_expr(
                col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
                col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64),
            )],
            residual_predicate: None,
            need_check_max_rows: false,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });
        assert!(!rule.matches(&apply, &ctx()));
    }

    // ---- precondition tests -----------------------------------------------------

    /// Helper: extract the ApplyNode and predicate from the canonical Filter(Apply) fixture.
    fn extract_filter_apply(plan: &LogicalPlan) -> (&TypedExpr, &ApplyNode) {
        let LogicalPlan::Filter(f) = plan else { panic!("expected Filter") };
        let LogicalPlan::Apply(a) = f.input.as_ref() else { panic!("expected Apply") };
        (&f.predicate, a)
    }

    #[test]
    fn precond_accepts_q17_shape() {
        let plan = winmagic_filter_apply();
        let (pred, a) = extract_filter_apply(&plan);
        assert!(
            check_preconditions(pred, a).is_some(),
            "q17-shaped Filter(Apply) must pass all preconditions"
        );
    }

    #[test]
    fn precond_rejects_non_whitelist_agg() {
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        // Replace avg with array_agg (not in whitelist)
        let mut a = a_orig.clone();
        let bad_agg = AggregateNode {
            aggregates: vec![AggregateCall {
                name: "array_agg".to_string(),
                ..a_orig.right.as_ref().as_aggregate().unwrap().aggregates[0].clone()
            }],
            ..a_orig.right.as_ref().as_aggregate().unwrap().clone()
        };
        a.right = Box::new(LogicalPlan::Aggregate(bad_agg));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "non-whitelist agg must reject"
        );
    }

    #[test]
    fn precond_rejects_distinct_agg() {
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        let bad_agg = AggregateNode {
            aggregates: vec![AggregateCall {
                distinct: true,
                ..a_orig.right.as_ref().as_aggregate().unwrap().aggregates[0].clone()
            }],
            ..a_orig.right.as_ref().as_aggregate().unwrap().clone()
        };
        a.right = Box::new(LogicalPlan::Aggregate(bad_agg));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "distinct agg must reject"
        );
    }

    #[test]
    fn precond_rejects_two_aggregates() {
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        let orig_agg = a_orig.right.as_ref().as_aggregate().unwrap();
        let two_agg = AggregateNode {
            aggregates: vec![
                orig_agg.aggregates[0].clone(),
                AggregateCall {
                    name: "min".to_string(),
                    output_column_id: ColumnId(99),
                    ..orig_agg.aggregates[0].clone()
                },
            ],
            ..orig_agg.clone()
        };
        a.right = Box::new(LogicalPlan::Aggregate(two_agg));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "two aggregates must reject"
        );
    }

    #[test]
    fn precond_rejects_self_join_outer() {
        // Outer: CrossJoin(lineitem(table_id=1), lineitem(table_id=1)) — two same-table scans
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        // Replace part scan with another lineitem scan (same table_id=1)
        let dup_lineitem = make_outer_lineitem_scan();
        a.left = Box::new(LogicalPlan::Join(JoinNode {
            left: Box::new(make_outer_lineitem_scan()),
            right: Box::new(dup_lineitem),
            join_type: JoinKind::Cross,
            condition: None,
            required_output_columns: None,
        }));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "self-join (duplicate table) on outer must reject"
        );
    }

    #[test]
    fn precond_rejects_table_set_mismatch() {
        // Subquery scans a table_id=99 absent from outer (outer has table_id=1 and 2)
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        // Replace inner scan with one from table_id=99
        let foreign_scan = LogicalPlan::Scan(ScanNode {
            database: "default".to_string(),
            table: TableDef {
                name: "other".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 99,
                },
            },
            alias: None,
            columns: vec![
                OutputColumn {
                    column_id: INNER_L_PARTKEY,
                    name: "l_partkey".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: INNER_L_QUANTITY,
                    name: "l_quantity".to_string(),
                    data_type: DataType::Float64,
                    nullable: false,
                    is_internal: false,
                },
            ],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        });
        let orig_agg = a_orig.right.as_ref().as_aggregate().unwrap();
        let foreign_agg = AggregateNode {
            input: Box::new(foreign_scan),
            ..orig_agg.clone()
        };
        a.right = Box::new(LogicalPlan::Aggregate(foreign_agg));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "subquery with foreign table must reject"
        );
    }

    #[test]
    fn precond_rejects_limit_in_subtree() {
        // Wrap outer left in a Limit node (not whitelisted)
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        a.left = Box::new(LogicalPlan::Limit(LimitNode {
            input: a_orig.left.clone(),
            limit: Some(10),
            offset: None,
            required_output_columns: None,
        }));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "Limit in outer subtree must reject"
        );
    }

    #[test]
    fn precond_rejects_predicate_mismatch() {
        // Add a residual Filter inside the subquery aggregate's input that
        // has no twin in the outer WHERE predicate.
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        // Add a Filter below the aggregate input: l_quantity > 0
        let residual_pred = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(INNER_L_QUANTITY, "l_quantity", DataType::Float64)),
                op: BinOp::Gt,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(0)),
                    data_type: DataType::Float64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let inner_with_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(make_inner_lineitem_scan()),
            predicate: residual_pred,
            required_output_columns: None,
        });
        let orig_agg = a_orig.right.as_ref().as_aggregate().unwrap();
        let agg_with_residual = AggregateNode {
            input: Box::new(inner_with_filter),
            ..orig_agg.clone()
        };
        a.right = Box::new(LogicalPlan::Aggregate(agg_with_residual));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "residual Filter conjunct without outer twin must reject"
        );
    }

    #[test]
    fn precond_rejects_no_subquery_conjunct() {
        // WHERE predicate doesn't reference APPLY_OUT at all
        let plan = winmagic_filter_apply();
        let (_, a) = extract_filter_apply(&plan);
        // Build a predicate that has no APPLY_OUT reference
        let pred_without_apply = eq_expr(
            col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
            col_ref(L_PARTKEY, "l_partkey", DataType::Int64),
        );
        assert!(
            check_preconditions(&pred_without_apply, a).is_none(),
            "WHERE predicate without APPLY_OUT reference must reject"
        );
    }

    // Helper trait to make test code more readable — extracts AggregateNode from plan.
    trait AsAggregate {
        fn as_aggregate(&self) -> Option<&AggregateNode>;
    }

    impl AsAggregate for LogicalPlan {
        fn as_aggregate(&self) -> Option<&AggregateNode> {
            match self {
                LogicalPlan::Aggregate(a) => Some(a),
                _ => None,
            }
        }
    }
}
