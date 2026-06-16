//! Aggregate pushdown collector — phase 1 of the rule.

use std::collections::HashMap;

use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::{
    LogicalAggregateNode, LogicalJoinNode, LogicalPlanNode, LogicalPlanNodeKind,
};

use super::context::{AggregatePushDownContext, ColumnRefIdentity, PushPlan, Side};

/// Examine the LogicalAggregateNode for entry-level rejections.
/// Returns Some(ctx) when the aggregate is a candidate to push;
/// returns None when an entry-level filter rejects it.
pub(crate) fn entry_safety_check(
    aggregate: &LogicalAggregateNode,
) -> Option<AggregatePushDownContext> {
    // Idempotency guard.
    if aggregate.already_pushed {
        return None;
    }
    // Empty group-by: partial collapses to a single row.
    if aggregate.group_by.is_empty() {
        return None;
    }
    // Per-call filters.
    for call in &aggregate.aggregates {
        // Distinct is SplitDistinctAgg's domain.
        if call.distinct {
            return None;
        }
        // Order-sensitive aggregate.
        if !call.order_by.is_empty() {
            return None;
        }
        // White-list check.
        let name = call.name.to_ascii_lowercase();
        if !matches!(name.as_str(), "sum" | "min" | "max" | "count") {
            return None;
        }
        // COUNT(*) has no args.
        if name == "count" && call.args.is_empty() {
            return None;
        }
        // Args must be bare ColumnRefs.
        for arg in &call.args {
            if !matches!(arg.kind, ExprKind::ColumnRef { .. }) {
                return None;
            }
            // Non-deterministic functions in args.
            if expr_uses_nondeterministic(arg) {
                return None;
            }
        }
    }

    Some(AggregatePushDownContext {
        original_groupby: aggregate.group_by.clone(),
        original_aggregates: aggregate.aggregates.clone(),
        required_column_refs: collect_required_column_refs(aggregate),
    })
}

fn collect_required_column_refs(aggregate: &LogicalAggregateNode) -> Vec<ColumnRefIdentity> {
    let mut out = Vec::new();
    for gb in &aggregate.group_by {
        collect_column_ref_identities_into(gb, &mut out);
    }
    for call in &aggregate.aggregates {
        for arg in &call.args {
            collect_column_ref_identities_into(arg, &mut out);
        }
    }
    out.sort();
    out.dedup();
    out
}

fn collect_column_ref_identities_into(expr: &TypedExpr, out: &mut Vec<ColumnRefIdentity>) {
    if let ExprKind::ColumnRef {
        qualifier, column, ..
    } = &expr.kind
    {
        out.push((qualifier.clone(), column.clone()));
    }
}

const NONDETERMINISTIC_FUNCTIONS: &[&str] = &[
    "rand",
    "random",
    "uuid",
    "now",
    "current_timestamp",
    "current_date",
];

fn expr_uses_nondeterministic(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } => {
            if NONDETERMINISTIC_FUNCTIONS
                .iter()
                .any(|n| n.eq_ignore_ascii_case(name))
            {
                return true;
            }
            args.iter().any(expr_uses_nondeterministic)
        }
        ExprKind::BinaryOp { left, right, .. } => {
            expr_uses_nondeterministic(left) || expr_uses_nondeterministic(right)
        }
        ExprKind::UnaryOp { expr: inner, .. } => expr_uses_nondeterministic(inner),
        _ => false,
    }
}

/// Top-level collector entry.
#[allow(dead_code)]
pub(crate) fn collect_push_plan(
    aggregate: &LogicalAggregateNode,
    aggregate_input: &LogicalPlanNode,
    _table_stats: &HashMap<String, TableStatistics>,
) -> Option<PushPlan> {
    let ctx = entry_safety_check(aggregate)?;
    let join = match &aggregate_input.kind {
        LogicalPlanNodeKind::Join(j) => j,
        _ => return None,
    };
    split_at_join(join, aggregate_input.left(), aggregate_input.right(), ctx)
}

fn split_at_join(
    join: &LogicalJoinNode,
    left: &LogicalPlanNode,
    right: &LogicalPlanNode,
    ctx: AggregatePushDownContext,
) -> Option<PushPlan> {
    use crate::sql::analysis::JoinKind;

    // Step 1: join-shape filter.
    match join.join_type {
        JoinKind::Inner | JoinKind::LeftOuter | JoinKind::RightOuter => {}
        _ => return None,
    }
    let cond = join.condition.as_ref()?;
    let equi_keys = extract_equi_key_pairs(cond);
    if equi_keys.is_empty() {
        return None;
    }

    // Step 2: per-side column visibility.
    let left_qcols = collect_qualified_output_names(left);
    let right_qcols = collect_qualified_output_names(right);

    let side = if ctx
        .required_column_refs
        .iter()
        .all(|c| column_ref_belongs_to_side(c, &left_qcols, &right_qcols))
    {
        Side::Left
    } else if ctx
        .required_column_refs
        .iter()
        .all(|c| column_ref_belongs_to_side(c, &right_qcols, &left_qcols))
    {
        Side::Right
    } else {
        return None;
    };

    // Step 3: outer-join amplifier rejection.
    match (join.join_type, side) {
        (JoinKind::RightOuter, Side::Left) => return None,
        (JoinKind::LeftOuter, Side::Right) => return None,
        _ => {}
    }

    // Step 4: chosen-side subtree MUST be a Scan in v1 (no nested joins,
    // no intermediate Filter/Project on the side).
    let side_subtree = match side {
        Side::Left => left,
        Side::Right => right,
    };
    if !matches!(&side_subtree.kind, LogicalPlanNodeKind::Scan(_)) {
        return None;
    }
    // Qualified columns of the chosen side (a bare Scan per Step 4), used to
    // disambiguate equi-keys that share a bare name across sides (`a.k = b.k`).
    let (side_qcols, other_qcols) = match side {
        Side::Left => (&left_qcols, &right_qcols),
        Side::Right => (&right_qcols, &left_qcols),
    };

    // Step 5: partial group-by = original group-by cols on this side
    //         + side-bound equi-keys.
    let mut partial_groupby: Vec<TypedExpr> = ctx
        .original_groupby
        .iter()
        .filter(|gb| match &gb.kind {
            ExprKind::ColumnRef {
                qualifier, column, ..
            } => column_ref_belongs_to_side(
                &(qualifier.clone(), column.clone()),
                side_qcols,
                other_qcols,
            ),
            _ => false,
        })
        .cloned()
        .collect();
    for (left_key, right_key) in &equi_keys {
        let candidate = side_bound_equi_key(left_key, right_key, side_qcols)?;
        let already = partial_groupby
            .iter()
            .any(|gb| match (&gb.kind, &candidate.kind) {
                (ExprKind::ColumnRef { column: a, .. }, ExprKind::ColumnRef { column: b, .. }) => {
                    a == b
                }
                _ => false,
            });
        if !already {
            partial_groupby.push(candidate.clone());
        }
    }

    Some(PushPlan {
        side,
        target_subtree: side_subtree.clone(),
        partial_groupby,
        partial_aggregates: ctx.original_aggregates,
    })
}

fn side_bound_equi_key<'a>(
    left_key: &'a TypedExpr,
    right_key: &'a TypedExpr,
    side_qcols: &[(Option<String>, String)],
) -> Option<&'a TypedExpr> {
    // Disambiguate by QUALIFIED identity (qualifier + name). Bare column names
    // are ambiguous when both join keys share a name (the common `a.k = b.k`
    // case): both would test as "in side" and the key would be dropped. Matching
    // on (qualifier, name) keeps the operand actually bound to the chosen side.
    let left_q = column_ref_qualified(left_key)?;
    let right_q = column_ref_qualified(right_key)?;
    let left_in_side = side_qcols.contains(&left_q);
    let right_in_side = side_qcols.contains(&right_q);
    match (left_in_side, right_in_side) {
        (true, false) => Some(left_key),
        (false, true) => Some(right_key),
        _ => None,
    }
}

fn column_ref_qualified(expr: &TypedExpr) -> Option<(Option<String>, String)> {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => Some((qualifier.clone(), column.clone())),
        _ => None,
    }
}

fn column_ref_belongs_to_side(
    column_ref: &ColumnRefIdentity,
    side_qcols: &[ColumnRefIdentity],
    other_qcols: &[ColumnRefIdentity],
) -> bool {
    match &column_ref.0 {
        Some(_) => side_qcols.contains(column_ref),
        None => side_qcols.contains(column_ref) && !other_qcols.contains(column_ref),
    }
}

/// Qualified output column identities `(qualifier, name)` for a plan subtree.
/// Scans contribute their alias (or table name) as the qualifier so equi-join
/// keys that share a bare name across sides can be told apart.
fn collect_qualified_output_names(plan: &LogicalPlanNode) -> Vec<(Option<String>, String)> {
    match &plan.kind {
        LogicalPlanNodeKind::Scan(s) => {
            // Each column is acceptable unqualified, by alias, and by table
            // name — the equi-key operand may be written any of these ways. A
            // `Some(qualifier)` operand only matches the side whose alias/table
            // equals it, so `a.k` vs `b.k` are told apart; a bare operand
            // matches by name via the unqualified entry.
            let mut out = Vec::new();
            for c in &s.columns {
                let name = c.name.clone();
                out.push((None, name.clone()));
                if let Some(alias) = &s.alias {
                    out.push((Some(alias.clone()), name.clone()));
                }
                out.push((Some(s.table.name.clone()), name));
            }
            out
        }
        LogicalPlanNodeKind::Filter(_) => collect_qualified_output_names(plan.unary_input()),
        LogicalPlanNodeKind::Project(p) => p
            .items
            .iter()
            .map(|i| (None, i.output_name.clone()))
            .collect(),
        LogicalPlanNodeKind::Join(_) => {
            let mut l = collect_qualified_output_names(plan.left());
            l.extend(collect_qualified_output_names(plan.right()));
            l
        }
        LogicalPlanNodeKind::Aggregate(a) => a
            .output_columns
            .iter()
            .map(|c| (None, c.name.clone()))
            .collect(),
        _ => Vec::new(),
    }
}

#[allow(dead_code)]
fn column_ref_name(expr: &TypedExpr) -> Option<&String> {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => Some(column),
        _ => None,
    }
}

fn extract_equi_key_pairs(cond: &TypedExpr) -> Vec<(TypedExpr, TypedExpr)> {
    let mut out = Vec::new();
    walk_and_collect_equi(cond, &mut out);
    out
}

fn walk_and_collect_equi(expr: &TypedExpr, out: &mut Vec<(TypedExpr, TypedExpr)>) {
    use crate::sql::analysis::BinOp;
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            if matches!(left.kind, ExprKind::ColumnRef { .. })
                && matches!(right.kind, ExprKind::ColumnRef { .. })
            {
                out.push(((**left).clone(), (**right).clone()));
            }
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            walk_and_collect_equi(left, out);
            walk_and_collect_equi(right, out);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::*;
    use crate::sql::planner::plan::{
        AggregateCall, LogicalAggregateNode, LogicalPlanNode, LogicalPlanNodeKind,
        LogicalValuesNode,
    };
    use arrow::datatypes::DataType;

    fn col_ref(name: &str, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: name.into(),
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn qualified_col_ref(qualifier: &str, name: &str, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: Some(qualifier.into()),
                column: name.into(),
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn make_agg(
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        already_pushed: bool,
    ) -> LogicalAggregateNode {
        LogicalAggregateNode {
            group_by,
            aggregates,
            output_columns: vec![],
            already_pushed,
        }
    }

    fn aggregate_plan(
        input: LogicalPlanNode,
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by,
                aggregates,
                output_columns: vec![],
                already_pushed: false,
            }),
            vec![input],
            None,
        )
    }

    fn collect_test_push_plan(
        aggregate_plan: &LogicalPlanNode,
    ) -> Option<super::super::context::PushPlan> {
        let LogicalPlanNodeKind::Aggregate(aggregate) = &aggregate_plan.kind else {
            panic!("expected Aggregate test plan");
        };
        collect_push_plan(aggregate, aggregate_plan.unary_input(), &HashMap::new())
    }

    fn sum_call(col: &str) -> AggregateCall {
        AggregateCall {
            name: "sum".into(),
            args: vec![col_ref(col, DataType::Int64)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: ColumnId::UNSET,
        }
    }

    #[test]
    fn rejects_empty_groupby() {
        let agg = make_agg(vec![], vec![sum_call("v")], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_distinct_aggregate() {
        let mut call = sum_call("v");
        call.distinct = true;
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_order_sensitive_aggregate() {
        let mut call = sum_call("v");
        call.order_by.push(crate::sql::analysis::SortItem {
            expr: col_ref("v", DataType::Int64),
            asc: true,
            nulls_first: false,
        });
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_count_star() {
        let count_star = AggregateCall {
            name: "count".into(),
            args: vec![],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: ColumnId::UNSET,
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![count_star], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_avg_function() {
        let avg = AggregateCall {
            name: "avg".into(),
            args: vec![col_ref("v", DataType::Int64)],
            distinct: false,
            result_type: DataType::Float64,
            order_by: vec![],
            output_column_id: ColumnId::UNSET,
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![avg], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_aggregate_expr_not_columnref() {
        let mut call = sum_call("v");
        call.args[0] = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref("a", DataType::Int64)),
                op: crate::sql::analysis::BinOp::Add,
                right: Box::new(col_ref("b", DataType::Int64)),
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_nondeterministic_arg() {
        let mut call = sum_call("v");
        // Replace the arg with a non-ColumnRef expression that contains
        // a non-deterministic call. This should be rejected at the
        // "args must be bare ColumnRef" check before the
        // non-deterministic check fires — both serve as belt-and-suspenders.
        call.args[0] = TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "rand".into(),
                args: vec![],
                distinct: false,
            },
            data_type: DataType::Float64,
            nullable: false,
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_already_pushed_aggregate() {
        let agg = make_agg(
            vec![col_ref("k", DataType::Int64)],
            vec![sum_call("v")],
            true,
        );
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn accepts_inner_join_candidate() {
        let agg = make_agg(
            vec![col_ref("k", DataType::Int64)],
            vec![sum_call("v")],
            false,
        );
        let ctx = entry_safety_check(&agg).expect("should pass entry checks");
        assert_eq!(ctx.original_groupby.len(), 1);
        assert_eq!(ctx.original_aggregates.len(), 1);
        assert!(ctx.required_column_refs.contains(&(None, "k".to_string())));
        assert!(ctx.required_column_refs.contains(&(None, "v".to_string())));
    }

    use crate::sql::analysis::{JoinKind, ProjectItem};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::planner::plan::{
        LogicalFilterNode, LogicalJoinNode, LogicalProjectNode, LogicalScanNode,
    };

    fn dummy_scan_with_cols(cols: &[(&str, DataType)]) -> LogicalPlanNode {
        dummy_scan_with_alias(None, cols)
    }

    fn dummy_scan_with_alias(alias: Option<&str>, cols: &[(&str, DataType)]) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "db".into(),
                table: TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: alias.map(str::to_string),
                columns: cols
                    .iter()
                    .map(|(n, ty)| OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: (*n).into(),
                        data_type: ty.clone(),
                        nullable: false,
                        is_internal: false,
                    })
                    .collect(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
            }),
            vec![],
            None,
        )
    }

    #[test]
    fn rejects_when_input_is_scan_directly() {
        // No Join means no work to do — would just wrap the scan with an
        // identity partial that buys nothing. v1 rejects.
        let scan = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let agg = aggregate_plan(
            scan,
            vec![col_ref("k", DataType::Int64)],
            vec![sum_call("v")],
        );
        assert!(collect_test_push_plan(&agg).is_none());
    }

    #[test]
    fn rejects_when_input_is_filter_above_join() {
        // Filter intermediation between Aggregate and Join is an OPT-1
        // follow-up. v1 rejects.
        let scan_a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let scan_b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(col_ref("k", DataType::Boolean)),
            }),
            vec![scan_a, scan_b],
            None,
        );
        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: col_ref("k", DataType::Boolean),
            }),
            vec![join],
            None,
        );
        let agg = aggregate_plan(
            filter,
            vec![col_ref("k", DataType::Int64)],
            vec![sum_call("v")],
        );
        assert!(collect_test_push_plan(&agg).is_none());
    }

    #[test]
    fn rejects_when_input_is_project_above_join() {
        let scan_a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let scan_b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(col_ref("k", DataType::Boolean)),
            }),
            vec![scan_a, scan_b],
            None,
        );
        let project = LogicalPlanNode::new(
            LogicalPlanNodeKind::Project(LogicalProjectNode {
                items: vec![ProjectItem {
                    expr: col_ref("k", DataType::Int64),
                    output_name: "k".into(),
                    output_column_id: crate::sql::column_id::ColumnId::UNSET,
                }],
                output_qualifier: None,
            }),
            vec![join],
            None,
        );
        let agg = aggregate_plan(
            project,
            vec![col_ref("k", DataType::Int64)],
            vec![sum_call("v")],
        );
        assert!(collect_test_push_plan(&agg).is_none());
    }

    use crate::sql::analysis::BinOp;

    fn eq(a: &str, b: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(a, DataType::Int64)),
                op: BinOp::Eq,
                right: Box::new(col_ref(b, DataType::Int64)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn eq_qualified(
        left_qual: &str,
        left_col: &str,
        right_qual: &str,
        right_col: &str,
    ) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(qualified_col_ref(left_qual, left_col, DataType::Int64)),
                op: BinOp::Eq,
                right: Box::new(qualified_col_ref(right_qual, right_col, DataType::Int64)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    #[test]
    fn pushes_sum_under_inner_join_to_left() {
        let a = dummy_scan_with_cols(&[("lk", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("rk", DataType::Int64)]);
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(eq("lk", "rk")),
            }),
            vec![a, b],
            None,
        );
        let agg = aggregate_plan(
            join,
            vec![col_ref("lk", DataType::Int64)],
            vec![sum_call("v")],
        );
        let plan = collect_test_push_plan(&agg).expect("should push to left");
        assert_eq!(plan.side, super::super::context::Side::Left);
        assert!(matches!(
            &plan.target_subtree.kind,
            LogicalPlanNodeKind::Scan(_)
        ));
    }

    #[test]
    fn orients_reversed_join_key_to_target_side() {
        let a = dummy_scan_with_cols(&[("lk", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("rk", DataType::Int64)]);
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(eq("rk", "lk")),
            }),
            vec![a, b],
            None,
        );
        let agg = aggregate_plan(
            join,
            vec![col_ref("lk", DataType::Int64)],
            vec![sum_call("v")],
        );
        let plan = collect_test_push_plan(&agg).expect("should push to left");
        let group_columns: Vec<_> = plan
            .partial_groupby
            .iter()
            .filter_map(|expr| column_ref_name(expr).map(String::as_str))
            .collect();
        assert!(group_columns.contains(&"lk"));
        assert!(!group_columns.contains(&"rk"));
    }

    #[test]
    fn rejects_outer_join_amplifier_side() {
        let a = dummy_scan_with_cols(&[("lk", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("rk", DataType::Int64), ("v", DataType::Int64)]);
        // LEFT OUTER JOIN; aggregate on right (amplifier) — must reject.
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::LeftOuter,
                condition: Some(eq("lk", "rk")),
            }),
            vec![a, b],
            None,
        );
        let agg = aggregate_plan(
            join,
            vec![col_ref("rk", DataType::Int64)],
            vec![sum_call("v")],
        );
        assert!(collect_test_push_plan(&agg).is_none());
    }

    #[test]
    fn accepts_left_outer_when_agg_on_preserved_left() {
        let a = dummy_scan_with_cols(&[("lk", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("rk", DataType::Int64)]);
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::LeftOuter,
                condition: Some(eq("rk", "lk")),
            }),
            vec![a, b],
            None,
        );
        let agg = aggregate_plan(
            join,
            vec![col_ref("lk", DataType::Int64)],
            vec![sum_call("v")],
        );
        let plan = collect_test_push_plan(&agg).expect("push to preserved left");
        assert!(matches!(
            &plan.target_subtree.kind,
            LogicalPlanNodeKind::Scan(_)
        ));
    }

    #[test]
    fn rejects_cross_join() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("x", DataType::Int64)]);
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Cross,
                condition: None,
            }),
            vec![a, b],
            None,
        );
        let agg = aggregate_plan(
            join,
            vec![col_ref("k", DataType::Int64)],
            vec![sum_call("v")],
        );
        assert!(collect_test_push_plan(&agg).is_none());
    }

    #[test]
    fn rejects_aggregate_columns_across_sides() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64), ("w", DataType::Int64)]);
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(eq("k", "k")),
            }),
            vec![a, b],
            None,
        );
        // sum(v) is on left; sum(w) is on right. Required = {k, v, w}.
        // Neither side covers all required cols → reject.
        let agg = aggregate_plan(
            join,
            vec![col_ref("k", DataType::Int64)],
            vec![sum_call("v"), sum_call("w")],
        );
        assert!(collect_test_push_plan(&agg).is_none());
    }

    #[test]
    fn rejects_qualified_required_columns_split_across_same_named_sides() {
        let a = dummy_scan_with_alias(
            Some("l"),
            &[
                ("c0", DataType::Int64),
                ("c1", DataType::Utf8),
                ("c2", DataType::Utf8),
                ("c3", DataType::Int64),
            ],
        );
        let b = dummy_scan_with_alias(
            Some("r"),
            &[
                ("c0", DataType::Int64),
                ("c1", DataType::Utf8),
                ("c2", DataType::Utf8),
                ("c3", DataType::Int64),
            ],
        );
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(eq_qualified("l", "c0", "r", "c0")),
                        op: BinOp::And,
                        right: Box::new(eq_qualified("l", "c1", "r", "c1")),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
            }),
            vec![a, b],
            None,
        );
        let agg = aggregate_plan(
            join,
            vec![
                qualified_col_ref("l", "c0", DataType::Int64),
                qualified_col_ref("r", "c1", DataType::Utf8),
                qualified_col_ref("r", "c2", DataType::Utf8),
                qualified_col_ref("r", "c3", DataType::Int64),
            ],
            vec![AggregateCall {
                name: "count".into(),
                args: vec![qualified_col_ref("l", "c0", DataType::Int64)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
                output_column_id: ColumnId::UNSET,
            }],
        );
        assert!(collect_test_push_plan(&agg).is_none());
    }

    #[test]
    fn rejects_semi_anti_join() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::LeftSemi,
                condition: Some(eq("k", "k")),
            }),
            vec![a, b],
            None,
        );
        let agg = aggregate_plan(
            join,
            vec![col_ref("k", DataType::Int64)],
            vec![sum_call("v")],
        );
        assert!(collect_test_push_plan(&agg).is_none());
    }

    #[test]
    fn rejects_nested_join_on_target_side() {
        // v1 only handles direct-Scan sides. A nested join on the
        // chosen side must be rejected; multi-table is OPT-1 follow-up.
        let inner_join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(eq("k", "k")),
            }),
            vec![
                dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]),
                dummy_scan_with_cols(&[("k", DataType::Int64)]),
            ],
            None,
        );
        let outer_join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(eq("k", "k")),
            }),
            vec![inner_join, dummy_scan_with_cols(&[("k", DataType::Int64)])],
            None,
        );
        let agg = aggregate_plan(
            outer_join,
            vec![col_ref("k", DataType::Int64)],
            vec![sum_call("v")],
        );
        assert!(collect_test_push_plan(&agg).is_none());
    }
}
