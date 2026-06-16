//! PushDownPredicateJoin — `Filter(Join)` rewrite.
//!
//! Classifies conjuncts of the filter predicate by which side of the join
//! they reference, pushes single-side predicates below the join (respecting
//! OUTER/SEMI/ANTI null-preservation), and merges genuine cross-side
//! conjuncts into the join condition. Also performs single-step OR-factoring
//! to extract common equi-joins from OR branches. Upgrades a CROSS join to
//! INNER when a predicate promotes it.
//!
//! Mirrors legacy `push_predicates_through_join` + `factor_common_eq_from_or`
//! from `src/sql/optimizer/predicate_pushdown.rs`. One step per apply — the
//! rewrite pipeline's fixed-point handles repeated firing when a newly-formed shape
//! exposes further opportunities.

use std::collections::HashSet;

use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::deriver::derive_inner_join_predicates;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
    PredicateGroup, PredicateOrigin, predicate_key as canonical_predicate_key,
};
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_column_id_refs_strict, collect_output_ids, combine_and, split_and,
    wrap_remaining_filter,
};
use crate::sql::planner::plan::*;
use arrow::datatypes::DataType;

#[cfg(test)]
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule as RewriteRule;

#[cfg(test)]
pub(crate) struct PushDownPredicateJoin;

#[cfg(test)]
impl RewriteRule for PushDownPredicateJoin {
    fn name(&self) -> &'static str {
        "PushDownPredicateJoin"
    }

    fn matches(&self, plan: &LogicalPlanNode) -> bool {
        matches!(&plan.kind, LogicalPlanNodeKind::Filter(_) if matches!(&plan.unary_input().kind, LogicalPlanNodeKind::Join(_))
        )
    }

    fn apply(&self, plan: LogicalPlanNode) -> Option<LogicalPlanNode> {
        let LogicalPlanNode {
            kind,
            mut children,
            required_output_columns: _,
        } = plan;
        let LogicalPlanNodeKind::Filter(filter) = kind else {
            return None;
        };
        let join_plan = children.remove(0);
        let LogicalPlanNode {
            kind: join_kind,
            mut children,
            required_output_columns,
        } = join_plan;
        let LogicalPlanNodeKind::Join(join) = join_kind else {
            return None;
        };
        if children.len() != 2 {
            return None;
        }
        let right = children.remove(1);
        let left = children.remove(0);
        let (rewritten, pushed_any) = push_filter_predicates_through_join(
            filter.predicate,
            join,
            left,
            right,
            required_output_columns,
        );
        if pushed_any { Some(rewritten) } else { None }
    }
}

// ============================================================
// Port of legacy helpers from src/sql/optimizer/predicate_pushdown.rs
// ============================================================

pub(crate) fn push_filter_predicates_through_join(
    predicate: TypedExpr,
    join: LogicalJoinNode,
    left: LogicalPlanNode,
    right: LogicalPlanNode,
    required_output_columns: Option<HashSet<ColumnId>>,
) -> (LogicalPlanNode, bool) {
    let mut left_ids = collect_output_ids(&left);
    let mut right_ids = collect_output_ids(&right);
    left_ids.remove(&ColumnId::UNSET);
    right_ids.remove(&ColumnId::UNSET);
    let filter_groups = PredicateGroup::from_predicate(predicate.clone(), PredicateOrigin::Filter);
    let join_groups = join
        .condition
        .clone()
        .map(|condition| PredicateGroup::from_predicate(condition, PredicateOrigin::JoinCondition))
        .unwrap_or_default();
    let mut conjuncts = split_and(predicate);
    if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
        let derived =
            derive_inner_join_predicates(&left_ids, &right_ids, &join_groups, &filter_groups);
        append_new_derived_conjuncts(
            &mut conjuncts,
            derived,
            &left,
            &right,
            &left_ids,
            &right_ids,
        );
    }

    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut join_preds = Vec::new();
    let mut remaining = Vec::new();

    for conj in conjuncts {
        let Some((in_left, in_right)) = classify_sides_by_column_ids(&conj, &left_ids, &right_ids)
        else {
            remaining.push(conj);
            continue;
        };

        match (in_left, in_right) {
            (true, false) => left_preds.push(conj),
            (false, true) => {
                // For LEFT OUTER / LEFT SEMI / LEFT ANTI / FULL OUTER joins,
                // right-side predicates affect NULL preservation semantics and
                // must NOT be pushed below the join.
                // For RIGHT OUTER, left-side predicates have the same issue
                // (handled below), but right-side predicates are safe to push.
                match join.join_type {
                    JoinKind::Inner
                    | JoinKind::Cross
                    | JoinKind::RightOuter
                    | JoinKind::RightSemi
                    | JoinKind::RightAnti => {
                        right_preds.push(conj);
                    }
                    _ => remaining.push(conj),
                }
            }
            (true, true) => {
                if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
                    let (implied_left, implied_right) =
                        extract_implied_or_side_filters(&conj, &left_ids, &right_ids);
                    for pred in implied_left {
                        if !subtree_has_predicate(&left, &pred) {
                            left_preds.push(pred);
                        }
                    }
                    for pred in implied_right {
                        if !subtree_has_predicate(&right, &pred) {
                            right_preds.push(pred);
                        }
                    }
                }

                if matches!(
                    join.join_type,
                    JoinKind::LeftOuter
                        | JoinKind::LeftSemi
                        | JoinKind::LeftAnti
                        | JoinKind::RightOuter
                        | JoinKind::FullOuter
                ) {
                    remaining.push(conj);
                } else {
                    let (factored, or_remaining) =
                        factor_common_eq_from_or(&conj, &left_ids, &right_ids);
                    if !factored.is_empty() {
                        join_preds.extend(factored);
                        if let Some(rem) = or_remaining {
                            remaining.push(rem);
                        }
                    } else {
                        join_preds.push(conj);
                    }
                }
            }
            (false, false) => {
                // Constant predicates — push to left side
                left_preds.push(conj);
            }
        }
    }

    // For RIGHT OUTER joins, left-side predicates cannot be pushed below
    // (left side is the nullable side). Move them to remaining.
    if matches!(
        join.join_type,
        JoinKind::RightOuter | JoinKind::RightSemi | JoinKind::RightAnti
    ) {
        remaining.append(&mut left_preds);
    }

    // For FULL OUTER joins, neither side can receive pushed predicates.
    if matches!(join.join_type, JoinKind::FullOuter) {
        remaining.append(&mut left_preds);
        remaining.append(&mut right_preds);
    }

    // Determine whether anything was actually pushed (after outer-join drain-back).
    let pushed_any = !left_preds.is_empty() || !right_preds.is_empty() || !join_preds.is_empty();

    // Build the new left child, applying pushed predicates then wrapping in
    // a Filter. The rewrite pipeline's fixed-point handles continued pushdown on
    // subsequent iterations.
    let new_left = if left_preds.is_empty() {
        left
    } else {
        let pushed = combine_and(left_preds);
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode { predicate: pushed }),
            vec![left],
            None,
        )
    };

    // Build the new right child.
    let new_right = if right_preds.is_empty() {
        right
    } else {
        let pushed = combine_and(right_preds);
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode { predicate: pushed }),
            vec![right],
            None,
        )
    };

    // Merge new join predicates with the existing join condition.
    let new_condition = merge_join_conditions(join.condition, join_preds);

    // When a CROSS JOIN gets join predicates extracted from the filter above,
    // upgrade it to INNER JOIN so the physical emitter can use hash join.
    let new_join_type = if join.join_type == JoinKind::Cross && new_condition.is_some() {
        JoinKind::Inner
    } else {
        join.join_type
    };

    let new_join = LogicalPlanNode::new(
        LogicalPlanNodeKind::Join(LogicalJoinNode {
            join_type: new_join_type,
            condition: new_condition,
        }),
        vec![new_left, new_right],
        required_output_columns,
    );

    (wrap_remaining_filter(new_join, remaining), pushed_any)
}

pub(crate) fn push_join_condition_predicates(
    join: LogicalJoinNode,
    left: LogicalPlanNode,
    right: LogicalPlanNode,
    required_output_columns: Option<HashSet<ColumnId>>,
) -> Option<LogicalPlanNode> {
    if !matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
        return None;
    }

    let condition = join.condition.clone()?;
    let mut left_ids = collect_output_ids(&left);
    let mut right_ids = collect_output_ids(&right);
    left_ids.remove(&ColumnId::UNSET);
    right_ids.remove(&ColumnId::UNSET);
    let condition_groups =
        PredicateGroup::from_predicate(condition.clone(), PredicateOrigin::JoinCondition);
    let mut conjuncts = split_and(condition);
    let derived =
        derive_inner_join_predicates(&left_ids, &right_ids, &condition_groups, &condition_groups);
    append_new_derived_conjuncts(
        &mut conjuncts,
        derived,
        &left,
        &right,
        &left_ids,
        &right_ids,
    );

    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut residual_preds = Vec::new();

    for conj in conjuncts {
        let Some((in_left, in_right)) = classify_sides_by_column_ids(&conj, &left_ids, &right_ids)
        else {
            residual_preds.push(conj);
            continue;
        };

        match (in_left, in_right) {
            (true, false) => left_preds.push(conj),
            (false, true) => right_preds.push(conj),
            (false, false) => left_preds.push(conj),
            (true, true) => residual_preds.push(conj),
        }
    }

    let pushed_any = !left_preds.is_empty() || !right_preds.is_empty();
    let new_condition = if residual_preds.is_empty() {
        None
    } else {
        Some(combine_and(residual_preds))
    };
    let upgrades_cross = join.join_type == JoinKind::Cross && new_condition.is_some();

    if !pushed_any && !upgrades_cross {
        return None;
    }

    let new_left = if left_preds.is_empty() {
        left
    } else {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: combine_and(left_preds),
            }),
            vec![left],
            None,
        )
    };

    let new_right = if right_preds.is_empty() {
        right
    } else {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: combine_and(right_preds),
            }),
            vec![right],
            None,
        )
    };

    let new_join_type = if upgrades_cross {
        JoinKind::Inner
    } else {
        join.join_type
    };

    Some(LogicalPlanNode::new(
        LogicalPlanNodeKind::Join(LogicalJoinNode {
            join_type: new_join_type,
            condition: new_condition,
        }),
        vec![new_left, new_right],
        required_output_columns,
    ))
}

fn append_new_derived_conjuncts(
    conjuncts: &mut Vec<TypedExpr>,
    derived: Vec<PredicateGroup>,
    left: &LogicalPlanNode,
    right: &LogicalPlanNode,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) {
    let mut seen: HashSet<String> = conjuncts.iter().map(predicate_key).collect();
    for group in derived {
        if derived_exists_below_child(&group.expr, left, right, left_ids, right_ids) {
            continue;
        }
        if seen.insert(predicate_key(&group.expr)) {
            conjuncts.push(group.expr);
        }
    }
}

fn derived_exists_below_child(
    expr: &TypedExpr,
    left: &LogicalPlanNode,
    right: &LogicalPlanNode,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> bool {
    let Some(ids) = collect_column_id_refs_strict(expr) else {
        return false;
    };
    if ids.is_empty() {
        return false;
    }
    if ids.iter().all(|id| left_ids.contains(id)) {
        return subtree_has_predicate(left, expr);
    }
    if ids.iter().all(|id| right_ids.contains(id)) {
        return subtree_has_predicate(right, expr);
    }
    false
}

fn classify_sides_by_column_ids(
    expr: &TypedExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<(bool, bool)> {
    let ids = collect_column_id_refs_strict(expr)?;
    if ids.is_empty() {
        return Some((false, false));
    }

    let mut in_left = false;
    let mut in_right = false;
    for id in ids {
        match (left_ids.contains(&id), right_ids.contains(&id)) {
            (true, false) => in_left = true,
            (false, true) => in_right = true,
            _ => return None,
        }
    }
    Some((in_left, in_right))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PredicateSide {
    Left,
    Right,
}

fn extract_implied_or_side_filters(
    expr: &TypedExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> (Vec<TypedExpr>, Vec<TypedExpr>) {
    let branches = split_or_branches(expr);
    if branches.len() < 2 {
        return (Vec::new(), Vec::new());
    }
    let branch_count = branches.len();

    let mut left_terms = Vec::with_capacity(branches.len());
    let mut right_terms = Vec::with_capacity(branches.len());
    for branch in branches {
        let mut left_conjuncts = Vec::new();
        let mut right_conjuncts = Vec::new();
        for conjunct in split_and_refs(branch) {
            match classify_implied_filter_side(conjunct, left_ids, right_ids) {
                Some(PredicateSide::Left) => left_conjuncts.push((*conjunct).clone()),
                Some(PredicateSide::Right) => right_conjuncts.push((*conjunct).clone()),
                None => {}
            }
        }

        if !left_conjuncts.is_empty() {
            left_terms.push(combine_and(left_conjuncts));
        }
        if !right_conjuncts.is_empty() {
            right_terms.push(combine_and(right_conjuncts));
        }
    }

    let left_filters = if left_terms.len() == branch_count {
        vec![combine_or(left_terms)]
    } else {
        Vec::new()
    };
    let right_filters = if right_terms.len() == branch_count {
        vec![combine_or(right_terms)]
    } else {
        Vec::new()
    };
    (left_filters, right_filters)
}

fn classify_implied_filter_side(
    expr: &TypedExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<PredicateSide> {
    let (in_left, in_right) = classify_sides_by_column_ids(expr, left_ids, right_ids)?;
    match (in_left, in_right) {
        (true, false) => Some(PredicateSide::Left),
        (false, true) => Some(PredicateSide::Right),
        _ => None,
    }
}

fn combine_or(mut exprs: Vec<TypedExpr>) -> TypedExpr {
    assert!(!exprs.is_empty());
    let mut result = exprs.pop().unwrap();
    while let Some(left) = exprs.pop() {
        result = TypedExpr {
            data_type: DataType::Boolean,
            nullable: left.nullable || result.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(result),
            },
        };
    }
    result
}

fn subtree_has_predicate(plan: &LogicalPlanNode, pred: &TypedExpr) -> bool {
    let key = predicate_key(pred);
    subtree_has_predicate_key(plan, &key)
}

fn subtree_has_predicate_key(plan: &LogicalPlanNode, key: &str) -> bool {
    match &plan.kind {
        LogicalPlanNodeKind::Scan(scan) => scan
            .predicates
            .iter()
            .any(|existing| predicate_has_conjunct_key(existing, key)),
        LogicalPlanNodeKind::Filter(filter) => {
            predicate_has_conjunct_key(&filter.predicate, key)
                || plan
                    .children
                    .iter()
                    .any(|child| subtree_has_predicate_key(child, key))
        }
        LogicalPlanNodeKind::Join(join) => {
            join.condition
                .as_ref()
                .is_some_and(|condition| predicate_has_conjunct_key(condition, key))
                || plan
                    .children
                    .iter()
                    .any(|child| subtree_has_predicate_key(child, key))
        }
        _ => plan
            .children
            .iter()
            .any(|child| subtree_has_predicate_key(child, key)),
    }
}

fn predicate_has_conjunct_key(expr: &TypedExpr, key: &str) -> bool {
    split_and_refs(expr)
        .into_iter()
        .any(|conjunct| predicate_key(conjunct) == key)
}

fn predicate_key(expr: &TypedExpr) -> String {
    canonical_predicate_key(expr).as_str().to_string()
}

/// Extract common equi-join conditions from all branches of an OR predicate.
/// Returns (extracted_join_preds, remaining_or_predicate).
///
/// For: `(A=B AND X) OR (A=B AND Y)` where A is from left and B from right,
/// extracts `A=B` as a join predicate and returns `X OR Y` as remaining.
fn factor_common_eq_from_or(
    expr: &TypedExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> (Vec<TypedExpr>, Option<TypedExpr>) {
    // Split OR into branches
    let branches = split_or_branches(expr);
    if branches.len() < 2 {
        return (vec![], None);
    }

    // Collect AND conjuncts per branch
    let branch_conjuncts: Vec<Vec<&TypedExpr>> =
        branches.iter().map(|b| split_and_refs(b)).collect();

    // Find equi-join predicates (col=col where one side left, one right)
    // that appear in ALL branches
    let mut common_eqs: Vec<TypedExpr> = Vec::new();
    if let Some(first) = branch_conjuncts.first() {
        for candidate in first {
            if !is_cross_side_eq(candidate, left_ids, right_ids) {
                continue;
            }
            let in_all = branch_conjuncts[1..]
                .iter()
                .all(|conjs| conjs.iter().any(|c| expr_eq(c, candidate)));
            if in_all {
                common_eqs.push((*candidate).clone());
            }
        }
    }

    if common_eqs.is_empty() {
        return (vec![], None);
    }

    // Build remaining OR: remove common eqs from each branch
    let mut new_branches: Vec<TypedExpr> = Vec::new();
    for branch in &branch_conjuncts {
        let remaining: Vec<TypedExpr> = branch
            .iter()
            .filter(|c| !common_eqs.iter().any(|eq| expr_eq(c, eq)))
            .map(|c| (*c).clone())
            .collect();
        if remaining.is_empty() {
            // Branch was only the common eq → TRUE
            new_branches.push(TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::Literal(LiteralValue::Bool(true)),
            });
        } else {
            new_branches.push(combine_and(remaining));
        }
    }

    let or_remaining = if new_branches
        .iter()
        .all(|b| matches!(b.kind, ExprKind::Literal(LiteralValue::Bool(true))))
    {
        None // All branches were just the common eq
    } else {
        let mut result = new_branches.remove(0);
        for branch in new_branches {
            result = TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::BinaryOp {
                    left: Box::new(result),
                    op: BinOp::Or,
                    right: Box::new(branch),
                },
            };
        }
        Some(result)
    };

    (common_eqs, or_remaining)
}

fn split_or_branches(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let mut v = split_or_branches(left);
            v.extend(split_or_branches(right));
            v
        }
        ExprKind::Nested(inner) => split_or_branches(inner),
        _ => vec![expr],
    }
}

fn split_and_refs(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut v = split_and_refs(left);
            v.extend(split_and_refs(right));
            v
        }
        ExprKind::Nested(inner) => split_and_refs(inner),
        _ => vec![expr],
    }
}

fn is_cross_side_eq(
    expr: &TypedExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> bool {
    if let ExprKind::BinaryOp {
        left,
        op: BinOp::Eq,
        right,
    } = &expr.kind
    {
        let l_id = match &left.kind {
            ExprKind::ColumnRef { column_id, .. } if *column_id != ColumnId::UNSET => {
                Some(*column_id)
            }
            _ => None,
        };
        let r_id = match &right.kind {
            ExprKind::ColumnRef { column_id, .. } if *column_id != ColumnId::UNSET => {
                Some(*column_id)
            }
            _ => None,
        };
        match (l_id, r_id) {
            (Some(l), Some(r)) => {
                (left_ids.contains(&l) && right_ids.contains(&r))
                    || (left_ids.contains(&r) && right_ids.contains(&l))
            }
            _ => false,
        }
    } else {
        false
    }
}

fn expr_eq(a: &TypedExpr, b: &TypedExpr) -> bool {
    format!("{:?}", a.kind) == format!("{:?}", b.kind)
}

/// Merge new predicates into an existing (optional) join condition.
fn merge_join_conditions(
    existing: Option<TypedExpr>,
    new_preds: Vec<TypedExpr>,
) -> Option<TypedExpr> {
    let mut all = Vec::new();
    let mut seen = HashSet::new();
    if let Some(cond) = existing {
        for pred in split_and(cond) {
            if seen.insert(predicate_key(&pred)) {
                all.push(pred);
            }
        }
    }
    for pred in new_preds {
        if seen.insert(predicate_key(&pred)) {
            all.push(pred);
        }
    }
    if all.is_empty() {
        None
    } else {
        Some(combine_and(all))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::*;
    use arrow::datatypes::DataType;

    fn test_col_id(name: &str) -> ColumnId {
        match name {
            "x" => ColumnId::new_for_test(1),
            "y" => ColumnId::new_for_test(2),
            "a" => ColumnId::new_for_test(3),
            "b" => ColumnId::new_for_test(4),
            "k" => ColumnId::new_for_test(5),
            _ => ColumnId::new_for_test(100),
        }
    }

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(name),
                qualifier: None,
                column: name.into(),
            },
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }

    fn eq(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Eq,
                right: Box::new(b),
            },
        }
    }

    fn and(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: a.nullable || b.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::And,
                right: Box::new(b),
            },
        }
    }

    fn or(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: a.nullable || b.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Or,
                right: Box::new(b),
            },
        }
    }

    /// Build a scan with stable test ColumnIds.
    fn scan(table_name: &str, cols: &[&str]) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "db".into(),
                table: TableDef {
                    name: table_name.into(),
                    columns: cols
                        .iter()
                        .map(|n| ColumnDef {
                            name: (*n).into(),
                            data_type: DataType::Int64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        })
                        .collect(),
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: Some(table_name.into()),
                columns: cols
                    .iter()
                    .map(|n| OutputColumn {
                        column_id: test_col_id(n),
                        name: (*n).into(),
                        data_type: DataType::Int64,
                        nullable: true,
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

    fn inner_join(
        left: LogicalPlanNode,
        right: LogicalPlanNode,
        condition: Option<TypedExpr>,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: condition,
            }),
            vec![left, right],
            None,
        )
    }

    fn cross_join(left: LogicalPlanNode, right: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Cross,
                condition: None,
            }),
            vec![left, right],
            None,
        )
    }

    #[test]
    fn extracts_implied_or_side_filters() {
        let left = scan("l", &["a"]);
        let right = scan("r", &["x"]);
        let pred = or(
            and(eq(col("a"), int_lit(1)), eq(col("x"), int_lit(10))),
            and(eq(col("a"), int_lit(2)), eq(col("x"), int_lit(20))),
        );
        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode { predicate: pred }),
            vec![cross_join(left, right)],
            None,
        );

        let out = PushDownPredicateJoin.apply(filter).expect("should rewrite");
        let LogicalPlanNodeKind::Join(join) = &out.kind else {
            panic!("expected Join after OR side-filter extraction");
        };
        assert!(
            join.condition.is_some(),
            "original OR predicate must remain as the join condition"
        );
        match &out.left().kind {
            LogicalPlanNodeKind::Filter(filter) => {
                let rendered = format!("{:?}", filter.predicate.kind);
                assert!(rendered.contains("\"a\""));
                assert!(rendered.contains("Or"));
            }
            other => panic!("expected left Filter, got {:?}", other),
        }
        match &out.right().kind {
            LogicalPlanNodeKind::Filter(filter) => {
                let rendered = format!("{:?}", filter.predicate.kind);
                assert!(rendered.contains("\"x\""));
                assert!(rendered.contains("Or"));
            }
            other => panic!("expected right Filter, got {:?}", other),
        }
    }

    #[test]
    fn merge_join_conditions_deduplicates_existing_condition() {
        let condition = eq(col("a"), col("x"));
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: condition.clone(),
            }),
            vec![inner_join(
                scan("l", &["a"]),
                scan("r", &["x"]),
                Some(condition.clone()),
            )],
            None,
        );

        let out = PushDownPredicateJoin.apply(plan).expect("should rewrite");
        let LogicalPlanNodeKind::Join(join) = &out.kind else {
            panic!("expected Join after predicate merge");
        };
        let condition = join.condition.as_ref().expect("join condition");
        assert_eq!(split_and_refs(&condition).len(), 1);
    }

    #[test]
    fn merge_join_conditions_deduplicates_reassociated_or_condition() {
        let a = eq(col_with_id("l", "a", 1), int_lit(1));
        let b = eq(col_with_id("r", "b", 2), int_lit(2));
        let c = eq(col_with_id("l", "v", 3), int_lit(3));
        let existing = or(or(a.clone(), b.clone()), c.clone());
        let reassociated = or(a, or(b, c));

        let merged =
            merge_join_conditions(Some(existing), vec![reassociated]).expect("merged condition");

        assert_eq!(
            split_and_refs(&merged).len(),
            1,
            "semantically identical OR residuals must not be duplicated"
        );
    }

    // Test 1: t1 INNER t2 WHERE t1.x = 1
    // x belongs only to t1 → pushed below left child.
    // Expected: Join(Filter(t1), t2) with no remaining filter above.
    #[test]
    fn pushes_left_only_predicate_below_inner_join() {
        let t1 = scan("t1", &["x", "y"]);
        let t2 = scan("t2", &["a", "b"]);
        let join = inner_join(t1, t2, None);
        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: eq(col("x"), int_lit(1)),
            }),
            vec![join],
            None,
        );

        let rule = PushDownPredicateJoin;
        assert!(rule.matches(&filter));
        let out = rule.apply(filter).expect("should rewrite");

        // Expected shape: Join(Filter(t1), t2) — no outer Filter
        match &out.kind {
            LogicalPlanNodeKind::Join(j) => {
                assert_eq!(j.join_type, JoinKind::Inner);
                match &out.left().kind {
                    LogicalPlanNodeKind::Filter(_) => match &out.left().unary_input().kind {
                        LogicalPlanNodeKind::Scan(_) => {}
                        other => panic!("expected Scan under left Filter, got {:?}", other),
                    },
                    other => panic!("expected Filter on left child, got {:?}", other),
                }
                // Right child must be unmodified scan
                assert!(matches!(&out.right().kind, LogicalPlanNodeKind::Scan(_)));
            }
            other => panic!("expected bare Join at top, got {:?}", other),
        }
    }

    // Test 2: t1 INNER t2 WHERE t2.a = 1
    // a belongs only to t2 → pushed below right child.
    // Expected: Join(t1, Filter(t2)) with no remaining filter above.
    #[test]
    fn pushes_right_only_below_inner_join() {
        let t1 = scan("t1", &["x", "y"]);
        let t2 = scan("t2", &["a", "b"]);
        let join = inner_join(t1, t2, None);
        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: eq(col("a"), int_lit(1)),
            }),
            vec![join],
            None,
        );

        let rule = PushDownPredicateJoin;
        assert!(rule.matches(&filter));
        let out = rule.apply(filter).expect("should rewrite");

        match &out.kind {
            LogicalPlanNodeKind::Join(j) => {
                assert_eq!(j.join_type, JoinKind::Inner);
                // Left child must be unmodified scan
                assert!(matches!(&out.left().kind, LogicalPlanNodeKind::Scan(_)));
                match &out.right().kind {
                    LogicalPlanNodeKind::Filter(_) => match &out.right().unary_input().kind {
                        LogicalPlanNodeKind::Scan(_) => {}
                        other => panic!("expected Scan under right Filter, got {:?}", other),
                    },
                    other => panic!("expected Filter on right child, got {:?}", other),
                }
            }
            other => panic!("expected bare Join at top, got {:?}", other),
        }
    }

    // Test 3: CROSS(t1, t2) WHERE t1.x = t2.a
    // x is left-only, a is right-only → cross-side equi-join condition.
    // Expected: INNER join with condition (x=a), no outer Filter.
    #[test]
    fn merges_cross_side_predicate_into_join_condition() {
        let t1 = scan("t1", &["x", "y"]);
        let t2 = scan("t2", &["a", "b"]);
        let join = cross_join(t1, t2);
        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: eq(col("x"), col("a")),
            }),
            vec![join],
            None,
        );

        let rule = PushDownPredicateJoin;
        assert!(rule.matches(&filter));
        let out = rule.apply(filter).expect("should rewrite");

        // Expected: INNER join with condition — no outer Filter
        match &out.kind {
            LogicalPlanNodeKind::Join(j) => {
                assert_eq!(
                    j.join_type,
                    JoinKind::Inner,
                    "CROSS should be upgraded to INNER"
                );
                assert!(j.condition.is_some(), "join condition must be set");
                // Children should be bare scans (no pushed filters)
                assert!(matches!(&out.left().kind, LogicalPlanNodeKind::Scan(_)));
                assert!(matches!(&out.right().kind, LogicalPlanNodeKind::Scan(_)));
            }
            other => panic!("expected bare Join at top, got {:?}", other),
        }
    }

    // Test 4: RIGHT OUTER JOIN(t1, t2) WHERE t1.x = 1
    // t1 is the left (nullable) side of a RIGHT OUTER join — predicates on
    // the nullable side must NOT be pushed below. The rule must either return
    // None (if the entire predicate ends up in `remaining` which reconstructs
    // the original shape) or keep the filter above the join.
    #[test]
    fn does_not_push_left_side_below_right_outer() {
        let t1 = scan("t1", &["x", "y"]);
        let t2 = scan("t2", &["a", "b"]);
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::RightOuter,
                condition: None,
            }),
            vec![t1, t2],
            None,
        );
        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: eq(col("x"), int_lit(1)),
            }),
            vec![join],
            None,
        );

        let rule = PushDownPredicateJoin;
        assert!(rule.matches(&filter));
        // The predicate references only the left (nullable) side — it cannot
        // be pushed, so the rule detects no change and returns None.
        let out = rule.apply(filter);
        assert!(
            out.is_none(),
            "left-side predicate must not be pushed below a RIGHT OUTER join; got {:?}",
            out
        );
    }

    fn col_with_id(qualifier: &str, name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(qualifier.to_string()),
                column: name.to_string(),
            },
        }
    }

    fn derived_project_with_output_id(
        name: &str,
        source_id: u32,
        output_id: u32,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Project(LogicalProjectNode {
                items: vec![ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId::new_for_test(source_id),
                            qualifier: None,
                            column: format!("{name}_source"),
                        },
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                    output_name: "k".to_string(),
                    output_column_id: ColumnId::new_for_test(output_id),
                }],
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanNodeKind::Values(LogicalValuesNode {
                    rows: vec![],
                    columns: vec![OutputColumn {
                        column_id: ColumnId::new_for_test(source_id),
                        name: format!("{name}_source"),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    }],
                }),
                vec![],
                None,
            )],
            None,
        )
    }

    #[test]
    fn pushes_alias_free_project_join_predicate_by_column_id() {
        let join = cross_join(
            derived_project_with_output_id("left", 11, 101),
            derived_project_with_output_id("right", 22, 202),
        );
        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: eq(col_with_id("a", "k", 101), col_with_id("b", "k", 202)),
            }),
            vec![join],
            None,
        );

        let rule = PushDownPredicateJoin;
        let out = rule
            .apply(filter)
            .expect("cross-side predicate should push");
        let LogicalPlanNodeKind::Join(join) = &out.kind else {
            panic!("expected bare Join with no remaining Filter");
        };
        assert_eq!(join.join_type, JoinKind::Inner);
        assert!(join.condition.is_some(), "join condition must be set");
        assert!(matches!(&out.left().kind, LogicalPlanNodeKind::Project(_)));
        assert!(matches!(&out.right().kind, LogicalPlanNodeKind::Project(_)));
    }

    fn scan_with_ids(alias: &str, cols: &[(&str, u32)]) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: alias.to_string(),
                    columns: cols
                        .iter()
                        .map(|(name, _)| ColumnDef {
                            name: name.to_string(),
                            data_type: DataType::Int64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        })
                        .collect(),
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: Some(alias.to_string()),
                columns: cols
                    .iter()
                    .map(|(name, id)| OutputColumn {
                        column_id: ColumnId::new_for_test(*id),
                        name: name.to_string(),
                        data_type: DataType::Int64,
                        nullable: true,
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

    fn join_with_ids(join_type: JoinKind, condition: Option<TypedExpr>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type,
                condition,
            }),
            vec![
                scan_with_ids("l", &[("a", 1), ("v", 3)]),
                scan_with_ids("r", &[("b", 2), ("w", 4)]),
            ],
            None,
        )
    }

    fn push_test_filter_predicates_through_join(
        predicate: TypedExpr,
        join_plan: LogicalPlanNode,
    ) -> (LogicalPlanNode, bool) {
        let LogicalPlanNode {
            kind,
            mut children,
            required_output_columns,
        } = join_plan;
        let LogicalPlanNodeKind::Join(join) = kind else {
            panic!("expected Join test plan");
        };
        let right = children.remove(1);
        let left = children.remove(0);
        push_filter_predicates_through_join(predicate, join, left, right, required_output_columns)
    }

    fn push_test_join_condition_predicates(join_plan: LogicalPlanNode) -> Option<LogicalPlanNode> {
        let LogicalPlanNode {
            kind,
            mut children,
            required_output_columns,
        } = join_plan;
        let LogicalPlanNodeKind::Join(join) = kind else {
            panic!("expected Join test plan");
        };
        let right = children.remove(1);
        let left = children.remove(0);
        push_join_condition_predicates(join, left, right, required_output_columns)
    }

    #[test]
    fn filter_join_pushes_left_right_and_keeps_cross_side_residual() {
        let predicate = combine_and(vec![
            eq(col_with_id("l", "a", 1), int_lit(10)),
            eq(col_with_id("r", "b", 2), int_lit(20)),
            eq(col_with_id("l", "v", 3), col_with_id("r", "w", 4)),
        ]);

        let (plan, changed) = push_test_filter_predicates_through_join(
            predicate,
            join_with_ids(JoinKind::Inner, None),
        );

        assert!(changed);
        let LogicalPlanNodeKind::Join(join) = &plan.kind else {
            panic!("expected bare Join");
        };
        assert!(matches!(&plan.left().kind, LogicalPlanNodeKind::Filter(_)));
        assert!(matches!(&plan.right().kind, LogicalPlanNodeKind::Filter(_)));
        assert!(join.condition.is_some());
    }

    #[test]
    fn filter_join_derives_right_filter_from_filter_join_key_constant() {
        let predicate = combine_and(vec![
            eq(col_with_id("l", "a", 1), col_with_id("r", "b", 2)),
            eq(col_with_id("l", "a", 1), int_lit(7)),
        ]);

        let (plan, changed) = push_test_filter_predicates_through_join(
            predicate,
            join_with_ids(JoinKind::Inner, None),
        );

        assert!(changed);
        let LogicalPlanNodeKind::Join(join) = &plan.kind else {
            panic!("expected bare Join");
        };
        match &plan.right().kind {
            LogicalPlanNodeKind::Filter(filter) => {
                let rendered = format!("{:?}", filter.predicate.kind);
                assert!(rendered.contains("\"b\""));
                assert!(rendered.contains("Int(7)"));
            }
            other => panic!("expected right Filter, got {:?}", other),
        }
        let condition = join.condition.as_ref().expect("join condition");
        let rendered = format!("{:?}", condition.kind);
        assert!(rendered.contains("\"a\""));
        assert!(rendered.contains("\"b\""));
    }

    #[test]
    fn derived_predicate_detects_existing_child_filter_conjunct() {
        let existing_left = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: combine_and(vec![
                    eq(col_with_id("l", "a", 1), int_lit(7)),
                    eq(col_with_id("l", "v", 3), int_lit(3)),
                ]),
            }),
            vec![scan_with_ids("l", &[("a", 1), ("v", 3)])],
            None,
        );
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(eq(col_with_id("l", "a", 1), col_with_id("r", "b", 2))),
            }),
            vec![existing_left, scan_with_ids("r", &[("b", 2), ("w", 4)])],
            None,
        );

        let (plan, changed) = push_test_filter_predicates_through_join(
            eq(col_with_id("r", "b", 2), int_lit(7)),
            join,
        );

        assert!(changed);
        let LogicalPlanNodeKind::Join(join) = &plan.kind else {
            panic!("expected Join");
        };
        match &plan.left().kind {
            LogicalPlanNodeKind::Filter(filter) => {
                assert!(
                    !matches!(
                        &plan.left().unary_input().kind,
                        LogicalPlanNodeKind::Filter(_)
                    ),
                    "must not add duplicate derived left filter over existing conjunct"
                );
                let rendered = format!("{:?}", filter.predicate.kind);
                assert!(rendered.contains("\"a\""));
                assert!(rendered.contains("\"v\""));
            }
            other => panic!("expected existing left Filter, got {:?}", other),
        }
    }

    #[test]
    fn join_condition_pushes_single_side_terms_below_inner_join() {
        let condition = combine_and(vec![
            eq(col_with_id("l", "a", 1), int_lit(10)),
            eq(col_with_id("l", "v", 3), col_with_id("r", "w", 4)),
        ]);

        let plan =
            push_test_join_condition_predicates(join_with_ids(JoinKind::Inner, Some(condition)))
                .expect("join condition should be rewritten");

        let LogicalPlanNodeKind::Join(join) = &plan.kind else {
            panic!("expected Join");
        };
        assert!(matches!(&plan.left().kind, LogicalPlanNodeKind::Filter(_)));
        let condition = join.condition.as_ref().expect("join condition");
        let rendered = format!("{:?}", condition.kind);
        assert!(rendered.contains("\"v\""));
        assert!(rendered.contains("\"w\""));
        assert!(!rendered.contains("Int(10)"));
    }

    #[test]
    fn join_condition_derives_right_filter_from_left_key_constant() {
        let condition = combine_and(vec![
            eq(col_with_id("l", "a", 1), col_with_id("r", "b", 2)),
            eq(col_with_id("l", "a", 1), int_lit(7)),
        ]);

        let plan =
            push_test_join_condition_predicates(join_with_ids(JoinKind::Inner, Some(condition)))
                .expect("join condition should derive side filters");

        let LogicalPlanNodeKind::Join(join) = &plan.kind else {
            panic!("expected Join");
        };
        match &plan.left().kind {
            LogicalPlanNodeKind::Filter(filter) => {
                let rendered = format!("{:?}", filter.predicate.kind);
                assert!(rendered.contains("\"a\""));
                assert!(rendered.contains("Int(7)"));
            }
            other => panic!("expected left Filter, got {:?}", other),
        }
        match &plan.right().kind {
            LogicalPlanNodeKind::Filter(filter) => {
                let rendered = format!("{:?}", filter.predicate.kind);
                assert!(rendered.contains("\"b\""));
                assert!(rendered.contains("Int(7)"));
            }
            other => panic!("expected right Filter, got {:?}", other),
        }
        let condition = join.condition.as_ref().expect("residual condition");
        let rendered = format!("{:?}", condition.kind);
        assert!(rendered.contains("\"a\""));
        assert!(rendered.contains("\"b\""));
        assert!(!rendered.contains("Int(7)"));
    }

    #[test]
    fn cross_join_with_residual_condition_upgrades_to_inner() {
        let condition = eq(col_with_id("l", "v", 3), col_with_id("r", "w", 4));

        let plan =
            push_test_join_condition_predicates(join_with_ids(JoinKind::Cross, Some(condition)))
                .expect("cross join should be upgraded");

        let LogicalPlanNodeKind::Join(join) = &plan.kind else {
            panic!("expected Join");
        };
        assert_eq!(join.join_type, JoinKind::Inner);
    }

    #[test]
    fn join_condition_entrypoint_ignores_non_inner_cross_joins() {
        let condition = eq(col_with_id("l", "a", 1), int_lit(10));

        let plan = push_test_join_condition_predicates(join_with_ids(
            JoinKind::LeftOuter,
            Some(condition),
        ));

        assert!(plan.is_none());
    }
}
