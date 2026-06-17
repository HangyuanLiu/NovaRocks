//! PushDownPredicateJoin rule — `Filter(Join)` and `Join` (condition pushdown).
//!
//! Migrated to `OptExpr` / `LogicalRewriteRule`. The join predicate
//! classification logic is re-implemented locally in OptExpr terms (calling
//! through to TypedExpr helpers after materializing ScalarIds), because the
//! existing `join_pushdown.rs` functions take `LogicalPlanNode` arguments.
//!
//! No tests in this file: the underlying logic is tested in `join_pushdown.rs`.

use std::collections::HashSet;

use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{FilterOp, LogicalJoinOp, Operator};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::deriver::derive_inner_join_predicates;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
    PredicateGroup, PredicateOrigin, predicate_key as canonical_predicate_key,
};
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_column_id_refs_strict, collect_output_ids_opt, combine_and, split_and,
    wrap_remaining_filter_opt,
};
use crate::sql::optimizer::scalar::{self, ScalarArena};
use arrow::datatypes::DataType;

pub(crate) struct PushDownPredicateJoin;

impl LogicalRewriteRule for PushDownPredicateJoin {
    fn name(&self) -> &'static str {
        "PushDownPredicateJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        (matches!(&expr.op, Operator::LogicalFilter(_))
            && !expr.children.is_empty()
            && matches!(&expr.unary_input().op, Operator::LogicalJoin(_)))
            || matches!(&expr.op, Operator::LogicalJoin(join) if join.condition.is_some())
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let arena_rc = ctx.scalar_arena();
        let mut arena = arena_rc.borrow_mut();

        let OptExpr {
            op,
            mut children,
            required_output_columns,
        } = expr;

        match op {
            Operator::LogicalFilter(filter_op) => {
                if children.len() != 1 {
                    return Ok(RewriteResult::Unchanged);
                }
                let join_expr = children.remove(0);
                let OptExpr {
                    op: join_op_kind,
                    children: join_children_owned,
                    required_output_columns: join_req,
                } = join_expr;
                let mut join_children = join_children_owned;
                let Operator::LogicalJoin(join_op) = join_op_kind else {
                    return Ok(RewriteResult::Unchanged);
                };
                if join_children.len() != 2 {
                    return Ok(RewriteResult::Unchanged);
                }
                let right = join_children.remove(1);
                let left = join_children.remove(0);

                let predicate = scalar::materialize(&arena, filter_op.predicate);
                let (new_join_expr, changed) = push_filter_predicates_opt(
                    predicate, join_op, left, right, join_req, &mut arena,
                );
                if changed {
                    Ok(RewriteResult::Changed(new_join_expr))
                } else {
                    Ok(RewriteResult::Unchanged)
                }
            }
            Operator::LogicalJoin(join_op) => {
                if children.len() != 2 {
                    return Ok(RewriteResult::Unchanged);
                }
                let right = children.remove(1);
                let left = children.remove(0);
                match push_join_condition_predicates_opt(
                    join_op,
                    left,
                    right,
                    required_output_columns,
                    &mut arena,
                ) {
                    Some(result) => Ok(RewriteResult::Changed(result)),
                    None => Ok(RewriteResult::Unchanged),
                }
            }
            _ => Ok(RewriteResult::Unchanged),
        }
    }
}

// ---------------------------------------------------------------------------
// Local re-implementation of the join pushdown logic in OptExpr output terms.
// The predicate classification and derivation logic uses TypedExpr throughout
// (after materializing ScalarIds). Only the tree construction uses OptExpr.
// ---------------------------------------------------------------------------

fn push_filter_predicates_opt(
    predicate: TypedExpr,
    join: LogicalJoinOp,
    left: OptExpr,
    right: OptExpr,
    required_output_columns: Option<HashSet<ColumnId>>,
    arena: &mut ScalarArena,
) -> (OptExpr, bool) {
    let mut left_ids = collect_output_ids_opt(&left);
    let mut right_ids = collect_output_ids_opt(&right);
    left_ids.remove(&ColumnId::UNSET);
    right_ids.remove(&ColumnId::UNSET);

    let filter_groups = PredicateGroup::from_predicate(predicate.clone(), PredicateOrigin::Filter);
    let join_groups = join
        .condition
        .map(|cond_id| {
            let cond = scalar::materialize(arena, cond_id);
            PredicateGroup::from_predicate(cond, PredicateOrigin::JoinCondition)
        })
        .unwrap_or_default();

    let mut conjuncts = split_and(predicate);

    if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
        let derived =
            derive_inner_join_predicates(&left_ids, &right_ids, &join_groups, &filter_groups);
        append_new_derived_conjuncts_opt(
            &mut conjuncts,
            derived,
            &left,
            &right,
            &left_ids,
            &right_ids,
            arena,
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
            (false, true) => match join.join_type {
                JoinKind::Inner
                | JoinKind::Cross
                | JoinKind::RightOuter
                | JoinKind::RightSemi
                | JoinKind::RightAnti => {
                    right_preds.push(conj);
                }
                _ => remaining.push(conj),
            },
            (true, true) => {
                if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
                    let (implied_left, implied_right) =
                        extract_implied_or_side_filters(&conj, &left_ids, &right_ids);
                    for pred in implied_left {
                        if !subtree_has_predicate_opt(&left, &pred, arena) {
                            left_preds.push(pred);
                        }
                    }
                    for pred in implied_right {
                        if !subtree_has_predicate_opt(&right, &pred, arena) {
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
                left_preds.push(conj);
            }
        }
    }

    // For RIGHT OUTER joins, left-side predicates cannot be pushed below.
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

    let pushed_any = !left_preds.is_empty() || !right_preds.is_empty() || !join_preds.is_empty();

    let new_left = if left_preds.is_empty() {
        left
    } else {
        let pushed_id = scalar::intern_typed(arena, &combine_and(left_preds));
        OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![left],
        )
    };

    let new_right = if right_preds.is_empty() {
        right
    } else {
        let pushed_id = scalar::intern_typed(arena, &combine_and(right_preds));
        OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![right],
        )
    };

    // Merge new join predicates with the existing join condition.
    let existing_condition = join
        .condition
        .map(|cond_id| scalar::materialize(arena, cond_id));
    let new_condition_expr = merge_join_conditions(existing_condition, join_preds);
    let new_condition = new_condition_expr.map(|expr| scalar::intern_typed(arena, &expr));

    // Upgrade CROSS JOIN to INNER when join predicates were extracted.
    let new_join_type = if join.join_type == JoinKind::Cross && new_condition.is_some() {
        JoinKind::Inner
    } else {
        join.join_type
    };

    let mut new_join = OptExpr::new(
        Operator::LogicalJoin(LogicalJoinOp {
            join_type: new_join_type,
            condition: new_condition,
        }),
        vec![new_left, new_right],
    );
    new_join.required_output_columns = required_output_columns;

    let result = wrap_remaining_filter_opt(new_join, remaining, arena);
    (result, pushed_any)
}

fn push_join_condition_predicates_opt(
    join: LogicalJoinOp,
    left: OptExpr,
    right: OptExpr,
    required_output_columns: Option<HashSet<ColumnId>>,
    arena: &mut ScalarArena,
) -> Option<OptExpr> {
    if !matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
        return None;
    }

    let cond_id = join.condition?;
    let condition = scalar::materialize(arena, cond_id);

    let mut left_ids = collect_output_ids_opt(&left);
    let mut right_ids = collect_output_ids_opt(&right);
    left_ids.remove(&ColumnId::UNSET);
    right_ids.remove(&ColumnId::UNSET);

    let condition_groups =
        PredicateGroup::from_predicate(condition.clone(), PredicateOrigin::JoinCondition);
    let mut conjuncts = split_and(condition);

    let derived =
        derive_inner_join_predicates(&left_ids, &right_ids, &condition_groups, &condition_groups);
    append_new_derived_conjuncts_opt(
        &mut conjuncts,
        derived,
        &left,
        &right,
        &left_ids,
        &right_ids,
        arena,
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
    let new_condition_expr = if residual_preds.is_empty() {
        None
    } else {
        Some(combine_and(residual_preds))
    };
    let new_condition = new_condition_expr.map(|expr| scalar::intern_typed(arena, &expr));
    let upgrades_cross = join.join_type == JoinKind::Cross && new_condition.is_some();

    if !pushed_any && !upgrades_cross {
        return None;
    }

    let new_left = if left_preds.is_empty() {
        left
    } else {
        let pushed_id = scalar::intern_typed(arena, &combine_and(left_preds));
        OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![left],
        )
    };

    let new_right = if right_preds.is_empty() {
        right
    } else {
        let pushed_id = scalar::intern_typed(arena, &combine_and(right_preds));
        OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![right],
        )
    };

    let new_join_type = if upgrades_cross {
        JoinKind::Inner
    } else {
        join.join_type
    };

    let mut result = OptExpr::new(
        Operator::LogicalJoin(LogicalJoinOp {
            join_type: new_join_type,
            condition: new_condition,
        }),
        vec![new_left, new_right],
    );
    result.required_output_columns = required_output_columns;
    Some(result)
}

// ---------------------------------------------------------------------------
// Predicate classification helpers (TypedExpr-based, same as join_pushdown.rs)
// ---------------------------------------------------------------------------

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

fn append_new_derived_conjuncts_opt(
    conjuncts: &mut Vec<TypedExpr>,
    derived: Vec<PredicateGroup>,
    left: &OptExpr,
    right: &OptExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    arena: &ScalarArena,
) {
    let mut seen: HashSet<String> = conjuncts.iter().map(predicate_key_str).collect();
    for group in derived {
        if derived_exists_below_child_opt(&group.expr, left, right, left_ids, right_ids, arena) {
            continue;
        }
        if seen.insert(predicate_key_str(&group.expr)) {
            conjuncts.push(group.expr);
        }
    }
}

fn derived_exists_below_child_opt(
    expr: &TypedExpr,
    left: &OptExpr,
    right: &OptExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    arena: &ScalarArena,
) -> bool {
    let Some(ids) = collect_column_id_refs_strict(expr) else {
        return false;
    };
    if ids.is_empty() {
        return false;
    }
    if ids.iter().all(|id| left_ids.contains(id)) {
        return subtree_has_predicate_opt(left, expr, arena);
    }
    if ids.iter().all(|id| right_ids.contains(id)) {
        return subtree_has_predicate_opt(right, expr, arena);
    }
    false
}

fn subtree_has_predicate_opt(expr: &OptExpr, pred: &TypedExpr, arena: &ScalarArena) -> bool {
    let key = predicate_key_str(pred);
    subtree_has_predicate_key_opt(expr, &key, arena)
}

fn subtree_has_predicate_key_opt(expr: &OptExpr, key: &str, arena: &ScalarArena) -> bool {
    match &expr.op {
        Operator::LogicalScan(scan) => scan.predicates.iter().any(|&pred_id| {
            let pred_expr = scalar::materialize(arena, pred_id);
            predicate_has_conjunct_key(&pred_expr, key)
        }),
        Operator::LogicalFilter(filter) => {
            let filter_expr = scalar::materialize(arena, filter.predicate);
            predicate_has_conjunct_key(&filter_expr, key)
                || expr
                    .children
                    .iter()
                    .any(|child| subtree_has_predicate_key_opt(child, key, arena))
        }
        Operator::LogicalJoin(join) => {
            join.condition
                .map(|cond_id| {
                    let cond_expr = scalar::materialize(arena, cond_id);
                    predicate_has_conjunct_key(&cond_expr, key)
                })
                .unwrap_or(false)
                || expr
                    .children
                    .iter()
                    .any(|child| subtree_has_predicate_key_opt(child, key, arena))
        }
        _ => expr
            .children
            .iter()
            .any(|child| subtree_has_predicate_key_opt(child, key, arena)),
    }
}

fn predicate_has_conjunct_key(expr: &TypedExpr, key: &str) -> bool {
    split_and_refs(expr)
        .into_iter()
        .any(|conjunct| predicate_key_str(conjunct) == key)
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

fn predicate_key_str(expr: &TypedExpr) -> String {
    canonical_predicate_key(expr).as_str().to_string()
}

fn merge_join_conditions(
    existing: Option<TypedExpr>,
    new_preds: Vec<TypedExpr>,
) -> Option<TypedExpr> {
    let mut all = Vec::new();
    let mut seen = HashSet::new();
    if let Some(cond) = existing {
        for pred in split_and(cond) {
            if seen.insert(predicate_key_str(&pred)) {
                all.push(pred);
            }
        }
    }
    for pred in new_preds {
        if seen.insert(predicate_key_str(&pred)) {
            all.push(pred);
        }
    }
    if all.is_empty() {
        None
    } else {
        Some(combine_and(all))
    }
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

fn factor_common_eq_from_or(
    expr: &TypedExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> (Vec<TypedExpr>, Option<TypedExpr>) {
    let branches = split_or_branches(expr);
    if branches.len() < 2 {
        return (vec![], None);
    }

    let branch_conjuncts: Vec<Vec<&TypedExpr>> =
        branches.iter().map(|b| split_and_refs(b)).collect();

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

    let mut new_branches: Vec<TypedExpr> = Vec::new();
    for branch in &branch_conjuncts {
        let remaining: Vec<TypedExpr> = branch
            .iter()
            .filter(|c| !common_eqs.iter().any(|eq| expr_eq(c, eq)))
            .map(|c| (*c).clone())
            .collect();
        if remaining.is_empty() {
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
        None
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
