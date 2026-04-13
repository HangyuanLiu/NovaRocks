//! Implementation rules: logical operator -> physical operator(s).
//!
//! Each struct implements the `Rule` trait. The `apply` method constructs the
//! physical variant of the matched logical operator, preserving child GroupIds.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::cascades::memo::{GroupId, MExpr, Memo};
use crate::sql::cascades::operator::*;
use crate::sql::cascades::rule::{NewExpr, Rule, RuleType};
use crate::sql::ir::{BinOp, ExprKind, JoinKind, TypedExpr};

/// Get lowercase column names from a memo group's output columns.
fn get_group_column_names(memo: &Memo, group_id: GroupId) -> HashSet<String> {
    memo.groups
        .get(group_id)
        .and_then(|g| g.logical_props.as_ref())
        .map(|props| {
            props
                .output_columns
                .iter()
                .map(|c| c.name.to_lowercase())
                .collect()
        })
        .unwrap_or_default()
}

/// Walk a TypedExpr and return the set of lowercase column names it references.
fn collect_column_refs_lowercase(expr: &TypedExpr) -> HashSet<String> {
    let mut out = HashSet::new();
    walk_column_refs(expr, &mut out);
    out
}

fn walk_column_refs(expr: &TypedExpr, out: &mut HashSet<String>) {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => {
            out.insert(column.to_lowercase());
        }
        ExprKind::BinaryOp { left, right, .. } => {
            walk_column_refs(left, out);
            walk_column_refs(right, out);
        }
        ExprKind::UnaryOp { expr, .. } => {
            walk_column_refs(expr, out);
        }
        ExprKind::FunctionCall { args, .. } => {
            for a in args {
                walk_column_refs(a, out);
            }
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            for a in args {
                walk_column_refs(a, out);
            }
            for item in order_by {
                walk_column_refs(&item.expr, out);
            }
        }
        ExprKind::Cast { expr, .. } => {
            walk_column_refs(expr, out);
        }
        ExprKind::IsNull { expr, .. } => {
            walk_column_refs(expr, out);
        }
        ExprKind::InList { expr, list, .. } => {
            walk_column_refs(expr, out);
            for item in list {
                walk_column_refs(item, out);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            walk_column_refs(expr, out);
            walk_column_refs(low, out);
            walk_column_refs(high, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            walk_column_refs(expr, out);
            walk_column_refs(pattern, out);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                walk_column_refs(op, out);
            }
            for (cond, val) in when_then {
                walk_column_refs(cond, out);
                walk_column_refs(val, out);
            }
            if let Some(e) = else_expr {
                walk_column_refs(e, out);
            }
        }
        ExprKind::IsTruthValue { expr, .. } => {
            walk_column_refs(expr, out);
        }
        ExprKind::Nested(inner) => {
            walk_column_refs(inner, out);
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for a in args {
                walk_column_refs(a, out);
            }
            for p in partition_by {
                walk_column_refs(p, out);
            }
            for item in order_by {
                walk_column_refs(&item.expr, out);
            }
        }
        ExprKind::Literal(_) | ExprKind::SubqueryPlaceholder { .. } => {}
    }
}

/// Orient an eq pair so that the first element references the left child's
/// columns and the second references the right. Returns:
///   - `Some((a, b))` if natural order works (a from left, b from right).
///   - `Some((b, a))` if swapping works.
///   - `None` if both sides reference the same child (caller should demote
///     the pair into the residual "other" predicate).
fn orient_eq_pair(
    pair: (TypedExpr, TypedExpr),
    left_cols: &HashSet<String>,
    right_cols: &HashSet<String>,
) -> Option<(TypedExpr, TypedExpr)> {
    let (a, b) = pair;
    let a_cols = collect_column_refs_lowercase(&a);
    let b_cols = collect_column_refs_lowercase(&b);

    let a_in_left = !a_cols.is_empty() && a_cols.iter().all(|c| left_cols.contains(c));
    let a_in_right = !a_cols.is_empty() && a_cols.iter().all(|c| right_cols.contains(c));
    let b_in_left = !b_cols.is_empty() && b_cols.iter().all(|c| left_cols.contains(c));
    let b_in_right = !b_cols.is_empty() && b_cols.iter().all(|c| right_cols.contains(c));

    if a_in_left && b_in_right {
        Some((a, b))
    } else if a_in_right && b_in_left {
        Some((b, a))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Helper: extract equality conditions from a join predicate
// ---------------------------------------------------------------------------

/// Walk a join condition and split top-level AND-connected `a = b` pairs from
/// the remaining predicate. Returns `(eq_pairs, remaining_condition)`.
///
/// Also handles OR-connected disjuncts: if the top-level condition (or a
/// top-level conjunct) is `(A AND eq) OR (B AND eq) OR …`, the equality
/// pairs that appear in *every* OR branch are extracted as hash join keys.
///
/// For cross joins (condition is `None`) or when no equalities are found,
/// `eq_pairs` will be empty.
fn extract_eq_conditions(
    condition: &Option<TypedExpr>,
    _join_type: &JoinKind,
) -> (Vec<(TypedExpr, TypedExpr)>, Option<TypedExpr>) {
    let Some(cond) = condition else {
        return (vec![], None);
    };
    let mut eq_pairs = Vec::new();
    let mut others = Vec::new();
    collect_conjuncts(cond, &mut eq_pairs, &mut others);

    // If no equalities were found from top-level AND, try to extract common
    // equalities from OR branches among the "other" predicates.
    if eq_pairs.is_empty() {
        let mut new_others = Vec::new();
        for part in others {
            let (common, rewritten) = try_extract_common_eq_from_or(&part);
            eq_pairs.extend(common);
            if let Some(r) = rewritten {
                new_others.push(r);
            }
        }
        others = new_others;
    }

    let remaining = combine_conjuncts(others);
    (eq_pairs, remaining)
}

/// Recursively flatten top-level AND nodes and classify each conjunct as
/// either an equality pair or a residual predicate.
fn collect_conjuncts(
    expr: &TypedExpr,
    eq_pairs: &mut Vec<(TypedExpr, TypedExpr)>,
    others: &mut Vec<TypedExpr>,
) {
    match &expr.kind {
        // Unwrap parenthesized expressions transparently.
        ExprKind::Nested(inner) => {
            collect_conjuncts(inner, eq_pairs, others);
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_conjuncts(left, eq_pairs, others);
            collect_conjuncts(right, eq_pairs, others);
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            // Only treat as equi-join key if BOTH sides are column refs.
            // Expressions like `d_year = 2002` (column = constant) are filters,
            // not equi-join keys.
            let left_is_col = matches!(left.kind, ExprKind::ColumnRef { .. });
            let right_is_col = matches!(right.kind, ExprKind::ColumnRef { .. });
            if left_is_col && right_is_col {
                eq_pairs.push((*left.clone(), *right.clone()));
            } else {
                others.push(expr.clone());
            }
        }
        _ => {
            others.push(expr.clone());
        }
    }
}

/// Split a top-level OR expression into its disjuncts.
fn split_or(expr: &TypedExpr) -> Vec<TypedExpr> {
    match &expr.kind {
        ExprKind::Nested(inner) => split_or(inner),
        ExprKind::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let mut parts = split_or(left);
            parts.extend(split_or(right));
            parts
        }
        _ => vec![expr.clone()],
    }
}

/// Combine a list of disjuncts back into a single OR-connected expression.
fn combine_disjuncts(mut parts: Vec<TypedExpr>) -> Option<TypedExpr> {
    if parts.is_empty() {
        return None;
    }
    let mut result = parts.pop().unwrap();
    while let Some(p) = parts.pop() {
        result = TypedExpr {
            data_type: arrow::datatypes::DataType::Boolean,
            nullable: p.nullable || result.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(p),
                op: BinOp::Or,
                right: Box::new(result),
            },
        };
    }
    Some(result)
}

/// Structural equality for TypedExpr using Debug representation.
fn typed_expr_eq(a: &TypedExpr, b: &TypedExpr) -> bool {
    format!("{:?}", a) == format!("{:?}", b)
}

/// Check if two eq pairs are structurally equal (possibly with swapped sides).
fn eq_pair_matches(a: &(TypedExpr, TypedExpr), b: &(TypedExpr, TypedExpr)) -> bool {
    (typed_expr_eq(&a.0, &b.0) && typed_expr_eq(&a.1, &b.1))
        || (typed_expr_eq(&a.0, &b.1) && typed_expr_eq(&a.1, &b.0))
}

/// Try to extract common equality conditions from an OR expression.
///
/// Given `(A AND x=y AND B) OR (C AND x=y AND D)`, extracts `(x, y)` as
/// a common eq pair and rewrites the expression to `(A AND B) OR (C AND D)`.
///
/// Returns `(common_eq_pairs, rewritten_or_condition)`.
fn try_extract_common_eq_from_or(
    expr: &TypedExpr,
) -> (Vec<(TypedExpr, TypedExpr)>, Option<TypedExpr>) {
    let branches = split_or(expr);
    if branches.len() < 2 {
        return (vec![], Some(expr.clone()));
    }

    // For each branch, extract eq pairs and residual.
    let mut branch_eqs: Vec<Vec<(TypedExpr, TypedExpr)>> = Vec::new();
    let mut branch_others: Vec<Vec<TypedExpr>> = Vec::new();
    for branch in &branches {
        let mut eqs = Vec::new();
        let mut others = Vec::new();
        collect_conjuncts(branch, &mut eqs, &mut others);
        branch_eqs.push(eqs);
        branch_others.push(others);
    }

    // Find eq pairs that appear in ALL branches.
    let first_eqs = &branch_eqs[0];
    let mut common: Vec<(TypedExpr, TypedExpr)> = Vec::new();
    for eq in first_eqs {
        if branch_eqs[1..]
            .iter()
            .all(|branch| branch.iter().any(|b| eq_pair_matches(eq, b)))
        {
            common.push(eq.clone());
        }
    }

    if common.is_empty() {
        return (vec![], Some(expr.clone()));
    }

    // Rewrite each branch: remove the common eq pairs, recombine.
    let mut rewritten_branches = Vec::new();
    for (eqs, others) in branch_eqs.iter().zip(branch_others.iter()) {
        let mut remaining_parts: Vec<TypedExpr> = others.clone();
        for eq in eqs {
            if !common.iter().any(|c| eq_pair_matches(c, eq)) {
                // Keep non-common eq pairs as regular conjuncts.
                remaining_parts.push(TypedExpr {
                    data_type: arrow::datatypes::DataType::Boolean,
                    nullable: eq.0.nullable || eq.1.nullable,
                    kind: ExprKind::BinaryOp {
                        left: Box::new(eq.0.clone()),
                        op: BinOp::Eq,
                        right: Box::new(eq.1.clone()),
                    },
                });
            }
        }
        if let Some(branch_expr) = combine_conjuncts(remaining_parts) {
            rewritten_branches.push(branch_expr);
        }
        // If a branch becomes empty (only common eqs), skip it — it
        // effectively becomes TRUE, making the whole OR always true for
        // matched eq keys.  We represent this by omitting the branch.
    }

    let rewritten = if rewritten_branches.len() == branches.len() {
        combine_disjuncts(rewritten_branches)
    } else {
        // Some branches were pure eq-only; the entire OR condition is
        // satisfied whenever the common equalities hold.
        None
    };

    (common, rewritten)
}

/// Combine a list of residual predicates back into a single AND-connected
/// expression. Returns `None` if the list is empty.
fn combine_conjuncts(mut parts: Vec<TypedExpr>) -> Option<TypedExpr> {
    if parts.is_empty() {
        return None;
    }
    let mut result = parts.pop().unwrap();
    while let Some(p) = parts.pop() {
        result = TypedExpr {
            data_type: arrow::datatypes::DataType::Boolean,
            nullable: p.nullable || result.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(p),
                op: BinOp::And,
                right: Box::new(result),
            },
        };
    }
    Some(result)
}

// ===========================================================================
// Implementation rule structs
// ===========================================================================

// ---------------------------------------------------------------------------
// 1. ScanToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct ScanToPhysical;

impl Rule for ScanToPhysical {
    fn name(&self) -> &str {
        "ScanToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalScan(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalScan(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: op.database.clone(),
                table: op.table.clone(),
                alias: op.alias.clone(),
                columns: op.columns.clone(),
                predicates: op.predicates.clone(),
                required_columns: op.required_columns.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 2. FilterToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct FilterToPhysical;

impl Rule for FilterToPhysical {
    fn name(&self) -> &str {
        "FilterToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalFilter(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalFilter(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalFilter(PhysicalFilterOp {
                predicate: op.predicate.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 3. ProjectToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct ProjectToPhysical;

impl Rule for ProjectToPhysical {
    fn name(&self) -> &str {
        "ProjectToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalProject(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalProject(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalProject(PhysicalProjectOp {
                items: op.items.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 4. JoinToHashJoin
// ---------------------------------------------------------------------------

pub(crate) struct JoinToHashJoin;

impl Rule for JoinToHashJoin {
    fn name(&self) -> &str {
        "JoinToHashJoin"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalJoin(_))
    }
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalJoin(op) = &expr.op else {
            return vec![];
        };
        let (raw_eq_conds, mut other) = extract_eq_conditions(&op.condition, &op.join_type);

        // Orient eq_conditions so that pair.0 references the left child's
        // columns and pair.1 references the right child's columns.  Pairs
        // that reference only one side (e.g., inner predicates in a SEMI
        // JOIN condition) are demoted into other_condition.
        let mut eq_conds = Vec::new();
        if expr.children.len() == 2 {
            let left_cols = get_group_column_names(memo, expr.children[0]);
            let right_cols = get_group_column_names(memo, expr.children[1]);
            for pair in raw_eq_conds {
                let (a, b) = pair.clone();
                match orient_eq_pair(pair, &left_cols, &right_cols) {
                    Some(oriented) => eq_conds.push(oriented),
                    None => {
                        let demoted = TypedExpr {
                            data_type: DataType::Boolean,
                            nullable: false,
                            kind: ExprKind::BinaryOp {
                                left: Box::new(a),
                                op: BinOp::Eq,
                                right: Box::new(b),
                            },
                        };
                        other = Some(match other {
                            Some(existing) => TypedExpr {
                                data_type: DataType::Boolean,
                                nullable: false,
                                kind: ExprKind::BinaryOp {
                                    left: Box::new(existing),
                                    op: BinOp::And,
                                    right: Box::new(demoted),
                                },
                            },
                            None => demoted,
                        });
                    }
                }
            }
        } else {
            eq_conds = raw_eq_conds;
        }

        if eq_conds.is_empty() {
            // No equality conditions — JoinToNestLoop should handle this.
            return vec![];
        }
        vec![
            NewExpr {
                op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                    join_type: op.join_type,
                    eq_conditions: eq_conds.clone(),
                    other_condition: other.clone(),
                    distribution: JoinDistribution::Shuffle,
                }),
                children: expr.children.clone(),
            },
            NewExpr {
                op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                    join_type: op.join_type,
                    eq_conditions: eq_conds,
                    other_condition: other,
                    distribution: JoinDistribution::Broadcast,
                }),
                children: expr.children.clone(),
            },
        ]
    }
}

// ---------------------------------------------------------------------------
// 5. JoinToNestLoop
// ---------------------------------------------------------------------------

pub(crate) struct JoinToNestLoop;

impl Rule for JoinToNestLoop {
    fn name(&self) -> &str {
        "JoinToNestLoop"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalJoin(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalJoin(op) = &expr.op else {
            return vec![];
        };
        // NestLoop is used for cross joins or joins without equality conditions.
        let (eq_conds, _) = extract_eq_conditions(&op.condition, &op.join_type);
        if !eq_conds.is_empty() && op.join_type != JoinKind::Cross {
            // Has equality conditions — JoinToHashJoin should handle this.
            return vec![];
        }
        vec![NewExpr {
            op: Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type: op.join_type,
                condition: op.condition.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 6. AggToHashAgg
// ---------------------------------------------------------------------------

pub(crate) struct AggToHashAgg;

impl Rule for AggToHashAgg {
    fn name(&self) -> &str {
        "AggToHashAgg"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregate(_))
    }
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAggregate(op) = &expr.op else {
            return vec![];
        };

        // Alternative 1: Single-phase aggregation (always applicable).
        let single = NewExpr {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        };

        // Two-phase Local+Global aggregation is deferred — the Global
        // aggregate's input expressions must reference the Local output
        // columns (e.g., `sum(sum(x))`), which requires expression
        // rewriting not yet implemented.  Single-phase only for now.
        vec![single]
    }
}

// ---------------------------------------------------------------------------
// 7. SortToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct SortToPhysical;

impl Rule for SortToPhysical {
    fn name(&self) -> &str {
        "SortToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalSort(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalSort(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalSort(PhysicalSortOp {
                items: op.items.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 8. LimitToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct LimitToPhysical;

impl Rule for LimitToPhysical {
    fn name(&self) -> &str {
        "LimitToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalLimit(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalLimit(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalLimit(PhysicalLimitOp {
                limit: op.limit,
                offset: op.offset,
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 8b. TopNToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct TopNToPhysical;

impl Rule for TopNToPhysical {
    fn name(&self) -> &str {
        "TopNToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalTopN(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalTopN(PhysicalTopNOp {
                items: op.items.clone(),
                limit: op.limit,
                offset: op.offset,
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 9. WindowToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct WindowToPhysical;

impl Rule for WindowToPhysical {
    fn name(&self) -> &str {
        "WindowToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalWindow(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalWindow(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalWindow(PhysicalWindowOp {
                window_exprs: op.window_exprs.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 10. CTEAnchorToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct CTEAnchorToPhysical;

impl Rule for CTEAnchorToPhysical {
    fn name(&self) -> &str {
        "CTEAnchorToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalCTEAnchor(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalCTEAnchor(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalCTEAnchor(PhysicalCTEAnchorOp { cte_id: op.cte_id }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 11. CTEProduceToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct CTEProduceToPhysical;

impl Rule for CTEProduceToPhysical {
    fn name(&self) -> &str {
        "CTEProduceToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalCTEProduce(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalCTEProduce(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalCTEProduce(PhysicalCTEProduceOp {
                cte_id: op.cte_id,
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 12. CTEConsumeToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct CTEConsumeToPhysical;

impl Rule for CTEConsumeToPhysical {
    fn name(&self) -> &str {
        "CTEConsumeToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalCTEConsume(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalCTEConsume(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalCTEConsume(PhysicalCTEConsumeOp {
                cte_id: op.cte_id,
                alias: op.alias.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 13. RepeatToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct RepeatToPhysical;

impl Rule for RepeatToPhysical {
    fn name(&self) -> &str {
        "RepeatToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalRepeat(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalRepeat(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalRepeat(PhysicalRepeatOp {
                repeat_column_ref_list: op.repeat_column_ref_list.clone(),
                grouping_ids: op.grouping_ids.clone(),
                all_rollup_columns: op.all_rollup_columns.clone(),
                grouping_fn_args: op.grouping_fn_args.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 14. UnionToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct UnionToPhysical;

impl Rule for UnionToPhysical {
    fn name(&self) -> &str {
        "UnionToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalUnion(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalUnion(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalUnion(PhysicalUnionOp { all: op.all }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 15. IntersectToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct IntersectToPhysical;

impl Rule for IntersectToPhysical {
    fn name(&self) -> &str {
        "IntersectToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalIntersect(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        vec![NewExpr {
            op: Operator::PhysicalIntersect(PhysicalIntersectOp),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 15. ExceptToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct ExceptToPhysical;

impl Rule for ExceptToPhysical {
    fn name(&self) -> &str {
        "ExceptToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalExcept(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        vec![NewExpr {
            op: Operator::PhysicalExcept(PhysicalExceptOp),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 16. ValuesToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct ValuesToPhysical;

impl Rule for ValuesToPhysical {
    fn name(&self) -> &str {
        "ValuesToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalValues(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalValues(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalValues(PhysicalValuesOp {
                rows: op.rows.clone(),
                columns: op.columns.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 17. GenerateSeriesToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct GenerateSeriesToPhysical;

impl Rule for GenerateSeriesToPhysical {
    fn name(&self) -> &str {
        "GenerateSeriesToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalGenerateSeries(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalGenerateSeries(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalGenerateSeries(PhysicalGenerateSeriesOp {
                start: op.start,
                end: op.end,
                step: op.step,
                column_name: op.column_name.clone(),
                alias: op.alias.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 18. SubqueryAliasToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct SubqueryAliasToPhysical;

impl Rule for SubqueryAliasToPhysical {
    fn name(&self) -> &str {
        "SubqueryAliasToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalSubqueryAlias(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalSubqueryAlias(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalSubqueryAlias(PhysicalSubqueryAliasOp {
                alias: op.alias.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

#[cfg(test)]
mod top_n_tests {
    use super::*;
    use crate::sql::cascades::memo::{MExpr, Memo};
    use crate::sql::cascades::operator::LogicalTopNOp;

    #[test]
    fn top_n_to_physical_produces_physical_top_n() {
        let mut memo = Memo::new();
        let values_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let dummy_child = memo.new_group(values_mexpr);

        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: Some(50),
                offset: Some(10),
            }),
            children: vec![dummy_child],
        };
        let rule = TopNToPhysical;
        let out = rule.apply(&expr, &mut memo);
        assert_eq!(out.len(), 1);
        match &out[0].op {
            Operator::PhysicalTopN(p) => {
                assert_eq!(p.limit, Some(50));
                assert_eq!(p.offset, Some(10));
            }
            other => panic!("expected PhysicalTopN, got {:?}", other),
        }
        assert_eq!(out[0].children, vec![dummy_child]);
    }
}

#[cfg(test)]
mod eq_pair_tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn cols(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_lowercase()).collect()
    }

    #[test]
    fn orient_natural_order_keeps_order() {
        let left = cols(&["a_id"]);
        let right = cols(&["b_id"]);
        let pair = (col("a_id"), col("b_id"));
        let out = orient_eq_pair(pair, &left, &right).expect("should orient");
        match &out.0.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "a_id"),
            _ => panic!("expected ColumnRef"),
        }
        match &out.1.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "b_id"),
            _ => panic!("expected ColumnRef"),
        }
    }

    #[test]
    fn orient_swapped_pair_returns_swapped() {
        let left = cols(&["a_id"]);
        let right = cols(&["b_id"]);
        let pair = (col("b_id"), col("a_id"));
        let out = orient_eq_pair(pair, &left, &right).expect("should orient");
        match &out.0.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "a_id"),
            _ => panic!("expected ColumnRef"),
        }
        match &out.1.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "b_id"),
            _ => panic!("expected ColumnRef"),
        }
    }

    #[test]
    fn orient_single_side_pair_returns_none() {
        let left = cols(&["a_id", "a_name"]);
        let right = cols(&["b_id"]);
        let pair = (col("a_id"), col("a_name"));
        assert!(orient_eq_pair(pair, &left, &right).is_none());
    }
}

#[cfg(test)]
mod join_demotion_tests {
    use super::*;
    use crate::sql::cascades::memo::{LogicalProperties, MExpr, Memo};
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::ir::OutputColumn;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    /// Create a scan group whose logical_props report the given output columns.
    fn mk_scan_group(memo: &mut Memo, col_names: &[&str]) -> usize {
        let output_columns: Vec<OutputColumn> = col_names
            .iter()
            .map(|name| OutputColumn {
                name: (*name).into(),
                data_type: DataType::Int32,
                nullable: false,
            })
            .collect();
        let scan_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".into(),
                table: TableDef {
                    name: "t".into(),
                    columns: vec![],
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: output_columns.clone(),
                predicates: vec![],
                required_columns: None,
            }),
            children: vec![],
        };
        let gid = memo.new_group(scan_mexpr);
        // Inject logical_props so get_group_column_names returns the column names.
        memo.groups[gid].logical_props = Some(LogicalProperties {
            output_columns,
            row_count: 100.0,
        });
        gid
    }

    /// Build `left op right` as a TypedExpr.
    fn bin(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
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

    /// The full demotion path: a same-side eq pair must land in other_condition
    /// while an orientable pair lands (correctly oriented) in eq_conditions.
    #[test]
    fn demoted_single_side_pair_ends_in_other_condition() {
        let mut memo = Memo::new();

        // Left side: columns [a_id, a_name].  Right side: column [b_id].
        let left_group = mk_scan_group(&mut memo, &["a_id", "a_name"]);
        let right_group = mk_scan_group(&mut memo, &["b_id"]);

        // Condition: (a_id = b_id) AND (a_id = a_name)
        //   • First pair  (a_id, b_id)  — orientable (a_id left, b_id right).
        //   • Second pair (a_id, a_name) — same-side (both left) → must demote.
        let first_eq = bin(col("a_id"), BinOp::Eq, col("b_id"));
        let second_eq = bin(col("a_id"), BinOp::Eq, col("a_name"));
        let condition = bin(first_eq, BinOp::And, second_eq);

        let join_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            children: vec![left_group, right_group],
        };

        let rule = JoinToHashJoin;
        let alternatives = rule.apply(&join_mexpr, &mut memo);

        // Expect two alternatives (Shuffle + Broadcast).
        assert!(
            !alternatives.is_empty(),
            "expected at least one alternative from JoinToHashJoin"
        );

        // Both alternatives must have the same eq_conditions / other_condition shape;
        // spot-check the first one.
        let alt = &alternatives[0];
        let Operator::PhysicalHashJoin(phys) = &alt.op else {
            panic!("expected PhysicalHashJoin, got {:?}", alt.op);
        };

        // ── eq_conditions: exactly one pair, (a_id, b_id) ──────────────────
        assert_eq!(
            phys.eq_conditions.len(),
            1,
            "expected 1 eq pair in eq_conditions, got {:?}",
            phys.eq_conditions
        );
        let (lhs, rhs) = &phys.eq_conditions[0];
        match &lhs.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(
                column, "a_id",
                "left side of eq_condition should be a_id"
            ),
            other => panic!("expected ColumnRef on left of eq pair, got {:?}", other),
        }
        match &rhs.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(
                column, "b_id",
                "right side of eq_condition should be b_id"
            ),
            other => panic!("expected ColumnRef on right of eq pair, got {:?}", other),
        }

        // ── other_condition: the demoted (a_id = a_name) pair ───────────────
        let other = phys
            .other_condition
            .as_ref()
            .expect("demoted same-side pair must appear in other_condition");
        match &other.kind {
            ExprKind::BinaryOp { left, op, right } => {
                assert!(
                    matches!(op, BinOp::Eq),
                    "demoted condition should be BinaryOp::Eq, got {:?}",
                    op
                );
                match (&left.kind, &right.kind) {
                    (
                        ExprKind::ColumnRef { column: l, .. },
                        ExprKind::ColumnRef { column: r, .. },
                    ) => {
                        assert!(
                            (l == "a_id" && r == "a_name") || (l == "a_name" && r == "a_id"),
                            "expected (a_id, a_name) in demoted eq, got ({}, {})",
                            l,
                            r
                        );
                    }
                    other => panic!(
                        "expected two ColumnRef nodes inside demoted eq, got {:?}",
                        other
                    ),
                }
            }
            other => panic!(
                "expected BinaryOp::Eq in other_condition, got {:?}",
                other
            ),
        }
    }
}
