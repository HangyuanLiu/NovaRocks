//! Common-subexpression elimination (CSE v1): a post-CBO physical-tree pass that
//! detects repeated non-trivial scalar subexpressions within an operator's
//! expression set and materializes each as a Project output column computed once,
//! rewriting consumers to reference it by ColumnId.
//!
//! See docs/design/specs/2026-06-21-optimizer-cse-v1-design.md.

use std::collections::HashMap;

use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::options::OptimizerOptions;
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::scalar_expr;

/// Stable rule name for `SET disable_optimizer_rules`.
pub(crate) const CSE_RULE: &str = "CommonSubexpressionReuse";

/// Entry point: rewrite the physical tree in place. Gated by `CSE_RULE`.
pub(crate) fn rewrite(
    root: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
    options: &OptimizerOptions,
) {
    if !options.is_enabled(CSE_RULE) {
        return;
    }
    rewrite_node(root, scalars, factory);
}

/// Post-order walk. Per-operator drivers are added in later tasks.
fn rewrite_node(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    for child in &mut node.children {
        rewrite_node(child, scalars, factory);
    }
    // Per-operator rewrite dispatch added in Tasks 3-6.
    let _ = (scalars, factory, &node.op);
}

fn child_ids(scalars: &ScalarArena, id: ScalarId) -> Vec<ScalarId> {
    match scalars.node(id) {
        ScalarNode::BinaryOp { left, right, .. } => vec![*left, *right],
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => vec![*child],
        ScalarNode::FunctionCall { args, .. } => args.clone(),
        ScalarNode::AggregateCall { args, order_by, .. } => {
            let mut children = Vec::with_capacity(args.len() + order_by.len());
            children.extend(args.iter().copied());
            children.extend(order_by.iter().map(|key| key.expr));
            children
        }
        ScalarNode::InList { child, list, .. } => {
            let mut children = Vec::with_capacity(1 + list.len());
            children.push(*child);
            children.extend(list.iter().copied());
            children
        }
        ScalarNode::Between {
            child, low, high, ..
        } => vec![*child, *low, *high],
        ScalarNode::Like { child, pattern, .. } => vec![*child, *pattern],
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut children = Vec::with_capacity(operand.iter().count() + when_then.len() * 2 + 1);
            children.extend(operand.iter().copied());
            for (when, then) in when_then {
                children.push(*when);
                children.push(*then);
            }
            children.extend(else_expr.iter().copied());
            children
        }
        ScalarNode::ColumnRef(_)
        | ScalarNode::Literal(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::WindowCall { .. }
        | ScalarNode::Lambda { .. }
        | ScalarNode::LambdaFunction { .. } => Vec::new(),
    }
}

fn count_subexprs(scalars: &ScalarArena, roots: &[ScalarId]) -> HashMap<ScalarId, usize> {
    let mut counts = HashMap::new();
    for &root in roots {
        count_subexprs_inner(scalars, root, &mut counts);
    }
    counts
}

fn count_subexprs_inner(
    scalars: &ScalarArena,
    id: ScalarId,
    counts: &mut HashMap<ScalarId, usize>,
) {
    *counts.entry(id).or_default() += 1;
    for child in child_ids(scalars, id) {
        count_subexprs_inner(scalars, child, counts);
    }
}

fn subtree_size(scalars: &ScalarArena, id: ScalarId) -> usize {
    1 + child_ids(scalars, id)
        .into_iter()
        .map(|child| subtree_size(scalars, child))
        .sum::<usize>()
}

fn eligible(scalars: &ScalarArena, id: ScalarId) -> bool {
    match scalars.node(id) {
        ScalarNode::ColumnRef(_)
        | ScalarNode::Literal(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::WindowCall { .. }
        | ScalarNode::Lambda { .. }
        | ScalarNode::LambdaFunction { .. } => false,
        ScalarNode::Cast { child, .. } => {
            !matches!(scalars.node(*child), ScalarNode::ColumnRef(_))
                && !scalar_expr::contains_non_deterministic_function(scalars, id)
        }
        _ => !scalar_expr::contains_non_deterministic_function(scalars, id),
    }
}

fn first_seen_order(scalars: &ScalarArena, roots: &[ScalarId]) -> HashMap<ScalarId, usize> {
    let mut order = HashMap::new();
    let mut next = 0;
    for &root in roots {
        record_first_seen(scalars, root, &mut order, &mut next);
    }
    order
}

fn pick_commons(scalars: &ScalarArena, roots: &[ScalarId]) -> Vec<ScalarId> {
    let counts = count_subexprs(scalars, roots);
    let first_seen = first_seen_order(scalars, roots);
    let mut candidates = counts
        .into_iter()
        .filter_map(|(id, count)| {
            if count >= 2 && eligible(scalars, id) {
                Some(id)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|&id| {
        (
            subtree_size(scalars, id),
            first_seen.get(&id).copied().unwrap_or(usize::MAX),
        )
    });
    candidates
}

fn record_first_seen(
    scalars: &ScalarArena,
    id: ScalarId,
    order: &mut HashMap<ScalarId, usize>,
    next: &mut usize,
) {
    if order.contains_key(&id) {
        return;
    }
    order.insert(id, *next);
    *next += 1;
    for child in child_ids(scalars, id) {
        record_first_seen(scalars, child, order, next);
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::sql::column_id::ColumnId;
    use crate::sql::common::BinOp;
    use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

    use super::pick_commons;

    fn col(arena: &mut ScalarArena, id: u32) -> ScalarId {
        arena.intern(ScalarNode::ColumnRef(ColumnId(id)), DataType::Int64, true)
    }

    fn add(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Add,
                left,
                right,
            },
            DataType::Int64,
            true,
        )
    }

    fn call(arena: &mut ScalarArena, name: &str, args: Vec<ScalarId>) -> ScalarId {
        arena.intern(
            ScalarNode::FunctionCall {
                name: name.to_string(),
                args,
                distinct: false,
            },
            DataType::Int64,
            true,
        )
    }

    fn cast(arena: &mut ScalarArena, child: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::Cast {
                child,
                target: DataType::Int64,
            },
            DataType::Int64,
            true,
        )
    }

    #[test]
    fn repeated_binary_op_is_common_candidate() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let a_plus_b = add(&mut arena, a, b);
        let root = add(&mut arena, a_plus_b, a);

        assert_eq!(pick_commons(&arena, &[a_plus_b, root]), vec![a_plus_b]);
    }

    #[test]
    fn repeated_columns_are_not_common_candidates() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let root = add(&mut arena, a, b);

        assert_eq!(pick_commons(&arena, &[a, root]), Vec::<ScalarId>::new());
    }

    #[test]
    fn volatile_functions_are_not_common_candidates() {
        let mut arena = ScalarArena::new();
        let rand = arena.intern(
            ScalarNode::FunctionCall {
                name: "rand".to_string(),
                args: vec![],
                distinct: false,
            },
            DataType::Float64,
            false,
        );

        assert_eq!(pick_commons(&arena, &[rand, rand]), Vec::<ScalarId>::new());
    }

    #[test]
    fn repeated_current_timestamp_is_not_common_candidate() {
        let mut arena = ScalarArena::new();
        let current_timestamp = call(&mut arena, "current_timestamp", vec![]);

        assert_eq!(
            pick_commons(&arena, &[current_timestamp, current_timestamp]),
            Vec::<ScalarId>::new()
        );
    }

    #[test]
    fn nested_non_deterministic_expression_is_not_common_candidate() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let rand = call(&mut arena, "rand", vec![]);
        let rand_plus_a = add(&mut arena, rand, a);

        assert_eq!(
            pick_commons(&arena, &[rand_plus_a, rand_plus_a]),
            Vec::<ScalarId>::new()
        );
    }

    #[test]
    fn repeated_cast_column_ref_is_not_common_candidate() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let cast_a = cast(&mut arena, a);

        assert_eq!(
            pick_commons(&arena, &[cast_a, cast_a]),
            Vec::<ScalarId>::new()
        );
    }

    #[test]
    fn equal_size_candidates_follow_first_seen_order() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let c = col(&mut arena, 3);
        let d = col(&mut arena, 4);
        let c_plus_d = add(&mut arena, c, d);
        let a_plus_b = add(&mut arena, a, b);

        assert_eq!(
            pick_commons(&arena, &[c_plus_d, a_plus_b, c_plus_d, a_plus_b]),
            vec![c_plus_d, a_plus_b]
        );
    }
}
