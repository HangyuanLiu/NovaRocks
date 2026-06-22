//! Common-subexpression elimination (CSE v1): a post-CBO physical-tree pass that
//! detects repeated non-trivial scalar subexpressions within an operator's
//! expression set and materializes each as a Project output column computed once,
//! rewriting consumers to reference it by ColumnId.
//!
//! See docs/design/specs/2026-06-21-optimizer-cse-v1-design.md.

use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::common::OutputColumn;
use crate::sql::optimizer::operator::{Operator, ScalarProjectItem};
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
    match &node.op {
        Operator::PhysicalProject(_) => rewrite_project(node, scalars, factory),
        Operator::PhysicalFilter(_) => rewrite_filter(node, scalars, factory),
        _ => {}
    }
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

fn collect_column_refs(
    scalars: &ScalarArena,
    roots: &[ScalarId],
) -> Vec<(ColumnId, DataType, bool)> {
    let mut seen = HashSet::new();
    let mut refs = Vec::new();
    for &root in roots {
        collect_column_refs_inner(scalars, root, &mut seen, &mut refs);
    }
    refs
}

fn collect_column_refs_inner(
    scalars: &ScalarArena,
    id: ScalarId,
    seen: &mut HashSet<ColumnId>,
    refs: &mut Vec<(ColumnId, DataType, bool)>,
) {
    match scalars.node(id) {
        ScalarNode::ColumnRef(column_id) => {
            if seen.insert(*column_id) {
                refs.push((
                    *column_id,
                    scalars.data_type(id).clone(),
                    scalars.nullable(id),
                ));
            }
        }
        ScalarNode::BinaryOp { left, right, .. } => {
            collect_column_refs_inner(scalars, *left, seen, refs);
            collect_column_refs_inner(scalars, *right, seen, refs);
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => collect_column_refs_inner(scalars, *child, seen, refs),
        ScalarNode::FunctionCall { args, .. } => {
            for &arg in args {
                collect_column_refs_inner(scalars, arg, seen, refs);
            }
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            for &arg in args {
                collect_column_refs_inner(scalars, arg, seen, refs);
            }
            for key in order_by {
                collect_column_refs_inner(scalars, key.expr, seen, refs);
            }
        }
        ScalarNode::InList { child, list, .. } => {
            collect_column_refs_inner(scalars, *child, seen, refs);
            for &item in list {
                collect_column_refs_inner(scalars, item, seen, refs);
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            collect_column_refs_inner(scalars, *child, seen, refs);
            collect_column_refs_inner(scalars, *low, seen, refs);
            collect_column_refs_inner(scalars, *high, seen, refs);
        }
        ScalarNode::Like { child, pattern, .. } => {
            collect_column_refs_inner(scalars, *child, seen, refs);
            collect_column_refs_inner(scalars, *pattern, seen, refs);
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_column_refs_inner(scalars, *operand, seen, refs);
            }
            for (when, then) in when_then {
                collect_column_refs_inner(scalars, *when, seen, refs);
                collect_column_refs_inner(scalars, *then, seen, refs);
            }
            if let Some(else_expr) = else_expr {
                collect_column_refs_inner(scalars, *else_expr, seen, refs);
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for &arg in args {
                collect_column_refs_inner(scalars, arg, seen, refs);
            }
            for &partition in partition_by {
                collect_column_refs_inner(scalars, partition, seen, refs);
            }
            for key in order_by {
                collect_column_refs_inner(scalars, key.expr, seen, refs);
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            collect_column_refs_inner(scalars, *body, seen, refs);
        }
        ScalarNode::Literal(_) | ScalarNode::LambdaParamRef { .. } => {}
    }
}

fn substitute(
    scalars: &mut ScalarArena,
    id: ScalarId,
    subst: &HashMap<ScalarId, ScalarId>,
) -> ScalarId {
    if let Some(&replacement) = subst.get(&id) {
        return replacement;
    }

    let node = scalars.node(id).clone();
    let data_type = scalars.data_type(id).clone();
    let nullable = scalars.nullable(id);
    let rewritten = match node {
        ScalarNode::BinaryOp { op, left, right } => ScalarNode::BinaryOp {
            op,
            left: substitute(scalars, left, subst),
            right: substitute(scalars, right, subst),
        },
        ScalarNode::UnaryOp { op, child } => ScalarNode::UnaryOp {
            op,
            child: substitute(scalars, child, subst),
        },
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
        } => ScalarNode::FunctionCall {
            name,
            args: args
                .into_iter()
                .map(|arg| substitute(scalars, arg, subst))
                .collect(),
            distinct,
        },
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => ScalarNode::AggregateCall {
            name,
            args: args
                .into_iter()
                .map(|arg| substitute(scalars, arg, subst))
                .collect(),
            distinct,
            order_by: order_by
                .into_iter()
                .map(|mut key| {
                    key.expr = substitute(scalars, key.expr, subst);
                    key
                })
                .collect(),
        },
        ScalarNode::Cast { child, target } => ScalarNode::Cast {
            child: substitute(scalars, child, subst),
            target,
        },
        ScalarNode::IsNull { child, negated } => ScalarNode::IsNull {
            child: substitute(scalars, child, subst),
            negated,
        },
        ScalarNode::InList {
            child,
            list,
            negated,
        } => ScalarNode::InList {
            child: substitute(scalars, child, subst),
            list: list
                .into_iter()
                .map(|item| substitute(scalars, item, subst))
                .collect(),
            negated,
        },
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => ScalarNode::Between {
            child: substitute(scalars, child, subst),
            low: substitute(scalars, low, subst),
            high: substitute(scalars, high, subst),
            negated,
        },
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => ScalarNode::Like {
            child: substitute(scalars, child, subst),
            pattern: substitute(scalars, pattern, subst),
            negated,
        },
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => ScalarNode::Case {
            operand: operand.map(|operand| substitute(scalars, operand, subst)),
            when_then: when_then
                .into_iter()
                .map(|(when, then)| {
                    (
                        substitute(scalars, when, subst),
                        substitute(scalars, then, subst),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|else_expr| substitute(scalars, else_expr, subst)),
        },
        ScalarNode::IsTruthValue {
            child,
            value,
            negated,
        } => ScalarNode::IsTruthValue {
            child: substitute(scalars, child, subst),
            value,
            negated,
        },
        ScalarNode::Nested(child) => ScalarNode::Nested(substitute(scalars, child, subst)),
        ScalarNode::ColumnRef(_)
        | ScalarNode::Literal(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::WindowCall { .. }
        | ScalarNode::Lambda { .. }
        | ScalarNode::LambdaFunction { .. } => return id,
    };

    scalars.intern(rewritten, data_type, nullable)
}

fn build_commons(
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
    commons: &[ScalarId],
) -> (Vec<ScalarProjectItem>, HashMap<ScalarId, ScalarId>) {
    let mut items = Vec::with_capacity(commons.len());
    let mut subst = HashMap::new();

    for &common in commons {
        let expr = common;
        let data_type = scalars.data_type(common).clone();
        let nullable = scalars.nullable(common);
        let output_name = format!("__cse_{}", items.len());
        let output_column_id =
            factory.create(None, output_name.clone(), data_type.clone(), nullable);
        scalars.remember_project_output_display(output_column_id, None, output_name.clone());
        items.push(ScalarProjectItem {
            expr,
            output_name,
            output_column_id,
            expr_display: None,
        });

        let replacement =
            scalars.intern(ScalarNode::ColumnRef(output_column_id), data_type, nullable);
        subst.insert(common, replacement);
    }

    (items, subst)
}

fn output_column_for_project_item(scalars: &ScalarArena, item: &ScalarProjectItem) -> OutputColumn {
    OutputColumn {
        column_id: item.output_column_id,
        name: item.output_name.clone(),
        data_type: scalars.data_type(item.expr).clone(),
        nullable: scalars.nullable(item.expr),
        is_internal: true,
    }
}

fn prelude_binds_to_outputs(
    scalars: &ScalarArena,
    prelude: &[ScalarProjectItem],
    output_columns: &[OutputColumn],
) -> bool {
    let available = output_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<HashSet<_>>();
    let roots = prelude.iter().map(|item| item.expr).collect::<Vec<_>>();
    collect_column_refs(scalars, &roots)
        .into_iter()
        .all(|(column_id, _, _)| available.contains(&column_id))
}

fn wrap_project_around_child(
    child: &mut PhysicalPlanNode,
    prelude: Vec<ScalarProjectItem>,
    scalars: &mut ScalarArena,
) {
    let original = child.clone();
    let mut items = Vec::with_capacity(original.output_columns.len() + prelude.len());
    for column in &original.output_columns {
        let expr = scalars.intern(
            ScalarNode::ColumnRef(column.column_id),
            column.data_type.clone(),
            column.nullable,
        );
        items.push(ScalarProjectItem {
            expr,
            output_name: column.name.clone(),
            output_column_id: column.column_id,
            expr_display: None,
        });
    }
    items.extend(prelude.iter().cloned());

    let mut output_columns = original.output_columns.clone();
    output_columns.extend(
        prelude
            .iter()
            .map(|item| output_column_for_project_item(scalars, item)),
    );

    *child = PhysicalPlanNode {
        op: Operator::PhysicalProject(crate::sql::optimizer::operator::ProjectOp {
            items,
            output_qualifier: None,
        }),
        stats: original.stats.clone(),
        output_columns,
        execution_props: original.execution_props.clone(),
        children: vec![original],
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
    };
}

fn insert_or_reuse_project_below(
    child: &mut PhysicalPlanNode,
    prelude: Vec<ScalarProjectItem>,
    scalars: &mut ScalarArena,
) {
    if prelude.is_empty() {
        return;
    }

    let can_reuse_project = match &child.op {
        Operator::PhysicalProject(_) if child.children.len() == 1 => {
            prelude_binds_to_outputs(scalars, &prelude, &child.children[0].output_columns)
        }
        _ => false,
    };
    if can_reuse_project {
        if let Operator::PhysicalProject(project) = &mut child.op {
            child.output_columns.extend(
                prelude
                    .iter()
                    .map(|item| output_column_for_project_item(scalars, item)),
            );
            project.items.extend(prelude);
            return;
        }
    }

    wrap_project_around_child(child, prelude, scalars);
}

fn rewrite_project(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    let Operator::PhysicalProject(project) = &node.op else {
        return;
    };
    let roots = project
        .items
        .iter()
        .map(|item| item.expr)
        .collect::<Vec<_>>();
    let commons = pick_commons(scalars, &roots);
    if commons.is_empty() {
        return;
    }
    if node.children.len() != 1 {
        return;
    }

    let (prelude, subst) = build_commons(scalars, factory, &commons);
    let Operator::PhysicalProject(project) = &mut node.op else {
        unreachable!("checked project operator above");
    };
    for item in &mut project.items {
        item.expr = substitute(scalars, item.expr, &subst);
    }

    let input_refs = collect_column_refs(scalars, &roots);
    let child = node.children.remove(0);
    let mut child_project_items = input_refs
        .iter()
        .map(|&(column_id, ref data_type, nullable)| {
            let expr = scalars.intern(
                ScalarNode::ColumnRef(column_id),
                data_type.clone(),
                nullable,
            );
            let child_column = child
                .output_columns
                .iter()
                .find(|column| column.column_id == column_id);
            ScalarProjectItem {
                expr,
                output_name: child_column
                    .map(|column| column.name.clone())
                    .unwrap_or_else(|| column_id.to_string()),
                output_column_id: column_id,
                expr_display: None,
            }
        })
        .collect::<Vec<_>>();
    let mut child_project_output_columns = input_refs
        .iter()
        .map(|&(column_id, ref data_type, nullable)| {
            let child_column = child
                .output_columns
                .iter()
                .find(|column| column.column_id == column_id);
            OutputColumn {
                column_id,
                name: child_column
                    .map(|column| column.name.clone())
                    .unwrap_or_else(|| column_id.to_string()),
                data_type: child_column
                    .map(|column| column.data_type.clone())
                    .unwrap_or_else(|| data_type.clone()),
                nullable: child_column
                    .map(|column| column.nullable)
                    .unwrap_or(nullable),
                is_internal: child_column
                    .map(|column| column.is_internal)
                    .unwrap_or(false),
            }
        })
        .collect::<Vec<_>>();
    child_project_output_columns.extend(prelude.iter().map(|item| OutputColumn {
        column_id: item.output_column_id,
        name: item.output_name.clone(),
        data_type: scalars.data_type(item.expr).clone(),
        nullable: scalars.nullable(item.expr),
        is_internal: true,
    }));
    child_project_items.extend(prelude);

    let cse_project = PhysicalPlanNode {
        op: Operator::PhysicalProject(crate::sql::optimizer::operator::ProjectOp {
            items: child_project_items,
            output_qualifier: None,
        }),
        stats: child.stats.clone(),
        output_columns: child_project_output_columns,
        execution_props: child.execution_props.clone(),
        children: vec![child],
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
    };
    node.children.push(cse_project);
}

fn rewrite_filter(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    let Operator::PhysicalFilter(filter) = &node.op else {
        return;
    };
    let roots = [filter.predicate];
    let commons = pick_commons(scalars, &roots);
    if commons.is_empty() {
        return;
    }
    if node.children.len() != 1 {
        return;
    }

    let (prelude, subst) = build_commons(scalars, factory, &commons);
    let Operator::PhysicalFilter(filter) = &mut node.op else {
        unreachable!("checked filter operator above");
    };
    filter.predicate = substitute(scalars, filter.predicate, &subst);
    insert_or_reuse_project_below(&mut node.children[0], prelude, scalars);
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::sql::column_id::ColumnId;
    use crate::sql::common::OutputColumn;
    use crate::sql::common::{BinOp, LiteralValue};
    use crate::sql::optimizer::operator::{
        FilterOp, Operator, ProjectOp, ScalarProjectItem, ValuesOp,
    };
    use crate::sql::optimizer::physical_plan::{PhysicalPlanNode, PlanExecutionProps};
    use crate::sql::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};
    use crate::sql::optimizer::statistics::Statistics;

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

    fn gt(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Gt,
                left,
                right,
            },
            DataType::Boolean,
            true,
        )
    }

    fn lt(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Lt,
                left,
                right,
            },
            DataType::Boolean,
            true,
        )
    }

    fn and(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left,
                right,
            },
            DataType::Boolean,
            true,
        )
    }

    fn int_lit(arena: &mut ScalarArena, value: i64) -> ScalarId {
        arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
            DataType::Int64,
            false,
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

    fn project_item(expr: ScalarId, output_column_id: u32, output_name: &str) -> ScalarProjectItem {
        ScalarProjectItem {
            expr,
            output_name: output_name.to_string(),
            output_column_id: ColumnId(output_column_id),
            expr_display: None,
        }
    }

    fn output_column(column_id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(column_id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
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

    #[test]
    fn substitute_replaces_common_and_reinterns() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let a_plus_b = add(&mut arena, a, b);
        let cse_ref = arena.intern(ScalarNode::ColumnRef(ColumnId(99)), DataType::Int64, true);
        let mut subst = std::collections::HashMap::new();
        subst.insert(a_plus_b, cse_ref);

        let root = add(&mut arena, a_plus_b, a);
        let rewritten = super::substitute(&mut arena, root, &subst);

        match arena.node(rewritten) {
            ScalarNode::BinaryOp { left, right, .. } => {
                assert!(matches!(
                    arena.node(*left),
                    ScalarNode::ColumnRef(ColumnId(99))
                ));
                assert!(matches!(
                    arena.node(*right),
                    ScalarNode::ColumnRef(ColumnId(1))
                ));
            }
            other => panic!("unexpected node: {other:?}"),
        }
    }

    #[test]
    fn build_commons_keeps_prelude_items_independent() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let a_plus_b = add(&mut arena, a, b);
        let doubled = add(&mut arena, a_plus_b, a_plus_b);

        let (items, subst) = super::build_commons(&mut arena, &mut factory, &[a_plus_b, doubled]);

        assert_eq!(items.len(), 2);
        let first_cse = items[0].output_column_id;
        assert!(matches!(
            arena.node(items[1].expr),
            ScalarNode::BinaryOp { left, right, .. }
                if !matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == first_cse)
                    && !matches!(arena.node(*right), ScalarNode::ColumnRef(column_id) if *column_id == first_cse)
        ));
        assert!(matches!(
            arena.node(*subst.get(&doubled).expect("doubled replacement")),
            ScalarNode::ColumnRef(column_id) if *column_id == items[1].output_column_id
        ));
    }

    #[test]
    fn collect_column_refs_includes_lambda_captures() {
        let mut arena = ScalarArena::new();
        let captured = col(&mut arena, 1);
        let lambda_param = arena.intern(
            ScalarNode::LambdaParamRef {
                name: "x".to_string(),
                slot_id: 7,
            },
            DataType::Int64,
            true,
        );
        let body = add(&mut arena, lambda_param, captured);
        let lambda = arena.intern(
            ScalarNode::Lambda {
                params: vec!["x".to_string()],
                body,
            },
            DataType::Int64,
            true,
        );

        let refs = super::collect_column_refs(&arena, &[lambda]);

        assert_eq!(
            refs.into_iter()
                .map(|(column_id, _, _)| column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId(1)]
        );
    }

    #[test]
    fn rewrite_project_factors_repeated_subexpr() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_plus_b = add(&mut arena, a, b);
        let doubled = add(&mut arena, a_plus_b, a_plus_b);
        let child = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: 42.0,
                ..Statistics::default()
            },
            output_columns: vec![
                output_column(101, "a"),
                output_column(102, "b"),
                OutputColumn {
                    is_internal: true,
                    ..output_column(199, "__stale_internal")
                },
            ],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![
                    project_item(a_plus_b, 110, "x"),
                    project_item(doubled, 111, "y"),
                ],
                output_qualifier: None,
            }),
            children: vec![child],
            stats: Statistics {
                output_row_count: 7.0,
                ..Statistics::default()
            },
            output_columns: vec![output_column(110, "x"), output_column(111, "y")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(project) = &node.op else {
            panic!("expected physical project");
        };
        assert_eq!(project.items.len(), 2);
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y"]
        );
        let Operator::PhysicalProject(cse_project) = &node.children[0].op else {
            panic!("expected inserted CSE project");
        };
        assert_eq!(cse_project.items[2].output_name, "__cse_0");
        let common_col = cse_project.items[2].output_column_id;
        assert!(matches!(
            arena.node(cse_project.items[2].expr),
            ScalarNode::BinaryOp { .. }
        ));
        assert!(matches!(
            arena.node(project.items[0].expr),
            ScalarNode::ColumnRef(column_id) if *column_id == common_col
        ));
        assert!(matches!(
            arena.node(project.items[1].expr),
            ScalarNode::BinaryOp { left, right, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == common_col)
                    && matches!(arena.node(*right), ScalarNode::ColumnRef(column_id) if *column_id == common_col)
        ));
        assert_eq!(
            node.output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y"],
            "Project node output_columns remains the visible result contract"
        );
        assert_eq!(
            node.children[0]
                .output_columns
                .iter()
                .map(|column| (column.name.as_str(), column.is_internal))
                .collect::<Vec<_>>(),
            vec![("a", false), ("b", false), ("__cse_0", true)]
        );
        assert_eq!(node.children[0].stats.output_row_count, 42.0);
    }

    #[test]
    fn rewrite_project_preserves_lambda_capture_input() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let c = col(&mut arena, 103);
        let b_plus_c = add(&mut arena, b, c);
        let doubled = add(&mut arena, b_plus_c, b_plus_c);
        let lambda_param = arena.intern(
            ScalarNode::LambdaParamRef {
                name: "x".to_string(),
                slot_id: 7,
            },
            DataType::Int64,
            true,
        );
        let lambda_body = add(&mut arena, lambda_param, a);
        let lambda = arena.intern(
            ScalarNode::Lambda {
                params: vec!["x".to_string()],
                body: lambda_body,
            },
            DataType::Int64,
            true,
        );
        let child = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![
                    output_column(101, "a"),
                    output_column(102, "b"),
                    output_column(103, "c"),
                ],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![
                output_column(101, "a"),
                output_column(102, "b"),
                output_column(103, "c"),
            ],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![
                    project_item(b_plus_c, 110, "x"),
                    project_item(doubled, 111, "y"),
                    project_item(lambda, 112, "lambda_capture"),
                ],
                output_qualifier: None,
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![
                output_column(110, "x"),
                output_column(111, "y"),
                output_column(112, "lambda_capture"),
            ],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        assert_eq!(
            node.children[0]
                .output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c", "a", "__cse_0"]
        );
    }

    #[test]
    fn insert_or_reuse_project_below_wraps_non_project_child() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_plus_b = add(&mut arena, a, b);
        let (prelude, _) = super::build_commons(&mut arena, &mut factory, &[a_plus_b]);
        let child = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: 42.0,
                ..Statistics::default()
            },
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut parent = PhysicalPlanNode {
            op: Operator::PhysicalFilter(FilterOp {
                predicate: gt(&mut arena, a, b),
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::insert_or_reuse_project_below(&mut parent.children[0], prelude, &mut arena);

        let Operator::PhysicalProject(project) = &parent.children[0].op else {
            panic!("expected inserted physical project");
        };
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "__cse_0"]
        );
        assert_eq!(
            parent.children[0]
                .output_columns
                .iter()
                .map(|column| (column.name.as_str(), column.is_internal))
                .collect::<Vec<_>>(),
            vec![("a", false), ("b", false), ("__cse_0", true)]
        );
        assert_eq!(parent.children[0].children.len(), 1);
        assert_eq!(parent.children[0].stats.output_row_count, 42.0);
    }

    #[test]
    fn insert_or_reuse_project_below_wraps_project_when_producer_refs_project_outputs() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let x = col(&mut arena, 201);
        let y = col(&mut arena, 202);
        let x_plus_y = add(&mut arena, x, y);
        let (prelude, _) = super::build_commons(&mut arena, &mut factory, &[x_plus_y]);
        let values = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut child_project = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![project_item(a, 201, "x"), project_item(b, 202, "y")],
                output_qualifier: None,
            }),
            children: vec![values],
            stats: Statistics::default(),
            output_columns: vec![output_column(201, "x"), output_column(202, "y")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::insert_or_reuse_project_below(&mut child_project, prelude, &mut arena);

        let Operator::PhysicalProject(outer_project) = &child_project.op else {
            panic!("expected outer wrapper project");
        };
        assert_eq!(
            outer_project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y", "__cse_0"]
        );
        assert_eq!(
            child_project
                .output_columns
                .iter()
                .map(|column| (column.name.as_str(), column.is_internal))
                .collect::<Vec<_>>(),
            vec![("x", false), ("y", false), ("__cse_0", true)]
        );
        let Operator::PhysicalProject(inner_project) = &child_project.children[0].op else {
            panic!("expected original inner project");
        };
        assert_eq!(
            inner_project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y"]
        );
    }

    #[test]
    fn insert_or_reuse_project_below_reuses_passthrough_project_when_producer_refs_input() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_plus_b = add(&mut arena, a, b);
        let (prelude, _) = super::build_commons(&mut arena, &mut factory, &[a_plus_b]);
        let values = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut child_project = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![project_item(a, 101, "a"), project_item(b, 102, "b")],
                output_qualifier: None,
            }),
            children: vec![values],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::insert_or_reuse_project_below(&mut child_project, prelude, &mut arena);

        let Operator::PhysicalProject(project) = &child_project.op else {
            panic!("expected reused physical project");
        };
        assert!(matches!(
            child_project.children[0].op,
            Operator::PhysicalValues(_)
        ));
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "__cse_0"]
        );
        assert_eq!(
            child_project
                .output_columns
                .iter()
                .map(|column| (column.name.as_str(), column.is_internal))
                .collect::<Vec<_>>(),
            vec![("a", false), ("b", false), ("__cse_0", true)]
        );
        let cse_expr = project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("CSE project item")
            .expr;
        assert!(matches!(
            arena.node(cse_expr),
            ScalarNode::BinaryOp { left, right, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(ColumnId(101)))
                    && matches!(arena.node(*right), ScalarNode::ColumnRef(ColumnId(102)))
        ));
    }

    #[test]
    fn rewrite_filter_factors_repeated_predicate_subexpr() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_plus_b = add(&mut arena, a, b);
        let ten = int_lit(&mut arena, 10);
        let twenty = int_lit(&mut arena, 20);
        let lower = gt(&mut arena, a_plus_b, ten);
        let upper = lt(&mut arena, a_plus_b, twenty);
        let predicate = and(&mut arena, lower, upper);
        let child = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalFilter(FilterOp { predicate }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(cse_project) = &node.children[0].op else {
            panic!("expected inserted CSE project");
        };
        let cse_item = cse_project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("CSE project item");
        let cse_column = cse_item.output_column_id;
        assert!(
            node.children[0]
                .output_columns
                .iter()
                .any(|column| column.column_id == cse_column
                    && column.name == "__cse_0"
                    && column.is_internal)
        );
        let Operator::PhysicalFilter(filter) = &node.op else {
            panic!("expected physical filter");
        };
        let ScalarNode::BinaryOp { left, right, .. } = arena.node(filter.predicate) else {
            panic!("expected conjunction");
        };
        assert!(matches!(
            arena.node(*left),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
        assert!(matches!(
            arena.node(*right),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
        assert_eq!(
            node.output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn rewrite_filter_wraps_existing_project_when_predicate_refs_project_outputs() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let x = col(&mut arena, 201);
        let y = col(&mut arena, 202);
        let x_plus_y = add(&mut arena, x, y);
        let ten = int_lit(&mut arena, 10);
        let twenty = int_lit(&mut arena, 20);
        let lower = gt(&mut arena, x_plus_y, ten);
        let upper = lt(&mut arena, x_plus_y, twenty);
        let predicate = and(&mut arena, lower, upper);
        let values = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let project = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![project_item(a, 201, "x"), project_item(b, 202, "y")],
                output_qualifier: None,
            }),
            children: vec![values],
            stats: Statistics::default(),
            output_columns: vec![output_column(201, "x"), output_column(202, "y")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalFilter(FilterOp { predicate }),
            children: vec![project],
            stats: Statistics::default(),
            output_columns: vec![output_column(201, "x"), output_column(202, "y")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(outer_project) = &node.children[0].op else {
            panic!("expected outer CSE project");
        };
        assert_eq!(
            outer_project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y", "__cse_0"]
        );
        let cse_item = outer_project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("CSE project item");
        let cse_column = cse_item.output_column_id;
        assert_eq!(
            node.children[0]
                .output_columns
                .iter()
                .map(|column| (column.name.as_str(), column.is_internal))
                .collect::<Vec<_>>(),
            vec![("x", false), ("y", false), ("__cse_0", true)]
        );
        let Operator::PhysicalProject(inner_project) = &node.children[0].children[0].op else {
            panic!("expected original inner project");
        };
        assert_eq!(
            inner_project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y"]
        );
        let Operator::PhysicalFilter(filter) = &node.op else {
            panic!("expected physical filter");
        };
        let ScalarNode::BinaryOp { left, right, .. } = arena.node(filter.predicate) else {
            panic!("expected conjunction");
        };
        assert!(matches!(
            arena.node(*left),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
        assert!(matches!(
            arena.node(*right),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
        assert_eq!(
            node.output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y"]
        );
    }
}
