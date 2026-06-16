use std::collections::HashSet;

use crate::sql::analysis::JoinKind;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::deriver::derive_inner_join_predicates;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
    PredicateGroup, PredicateKey, PredicateOrigin, predicate_key, split_and_refs,
};
use crate::sql::optimizer::rewrite::rules::utils::{collect_output_ids, combine_and};
use crate::sql::planner::plan::*;

pub(crate) struct JoinPredicateMoveAround;

impl PlanRewriteRule for JoinPredicateMoveAround {
    fn name(&self) -> &'static str {
        "JoinPredicateMoveAround"
    }

    fn matches(&self, plan: &LogicalPlanNode) -> bool {
        matches!(
            &plan.kind,
            LogicalPlanNodeKind::Join(join)
                if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross)
                    && join.condition.is_some()
        )
    }

    fn apply(&self, plan: LogicalPlanNode) -> Option<LogicalPlanNode> {
        let LogicalPlanNode {
            kind,
            mut children,
            required_output_columns,
        } = plan;
        let LogicalPlanNodeKind::Join(join) = kind else {
            return None;
        };
        if children.len() != 2 {
            return None;
        }
        let right = children.remove(1);
        let left = children.remove(0);
        if !matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
            return None;
        }

        let condition = join.condition.clone()?;
        let left_ids = collect_output_ids(&left);
        let right_ids = collect_output_ids(&right);
        let join_groups = PredicateGroup::from_predicate(condition, PredicateOrigin::JoinCondition);
        let mut child_groups = Vec::new();
        collect_child_predicate_groups(&left, &mut child_groups);
        collect_child_predicate_groups(&right, &mut child_groups);

        let derived =
            derive_inner_join_predicates(&left_ids, &right_ids, &join_groups, &child_groups);
        let left_existing = existing_child_predicate_keys(&left);
        let right_existing = existing_child_predicate_keys(&right);
        let mut left_fresh = Vec::new();
        let mut right_fresh = Vec::new();

        for group in derived {
            match classify_group_side(&group, &left_ids, &right_ids) {
                Some(ChildSide::Left) if !left_existing.contains(&group.key) => {
                    left_fresh.push(group.expr);
                }
                Some(ChildSide::Right) if !right_existing.contains(&group.key) => {
                    right_fresh.push(group.expr);
                }
                _ => {}
            }
        }

        if left_fresh.is_empty() && right_fresh.is_empty() {
            return None;
        }

        let new_left = if left_fresh.is_empty() {
            left
        } else {
            LogicalPlanNode::new(
                LogicalPlanNodeKind::Filter(LogicalFilterNode {
                    predicate: combine_and(left_fresh),
                }),
                vec![left],
                None,
            )
        };
        let new_right = if right_fresh.is_empty() {
            right
        } else {
            LogicalPlanNode::new(
                LogicalPlanNodeKind::Filter(LogicalFilterNode {
                    predicate: combine_and(right_fresh),
                }),
                vec![right],
                None,
            )
        };

        Some(LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: join.join_type,
                condition: join.condition,
            }),
            vec![new_left, new_right],
            required_output_columns,
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChildSide {
    Left,
    Right,
}

fn classify_group_side(
    group: &PredicateGroup,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<ChildSide> {
    if group.referenced_ids.is_empty() {
        return None;
    }

    let all_left = group.referenced_ids.iter().all(|id| left_ids.contains(id));
    let all_right = group.referenced_ids.iter().all(|id| right_ids.contains(id));
    match (all_left, all_right) {
        (true, false) => Some(ChildSide::Left),
        (false, true) => Some(ChildSide::Right),
        _ => None,
    }
}

fn collect_child_predicate_groups(plan: &LogicalPlanNode, out: &mut Vec<PredicateGroup>) {
    match &plan.kind {
        LogicalPlanNodeKind::Filter(filter) => {
            out.extend(PredicateGroup::from_predicate(
                filter.predicate.clone(),
                PredicateOrigin::Filter,
            ));
            collect_child_predicate_groups(plan.unary_input(), out);
        }
        LogicalPlanNodeKind::Scan(scan) => {
            for predicate in &scan.predicates {
                out.extend(PredicateGroup::from_predicate(
                    predicate.clone(),
                    PredicateOrigin::Filter,
                ));
            }
        }
        LogicalPlanNodeKind::Project(_)
        | LogicalPlanNodeKind::Sort(_)
        | LogicalPlanNodeKind::Limit(_) => collect_child_predicate_groups(plan.unary_input(), out),
        LogicalPlanNodeKind::Join(join)
            if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) =>
        {
            if let Some(condition) = &join.condition {
                out.extend(PredicateGroup::from_predicate(
                    condition.clone(),
                    PredicateOrigin::JoinCondition,
                ));
            }
            collect_child_predicate_groups(plan.left(), out);
            collect_child_predicate_groups(plan.right(), out);
        }
        _ => {}
    }
}

fn existing_child_predicate_keys(plan: &LogicalPlanNode) -> HashSet<PredicateKey> {
    let mut keys = HashSet::new();
    collect_existing_child_predicate_keys(plan, &mut keys);
    keys
}

fn collect_existing_child_predicate_keys(plan: &LogicalPlanNode, out: &mut HashSet<PredicateKey>) {
    match &plan.kind {
        LogicalPlanNodeKind::Filter(filter) => {
            collect_top_level_conjunct_keys(&filter.predicate, out);
            collect_existing_child_predicate_keys(plan.unary_input(), out);
        }
        LogicalPlanNodeKind::Scan(scan) => {
            for predicate in &scan.predicates {
                collect_top_level_conjunct_keys(predicate, out);
            }
        }
        LogicalPlanNodeKind::Project(_)
        | LogicalPlanNodeKind::Sort(_)
        | LogicalPlanNodeKind::Limit(_) => {
            collect_existing_child_predicate_keys(plan.unary_input(), out)
        }
        LogicalPlanNodeKind::Join(join)
            if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) =>
        {
            if let Some(condition) = &join.condition {
                collect_top_level_conjunct_keys(condition, out);
            }
            collect_existing_child_predicate_keys(plan.left(), out);
            collect_existing_child_predicate_keys(plan.right(), out);
        }
        _ => {}
    }
}

fn collect_top_level_conjunct_keys(
    expr: &crate::sql::analysis::TypedExpr,
    out: &mut HashSet<PredicateKey>,
) {
    for conjunct in split_and_refs(expr) {
        out.insert(predicate_key(conjunct));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::rule::PlanRewriteRule;
    use crate::sql::planner::plan::*;
    use arrow::datatypes::DataType;

    fn scan(alias: &str, cols: &[(&str, u32)]) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: alias.to_string(),
                    columns: cols
                        .iter()
                        .map(|(name, _)| ColumnDef {
                            name: name.to_string(),
                            data_type: DataType::Int32,
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
                        data_type: DataType::Int32,
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

    fn col(alias: &str, name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(alias.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: true,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn and(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn assert_filter_eq_literal(
        plan: &LogicalPlanNode,
        alias: &str,
        column: &str,
        id: u32,
        value: i64,
    ) {
        let LogicalPlanNodeKind::Filter(filter) = &plan.kind else {
            panic!("expected Filter");
        };
        let ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = &filter.predicate.kind
        else {
            panic!("expected equality predicate, got {:?}", filter.predicate);
        };
        let ExprKind::ColumnRef {
            column_id,
            qualifier,
            column: actual_column,
        } = &left.kind
        else {
            panic!("expected left column ref, got {left:?}");
        };
        assert_eq!(*column_id, ColumnId::new_for_test(id));
        assert_eq!(qualifier.as_deref(), Some(alias));
        assert_eq!(actual_column, column);
        assert!(matches!(
            &right.kind,
            ExprKind::Literal(LiteralValue::Int(actual)) if *actual == value
        ));
    }

    #[test]
    fn derives_opposite_side_filter_from_child_filter_and_join_equality() {
        let left_filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: eq(col("l", "a", 1), int_lit(5)),
            }),
            vec![scan("l", &[("a", 1)])],
            None,
        );
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(eq(col("l", "a", 1), col("r", "b", 2))),
            }),
            vec![left_filter, scan("r", &[("b", 2)])],
            None,
        );

        let out = JoinPredicateMoveAround
            .apply(plan)
            .expect("move-around should derive right filter");
        let LogicalPlanNodeKind::Join(join) = &out.kind else {
            panic!("expected Join");
        };
        assert!(matches!(&out.right().kind, LogicalPlanNodeKind::Filter(_)));
    }

    #[test]
    fn skips_left_outer_nullable_side_derivation() {
        let left_filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: eq(col("l", "a", 1), int_lit(5)),
            }),
            vec![scan("l", &[("a", 1)])],
            None,
        );
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::LeftOuter,
                condition: Some(eq(col("l", "a", 1), col("r", "b", 2))),
            }),
            vec![left_filter, scan("r", &[("b", 2)])],
            None,
        );

        assert!(JoinPredicateMoveAround.apply(plan).is_none());
    }

    #[test]
    fn skips_when_derived_filter_already_exists_on_child() {
        let left_filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: eq(col("l", "a", 1), int_lit(5)),
            }),
            vec![scan("l", &[("a", 1)])],
            None,
        );
        let right_filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: eq(col("r", "b", 2), int_lit(5)),
            }),
            vec![scan("r", &[("b", 2)])],
            None,
        );
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(eq(col("l", "a", 1), col("r", "b", 2))),
            }),
            vec![left_filter, right_filter],
            None,
        );

        assert!(JoinPredicateMoveAround.apply(plan).is_none());
    }

    #[test]
    fn skips_when_derived_filter_exists_as_top_level_and_conjunct() {
        let left_filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: eq(col("l", "a", 1), int_lit(5)),
            }),
            vec![scan("l", &[("a", 1)])],
            None,
        );
        let right_filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: and(
                    eq(col("r", "b", 2), int_lit(5)),
                    eq(col("r", "c", 3), int_lit(9)),
                ),
            }),
            vec![scan("r", &[("b", 2), ("c", 3)])],
            None,
        );
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(eq(col("l", "a", 1), col("r", "b", 2))),
            }),
            vec![left_filter, right_filter],
            None,
        );

        assert!(JoinPredicateMoveAround.apply(plan).is_none());
    }

    #[test]
    fn derives_from_nested_inner_join_child_filter() {
        let b_filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: eq(col("b", "k", 2), int_lit(7)),
            }),
            vec![scan("b", &[("k", 2)])],
            None,
        );
        let left_child = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(eq(col("a", "k", 1), col("b", "k", 2))),
            }),
            vec![scan("a", &[("k", 1)]), b_filter],
            None,
        );
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(eq(col("b", "k", 2), col("c", "k", 3))),
            }),
            vec![left_child, scan("c", &[("k", 3)])],
            None,
        );

        let out = JoinPredicateMoveAround
            .apply(plan)
            .expect("move-around should derive filter for the parent join sibling");
        let LogicalPlanNodeKind::Join(join) = &out.kind else {
            panic!("expected Join");
        };
        assert_filter_eq_literal(out.right(), "c", "k", 3, 7);
    }

    #[test]
    fn outer_child_join_condition_does_not_hide_fresh_parent_filter() {
        let left_child = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::LeftOuter,
                condition: Some(and(
                    eq(col("a", "k", 1), col("b", "k", 2)),
                    eq(col("b", "k", 2), int_lit(7)),
                )),
            }),
            vec![scan("a", &[("k", 1)]), scan("b", &[("k", 2)])],
            None,
        );
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(and(
                    eq(col("b", "k", 2), col("c", "k", 3)),
                    eq(col("c", "k", 3), int_lit(7)),
                )),
            }),
            vec![left_child, scan("c", &[("k", 3)])],
            None,
        );

        let out = JoinPredicateMoveAround
            .apply(plan)
            .expect("outer child ON condition must not suppress a fresh parent filter");
        let LogicalPlanNodeKind::Join(join) = &out.kind else {
            panic!("expected Join");
        };
        assert_filter_eq_literal(out.left(), "b", "k", 2, 7);
    }
}
