use std::collections::HashSet;

use crate::sql::optimizer::binder::Binding;
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalAggregateOp, Operator};
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::optimizer::scalar::{ScalarArena, ScalarNode, SortKey};

pub(crate) struct PushDownTopNToPreAgg;

impl Rule for PushDownTopNToPreAgg {
    fn name(&self) -> &str {
        "PushDownTopNToPreAgg"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, _expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        Vec::new()
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::TopN,
            children: vec![Pattern::Op {
                kind: OpKind::Aggregate,
                children: vec![Pattern::Op {
                    kind: OpKind::Aggregate,
                    children: vec![Pattern::Leaf],
                }],
            }],
        }
    }

    fn apply_bound(&self, _binding: &Binding, _memo: &mut Memo) -> Vec<NewExpr> {
        Vec::new()
    }
}

#[allow(dead_code)]
fn order_by_subset_of_group_by(
    items: &[SortKey],
    global: &LogicalAggregateOp,
    arena: &ScalarArena,
) -> bool {
    if items.is_empty() {
        return false;
    }

    let group_key_output_ids: HashSet<_> = global
        .output_columns
        .iter()
        .take(global.group_by.len())
        .map(|column| column.column_id)
        .collect();

    items.iter().all(|item| match arena.node(item.expr) {
        ScalarNode::ColumnRef(column_id) => group_key_output_ids.contains(column_id),
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::OutputColumn;
    use crate::sql::optimizer::operator::{AggStage, LogicalAggregateOp};
    use crate::sql::optimizer::scalar::{ScalarArena, ScalarNode, SortKey};
    use arrow::datatypes::DataType;

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_ref(arena: &mut ScalarArena, id: u32) -> crate::sql::optimizer::scalar::ScalarId {
        arena.intern(
            ScalarNode::ColumnRef(ColumnId::new_for_test(id)),
            DataType::Int64,
            false,
        )
    }

    fn sort_key(arena: &mut ScalarArena, id: u32) -> SortKey {
        SortKey {
            expr: col_ref(arena, id),
            asc: true,
            nulls_first: true,
            display: None,
        }
    }

    fn global_agg(arena: &mut ScalarArena) -> LogicalAggregateOp {
        LogicalAggregateOp::staged(
            AggStage::Global,
            vec![col_ref(arena, 1), col_ref(arena, 2)],
            vec![],
            vec![
                output_column(101, "k1"),
                output_column(102, "k2"),
                output_column(201, "sum_v"),
            ],
            vec![],
            true,
        )
    }

    #[test]
    fn order_by_group_key_is_subset() {
        let mut arena = ScalarArena::new();
        let global = global_agg(&mut arena);
        let items = vec![sort_key(&mut arena, 101), sort_key(&mut arena, 102)];

        assert!(order_by_subset_of_group_by(&items, &global, &arena));
    }

    #[test]
    fn order_by_aggregate_output_is_not_subset() {
        let mut arena = ScalarArena::new();
        let global = global_agg(&mut arena);
        let items = vec![sort_key(&mut arena, 201)];

        assert!(!order_by_subset_of_group_by(&items, &global, &arena));
    }
}
