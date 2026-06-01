use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{AggStage, LogicalAggregateOp, Operator};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::planner::plan::AggregateCall;

pub(crate) struct SplitAggregateRule;

impl Rule for SplitAggregateRule {
    fn name(&self) -> &str {
        "SplitAggregateRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregate(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAggregate(agg) = &expr.op else {
            return Vec::new();
        };
        if !is_eligible(agg) {
            return Vec::new();
        }

        let local = LogicalAggregateOp::staged(
            AggStage::Local,
            agg.group_by.clone(),
            agg.aggregates.clone(),
            agg.output_columns.clone(),
            vec![false; agg.aggregates.len()],
            true,
        );
        let local_id = memo.next_expr_id();
        let local_group = memo.new_group(MExpr {
            id: local_id,
            op: Operator::LogicalAggregate(local),
            children: expr.children.clone(),
        });
        let global_group_by = aggregate_group_key_output_ref(&agg.group_by, &agg.output_columns);
        let global = LogicalAggregateOp::staged(
            AggStage::Global,
            global_group_by,
            agg.aggregates.clone(),
            agg.output_columns.clone(),
            vec![true; agg.aggregates.len()],
            true,
        );

        vec![NewExpr {
            op: Operator::LogicalAggregate(global),
            children: vec![local_group],
        }]
    }
}

fn is_eligible(agg: &LogicalAggregateOp) -> bool {
    agg.stage == AggStage::Single
        && !agg.is_split
        && agg.is_merge.iter().all(|flag| !*flag)
        && (!agg.aggregates.is_empty() || !agg.group_by.is_empty())
        && agg.aggregates.iter().all(is_splittable_aggregate)
}

fn is_splittable_aggregate(call: &AggregateCall) -> bool {
    !call.distinct
        && call.order_by.is_empty()
        && matches!(
            call.name.to_ascii_lowercase().as_str(),
            "sum" | "min" | "max" | "count"
        )
}

fn aggregate_group_key_output_ref(
    group_by: &[TypedExpr],
    output_columns: &[OutputColumn],
) -> Vec<TypedExpr> {
    group_by
        .iter()
        .zip(output_columns.iter())
        .map(|(_, output)| TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: output.column_id,
                qualifier: None,
                column: output.name.clone(),
            },
            data_type: output.data_type.clone(),
            nullable: output.nullable,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{AggStage, LogicalAggregateOp, LogicalValuesOp};
    use crate::sql::planner::plan::AggregateCall;
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

    fn col_ref(id: u32, name: &str) -> TypedExpr {
        nullable_col_ref(id, name, false)
    }

    fn nullable_col_ref(id: u32, name: &str, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable,
        }
    }

    fn count_call(distinct: bool) -> AggregateCall {
        AggregateCall {
            name: "count".to_string(),
            args: vec![col_ref(2, "v")],
            distinct,
            result_type: DataType::Int64,
            order_by: vec![],
        }
    }

    fn values_group(memo: &mut Memo) -> usize {
        let id = memo.next_expr_id();
        memo.new_group(MExpr {
            id,
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        })
    }

    fn single_grouped_expr(memo: &mut Memo) -> MExpr {
        let child = values_group(memo);
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![nullable_col_ref(1, "k", true)],
                vec![count_call(false)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
            )),
            children: vec![child],
        }
    }

    fn single_scalar_expr(memo: &mut Memo) -> MExpr {
        let child = values_group(memo);
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![],
                vec![count_call(false)],
                vec![output_column(3, "count(v)")],
            )),
            children: vec![child],
        }
    }

    #[test]
    fn splits_grouped_aggregate_into_global_over_local() {
        let mut memo = Memo::new();
        let expr = single_grouped_expr(&mut memo);
        let out = SplitAggregateRule.apply(&expr, &mut memo);
        assert_eq!(out.len(), 1);
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.stage, AggStage::Global);
        assert_eq!(global.is_merge, vec![true]);
        assert!(global.is_split);
        assert_eq!(global.group_by.len(), 1);
        assert!(!global.group_by[0].nullable);
        assert_eq!(out[0].children.len(), 1);
        let local_group_id = out[0].children[0];
        let local_group = &memo.groups[local_group_id];
        assert_eq!(local_group.logical_exprs.len(), 1);
        let Operator::LogicalAggregate(local) = &local_group.logical_exprs[0].op else {
            panic!("expected local aggregate child");
        };
        assert_eq!(local.stage, AggStage::Local);
        assert_eq!(local.is_merge, vec![false]);
        assert!(local.is_split);
    }

    #[test]
    fn splits_scalar_aggregate() {
        let mut memo = Memo::new();
        let expr = single_scalar_expr(&mut memo);
        let out = SplitAggregateRule.apply(&expr, &mut memo);
        assert_eq!(out.len(), 1);
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.stage, AggStage::Global);
        assert!(global.group_by.is_empty());
        let local_group_id = out[0].children[0];
        let local_group = &memo.groups[local_group_id];
        let Operator::LogicalAggregate(local) = &local_group.logical_exprs[0].op else {
            panic!("expected local aggregate child");
        };
        assert_eq!(local.stage, AggStage::Local);
        assert!(local.group_by.is_empty());
    }

    #[test]
    fn rejects_distinct_and_already_split_aggregate() {
        let mut memo = Memo::new();
        let child = values_group(&mut memo);
        let distinct = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![col_ref(1, "k")],
                vec![count_call(true)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
            )),
            children: vec![child],
        };
        assert!(SplitAggregateRule.apply(&distinct, &mut memo).is_empty());

        let already_split = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Local,
                vec![col_ref(1, "k")],
                vec![count_call(false)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
                vec![false],
                true,
            )),
            children: vec![child],
        };
        assert!(
            SplitAggregateRule
                .apply(&already_split, &mut memo)
                .is_empty()
        );
    }
}
