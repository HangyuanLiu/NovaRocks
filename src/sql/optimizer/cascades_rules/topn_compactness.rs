use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalTopNOp, Operator, TopNPhase};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::optimizer::topn_proof::{TopNWindow, sort_items_to_keys, sort_keys_equivalent};

pub(crate) struct MergeConsecutiveTopN;

impl Rule for MergeConsecutiveTopN {
    fn name(&self) -> &str {
        "MergeConsecutiveTopN"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        merge_consecutive_topn(expr, memo)
    }
}

fn merge_consecutive_topn(expr: &MExpr, memo: &Memo) -> Vec<NewExpr> {
    let Operator::LogicalTopN(outer) = &expr.op else {
        return vec![];
    };
    if expr.children.len() != 1 {
        return vec![];
    }

    let child_group_id = expr.children[0];
    let Some(child_group) = memo.groups.get(child_group_id) else {
        return vec![];
    };

    let mut results = Vec::new();
    for child_expr in child_group.logical_exprs.iter() {
        let Operator::LogicalTopN(inner) = &child_expr.op else {
            continue;
        };
        if child_expr.children.len() != 1 {
            continue;
        }
        if !topn_phase_can_merge(outer, inner) {
            continue;
        }
        if inner.offset.unwrap_or(0) != 0 {
            continue;
        }
        let Some(outer_window) = TopNWindow::from_limit_offset(outer.limit, outer.offset) else {
            continue;
        };
        let Some(inner_window) = TopNWindow::from_limit_offset(inner.limit, inner.offset) else {
            continue;
        };
        if !inner_window.covers(outer_window) {
            continue;
        }

        let Some(outer_keys) = sort_items_to_keys(&outer.items) else {
            continue;
        };
        let Some(inner_keys) = sort_items_to_keys(&inner.items) else {
            continue;
        };
        let inner_child_group_id = child_expr.children[0];
        let equivalences = memo
            .groups
            .get(inner_child_group_id)
            .and_then(|group| group.logical_props.as_ref())
            .map(|props| &props.equivalence_classes);
        if !sort_keys_equivalent(&outer_keys, &inner_keys, equivalences) {
            continue;
        }

        results.push(NewExpr {
            op: Operator::LogicalTopN(outer.clone()),
            children: vec![inner_child_group_id],
        });
    }
    results
}

fn topn_phase_can_merge(outer: &LogicalTopNOp, inner: &LogicalTopNOp) -> bool {
    matches!(
        (outer.phase, inner.phase, outer.is_split, inner.is_split),
        (TopNPhase::Final, TopNPhase::Final, false, false)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
    use crate::sql::optimizer::operator::{LogicalScanOp, LogicalTopNOp, TopNPhase};
    use arrow::datatypes::DataType;

    fn col(id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: format!("c{id}"),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn sort_item(id: u32) -> SortItem {
        SortItem {
            expr: col(id),
            asc: true,
            nulls_first: false,
        }
    }

    fn scan_group(memo: &mut Memo) -> usize {
        let scan = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
            }),
            children: vec![],
        };
        memo.new_group(scan)
    }

    fn topn_with_item(
        memo: &Memo,
        item: SortItem,
        limit: i64,
        offset: i64,
        phase: TopNPhase,
        is_split: bool,
        child_group: usize,
    ) -> MExpr {
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![item],
                limit: Some(limit),
                offset: Some(offset),
                phase,
                is_split,
            }),
            children: vec![child_group],
        }
    }

    fn topn(
        memo: &Memo,
        limit: i64,
        offset: i64,
        phase: TopNPhase,
        is_split: bool,
        child_group: usize,
    ) -> MExpr {
        topn_with_item(
            memo,
            sort_item(1),
            limit,
            offset,
            phase,
            is_split,
            child_group,
        )
    }

    #[test]
    fn merges_consecutive_topn_when_inner_window_covers_outer() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let inner_group = memo.new_group(topn(&memo, 20, 0, TopNPhase::Final, false, scan_group));
        let outer = topn(&memo, 5, 10, TopNPhase::Final, false, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert_eq!(out.len(), 1, "expected one merged TopN alternative");
        match &out[0].op {
            Operator::LogicalTopN(topn) => {
                assert_eq!(topn.limit, Some(5));
                assert_eq!(topn.offset, Some(10));
                assert_eq!(topn.phase, TopNPhase::Final);
                assert!(!topn.is_split);
            }
            other => panic!("expected LogicalTopN, got {other:?}"),
        }
        assert_eq!(
            out[0].children,
            vec![scan_group],
            "merged TopN should bypass the inner TopN"
        );
    }

    #[test]
    fn does_not_merge_when_inner_window_is_too_small() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let inner_group = memo.new_group(topn(&memo, 12, 0, TopNPhase::Final, false, scan_group));
        let outer = topn(&memo, 5, 10, TopNPhase::Final, false, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert!(
            out.is_empty(),
            "inner TopN that ends before the outer window must not be removed"
        );
    }

    #[test]
    fn does_not_merge_when_inner_offset_is_non_zero() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let inner_group = memo.new_group(topn(&memo, 20, 3, TopNPhase::Final, false, scan_group));
        let outer = topn(&memo, 5, 10, TopNPhase::Final, false, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert!(
            out.is_empty(),
            "inner offset must be preserved instead of dropping the inner TopN"
        );
    }

    #[test]
    fn does_not_merge_split_final_over_partial_topn() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let inner_group = memo.new_group(topn(&memo, 20, 0, TopNPhase::Partial, false, scan_group));
        let outer = topn(&memo, 5, 0, TopNPhase::Final, true, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert!(
            out.is_empty(),
            "split final TopN must keep its partial TopN child"
        );
    }

    #[test]
    fn does_not_merge_unsplit_final_over_partial_topn() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let inner_group = memo.new_group(topn(&memo, 20, 0, TopNPhase::Partial, false, scan_group));
        let outer = topn(&memo, 5, 0, TopNPhase::Final, false, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert!(
            out.is_empty(),
            "unsplit final TopN must not merge over partial TopN"
        );
    }

    #[test]
    fn merges_when_child_equivalence_classes_prove_sort_keys_equivalent() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let mut props = LogicalProperties::new(vec![], 100.0);
        props
            .equivalence_classes
            .merge_pair(ColumnId(1), ColumnId(2));
        memo.groups[scan_group].logical_props = Some(props);
        let inner_group = memo.new_group(topn_with_item(
            &memo,
            sort_item(2),
            20,
            0,
            TopNPhase::Final,
            false,
            scan_group,
        ));
        let outer = topn(&memo, 5, 0, TopNPhase::Final, false, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert_eq!(out.len(), 1, "equivalent sort keys should allow merge");
        assert_eq!(out[0].children, vec![scan_group]);
    }
}
