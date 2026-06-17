//! Conversion between `LogicalPlanNode`, `OptExpr`, and Memo groups
//! (Bridge 1 + copy-in).

use super::memo::{GroupId, MExpr, Memo};
use super::operator::{
    AggregateStateMergeOp, ApplyOp, AssertOneRowOp, CTEAnchorOp, CTEConsumeOp, CTEProduceOp,
    DecodeOp, ExceptOp, FilterOp, GenerateSeriesOp, ImvDeltaOp, ImvVersionOp, IntersectOp,
    LimitOp, LogicalAggregateOp, LogicalJoinOp, Operator, ProjectOp, RepeatOp, ScanOp, SortOp,
    TableFunctionOp, UnionOp, ValuesOp, WindowOp,
};
use super::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{ScalarArena, intern_typed};
use crate::sql::optimizer::scalar_bridge::{
    intern_aggregate_calls, intern_exprs, intern_project_items, intern_sort_items,
    intern_window_exprs,
};
use crate::sql::planner::plan::{LogicalPlanNode, LogicalPlanNodeKind};

/// Copy an `OptExpr` tree into the Memo as Groups (one Group per node).
/// The operator already holds interned `ScalarId`s, so no scalar interning
/// happens here — this is the trivial StarRocks-style `copyIn`.
pub(crate) fn opt_expr_to_memo(expr: &OptExpr, memo: &mut Memo) -> GroupId {
    let children: Vec<GroupId> = expr
        .children
        .iter()
        .map(|c| opt_expr_to_memo(c, memo))
        .collect();
    let mexpr = MExpr {
        id: memo.next_expr_id(),
        op: expr.op.clone(),
        children,
    };
    let group_id = memo.new_group(mexpr);
    // Register CTEProduce groups so CTEConsume can look up their stats.
    if let Operator::LogicalCTEProduce(op) = &expr.op {
        memo.cte_produce_groups.insert(op.cte_id, group_id);
    }
    group_id
}

/// Bridge 1: convert a `LogicalPlanNode` tree into an `OptExpr` tree, interning
/// all scalars into the provided `ScalarArena`. No Memo groups are minted here.
pub(crate) fn logical_plan_to_opt_expr(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
) -> OptExpr {
    let mut expr = match &plan.kind {
        LogicalPlanNodeKind::Scan(node) => {
            for column in &node.columns {
                scalars.remember_source_column_display(
                    column.column_id,
                    node.alias.clone(),
                    column.name.clone(),
                );
            }
            let op = Operator::LogicalScan(ScanOp {
                database: node.database.clone(),
                table: node.table.clone(),
                alias: node.alias.clone(),
                columns: node.columns.clone(),
                predicates: intern_exprs(scalars, &node.predicates),
                required_columns: node.required_columns.clone(),
                dict_columns: node.dict_columns.clone(),
                variant_columns: node.variant_columns.clone(),
                mv_rewritten_from: None,
            });
            OptExpr::leaf(op)
        }

        LogicalPlanNodeKind::Filter(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalFilter(FilterOp {
                predicate: intern_typed(scalars, &node.predicate),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::Project(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalProject(ProjectOp {
                items: intern_project_items(scalars, &node.items),
                output_qualifier: node.output_qualifier.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::Aggregate(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let group_by = intern_exprs(scalars, &node.group_by);
            for (scalar_id, output) in group_by.iter().zip(node.output_columns.iter()) {
                scalars.remember_column_display_from_scalar(output.column_id, *scalar_id);
            }
            let aggregates = intern_aggregate_calls(scalars, &node.aggregates);
            let op = Operator::LogicalAggregate(LogicalAggregateOp::single(
                group_by,
                aggregates,
                node.output_columns.clone(),
            ));
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::Join(node) => {
            let left = logical_plan_to_opt_expr(plan.left(), scalars);
            let right = logical_plan_to_opt_expr(plan.right(), scalars);
            let op = Operator::LogicalJoin(LogicalJoinOp {
                join_type: node.join_type,
                condition: node
                    .condition
                    .as_ref()
                    .map(|condition| intern_typed(scalars, condition)),
            });
            OptExpr::new(op, vec![left, right])
        }

        LogicalPlanNodeKind::Sort(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalSort(SortOp {
                items: intern_sort_items(scalars, &node.items),
                analytic_partition_exprs: intern_exprs(scalars, &node.analytic_partition_by),
                partition_limit: node.partition_limit,
                topn_type: node.topn_type,
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::Limit(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalLimit(LimitOp {
                limit: node.limit,
                offset: node.offset,
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::Union(node) => {
            let child_output_columns = plan
                .children
                .iter()
                .map(|input| crate::sql::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<OptExpr> = plan
                .children
                .iter()
                .map(|input| logical_plan_to_opt_expr(input, scalars))
                .collect();
            let op = Operator::LogicalUnion(UnionOp {
                all: node.all,
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            OptExpr::new(op, children)
        }

        LogicalPlanNodeKind::Intersect(node) => {
            let child_output_columns = plan
                .children
                .iter()
                .map(|input| crate::sql::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<OptExpr> = plan
                .children
                .iter()
                .map(|input| logical_plan_to_opt_expr(input, scalars))
                .collect();
            let op = Operator::LogicalIntersect(IntersectOp {
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            OptExpr::new(op, children)
        }

        LogicalPlanNodeKind::Except(node) => {
            let child_output_columns = plan
                .children
                .iter()
                .map(|input| crate::sql::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<OptExpr> = plan
                .children
                .iter()
                .map(|input| logical_plan_to_opt_expr(input, scalars))
                .collect();
            let op = Operator::LogicalExcept(ExceptOp {
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            OptExpr::new(op, children)
        }

        LogicalPlanNodeKind::Values(node) => {
            let op = Operator::LogicalValues(ValuesOp {
                rows: node
                    .rows
                    .iter()
                    .map(|row| intern_exprs(scalars, row))
                    .collect(),
                columns: node.columns.clone(),
            });
            OptExpr::leaf(op)
        }

        LogicalPlanNodeKind::GenerateSeries(node) => {
            let op = Operator::LogicalGenerateSeries(GenerateSeriesOp {
                start: node.start,
                end: node.end,
                step: node.step,
                column_name: node.column_name.clone(),
                alias: node.alias.clone(),
                output_column_id: node.output_column_id,
            });
            OptExpr::leaf(op)
        }

        LogicalPlanNodeKind::TableFunction(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalTableFunction(TableFunctionOp {
                function_name: node.function_name.clone(),
                args: intern_exprs(scalars, &node.args),
                output_columns: node.output_columns.clone(),
                alias: node.alias.clone(),
                is_left_join: node.is_left_join,
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::Window(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalWindow(WindowOp {
                window_exprs: intern_window_exprs(scalars, &node.window_exprs),
                output_columns: node.output_columns.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::Repeat(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalRepeat(RepeatOp {
                repeat_column_ref_list: node.repeat_column_ref_list.clone(),
                repeat_column_ref_ids: node.repeat_column_ref_ids.clone(),
                grouping_ids: node.grouping_ids.clone(),
                all_rollup_columns: node.all_rollup_columns.clone(),
                all_rollup_column_ids: node.all_rollup_column_ids.clone(),
                grouping_key_aliases: node.grouping_key_aliases.clone(),
                grouping_fn_args: node.grouping_fn_args.clone(),
                grouping_fn_arg_ids: node.grouping_fn_arg_ids.clone(),
                grouping_fn_ids: node.grouping_fn_ids.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::CTEConsume(node) => {
            let op = Operator::LogicalCTEConsume(CTEConsumeOp {
                cte_id: node.cte_id,
                alias: node.alias.clone(),
                output_columns: node.output_columns.clone(),
            });
            OptExpr::leaf(op)
        }

        LogicalPlanNodeKind::CTEAnchor(node) => {
            let produce = logical_plan_to_opt_expr(plan.child(0), scalars);
            let consumer = logical_plan_to_opt_expr(plan.child(1), scalars);
            let op = Operator::LogicalCTEAnchor(CTEAnchorOp {
                cte_id: node.cte_id,
            });
            OptExpr::new(op, vec![produce, consumer])
        }

        LogicalPlanNodeKind::CTEProduce(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id: node.cte_id,
                output_columns: node.output_columns.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::Decode(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalDecode(DecodeOp {
                mappings: node.mappings.clone(),
                output_columns: node.output_columns.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::AggregateStateMerge(node) => {
            let old_input = logical_plan_to_opt_expr(plan.left(), scalars);
            let delta_input = logical_plan_to_opt_expr(plan.right(), scalars);
            let op = Operator::LogicalAggregateStateMerge(AggregateStateMergeOp {
                group_key_names: node.group_key_names.clone(),
                aggregate_state_names: node.aggregate_state_names.clone(),
                change_op_column: node.change_op_column.clone(),
                output_columns: node.output_columns.clone(),
            });
            OptExpr::new(op, vec![old_input, delta_input])
        }

        LogicalPlanNodeKind::AssertOneRow(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalAssertOneRow(AssertOneRowOp {
                subquery_text: node.subquery_text.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::Apply(node) => {
            // Apply is consumed by the subquery/imv rewrite rules BEFORE memo
            // conversion. Building an OptExpr here allows the rewrite rules
            // (subquery/ and imv/ dirs) to operate on OptExpr trees. After
            // rewrite the SubqueryRewrite backstop asserts no Apply remains.
            let outer = logical_plan_to_opt_expr(plan.left(), scalars);
            let inner = logical_plan_to_opt_expr(plan.right(), scalars);
            let op = Operator::LogicalApply(ApplyOp {
                kind: node.kind,
                subquery_expr: intern_typed(scalars, &node.subquery_expr),
                output_column: node.output_column.clone(),
                inner_output_column_id: node.inner_output_column_id,
                correlation_column_ids: node.correlation_column_ids.clone(),
                correlation_conjuncts: intern_exprs(scalars, &node.correlation_conjuncts),
                residual_predicate: node
                    .residual_predicate
                    .as_ref()
                    .map(|e| intern_typed(scalars, e)),
                need_check_max_rows: node.need_check_max_rows,
                use_semi_anti: node.use_semi_anti,
                uncorrelated_outer_predicate_columns: node
                    .uncorrelated_outer_predicate_columns
                    .clone(),
            });
            OptExpr::new(op, vec![outer, inner])
        }

        LogicalPlanNodeKind::ImvDelta(node) => {
            // ImvDelta wraps a child subtree (the base plan being rewritten).
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalImvDelta(ImvDeltaOp {
                is_root: node.is_root,
                action_column: node.action_column,
                branch_scope: node.branch_scope.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanNodeKind::ImvVersion(node) => {
            // ImvVersion wraps a child plan (the snapshot scan subtree).
            let op = Operator::LogicalImvVersion(ImvVersionOp {
                version_ref: node.version_ref.clone(),
            });
            if plan.children.is_empty() {
                OptExpr::leaf(op)
            } else {
                let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
                OptExpr::new(op, vec![child])
            }
        }
    };
    expr.required_output_columns = plan.required_output_columns.clone();
    expr
}

/// Convert a `LogicalPlanNode` tree into Memo groups (Bridge 1 + copy-in).
/// Kept as a thin wrapper so existing call sites are unchanged.
pub(crate) fn logical_plan_to_memo(plan: &LogicalPlanNode, memo: &mut Memo) -> GroupId {
    let opt_expr = logical_plan_to_opt_expr(plan, &mut memo.scalars);
    opt_expr_to_memo(&opt_expr, memo)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::cascades_rules::implement::ScanToPhysical;
    use crate::sql::optimizer::rule::Rule;
    use crate::sql::planner::plan::*;
    use crate::sql::planner::plan::{
        LogicalFilterNode, LogicalPlanNodeKind, LogicalScanNode, LogicalUnionNode,
        LogicalValuesNode, ScanVariantColumn,
    };
    use arrow::datatypes::DataType;

    fn dummy_table_def() -> TableDef {
        TableDef {
            name: "t1".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn dummy_output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }]
    }

    fn test_output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn values_with_columns(columns: Vec<OutputColumn>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: columns,
            }),
            vec![],
            None,
        )
    }

    #[test]
    fn set_op_output_columns_survive_memo_stats_with_duplicate_names() {
        let target = vec![test_output_column(20, "dup"), test_output_column(21, "dup")];
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: target.clone(),
            }),
            vec![
                values_with_columns(vec![
                    test_output_column(10, "dup"),
                    test_output_column(11, "dup"),
                ]),
                values_with_columns(vec![
                    test_output_column(12, "dup"),
                    test_output_column(13, "dup"),
                ]),
            ],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo(&plan, &mut memo);
        crate::sql::optimizer::stats::derive_group_statistics(
            &mut memo,
            &std::collections::HashMap::new(),
        );

        let output_columns = &memo.groups[root]
            .logical_props
            .as_ref()
            .expect("set-op root should have logical properties")
            .output_columns;
        assert_eq!(output_columns.len(), target.len());
        assert_eq!(output_columns[0].name, "dup");
        assert_eq!(output_columns[1].name, "dup");
        assert_eq!(output_columns[0].column_id, target[0].column_id);
        assert_eq!(output_columns[1].column_id, target[1].column_id);

        let root_expr = memo.groups[root]
            .logical_exprs
            .first()
            .expect("root logical expression");
        let Operator::LogicalUnion(op) = &root_expr.op else {
            panic!("expected logical union");
        };
        assert_eq!(op.child_output_columns.len(), 2);
        assert_eq!(op.child_output_columns[0][0].column_id, ColumnId(10));
        assert_eq!(op.child_output_columns[0][1].column_id, ColumnId(11));
        assert_eq!(op.child_output_columns[1][0].column_id, ColumnId(12));
        assert_eq!(op.child_output_columns[1][1].column_id, ColumnId(13));
    }

    #[test]
    fn test_scan_to_memo() {
        let scan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
            }),
            vec![],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo(&scan, &mut memo);

        assert_eq!(gid, 0);
        assert_eq!(memo.groups.len(), 1);
        assert_eq!(memo.groups[0].logical_exprs.len(), 1);
        assert!(memo.groups[0].physical_exprs.is_empty());
        assert!(matches!(
            &memo.groups[0].logical_exprs[0].op,
            Operator::LogicalScan(_)
        ));
        assert!(memo.groups[0].logical_exprs[0].children.is_empty());
    }

    #[test]
    fn variant_path_scan_descriptor_survives_physical_conversion() {
        let source_column_id = ColumnId::new_for_test(100);
        let synthetic_column_id = ColumnId::new_for_test(101);
        let variant_descriptor = ScanVariantColumn {
            source_column_id,
            source_column: "payload".to_string(),
            synthetic_column_id,
            synthetic_column: "__nr_var_payload_0".to_string(),
            canonical_path: "$.user.id".to_string(),
            requested_type: DataType::Int64,
            strict: true,
        };

        let scan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: source_column_id,
                        name: "payload".to_string(),
                        data_type: DataType::LargeBinary,
                        nullable: true,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: synthetic_column_id,
                        name: "__nr_var_payload_0".to_string(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: true,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![variant_descriptor.clone()],
            }),
            vec![],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo(&scan, &mut memo);
        let logical_expr = memo.groups[gid].logical_exprs[0].clone();

        let physical = ScanToPhysical.apply(&logical_expr, &mut memo);

        assert_eq!(physical.len(), 1);
        let Operator::PhysicalScan(scan) = &physical[0].op else {
            panic!("expected PhysicalScan");
        };
        assert_eq!(scan.variant_columns.len(), 1);
        let actual = &scan.variant_columns[0];
        assert_eq!(actual.source_column_id, variant_descriptor.source_column_id);
        assert_eq!(actual.source_column, variant_descriptor.source_column);
        assert_eq!(
            actual.synthetic_column_id,
            variant_descriptor.synthetic_column_id
        );
        assert_eq!(actual.synthetic_column, variant_descriptor.synthetic_column);
        assert_eq!(actual.canonical_path, variant_descriptor.canonical_path);
        assert_eq!(actual.requested_type, variant_descriptor.requested_type);
        assert_eq!(actual.strict, variant_descriptor.strict);
    }

    #[test]
    fn test_filter_scan_to_memo() {
        let scan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
            }),
            vec![],
            None,
        );

        let predicate = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            data_type: DataType::Boolean,
            nullable: false,
        };

        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: predicate,
            }),
            vec![scan],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo(&filter, &mut memo);

        // Should produce 2 groups: Scan (group 0) and Filter (group 1).
        assert_eq!(memo.groups.len(), 2);
        assert_eq!(gid, 1);

        // Group 0: Scan, no children.
        assert_eq!(memo.groups[0].logical_exprs.len(), 1);
        assert!(matches!(
            &memo.groups[0].logical_exprs[0].op,
            Operator::LogicalScan(_)
        ));
        assert!(memo.groups[0].logical_exprs[0].children.is_empty());

        // Group 1: Filter, child = group 0.
        assert_eq!(memo.groups[1].logical_exprs.len(), 1);
        assert!(matches!(
            &memo.groups[1].logical_exprs[0].op,
            Operator::LogicalFilter(_)
        ));
        assert_eq!(memo.groups[1].logical_exprs[0].children, vec![0]);
    }

    #[test]
    fn test_cte_anchor_to_memo() {
        let scan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
            }),
            vec![],
            None,
        );

        let produce = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                cte_id: 7,
                output_columns: dummy_output_columns(),
            }),
            vec![scan.clone()],
            None,
        );

        let consume = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: 7,
                alias: "t".to_string(),
                output_columns: dummy_output_columns(),
            }),
            vec![],
            None,
        );

        let anchor = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: 7 }),
            vec![produce, consume],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo(&anchor, &mut memo);

        assert_eq!(gid, 3);
        assert!(matches!(
            memo.groups[3].logical_exprs[0].op,
            Operator::LogicalCTEAnchor(_)
        ));
        assert_eq!(memo.groups[3].logical_exprs[0].children, vec![1, 2]);
    }
}
