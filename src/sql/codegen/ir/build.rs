use crate::sql::analysis::TypedExpr;
use crate::sql::codegen::helpers::split_and_conjuncts_typed;
use crate::sql::optimizer::operator::{Operator, TopNPhase};
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;

use super::FragmentId;
use super::body::{
    AssertOneRowBody, DecodeBody, HashAggregateBody, HashJoinBody, NestLoopJoinBody, ProjectBody,
    RepeatBody, ScanBody, SetOpBody, SetOpKind, SortBody, TopNBody, ValuesBody,
};
use super::fragment::{DataPartition, DataSink, DistributedPlan, PlanFragment};
use super::node::{DistributedPlanNode, DistributedPlanNodeBody, PlanNodeStats};

struct DistributedPlanBuilder {
    next_node_id: i32,
    next_tuple_id: i32,
}

impl DistributedPlanBuilder {
    fn alloc_node(&mut self) -> i32 {
        let node_id = self.next_node_id;
        self.next_node_id += 1;
        node_id
    }

    fn alloc_tuple(&mut self) -> i32 {
        let tuple_id = self.next_tuple_id;
        self.next_tuple_id += 1;
        tuple_id
    }

    fn visit(
        &mut self,
        node: &PhysicalPlanNode,
        fragment_id: FragmentId,
    ) -> Result<DistributedPlanNode, String> {
        match &node.op {
            Operator::PhysicalScan(op) => {
                let node_id = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: vec![tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: DistributedPlanNodeBody::Scan(Box::new(ScanBody {
                        database: op.database.clone(),
                        table: op.table.clone(),
                        alias: op.alias.clone(),
                        columns: op.columns.clone(),
                        predicates: op.predicates.clone(),
                        required_columns: op.required_columns.clone(),
                        dict_columns: op.dict_columns.clone(),
                        variant_columns: op.variant_columns.clone(),
                        mv_rewritten_from: op.mv_rewritten_from.clone(),
                    })),
                })
            }
            Operator::PhysicalFilter(op) => {
                let child_plan = expect_single_child(node, "PhysicalFilter")?;
                let mut child = self.visit(child_plan, fragment_id)?;
                fold_filter_into_scan(&mut child, &op.predicate)?;
                child.stats = PlanNodeStats::from_statistics(&node.stats);
                child
                    .probe_runtime_filters
                    .extend(node.probe_runtime_filters.clone());
                Ok(child)
            }
            Operator::PhysicalProject(op) => {
                let child_plan = expect_single_child(node, "PhysicalProject")?;
                let child = self.visit(child_plan, fragment_id)?;
                let node_id = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: vec![tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: DistributedPlanNodeBody::Project(ProjectBody {
                        items: op.items.clone(),
                        output_qualifier: op.output_qualifier.clone(),
                    }),
                })
            }
            Operator::PhysicalSort(op) => {
                let child_plan = expect_single_child(node, "PhysicalSort")?;
                let child = self.visit(child_plan, fragment_id)?;
                let node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: child.tuple_ids.clone(),
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: DistributedPlanNodeBody::Sort(SortBody {
                        items: op.items.clone(),
                        analytic_partition_exprs: op.analytic_partition_exprs.clone(),
                        output_columns: node.output_columns.clone(),
                        offset: None,
                    }),
                })
            }
            Operator::PhysicalLimit(op) => {
                let child_plan = expect_single_child(node, "PhysicalLimit")?;
                let offset = op.offset.unwrap_or(0);
                if offset > 0 {
                    if matches!(&child_plan.op, Operator::PhysicalDistribution(_)) {
                        return Err("limit-offset-exchange is Phase 2".to_string());
                    }
                    if !limit_child_can_apply_offset_locally(child_plan) {
                        return Err(
                            "LIMIT/OFFSET without a local SORT/TOPN child is not supported"
                                .to_string(),
                        );
                    }
                }

                let mut child = self.visit(child_plan, fragment_id)?;
                child.limit = op.limit.unwrap_or(-1);
                child.stats = PlanNodeStats::from_statistics(&node.stats);
                match &mut child.body {
                    DistributedPlanNodeBody::Sort(sort) => {
                        sort.offset = op.offset;
                    }
                    DistributedPlanNodeBody::TopN(topn) => {
                        topn.limit = op.limit;
                        topn.offset = op.offset;
                    }
                    _ if offset > 0 => {
                        return Err(
                            "LIMIT/OFFSET without a local SORT/TOPN child is not supported"
                                .to_string(),
                        );
                    }
                    _ => {}
                }
                Ok(child)
            }
            Operator::PhysicalTopN(op) => {
                let child_plan = expect_single_child(node, "PhysicalTopN")?;
                match (op.phase, op.is_split) {
                    (TopNPhase::Final, true) => Err("TopN split is Phase 2".to_string()),
                    (TopNPhase::Final, false) | (TopNPhase::Partial, _) => {
                        let child = self.visit(child_plan, fragment_id)?;
                        let node_id = self.alloc_node();
                        Ok(DistributedPlanNode {
                            node_id,
                            fragment_id,
                            tuple_ids: child.tuple_ids.clone(),
                            nullable_tuple_ids: vec![],
                            limit: op.limit.unwrap_or(-1),
                            execution_join_distribution: node.execution_props.join_distribution,
                            build_runtime_filters: node.build_runtime_filters.clone(),
                            probe_runtime_filters: node.probe_runtime_filters.clone(),
                            children: vec![child],
                            stats: PlanNodeStats::from_statistics(&node.stats),
                            body: DistributedPlanNodeBody::TopN(TopNBody {
                                items: op.items.clone(),
                                limit: op.limit,
                                offset: op.offset,
                                phase: op.phase,
                                is_split: op.is_split,
                            }),
                        })
                    }
                }
            }
            Operator::PhysicalHashAggregate(op) => {
                let child_plan = expect_single_child(node, "PhysicalHashAggregate")?;
                let child = self.visit(child_plan, fragment_id)?;
                let agg_tuple_id = self.alloc_tuple();
                let agg_node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id: agg_node_id,
                    fragment_id,
                    tuple_ids: vec![agg_tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: DistributedPlanNodeBody::HashAggregate(Box::new(HashAggregateBody {
                        mode: op.mode,
                        group_by: op.group_by.clone(),
                        aggregates: op.aggregates.clone(),
                        is_merge: op.is_merge.clone(),
                        output_columns: op.output_columns.clone(),
                    })),
                })
            }
            Operator::PhysicalHashJoin(op) => {
                let (left_plan, right_plan) = expect_binary_children(node, "PhysicalHashJoin")?;
                let left = self.visit(left_plan, fragment_id)?;
                let right = self.visit(right_plan, fragment_id)?;
                let node_id = self.alloc_node();
                let mut tuple_ids = left.tuple_ids.clone();
                tuple_ids.extend(right.tuple_ids.iter().copied());
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids,
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![left, right],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: DistributedPlanNodeBody::HashJoin(Box::new(HashJoinBody {
                        join_type: op.join_type,
                        eq_conditions: op.eq_conditions.clone(),
                        other_condition: op.other_condition.clone(),
                        distribution: op.distribution.clone(),
                    })),
                })
            }
            Operator::PhysicalNestLoopJoin(op) => {
                let (left_plan, right_plan) = expect_binary_children(node, "PhysicalNestLoopJoin")?;
                let left = self.visit(left_plan, fragment_id)?;
                let right = self.visit(right_plan, fragment_id)?;
                let node_id = self.alloc_node();
                let mut tuple_ids = left.tuple_ids.clone();
                tuple_ids.extend(right.tuple_ids.iter().copied());
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids,
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![left, right],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: DistributedPlanNodeBody::NestLoopJoin(NestLoopJoinBody {
                        join_type: op.join_type,
                        condition: op.condition.clone(),
                    }),
                })
            }
            Operator::PhysicalValues(op) => {
                if !node.children.is_empty() {
                    return Err(format!(
                        "build_distributed_plan M0: PhysicalValues expected 0 children, got {}",
                        node.children.len()
                    ));
                }
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: vec![tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: DistributedPlanNodeBody::Values(ValuesBody {
                        rows: op.rows.clone(),
                        columns: op.columns.clone(),
                    }),
                })
            }
            Operator::PhysicalAssertOneRow(op) => {
                let child_plan = expect_single_child(node, "PhysicalAssertOneRow")?;
                let child = self.visit(child_plan, fragment_id)?;
                let node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: child.tuple_ids.clone(),
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: DistributedPlanNodeBody::AssertOneRow(AssertOneRowBody {
                        subquery_text: op.subquery_text.clone(),
                    }),
                })
            }
            Operator::PhysicalDecode(op) => {
                let child_plan = expect_single_child(node, "PhysicalDecode")?;
                let child = self.visit(child_plan, fragment_id)?;
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: vec![tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: DistributedPlanNodeBody::Decode(DecodeBody {
                        mappings: op.mappings.clone(),
                        output_columns: op.output_columns.clone(),
                    }),
                })
            }
            Operator::PhysicalRepeat(op) => {
                let child_plan = expect_single_child(node, "PhysicalRepeat")?;
                let child = self.visit(child_plan, fragment_id)?;
                let node_id = self.alloc_node();
                let virtual_tuple_id = self.alloc_tuple();
                let mut tuple_ids = child.tuple_ids.clone();
                if !op.grouping_fn_args.is_empty() {
                    tuple_ids.push(virtual_tuple_id);
                }
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids,
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: DistributedPlanNodeBody::Repeat(Box::new(RepeatBody {
                        virtual_tuple_id,
                        repeat_column_ref_list: op.repeat_column_ref_list.clone(),
                        repeat_column_ref_ids: op.repeat_column_ref_ids.clone(),
                        grouping_ids: op.grouping_ids.clone(),
                        all_rollup_columns: op.all_rollup_columns.clone(),
                        all_rollup_column_ids: op.all_rollup_column_ids.clone(),
                        grouping_key_aliases: op.grouping_key_aliases.clone(),
                        grouping_fn_args: op.grouping_fn_args.clone(),
                        grouping_fn_arg_ids: op.grouping_fn_arg_ids.clone(),
                        grouping_fn_ids: op.grouping_fn_ids.clone(),
                    })),
                })
            }
            Operator::PhysicalUnion(op) => {
                if !op.all {
                    return Err("UNION DISTINCT is Phase 2".to_string());
                }
                self.visit_set_op(
                    node,
                    fragment_id,
                    SetOpKind::UnionAll,
                    &op.output_columns,
                    &op.child_output_columns,
                )
            }
            Operator::PhysicalIntersect(op) => self.visit_set_op(
                node,
                fragment_id,
                SetOpKind::Intersect,
                &op.output_columns,
                &op.child_output_columns,
            ),
            Operator::PhysicalExcept(op) => self.visit_set_op(
                node,
                fragment_id,
                SetOpKind::Except,
                &op.output_columns,
                &op.child_output_columns,
            ),
            other => Err(format!(
                "build_distributed_plan slice 1 does not handle operator {other:?}"
            )),
        }
    }

    fn visit_set_op(
        &mut self,
        node: &PhysicalPlanNode,
        fragment_id: FragmentId,
        kind: SetOpKind,
        explicit_output_columns: &[crate::sql::analysis::OutputColumn],
        child_output_columns: &[Vec<crate::sql::analysis::OutputColumn>],
    ) -> Result<DistributedPlanNode, String> {
        if node.children.is_empty() {
            return Err("set operation node has no inputs".to_string());
        }

        let mut children = Vec::with_capacity(node.children.len());
        for child in &node.children {
            children.push(self.visit(child, fragment_id)?);
        }

        let output_columns = if !explicit_output_columns.is_empty() {
            explicit_output_columns.to_vec()
        } else if !node.output_columns.is_empty() {
            node.output_columns.clone()
        } else {
            node.children[0].output_columns.clone()
        };
        let tuple_id = self.alloc_tuple();
        let node_id = self.alloc_node();

        Ok(DistributedPlanNode {
            node_id,
            fragment_id,
            tuple_ids: vec![tuple_id],
            nullable_tuple_ids: vec![],
            limit: -1,
            execution_join_distribution: node.execution_props.join_distribution,
            build_runtime_filters: node.build_runtime_filters.clone(),
            probe_runtime_filters: node.probe_runtime_filters.clone(),
            children,
            stats: PlanNodeStats::from_statistics(&node.stats),
            body: DistributedPlanNodeBody::SetOp(SetOpBody {
                kind,
                output_columns,
                child_output_columns: child_output_columns.to_vec(),
            }),
        })
    }
}

fn expect_binary_children<'a>(
    node: &'a PhysicalPlanNode,
    operator_name: &str,
) -> Result<(&'a PhysicalPlanNode, &'a PhysicalPlanNode), String> {
    if node.children.len() != 2 {
        return Err(format!(
            "build_distributed_plan M0: {operator_name} expected 2 children, got {}",
            node.children.len()
        ));
    }
    Ok((&node.children[0], &node.children[1]))
}

fn expect_single_child<'a>(
    node: &'a PhysicalPlanNode,
    operator_name: &str,
) -> Result<&'a PhysicalPlanNode, String> {
    if node.children.len() != 1 {
        return Err(format!(
            "build_distributed_plan slice 1: {operator_name} expected 1 child, got {}",
            node.children.len()
        ));
    }
    Ok(&node.children[0])
}

fn limit_child_can_apply_offset_locally(child: &PhysicalPlanNode) -> bool {
    matches!(
        &child.op,
        Operator::PhysicalSort(_) | Operator::PhysicalTopN(_)
    )
}

fn fold_filter_into_scan(
    node: &mut DistributedPlanNode,
    predicate: &TypedExpr,
) -> Result<(), String> {
    let DistributedPlanNodeBody::Scan(scan) = &mut node.body else {
        return Err("build_distributed_plan slice 1: Filter child is not a Scan".to_string());
    };
    scan.predicates
        .extend(split_and_conjuncts_typed(predicate).into_iter().cloned());
    Ok(())
}

pub(crate) fn build_distributed_plan(plan: &PhysicalPlanNode) -> Result<DistributedPlan, String> {
    let mut builder = DistributedPlanBuilder {
        next_node_id: 1,
        next_tuple_id: 1,
    };
    let root_fragment_id = 0;
    let root = builder.visit(plan, root_fragment_id)?;

    Ok(DistributedPlan {
        fragments: vec![PlanFragment {
            fragment_id: root_fragment_id,
            root,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: plan.output_columns.clone(),
        }],
        root_fragment_id,
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::build_distributed_plan;
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::codegen::ir::DistributedPlanNodeBody;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        Operator, PhysicalFilterOp, PhysicalProjectOp, PhysicalScanOp,
    };
    use crate::sql::optimizer::physical_plan::{PhysicalPlanNode, PlanExecutionProps};
    use crate::sql::optimizer::statistics::Statistics;

    #[test]
    fn build_distributed_plan_scan_project_shapes_one_fragment() {
        let physical = scan_then_project_plan();
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        assert_eq!(dp.fragments.len(), 1);
        assert_eq!(dp.root_fragment_id, 0);
        let root = &dp.fragments[0].root;
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert!(matches!(root.body, DistributedPlanNodeBody::Project(_)));
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].node_id, 1);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
        assert!(matches!(
            root.children[0].body,
            DistributedPlanNodeBody::Scan(_)
        ));
    }

    #[test]
    fn build_distributed_plan_folds_filter_predicate_into_scan() {
        let physical = filter_then_project_plan();
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let root = &dp.fragments[0].root;
        assert!(matches!(root.body, DistributedPlanNodeBody::Project(_)));
        let DistributedPlanNodeBody::Scan(scan) = &root.children[0].body else {
            panic!("project child should be scan");
        };
        assert_eq!(scan.predicates.len(), 3);
        assert_binary_predicate(&scan.predicates[0], "k", BinOp::Eq, 7);
        assert_binary_predicate(&scan.predicates[1], "k", BinOp::Gt, 10);
        assert_binary_predicate(&scan.predicates[2], "v", BinOp::Lt, 20);
    }

    #[test]
    fn build_distributed_plan_folded_filter_uses_filter_stats() {
        let physical = project_plan(filter_plan_with_row_count(
            scan_plan_with_row_count(100.0),
            5.0,
        ));
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let folded_scan = &dp.fragments[0].root.children[0];

        assert!(matches!(folded_scan.body, DistributedPlanNodeBody::Scan(_)));
        assert_eq!(folded_scan.stats.output_row_count, 5.0);
    }

    #[test]
    fn build_distributed_plan_rejects_filter_over_project() {
        let physical = filter_over_project_plan();
        let err = build_distributed_plan(&physical).expect_err("filter over project should fail");
        assert_eq!(
            err,
            "build_distributed_plan slice 1: Filter child is not a Scan"
        );
    }

    fn scan_then_project_plan() -> PhysicalPlanNode {
        project_plan(scan_plan())
    }

    fn filter_then_project_plan() -> PhysicalPlanNode {
        project_plan(filter_plan(scan_plan()))
    }

    fn filter_over_project_plan() -> PhysicalPlanNode {
        filter_plan(project_plan(scan_plan()))
    }

    fn scan_plan() -> PhysicalPlanNode {
        scan_plan_with_row_count(3.0)
    }

    fn scan_plan_with_row_count(row_count: f64) -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        physical_node_with_row_count(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: table_def(),
                alias: Some("t".to_string()),
                columns: vec![k.clone(), v.clone()],
                predicates: vec![cmp_expr(
                    column_ref_expr(1, "k", DataType::Int64, false),
                    BinOp::Eq,
                    int_lit(7),
                )],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
            row_count,
        )
    }

    fn filter_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        filter_plan_with_row_count(child, 3.0)
    }

    fn filter_plan_with_row_count(child: PhysicalPlanNode, row_count: f64) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node_with_row_count(
            Operator::PhysicalFilter(PhysicalFilterOp {
                predicate: and_expr(
                    cmp_expr(
                        column_ref_expr(1, "k", DataType::Int64, false),
                        BinOp::Gt,
                        int_lit(10),
                    ),
                    cmp_expr(
                        column_ref_expr(2, "v", DataType::Int64, true),
                        BinOp::Lt,
                        int_lit(20),
                    ),
                ),
            }),
            vec![child],
            output_columns,
            row_count,
        )
    }

    fn project_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = vec![output_col(1, "k", DataType::Int64, false)];
        physical_node(
            Operator::PhysicalProject(PhysicalProjectOp {
                items: vec![ProjectItem {
                    expr: column_ref_expr(1, "k", DataType::Int64, false),
                    output_name: "k".to_string(),
                    output_column_id: ColumnId::new_for_test(1),
                }],
                output_qualifier: None,
            }),
            vec![child],
            output_columns,
        )
    }

    fn physical_node(
        op: Operator,
        children: Vec<PhysicalPlanNode>,
        output_columns: Vec<OutputColumn>,
    ) -> PhysicalPlanNode {
        physical_node_with_row_count(op, children, output_columns, 3.0)
    }

    fn physical_node_with_row_count(
        op: Operator,
        children: Vec<PhysicalPlanNode>,
        output_columns: Vec<OutputColumn>,
        row_count: f64,
    ) -> PhysicalPlanNode {
        PhysicalPlanNode {
            op,
            children,
            stats: stats(row_count),
            output_columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    fn stats(row_count: f64) -> Statistics {
        Statistics {
            output_row_count: row_count,
            ..Default::default()
        }
    }

    fn table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![
                column_def("k", DataType::Int64, false),
                column_def("v", DataType::Int64, true),
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 1,
                table_id: 2,
            },
        }
    }

    fn column_def(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn output_col(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn column_ref_expr(id: u32, column: &str, data_type: DataType, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: column.to_string(),
            },
            data_type,
            nullable,
        }
    }

    fn int_lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn cmp_expr(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
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

    fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn assert_binary_predicate(expr: &TypedExpr, column: &str, op: BinOp, value: i64) {
        let ExprKind::BinaryOp {
            left,
            op: actual_op,
            right,
        } = &expr.kind
        else {
            panic!("expected binary predicate, got {expr:?}");
        };
        assert_eq!(*actual_op, op);
        let ExprKind::ColumnRef {
            column: actual_column,
            ..
        } = &left.kind
        else {
            panic!("expected column ref left predicate, got {left:?}");
        };
        assert_eq!(actual_column, column);
        let ExprKind::Literal(LiteralValue::Int(actual_value)) = &right.kind else {
            panic!("expected int literal right predicate, got {right:?}");
        };
        assert_eq!(*actual_value, value);
    }
}
