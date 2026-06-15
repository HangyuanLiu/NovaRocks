use crate::sql::analysis::TypedExpr;
use crate::sql::codegen::helpers::split_and_conjuncts_typed;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;

use super::FragmentId;
use super::body::{ProjectBody, ScanBody};
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
                    children: vec![child],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: DistributedPlanNodeBody::Project(ProjectBody {
                        items: op.items.clone(),
                        output_qualifier: op.output_qualifier.clone(),
                    }),
                })
            }
            other => Err(format!(
                "build_distributed_plan slice 1 does not handle operator {other:?}"
            )),
        }
    }
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
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        physical_node(
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
        )
    }

    fn filter_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
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
        PhysicalPlanNode {
            op,
            children,
            stats: stats(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    fn stats() -> Statistics {
        Statistics {
            output_row_count: 3.0,
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
