#![allow(dead_code)]

use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{OutputColumn, TypedExpr};
use crate::sql::codegen::{FragmentEdge, FragmentId};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed_fragment::{DataPartition, DataSink};
use crate::sql::planner::distributed_node::{DistributedNode, DistributedPayload};
use crate::sql::planner::plan::{PhysicalPlanKind, PhysicalPlanNode};

#[derive(Clone, Debug)]
pub(crate) struct DistributedPlanV2 {
    pub fragments: Vec<PlanFragmentV2>,
    pub root_fragment_id: FragmentId,
    pub edges: Vec<FragmentEdge>,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanFragmentV2 {
    pub fragment_id: FragmentId,
    pub root: DistributedNode,
    pub data_partition: DataPartition,
    pub output_partition: DataPartition,
    pub sink: DataSink,
    pub output_exprs: Option<Vec<TypedExpr>>,
    pub output_columns: Vec<OutputColumn>,
    pub cte_id: Option<CteId>,
    pub cte_exchange_nodes: Vec<(CteId, i32, Vec<ColumnId>)>,
}

pub(crate) fn build_distributed_plan_v2(
    plan: &PhysicalPlanNode,
) -> Result<DistributedPlanV2, String> {
    let mut builder = DistributedPlanBuilderV2 {
        next_node_id: 1,
        next_tuple_id: 1,
        next_fragment_id: 0,
    };
    let root_fragment_id = builder.alloc_fragment_id();
    let root = builder.visit(plan, root_fragment_id)?;

    Ok(DistributedPlanV2 {
        fragments: vec![PlanFragmentV2 {
            fragment_id: root_fragment_id,
            root,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: plan.output_columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id,
        edges: Vec::new(),
    })
}

struct DistributedPlanBuilderV2 {
    next_node_id: i32,
    next_tuple_id: i32,
    next_fragment_id: FragmentId,
}

impl DistributedPlanBuilderV2 {
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

    fn alloc_fragment_id(&mut self) -> FragmentId {
        let fragment_id = self.next_fragment_id;
        self.next_fragment_id += 1;
        fragment_id
    }

    fn visit(
        &mut self,
        node: &PhysicalPlanNode,
        fragment_id: FragmentId,
    ) -> Result<DistributedNode, String> {
        match &node.kind {
            PhysicalPlanKind::Values(_) => {
                expect_child_count(node, 0)?;
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], Vec::new()))
            }
            PhysicalPlanKind::Scan(_) => {
                expect_child_count(node, 0)?;
                let node_id = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], Vec::new()))
            }
            PhysicalPlanKind::Project(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0], fragment_id)?;
                let node_id = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], vec![child]))
            }
            other => Err(format!(
                "build_distributed_plan_v2 does not handle PhysicalPlanKind::{} yet",
                physical_kind_name(other)
            )),
        }
    }

    fn make_node(
        &self,
        node: &PhysicalPlanNode,
        fragment_id: FragmentId,
        node_id: i32,
        tuple_ids: Vec<i32>,
        children: Vec<DistributedNode>,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids,
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children,
            stats: node.stats.clone(),
            payload: DistributedPayload::Physical(node.kind.clone()),
        }
    }
}

fn expect_child_count(node: &PhysicalPlanNode, expected: usize) -> Result<(), String> {
    if node.children.len() == expected {
        return Ok(());
    }

    Err(format!(
        "build_distributed_plan_v2: PhysicalPlanKind::{} expected {} children, got {}",
        physical_kind_name(&node.kind),
        expected,
        node.children.len()
    ))
}

fn physical_kind_name(kind: &PhysicalPlanKind) -> &'static str {
    match kind {
        PhysicalPlanKind::Scan(_) => "Scan",
        PhysicalPlanKind::Filter(_) => "Filter",
        PhysicalPlanKind::Project(_) => "Project",
        PhysicalPlanKind::Sort(_) => "Sort",
        PhysicalPlanKind::Limit(_) => "Limit",
        PhysicalPlanKind::Values(_) => "Values",
        PhysicalPlanKind::Decode(_) => "Decode",
        PhysicalPlanKind::Repeat(_) => "Repeat",
        PhysicalPlanKind::Window(_) => "Window",
        PhysicalPlanKind::GenerateSeries(_) => "GenerateSeries",
        PhysicalPlanKind::TableFunction(_) => "TableFunction",
        PhysicalPlanKind::AssertOneRow(_) => "AssertOneRow",
        PhysicalPlanKind::TopN(_) => "TopN",
        PhysicalPlanKind::HashAggregate(_) => "HashAggregate",
        PhysicalPlanKind::HashJoin(_) => "HashJoin",
        PhysicalPlanKind::NestLoopJoin(_) => "NestLoopJoin",
        PhysicalPlanKind::SetOp(_) => "SetOp",
        PhysicalPlanKind::ChangeEventExpand(_) => "ChangeEventExpand",
        PhysicalPlanKind::CTEAnchor(_) => "CTEAnchor",
        PhysicalPlanKind::CTEProduce(_) => "CTEProduce",
        PhysicalPlanKind::CTEConsume(_) => "CTEConsume",
        PhysicalPlanKind::Redistribute(_) => "Redistribute",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::DataType;

    use super::build_distributed_plan_v2;
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed_fragment::{DataSink, PartitionKind};
    use crate::sql::planner::distributed_node::DistributedPayload;
    use crate::sql::planner::plan::{
        PhysicalPlanKind, PhysicalPlanNode, PlanFilterNode, PlanProjectNode, PlanScanNode,
        PlanValuesNode,
    };
    use crate::sql::planner::{PhysicalPlanStats, PlannerConfidence};

    #[test]
    fn build_distributed_plan_v2_values_shapes_root_fragment() {
        let output_columns = vec![output_col(1, "k", DataType::Int64, false)];
        let plan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: output_columns.clone(),
            }),
            children: vec![],
            output_columns: output_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&plan).expect("build_distributed_plan_v2");

        assert_eq!(dp.fragments.len(), 1);
        assert_eq!(dp.root_fragment_id, 0);
        assert!(dp.edges.is_empty());

        let fragment = &dp.fragments[0];
        assert_eq!(fragment.fragment_id, 0);
        assert!(matches!(fragment.sink, DataSink::Result));
        assert!(matches!(
            fragment.data_partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert!(matches!(
            fragment.output_partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert!(fragment.output_exprs.is_none());
        assert_eq!(fragment.output_columns.len(), output_columns.len());
        assert_eq!(
            fragment.output_columns[0].column_id,
            output_columns[0].column_id
        );
        assert_eq!(fragment.output_columns[0].name, output_columns[0].name);
        assert!(fragment.cte_id.is_none());
        assert!(fragment.cte_exchange_nodes.is_empty());

        assert!(matches!(
            &fragment.root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Values(_))
        ));
        assert_eq!(fragment.root.node_id, 1);
        assert_eq!(fragment.root.tuple_ids, vec![1]);
        assert_eq!(fragment.root.fragment_id, 0);
        assert!(fragment.root.children.is_empty());
    }

    #[test]
    fn build_distributed_plan_v2_scan_project_shapes_one_fragment() {
        let scan_columns = vec![output_col(1, "k", DataType::Int64, false)];
        let project_columns = vec![output_col(2, "k_alias", DataType::Int64, false)];
        let scan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: table_def(),
                alias: Some("t".to_string()),
                columns: scan_columns.clone(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            output_columns: scan_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref_expr(1, "k", DataType::Int64, false),
                    output_name: "k_alias".to_string(),
                    output_column_id: ColumnId::new_for_test(2),
                }],
                output_qualifier: None,
            }),
            children: vec![scan],
            output_columns: project_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&project).expect("build_distributed_plan_v2");

        assert_eq!(dp.fragments.len(), 1);
        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Project(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert_eq!(root.children.len(), 1);

        let child = &root.children[0];
        assert!(matches!(
            &child.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Scan(_))
        ));
        assert_eq!(child.node_id, 1);
        assert_eq!(child.tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_v2_rejects_unsupported_filter_root() {
        let scan_columns = vec![output_col(1, "k", DataType::Int64, false)];
        let scan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: table_def(),
                alias: Some("t".to_string()),
                columns: scan_columns.clone(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            output_columns: scan_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let filter = PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: column_ref_expr(1, "k", DataType::Boolean, false),
            }),
            children: vec![scan],
            output_columns: scan_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err = build_distributed_plan_v2(&filter).expect_err("Filter is not supported in M3a1");

        assert!(
            err.contains("PhysicalPlanKind::Filter"),
            "unexpected error: {err}"
        );
        assert!(err.contains("does not handle"), "unexpected error: {err}");
    }

    #[test]
    fn build_distributed_plan_v2_rejects_project_without_child() {
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![],
                output_qualifier: None,
            }),
            children: vec![],
            output_columns: vec![output_col(2, "k_alias", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err =
            build_distributed_plan_v2(&project).expect_err("Project with 0 children is malformed");

        assert!(err.contains("Project"), "unexpected error: {err}");
        assert!(
            err.contains("expected 1 children"),
            "unexpected error: {err}"
        );
        assert!(err.contains("got 0"), "unexpected error: {err}");
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![column_def("k", DataType::Int64, false)],
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
}
