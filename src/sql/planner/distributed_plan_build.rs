#![allow(dead_code)]

use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{OutputColumn, TypedExpr};
use crate::sql::codegen::helpers::{group_win_exprs_by_sig, split_and_conjuncts_typed};
use crate::sql::codegen::{FragmentEdge, FragmentId};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::property::OrderingSpec;
use crate::sql::planner::distributed_fragment::{DataPartition, DataSink};
use crate::sql::planner::distributed_node::{DistributedNode, DistributedPayload};
use crate::sql::planner::optimizer_bridge::property::{
    ordering_spec_from_sort_items, window_ordering_spec,
};
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
            PhysicalPlanKind::Filter(filter) => {
                expect_child_count(node, 1)?;
                let mut child = self.visit(&node.children[0], fragment_id)?;
                if let DistributedPayload::Physical(PhysicalPlanKind::Scan(scan)) =
                    &mut child.payload
                {
                    scan.predicates.extend(
                        split_and_conjuncts_typed(&filter.predicate)
                            .into_iter()
                            .cloned(),
                    );
                    child.stats = node.stats.clone();
                    Ok(child)
                } else {
                    let node_id = self.alloc_node();
                    let tuple_ids = child.tuple_ids.clone();
                    Ok(self.make_node(node, fragment_id, node_id, tuple_ids, vec![child]))
                }
            }
            PhysicalPlanKind::Sort(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0], fragment_id)?;
                let node_id = self.alloc_node();
                let tuple_ids = child.tuple_ids.clone();
                Ok(self.make_node(node, fragment_id, node_id, tuple_ids, vec![child]))
            }
            PhysicalPlanKind::HashAggregate(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0], fragment_id)?;
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], vec![child]))
            }
            PhysicalPlanKind::HashJoin(_) => {
                expect_child_count(node, 2)?;
                let left = self.visit(&node.children[0], fragment_id)?;
                let right = self.visit(&node.children[1], fragment_id)?;
                let node_id = self.alloc_node();
                let mut tuple_ids = left.tuple_ids.clone();
                tuple_ids.extend(right.tuple_ids.iter().copied());
                Ok(self.make_node(node, fragment_id, node_id, tuple_ids, vec![left, right]))
            }
            PhysicalPlanKind::NestLoopJoin(_) => {
                expect_child_count(node, 2)?;
                let left = self.visit(&node.children[0], fragment_id)?;
                let right = self.visit(&node.children[1], fragment_id)?;
                let node_id = self.alloc_node();
                let mut tuple_ids = left.tuple_ids.clone();
                tuple_ids.extend(right.tuple_ids.iter().copied());
                Ok(self.make_node(node, fragment_id, node_id, tuple_ids, vec![left, right]))
            }
            PhysicalPlanKind::AssertOneRow(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0], fragment_id)?;
                let node_id = self.alloc_node();
                let tuple_ids = child.tuple_ids.clone();
                Ok(self.make_node(node, fragment_id, node_id, tuple_ids, vec![child]))
            }
            PhysicalPlanKind::Decode(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0], fragment_id)?;
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], vec![child]))
            }
            PhysicalPlanKind::Repeat(repeat) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0], fragment_id)?;
                let node_id = self.alloc_node();
                let virtual_tuple_id = self.alloc_tuple();
                let mut tuple_ids = child.tuple_ids.clone();
                if !repeat.grouping_fn_args.is_empty() {
                    tuple_ids.push(virtual_tuple_id);
                }
                let mut payload = repeat.clone();
                payload.virtual_tuple_id = Some(virtual_tuple_id);
                Ok(self.make_node_with_payload(
                    node,
                    fragment_id,
                    node_id,
                    tuple_ids,
                    vec![child],
                    PhysicalPlanKind::Repeat(payload),
                ))
            }
            PhysicalPlanKind::Window(window) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0], fragment_id)?;
                let groups = group_win_exprs_by_sig(&window.window_exprs);
                if groups.is_empty() {
                    return Err(
                        "build_distributed_plan_v2: PhysicalWindow has no window expressions"
                            .to_string(),
                    );
                }

                let mut first_node_id = None;
                let mut tuple_ids = child.tuple_ids.clone();
                let mut current_ordering = distributed_node_ordering(&child);
                for group_indices in &groups {
                    let Some(first_idx) = group_indices.first().copied() else {
                        continue;
                    };
                    let first_win = &window.window_exprs[first_idx];
                    if groups.len() > 1 {
                        let required_ordering =
                            window_ordering_spec(&first_win.partition_by, &first_win.order_by);
                        let has_sort_keys =
                            !first_win.partition_by.is_empty() || !first_win.order_by.is_empty();
                        let ordering_is_representable =
                            !matches!(required_ordering, OrderingSpec::Any);
                        let needs_sort = has_sort_keys
                            && (!ordering_is_representable
                                || !current_ordering.satisfies(&required_ordering));
                        if needs_sort {
                            let sort_node_id = self.alloc_node();
                            first_node_id.get_or_insert(sort_node_id);
                            current_ordering = required_ordering;
                        }
                    }
                    let analytic_node_id = self.alloc_node();
                    first_node_id.get_or_insert(analytic_node_id);
                    let _ = self.alloc_tuple();
                    let output_tuple_id = self.alloc_tuple();
                    tuple_ids.push(output_tuple_id);
                }

                let node_id = first_node_id.ok_or_else(|| {
                    "build_distributed_plan_v2: PhysicalWindow produced no thrift node".to_string()
                })?;
                Ok(self.make_node_with_payload(
                    node,
                    fragment_id,
                    node_id,
                    tuple_ids,
                    vec![child],
                    PhysicalPlanKind::Window(window.clone()),
                ))
            }
            PhysicalPlanKind::ChangeEventExpand(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0], fragment_id)?;
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], vec![child]))
            }
            PhysicalPlanKind::GenerateSeries(_) => {
                expect_child_count(node, 0)?;
                let _ = self.alloc_tuple();
                let _ = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], Vec::new()))
            }
            PhysicalPlanKind::TableFunction(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0], fragment_id)?;
                let _ = self.alloc_tuple();
                let _ = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
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
        self.make_node_with_payload(
            node,
            fragment_id,
            node_id,
            tuple_ids,
            children,
            node.kind.clone(),
        )
    }

    fn make_node_with_payload(
        &self,
        node: &PhysicalPlanNode,
        fragment_id: FragmentId,
        node_id: i32,
        tuple_ids: Vec<i32>,
        children: Vec<DistributedNode>,
        payload: PhysicalPlanKind,
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
            payload: DistributedPayload::Physical(payload),
        }
    }
}

fn distributed_node_ordering(node: &DistributedNode) -> OrderingSpec {
    match &node.payload {
        DistributedPayload::Physical(PhysicalPlanKind::Sort(sort)) => {
            ordering_spec_from_sort_items(&sort.items)
        }
        DistributedPayload::Physical(PhysicalPlanKind::TopN(topn)) => {
            ordering_spec_from_sort_items(&topn.items)
        }
        DistributedPayload::Physical(PhysicalPlanKind::AssertOneRow(_)) => node
            .children
            .first()
            .map(distributed_node_ordering)
            .unwrap_or(OrderingSpec::Any),
        DistributedPayload::Physical(PhysicalPlanKind::Window(window)) => {
            let mut current_ordering = node
                .children
                .first()
                .map(distributed_node_ordering)
                .unwrap_or(OrderingSpec::Any);
            let groups = group_win_exprs_by_sig(&window.window_exprs);
            for group_indices in &groups {
                let Some(first_idx) = group_indices.first().copied() else {
                    continue;
                };
                let first_win = &window.window_exprs[first_idx];
                if groups.len() > 1 {
                    let required_ordering =
                        window_ordering_spec(&first_win.partition_by, &first_win.order_by);
                    let has_sort_keys =
                        !first_win.partition_by.is_empty() || !first_win.order_by.is_empty();
                    let ordering_is_representable = !matches!(required_ordering, OrderingSpec::Any);
                    let needs_sort = has_sort_keys
                        && (!ordering_is_representable
                            || !current_ordering.satisfies(&required_ordering));
                    if needs_sort {
                        current_ordering = required_ordering;
                    }
                }
            }
            current_ordering
        }
        _ => OrderingSpec::Any,
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
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, SortItem, TypedExpr,
    };
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{AggMode, AggregateOutputLayout, JoinDistribution};
    use crate::sql::planner::distributed_fragment::{DataSink, PartitionKind};
    use crate::sql::planner::distributed_node::DistributedPayload;
    use crate::sql::planner::plan::{
        DistributedChangeEventExpandNode, PhysicalHashAggregateNode, PhysicalHashJoinNode,
        PhysicalNestLoopJoinNode, PhysicalPlanKind, PhysicalPlanNode, PlanAssertOneRowNode,
        PlanDecodeNode, PlanFilterNode, PlanGenerateSeriesNode, PlanProjectNode, PlanRepeatNode,
        PlanScanNode, PlanSortNode, PlanTableFunctionNode, PlanValuesNode, PlanWindowNode,
        RedistributeMode, RedistributeNode, WindowExpr,
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
    fn build_distributed_plan_v2_folds_filter_predicate_into_scan() {
        let scan_columns = vec![output_col(1, "k", DataType::Int64, false)];
        let project_columns = vec![output_col(2, "k_alias", DataType::Int64, false)];
        let scan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: table_def(),
                alias: Some("t".to_string()),
                columns: scan_columns.clone(),
                predicates: vec![bool_lit(true)],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            output_columns: scan_columns.clone(),
            stats: stats_with_row_count(100.0),
            probe_runtime_filters: vec![],
        };
        let filter = PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: and_expr(
                    cmp_expr(1, "k", BinOp::Gt, 10),
                    cmp_expr(1, "k", BinOp::Lt, 20),
                ),
            }),
            children: vec![scan],
            output_columns: scan_columns,
            stats: stats_with_row_count(5.0),
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
            children: vec![filter],
            output_columns: project_columns,
            stats: stats_with_row_count(5.0),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&project).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Project(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert_eq!(root.children.len(), 1);
        let child = &root.children[0];
        let scan = match &child.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Scan(scan)) => scan,
            other => panic!("expected folded Scan child, got {other:?}"),
        };
        assert_eq!(child.node_id, 1);
        assert_eq!(child.tuple_ids, vec![1]);
        assert_eq!(scan.predicates.len(), 3);
        assert_bool_lit(&scan.predicates[0], true);
        assert_cmp_expr(&scan.predicates[1], 1, "k", BinOp::Gt, 10);
        assert_cmp_expr(&scan.predicates[2], 1, "k", BinOp::Lt, 20);
        assert_eq!(child.stats.output_row_count, 5.0);
    }

    #[test]
    fn build_distributed_plan_v2_preserves_filter_over_project() {
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
            stats: stats_with_row_count(100.0),
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
            output_columns: project_columns.clone(),
            stats: stats_with_row_count(10.0),
            probe_runtime_filters: vec![],
        };
        let filter = PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: cmp_expr(2, "k_alias", BinOp::Gt, 10),
            }),
            children: vec![project],
            output_columns: project_columns,
            stats: stats_with_row_count(5.0),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&filter).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        let root_filter = match &root.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Filter(filter)) => filter,
            other => panic!("expected Filter root, got {other:?}"),
        };
        assert_cmp_expr(&root_filter.predicate, 2, "k_alias", BinOp::Gt, 10);
        assert_eq!(root.node_id, 3);
        assert_eq!(root.tuple_ids, vec![2]);
        assert_eq!(root.stats.output_row_count, 5.0);
        assert_eq!(root.children.len(), 1);

        let child = &root.children[0];
        assert!(matches!(
            &child.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Project(_))
        ));
        assert_eq!(child.node_id, 2);
        assert_eq!(child.tuple_ids, vec![2]);
    }

    #[test]
    fn build_distributed_plan_v2_sort_reuses_child_tuple() {
        let scan = scan_node(1, "k");
        let sort = PhysicalPlanNode {
            kind: PhysicalPlanKind::Sort(PlanSortNode {
                items: vec![],
                analytic_partition_by: vec![],
                output_columns: scan.output_columns.clone(),
                offset: None,
                partition_limit: None,
                topn_type: None,
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&sort).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Sort(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1]);
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].node_id, 1);
        assert!(matches!(
            &root.children[0].payload,
            DistributedPayload::Physical(PhysicalPlanKind::Scan(_))
        ));
    }

    #[test]
    fn build_distributed_plan_v2_hash_aggregate_allocates_new_tuple() {
        let scan = scan_node(1, "k");
        let aggregate = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
                mode: AggMode::Single,
                group_by: vec![],
                aggregates: vec![],
                is_merge: vec![],
                output_layout: AggregateOutputLayout::new(vec![], vec![]),
                output_columns: vec![],
            })),
            children: vec![scan],
            output_columns: vec![],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&aggregate).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::HashAggregate(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_v2_hash_join_combines_child_tuples() {
        let left = scan_node(1, "l_k");
        let right = scan_node(2, "r_k");
        let output_columns = vec![
            output_col(1, "l_k", DataType::Int64, false),
            output_col(2, "r_k", DataType::Int64, false),
        ];
        let join = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: vec![],
                other_condition: None,
                distribution: JoinDistribution::Unknown,
                execution_mode: None,
                build_runtime_filters: vec![],
            })),
            children: vec![left, right],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&join).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::HashJoin(_))
        ));
        assert_eq!(root.node_id, 3);
        assert_eq!(root.tuple_ids, vec![1, 2]);
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[0].node_id, 1);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
        assert_eq!(root.children[1].node_id, 2);
        assert_eq!(root.children[1].tuple_ids, vec![2]);
    }

    #[test]
    fn build_distributed_plan_v2_nest_loop_join_combines_child_tuples() {
        let left = scan_node(1, "l_k");
        let right = scan_node(2, "r_k");
        let join = PhysicalPlanNode {
            kind: PhysicalPlanKind::NestLoopJoin(PhysicalNestLoopJoinNode {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            children: vec![left, right],
            output_columns: vec![
                output_col(1, "l_k", DataType::Int64, false),
                output_col(2, "r_k", DataType::Int64, false),
            ],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&join).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::NestLoopJoin(_))
        ));
        assert_eq!(root.node_id, 3);
        assert_eq!(root.tuple_ids, vec![1, 2]);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
        assert_eq!(root.children[1].tuple_ids, vec![2]);
    }

    #[test]
    fn build_distributed_plan_v2_assert_one_row_reuses_child_tuple() {
        let scan = scan_node(1, "k");
        let assert_one_row = PhysicalPlanNode {
            kind: PhysicalPlanKind::AssertOneRow(PlanAssertOneRowNode {
                subquery_text: "select k from t".to_string(),
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&assert_one_row).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::AssertOneRow(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1]);
        assert_eq!(root.children[0].node_id, 1);
    }

    #[test]
    fn build_distributed_plan_v2_decode_and_change_event_expand_allocate_new_tuple() {
        let scan = scan_node(1, "k");
        let decode = PhysicalPlanNode {
            kind: PhysicalPlanKind::Decode(PlanDecodeNode {
                mappings: vec![],
                output_columns: vec![output_col(2, "decoded", DataType::Utf8, false)],
            }),
            children: vec![scan],
            output_columns: vec![output_col(2, "decoded", DataType::Utf8, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&decode).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Decode(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert_eq!(root.children[0].tuple_ids, vec![1]);

        let scan = scan_node(1, "k");
        let expand = PhysicalPlanNode {
            kind: PhysicalPlanKind::ChangeEventExpand(DistributedChangeEventExpandNode {
                events: vec![],
                output_columns: vec![
                    output_col(2, "payload", DataType::Int64, false),
                    output_col(3, "change_op", DataType::Int64, false),
                ],
                change_op_column_id: ColumnId::new_for_test(3),
                data_route_column_id: None,
            }),
            children: vec![scan],
            output_columns: vec![
                output_col(2, "payload", DataType::Int64, false),
                output_col(3, "change_op", DataType::Int64, false),
            ],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&expand).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::ChangeEventExpand(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_v2_repeat_appends_virtual_tuple_only_when_grouping_fn_args_present() {
        let scan = scan_node(1, "k");
        let repeat = PhysicalPlanNode {
            kind: PhysicalPlanKind::Repeat(repeat_node(false)),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&repeat).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        let repeat = match &root.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Repeat(repeat)) => repeat,
            other => panic!("expected Repeat root, got {other:?}"),
        };
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1]);
        assert_eq!(repeat.virtual_tuple_id, Some(2));

        let scan = scan_node(1, "k");
        let repeat = PhysicalPlanNode {
            kind: PhysicalPlanKind::Repeat(repeat_node(true)),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&repeat).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        let repeat = match &root.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Repeat(repeat)) => repeat,
            other => panic!("expected Repeat root, got {other:?}"),
        };
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1, 2]);
        assert_eq!(repeat.virtual_tuple_id, Some(2));
    }

    #[test]
    fn build_distributed_plan_v2_generate_series_replicates_dummy_allocations() {
        let output_columns = vec![output_col(1, "x", DataType::Int64, false)];
        let generate_series = PhysicalPlanNode {
            kind: PhysicalPlanKind::GenerateSeries(PlanGenerateSeriesNode {
                start: 1,
                end: 3,
                step: 1,
                column_name: "x".to_string(),
                alias: None,
                output_column_id: ColumnId::new_for_test(1),
            }),
            children: vec![],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&generate_series).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::GenerateSeries(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert!(root.children.is_empty());
    }

    #[test]
    fn build_distributed_plan_v2_table_function_replicates_dummy_allocations() {
        let scan = scan_node(1, "k");
        let output_columns = vec![output_col(2, "item", DataType::Int64, false)];
        let table_function = PhysicalPlanNode {
            kind: PhysicalPlanKind::TableFunction(PlanTableFunctionNode {
                function_name: "unnest".to_string(),
                args: vec![],
                output_columns: output_columns.clone(),
                alias: None,
                is_left_join: false,
            }),
            children: vec![scan],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&table_function).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::TableFunction(_))
        ));
        assert_eq!(root.node_id, 3);
        assert_eq!(root.tuple_ids, vec![3]);
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].node_id, 1);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_v2_window_single_group_allocates_analytic_ids() {
        let scan = scan_node(1, "k");
        let rn = output_col(2, "rn", DataType::Int64, false);
        let output_columns = vec![output_col(1, "k", DataType::Int64, false), rn.clone()];
        let window = PhysicalPlanNode {
            kind: PhysicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![window_expr(rn, vec![], vec![])],
                output_columns: output_columns.clone(),
            }),
            children: vec![scan],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&window).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Window(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1, 3]);
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].node_id, 1);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_v2_window_multi_group_allocates_sort_when_ordering_changes() {
        let scan = scan_node_with_columns(vec![
            output_col(1, "k", DataType::Int64, false),
            output_col(2, "v", DataType::Int64, true),
        ]);
        let rn_by_k = output_col(3, "rn_by_k", DataType::Int64, false);
        let rn_by_v = output_col(4, "rn_by_v", DataType::Int64, false);
        let output_columns = vec![
            output_col(1, "k", DataType::Int64, false),
            output_col(2, "v", DataType::Int64, true),
            rn_by_k.clone(),
            rn_by_v.clone(),
        ];
        let window = PhysicalPlanNode {
            kind: PhysicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![
                    window_expr(
                        rn_by_k,
                        vec![],
                        vec![sort_item(column_ref_expr(1, "k", DataType::Int64, false))],
                    ),
                    window_expr(
                        rn_by_v,
                        vec![],
                        vec![sort_item(column_ref_expr(2, "v", DataType::Int64, true))],
                    ),
                ],
                output_columns: output_columns.clone(),
            }),
            children: vec![scan],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let project_columns = vec![output_col(5, "rn_alias", DataType::Int64, false)];
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref_expr(4, "rn_by_v", DataType::Int64, false),
                    output_name: "rn_alias".to_string(),
                    output_column_id: ColumnId::new_for_test(5),
                }],
                output_qualifier: None,
            }),
            children: vec![window],
            output_columns: project_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan_v2(&project).expect("build_distributed_plan_v2");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Project(_))
        ));
        assert_eq!(root.node_id, 6);
        assert_eq!(root.tuple_ids, vec![6]);
        assert_eq!(root.children.len(), 1);
        let window = &root.children[0];
        assert!(matches!(
            &window.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Window(_))
        ));
        assert_eq!(window.node_id, 2);
        assert_eq!(window.tuple_ids, vec![1, 3, 5]);
        assert_eq!(window.children.len(), 1);
        assert_eq!(window.children[0].node_id, 1);
        assert_eq!(window.children[0].tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_v2_window_rejects_empty_window_exprs() {
        let scan = scan_node(1, "k");
        let window = PhysicalPlanNode {
            kind: PhysicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![],
                output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err =
            build_distributed_plan_v2(&window).expect_err("empty Window expressions are invalid");

        assert!(
            err.contains("PhysicalWindow has no window expressions"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn build_distributed_plan_v2_rejects_unsupported_redistribute_root() {
        let scan = scan_node(1, "k");
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Gather,
                output_columns: scan.output_columns.clone(),
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err = build_distributed_plan_v2(&redistribute)
            .expect_err("Redistribute is not supported in M3a3");

        assert!(
            err.contains("PhysicalPlanKind::Redistribute"),
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
        stats_with_row_count(0.0)
    }

    fn stats_with_row_count(output_row_count: f64) -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count,
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

    fn scan_node(column_id: u32, column_name: &str) -> PhysicalPlanNode {
        let scan_columns = vec![output_col(column_id, column_name, DataType::Int64, false)];
        scan_node_with_columns(scan_columns)
    }

    fn scan_node_with_columns(scan_columns: Vec<OutputColumn>) -> PhysicalPlanNode {
        PhysicalPlanNode {
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
            output_columns: scan_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        }
    }

    fn window_expr(
        output_column: OutputColumn,
        partition_by: Vec<TypedExpr>,
        order_by: Vec<SortItem>,
    ) -> WindowExpr {
        WindowExpr {
            name: "row_number".to_string(),
            args: vec![],
            distinct: false,
            partition_by,
            order_by,
            window_frame: None,
            result_type: output_column.data_type,
            output_name: output_column.name,
            output_column_id: output_column.column_id,
            ignore_nulls: false,
        }
    }

    fn sort_item(expr: TypedExpr) -> SortItem {
        SortItem {
            expr,
            asc: true,
            nulls_first: false,
        }
    }

    fn repeat_node(with_grouping_fn_arg: bool) -> PlanRepeatNode {
        let grouping_fn_args = if with_grouping_fn_arg {
            vec![("grouping_k".to_string(), vec!["k".to_string()])]
        } else {
            vec![]
        };
        let grouping_fn_arg_ids = if with_grouping_fn_arg {
            vec![vec![ColumnId::new_for_test(1)]]
        } else {
            vec![]
        };
        let grouping_fn_ids = if with_grouping_fn_arg {
            vec![("grouping_k".to_string(), ColumnId::new_for_test(2))]
        } else {
            vec![]
        };

        PlanRepeatNode {
            repeat_column_ref_list: vec![],
            repeat_column_ref_ids: vec![],
            grouping_ids: vec![],
            all_rollup_columns: vec![],
            all_rollup_column_ids: vec![],
            grouping_key_aliases: vec![],
            grouping_fn_args,
            grouping_fn_arg_ids,
            grouping_fn_ids,
            virtual_tuple_id: None,
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

    fn bool_lit(value: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(value)),
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn int_lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn cmp_expr(column_id: u32, column: &str, op: BinOp, value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(column_ref_expr(column_id, column, DataType::Int64, false)),
                op,
                right: Box::new(int_lit(value)),
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

    fn assert_bool_lit(expr: &TypedExpr, expected: bool) {
        match &expr.kind {
            ExprKind::Literal(LiteralValue::Bool(value)) => assert_eq!(*value, expected),
            other => panic!("expected Bool literal, got {other:?}"),
        }
    }

    fn assert_cmp_expr(
        expr: &TypedExpr,
        expected_column_id: u32,
        expected_column: &str,
        expected_op: BinOp,
        expected_value: i64,
    ) {
        let (left, op, right) = match &expr.kind {
            ExprKind::BinaryOp { left, op, right } => (left, op, right),
            other => panic!("expected comparison expression, got {other:?}"),
        };
        assert_eq!(*op, expected_op);
        match &left.kind {
            ExprKind::ColumnRef {
                column_id, column, ..
            } => {
                assert_eq!(*column_id, ColumnId::new_for_test(expected_column_id));
                assert_eq!(column, expected_column);
            }
            other => panic!("expected column ref, got {other:?}"),
        }
        match &right.kind {
            ExprKind::Literal(LiteralValue::Int(value)) => assert_eq!(*value, expected_value),
            other => panic!("expected Int literal, got {other:?}"),
        }
    }
}
