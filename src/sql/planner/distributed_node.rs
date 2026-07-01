use std::collections::HashMap;

use crate::sql::codegen::FragmentId;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::cost::BroadcastDecision;
use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence, CostEstimate, Statistics};
use crate::sql::planner::plan::{
    DistributedExchangeNode, PhysicalHashAggregateNode, PhysicalHashJoinNode,
    PhysicalNestLoopJoinNode, PhysicalSetOpNode, PhysicalTopNNode, PlanAssertOneRowNode,
    PlanDecodeNode, PlanFilterNode, PlanGenerateSeriesNode, PlanProjectNode, PlanRepeatNode,
    PlanScanNode, PlanSortNode, PlanTableFunctionNode, PlanValuesNode, PlanWindowNode,
};

/// Migration-only lowering kind for the existing DistributedPlan node tree.
///
/// PIR-4/PIR-5 must decide whether this remains a thin fragment overlay or is
/// collapsed into planner physical plan plus fragment annotations. It must not
/// be used as a replacement public logical/physical taxonomy.
#[derive(Clone, Debug)]
pub(crate) enum DistributedPlanKind {
    Scan(PlanScanNode),
    Filter(PlanFilterNode),
    Project(PlanProjectNode),
    Sort(PlanSortNode),
    Values(PlanValuesNode),
    Decode(PlanDecodeNode),
    Repeat(PlanRepeatNode),
    Window(PlanWindowNode),
    GenerateSeries(PlanGenerateSeriesNode),
    TableFunction(PlanTableFunctionNode),
    AssertOneRow(PlanAssertOneRowNode),
    TopN(PhysicalTopNNode),
    Exchange(DistributedExchangeNode),
    HashAggregate(Box<PhysicalHashAggregateNode>),
    HashJoin(Box<PhysicalHashJoinNode>),
    NestLoopJoin(PhysicalNestLoopJoinNode),
    SetOp(PhysicalSetOpNode),
}

impl DistributedPlanKind {
    pub(crate) fn variant_name(&self) -> &'static str {
        match self {
            DistributedPlanKind::Scan(_) => "Scan",
            DistributedPlanKind::Filter(_) => "Filter",
            DistributedPlanKind::Project(_) => "Project",
            DistributedPlanKind::Sort(_) => "Sort",
            DistributedPlanKind::Values(_) => "Values",
            DistributedPlanKind::Decode(_) => "Decode",
            DistributedPlanKind::Repeat(_) => "Repeat",
            DistributedPlanKind::Window(_) => "Window",
            DistributedPlanKind::GenerateSeries(_) => "GenerateSeries",
            DistributedPlanKind::TableFunction(_) => "TableFunction",
            DistributedPlanKind::AssertOneRow(_) => "AssertOneRow",
            DistributedPlanKind::TopN(_) => "TopN",
            DistributedPlanKind::Exchange(_) => "Exchange",
            DistributedPlanKind::HashAggregate(_) => "HashAggregate",
            DistributedPlanKind::HashJoin(_) => "HashJoin",
            DistributedPlanKind::NestLoopJoin(_) => "NestLoopJoin",
            DistributedPlanKind::SetOp(_) => "SetOp",
        }
    }
}

/// Self-contained copy of the estimated stats this node carries, so EXPLAIN /
/// ANALYZE never reaches back into `OptimizerPhysicalNode`.
#[derive(Clone, Debug)]
pub(crate) struct PlanNodeStats {
    pub output_row_count: f64,
    pub row_count_confidence: Confidence,
    pub column_statistics: HashMap<ColumnId, ColumnStatistic>,
    pub cost_estimate: Option<CostEstimate>,
    pub broadcast_decision: Option<BroadcastDecision>,
}

impl PlanNodeStats {
    pub fn from_statistics(stats: &Statistics) -> Self {
        Self::from_statistics_with_cost(stats, None)
    }

    pub fn from_statistics_with_cost(
        stats: &Statistics,
        cost_estimate: Option<CostEstimate>,
    ) -> Self {
        Self::from_statistics_with_cost_and_broadcast(stats, cost_estimate, None)
    }

    pub fn from_statistics_with_cost_and_broadcast(
        stats: &Statistics,
        cost_estimate: Option<CostEstimate>,
        broadcast_decision: Option<BroadcastDecision>,
    ) -> Self {
        Self {
            output_row_count: stats.output_row_count,
            row_count_confidence: stats.row_count_confidence,
            column_statistics: stats.column_statistics.clone(),
            cost_estimate,
            broadcast_decision,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DistributedPlanNode {
    /// Allocated once in Pass 1; never reallocated. In a thrift-lowered
    /// fragment most DistributedPlanNode kinds produce exactly one TPlanNode.
    /// Multi-node kinds use this as the root emitted TPlanNode id.
    pub node_id: i32,
    pub fragment_id: FragmentId,
    /// Output tuples (thrift `row_tuples`). Allocated in Pass 1.
    pub tuple_ids: Vec<i32>,
    /// Subset of `tuple_ids` widened to nullable (outer-join side). Empty here.
    pub nullable_tuple_ids: Vec<i32>,
    /// -1 == no limit.
    pub limit: i64,
    pub execution_join_distribution:
        Option<crate::sql::optimizer::physical_tree::JoinExecutionDistribution>,
    pub build_runtime_filters: Vec<crate::sql::optimizer::runtime_filter_pass::RuntimeFilterDesc>,
    pub probe_runtime_filters: Vec<crate::sql::optimizer::runtime_filter_pass::RuntimeFilterProbe>,
    pub children: Vec<DistributedPlanNode>,
    pub stats: PlanNodeStats,
    pub kind: DistributedPlanKind,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence, Statistics};
    use crate::sql::planner::plan::PlanValuesNode;

    #[test]
    fn plan_node_stats_copies_statistics() {
        let column_id = ColumnId::new_for_test(11);
        let mut stats = Statistics {
            output_row_count: 7.0,
            ..Default::default()
        };
        stats.column_statistics.insert(
            column_id,
            ColumnStatistic {
                ..ColumnStatistic::for_test_with_ndv(3.0, Confidence::Exact)
            },
        );

        let s = PlanNodeStats::from_statistics(&stats);
        assert_eq!(s.output_row_count, 7.0);
        assert_eq!(
            s.column_statistics[&column_id].ndv_or_legacy_unknown_sentinel_for_test(),
            3.0
        );
    }

    #[test]
    fn plan_node_stats_can_carry_cost_estimate() {
        let stats = Statistics {
            output_row_count: 7.0,
            ..Default::default()
        };
        let cost = crate::sql::optimizer::statistics::CostEstimate {
            cpu_cost: 1.0,
            memory_cost: 2.0,
            network_cost: 3.0,
        };
        let s = PlanNodeStats::from_statistics_with_cost(&stats, Some(cost.clone()));

        assert_eq!(s.cost_estimate.unwrap().network_cost, 3.0);
    }

    #[test]
    fn distributed_plan_node_exposes_kind_and_children_uniformly() {
        let node = DistributedPlanNode {
            node_id: 1,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: vec![],
            limit: -1,
            execution_join_distribution: None,
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
            children: vec![],
            stats: PlanNodeStats::from_statistics(&Statistics::default()),
            kind: DistributedPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            }),
        };

        assert!(matches!(node.kind, DistributedPlanKind::Values(_)));
        assert!(node.children.is_empty());
    }

    #[test]
    fn distributed_plan_node_uses_migration_only_kind() {
        fn accepts_distributed_kind(_: &DistributedPlanKind) {}

        let node = DistributedPlanNode {
            node_id: 1,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: vec![],
            limit: -1,
            execution_join_distribution: None,
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
            children: vec![],
            stats: PlanNodeStats::from_statistics(&Statistics::default()),
            kind: DistributedPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            }),
        };

        accepts_distributed_kind(&node.kind);
    }
}
