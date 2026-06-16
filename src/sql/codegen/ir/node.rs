use std::collections::HashMap;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence, Statistics};

use super::FragmentId;
use super::kind::{
    DistributedAssertOneRowNode, DistributedDecodeNode, DistributedExchangeNode,
    DistributedFilterNode, DistributedGenerateSeriesNode, DistributedHashAggregateNode,
    DistributedHashJoinNode, DistributedNestLoopJoinNode, DistributedProjectNode,
    DistributedRepeatNode, DistributedScanNode, DistributedSetOpNode, DistributedSortNode,
    DistributedTableFunctionNode, DistributedTopNNode, DistributedValuesNode,
    DistributedWindowNode,
};

/// Self-contained copy of the estimated stats this node carries, so EXPLAIN /
/// ANALYZE never reach back into `PhysicalPlanNode`.
#[derive(Clone, Debug)]
pub(crate) struct PlanNodeStats {
    pub output_row_count: f64,
    pub row_count_confidence: Confidence,
    pub column_statistics: HashMap<ColumnId, ColumnStatistic>,
}

impl PlanNodeStats {
    pub fn from_statistics(stats: &Statistics) -> Self {
        Self {
            output_row_count: stats.output_row_count,
            row_count_confidence: stats.row_count_confidence,
            column_statistics: stats.column_statistics.clone(),
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
        Option<crate::sql::optimizer::physical_plan::JoinExecutionDistribution>,
    pub build_runtime_filters: Vec<crate::sql::optimizer::runtime_filter_pass::RuntimeFilterDesc>,
    pub probe_runtime_filters: Vec<crate::sql::optimizer::runtime_filter_pass::RuntimeFilterProbe>,
    pub children: Vec<DistributedPlanNode>,
    pub stats: PlanNodeStats,
    pub kind: DistributedPlanNodeKind,
}

/// Operator-specific payload. Grows one variant per operator as slices land.
#[derive(Clone, Debug)]
pub(crate) enum DistributedPlanNodeKind {
    Scan(Box<DistributedScanNode>),
    Project(DistributedProjectNode),
    Filter(DistributedFilterNode),
    Sort(DistributedSortNode),
    TopN(DistributedTopNNode),
    Exchange(DistributedExchangeNode),
    HashAggregate(Box<DistributedHashAggregateNode>),
    HashJoin(Box<DistributedHashJoinNode>),
    NestLoopJoin(DistributedNestLoopJoinNode),
    Values(DistributedValuesNode),
    AssertOneRow(DistributedAssertOneRowNode),
    Decode(DistributedDecodeNode),
    Repeat(Box<DistributedRepeatNode>),
    SetOp(DistributedSetOpNode),
    Window(Box<DistributedWindowNode>),
    GenerateSeries(DistributedGenerateSeriesNode),
    TableFunction(Box<DistributedTableFunctionNode>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::statistics::{ColumnStatistic, Statistics};

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
                distinct_values_count: 3.0,
                ..Default::default()
            },
        );

        let s = PlanNodeStats::from_statistics(&stats);
        assert_eq!(s.output_row_count, 7.0);
        assert_eq!(s.column_statistics[&column_id].distinct_values_count, 3.0);
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
            kind: DistributedPlanNodeKind::Values(DistributedValuesNode {
                rows: vec![],
                columns: vec![],
            }),
        };

        assert!(matches!(node.kind, DistributedPlanNodeKind::Values(_)));
        assert!(node.children.is_empty());
    }
}
