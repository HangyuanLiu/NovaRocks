use crate::sql::optimizer::statistics::{Confidence, Statistics};

use super::FragmentId;
use super::body::{ProjectBody, ScanBody};

/// Self-contained copy of the estimated stats this node carries, so EXPLAIN /
/// ANALYZE never reach back into `PhysicalPlanNode`.
#[derive(Clone, Debug)]
pub(crate) struct PlanNodeStats {
    pub output_row_count: f64,
    pub row_count_confidence: Confidence,
}

impl PlanNodeStats {
    pub fn from_statistics(stats: &Statistics) -> Self {
        Self {
            output_row_count: stats.output_row_count,
            row_count_confidence: stats.row_count_confidence,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DistributedPlanNode {
    /// Allocated once in Pass 1; never reallocated. In a thrift-lowered
    /// fragment every DistributedPlanNode produces exactly one TPlanNode, so
    /// `node_id == TPlanNode.node_id == profile plan_node_id`.
    pub node_id: i32,
    pub fragment_id: FragmentId,
    /// Output tuples (thrift `row_tuples`). Allocated in Pass 1.
    pub tuple_ids: Vec<i32>,
    /// Subset of `tuple_ids` widened to nullable (outer-join side). Empty here.
    pub nullable_tuple_ids: Vec<i32>,
    /// -1 == no limit.
    pub limit: i64,
    pub children: Vec<DistributedPlanNode>,
    pub stats: PlanNodeStats,
    pub body: DistributedPlanNodeBody,
}

/// Operator-specific payload. Grows one variant per operator as slices land.
/// Filter has no variant: its predicate folds into the child's `ScanBody.predicates`.
#[derive(Clone, Debug)]
pub(crate) enum DistributedPlanNodeBody {
    Scan(Box<ScanBody>),
    Project(ProjectBody),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::statistics::Statistics;

    #[test]
    fn plan_node_stats_copies_row_count() {
        let stats = Statistics {
            output_row_count: 7.0,
            ..Default::default()
        };
        let s = PlanNodeStats::from_statistics(&stats);
        assert_eq!(s.output_row_count, 7.0);
    }
}
