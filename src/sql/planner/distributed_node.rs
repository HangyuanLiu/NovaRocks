use crate::sql::analysis::{OutputColumn, TypedExpr};
use crate::sql::codegen::FragmentId;
use crate::sql::planner::PhysicalPlanStats;
use crate::sql::planner::plan::ExchangeFlavor;
use crate::sql::planner::plan::PhysicalPlanKind;
use crate::sql::planner::runtime_filter::{WiredRuntimeFilterBuild, WiredRuntimeFilterProbe};
use crate::thrift::partitions::TPartitionType;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct ExchangeReceiver {
    pub partition_type: TPartitionType,
    pub partition_exprs: Vec<TypedExpr>,
    pub source_fragment_id: FragmentId,
    pub output_columns: Vec<OutputColumn>,
    pub output_qualifier: Option<String>,
    pub flavor: ExchangeFlavor,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum DistributedPayload {
    Physical(PhysicalPlanKind),
    Exchange(ExchangeReceiver),
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedNode {
    pub node_id: i32,
    pub fragment_id: FragmentId,
    pub tuple_ids: Vec<i32>,
    pub nullable_tuple_ids: Vec<i32>,
    pub limit: i64,
    pub build_runtime_filters: Vec<WiredRuntimeFilterBuild>,
    pub probe_runtime_filters: Vec<WiredRuntimeFilterProbe>,
    pub children: Vec<DistributedNode>,
    pub stats: PhysicalPlanStats,
    pub payload: DistributedPayload,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::sql::planner::plan::{PhysicalPlanKind, PlanValuesNode};
    use crate::sql::planner::{PhysicalPlanStats, PlannerConfidence};

    #[test]
    fn distributed_node_can_wrap_physical_values_payload() {
        let node = DistributedNode {
            node_id: 1,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: vec![],
            limit: -1,
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
            children: vec![],
            stats: PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: HashMap::new(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedPayload::Physical(PhysicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            })),
        };

        assert!(matches!(node.payload, DistributedPayload::Physical(_)));
    }
}
