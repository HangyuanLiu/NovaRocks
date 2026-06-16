use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{OutputColumn, TypedExpr};

use super::FragmentId;
use super::node::DistributedPlanNode;

#[derive(Clone, Debug)]
pub(crate) enum PartitionKind {
    Unpartitioned,
    Random,
    Hash,
}

#[derive(Clone, Debug)]
pub(crate) struct DataPartition {
    pub kind: PartitionKind,
    pub exprs: Vec<TypedExpr>,
}

impl DataPartition {
    pub fn unpartitioned() -> Self {
        Self {
            kind: PartitionKind::Unpartitioned,
            exprs: Vec::new(),
        }
    }
}

/// Sink intent. This slice only produces the root result sink.
#[derive(Clone, Debug)]
pub(crate) enum DataSink {
    Result,
    Noop,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanFragment {
    pub fragment_id: FragmentId,
    pub root: DistributedPlanNode,
    pub data_partition: DataPartition,
    pub output_partition: DataPartition,
    pub sink: DataSink,
    pub output_exprs: Option<Vec<TypedExpr>>,
    pub output_columns: Vec<OutputColumn>,
    pub cte_id: Option<CteId>,
    pub cte_exchange_nodes: Vec<(CteId, i32)>,
}

#[derive(Clone, Debug)]
pub(crate) struct DistributedPlan {
    pub fragments: Vec<PlanFragment>,
    pub root_fragment_id: FragmentId,
    pub edges: Vec<crate::sql::codegen::FragmentEdge>,
}
