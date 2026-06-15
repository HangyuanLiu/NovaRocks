//! Owned DistributedPlanNode/PlanFragment IR (spec 2026-06-15-plannode-ir-explain-observability).
//! Single source from which both EXPLAIN and thrift derive. This slice covers
//! Scan/Filter/Project; later slices add the remaining operators.

pub(crate) mod body;
pub(crate) mod fragment;
pub(crate) mod node;

pub(crate) mod build {
    pub(crate) fn build_distributed_plan() {
        unimplemented!("build_distributed_plan is added by a later IR slice")
    }
}

pub(crate) mod lowering {
    pub(crate) fn lower_distributed_plan() {
        unimplemented!("lower_distributed_plan is added by a later IR slice")
    }
}

#[cfg(test)]
pub(crate) mod equiv {}

pub(crate) use build::build_distributed_plan;
pub(crate) use fragment::{DataPartition, DataSink, DistributedPlan, PartitionKind, PlanFragment};
pub(crate) use lowering::lower_distributed_plan;
pub(crate) use node::{DistributedPlanNode, DistributedPlanNodeBody, PlanNodeStats};

pub(crate) type FragmentId = u32;
