//! Compatibility exports for the planner-owned DistributedPlan Bridge 2 IR.

pub(crate) mod explain;
pub(crate) mod kind;
pub(crate) mod lowering;

#[cfg(test)]
pub(crate) mod equiv;

#[cfg(test)]
pub(crate) use crate::sql::planner::plan::PlanNodeKind;
#[cfg(test)]
pub(crate) use crate::sql::planner::{
    DataPartition, DataSink, DistributedPlan, DistributedPlanNode, PartitionKind, PlanFragment,
    PlanNodeStats, build_distributed_plan,
};
pub(crate) use explain::explain_distributed_plan;
pub(crate) use lowering::lower_distributed_plan;

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[test]
    fn bridge2_owner_modules_are_split_into_files() {
        for module_file in [
            "distributed_build.rs",
            "distributed_fragment.rs",
            "distributed_node.rs",
        ] {
            let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/sql/planner")
                .join(module_file);
            assert!(path.is_file(), "{} should exist", path.display());
        }
    }
}
