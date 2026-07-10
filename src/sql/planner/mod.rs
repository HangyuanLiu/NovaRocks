// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Planner — converts analyzed SQL into logical plans and distributed Bridge 2 IR.
//!
//! This is a structural transformation that builds a relational algebra tree
//! from the analyzed query IR. It also owns Bridge 2, which materializes
//! physical optimizer plans into planner-side distributed plan fragments before
//! codegen lowers them to Thrift.

pub(crate) mod change_stream_write;
pub(crate) mod distributed;
mod distributed_plan_build;
pub(crate) mod imv_rewrite;
pub(crate) mod logical;
pub(crate) mod optimizer_bridge;
pub(crate) mod ordering;
pub(crate) mod payload;
pub(crate) mod physical;
pub(crate) mod runtime_filter;
pub(crate) mod write_plan;
pub(crate) mod write_sink;

pub(crate) use change_stream_write::{
    ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec, IcebergChangeStreamBranchRoute,
    IcebergChangeStreamRouterSink, IcebergChangeStreamWriteTopology,
    IcebergChangeStreamWriterBranch, PlannedIcebergChangeStreamDistributedPlan,
};
pub(crate) use distributed_plan_build::{
    build_distributed_plan, union_distinct_must_be_rewritten_error,
};
pub(crate) use logical::build::{plan_output_columns, plan_query};
pub(crate) use runtime_filter::{
    PlannedRuntimeFilter, RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};
#[allow(unused_imports)]
pub(crate) use runtime_filter::{WiredRuntimeFilterBuild, WiredRuntimeFilterProbe};
pub(crate) use write_plan::{with_iceberg_change_stream_write, with_iceberg_write_sink};
pub(crate) use write_sink::{
    IcebergWriteFragmentSink, IcebergWriteInputBinding, IcebergWriteSinkMode, IcebergWriteSinkSpec,
    synthetic_iceberg_write_table_id,
};

#[cfg(test)]
mod bridge2_export_tests {
    use super::build_distributed_plan;
    use crate::sql::planner::distributed::{DistributedNode, DistributedPlan};
    use crate::sql::planner::physical::PhysicalPlanNode;

    #[test]
    fn planner_exports_bridge2_distributed_plan_api() {
        fn accepts_builder(_: fn(&PhysicalPlanNode) -> Result<DistributedPlan, String>) {}
        fn accepts_node(_: Option<DistributedNode>) {}

        accepts_builder(build_distributed_plan);
        accepts_node(None);
    }
}

#[cfg(test)]
mod write_export_tests {
    use super::{
        ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec, IcebergWriteSinkMode,
        IcebergWriteSinkSpec,
    };

    #[test]
    fn planner_exports_write_sink_dtos() {
        fn accepts_sink_spec(_: Option<IcebergWriteSinkSpec>) {}
        fn accepts_dag(_: Option<ChangeStreamWriteDagSpec>) {}

        accepts_sink_spec(None);
        accepts_dag(None);
        assert_eq!(IcebergWriteSinkMode::Data, IcebergWriteSinkMode::Data);
    }

    #[test]
    fn change_stream_branch_spec_stores_ordinals_not_slots() {
        let branch = ChangeStreamWriteBranchSpec::for_test(
            7,
            crate::sql::common::ChangeStreamBranchKind::ReuseData,
            vec![0, 2],
        );

        assert_eq!(branch.branch_id, 7);
        assert_eq!(branch.stream_output_ordinals, vec![0, 2]);
        assert!(branch.output_partition_ordinals.is_empty());
    }
}
