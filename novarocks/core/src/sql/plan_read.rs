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

//! Read-only public projections of a sealed distributed SQL plan.
//!
//! This is the single public SQL plan reading surface. Construction, draft
//! mutation, sealing, and validation remain private to the SQL compiler.

pub use crate::sql::column_id::ColumnId;
pub use crate::sql::common::change_stream::ChangeStreamBranchKind;
pub use crate::sql::common::expr::{
    BinOp, JoinKind, LiteralValue, UnOp, WindowBound, WindowFrame, WindowFrameType,
};
pub use crate::sql::common::plan_hints::{ScanVariantColumn, SqlTopNType};
pub use crate::sql::common::schema::OutputColumn;
pub use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
pub use crate::sql::planner::distributed::write::{
    ChangeStreamRouterSink, ConnectorWriteFragmentSink, ConnectorWriteInputBinding,
};
pub use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan, ExchangeFlavor,
    ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentEdgeOutputCatalog, FragmentId,
    FragmentStreamKind, NodeExecutionColumn, NodeExecutionOutput, NodeOutputCatalog,
    PartitionKind, PlanFragment, WriteContractCatalog, distributed_kind_to_physical,
};
pub use crate::sql::planner::payload::{PlanRowCountAssertion, PlanScanNode};
pub use crate::sql::planner::physical::node::{
    PhysicalPlanKind, PlanSetOpKind, RedistributeMode,
};
pub use crate::sql::planner::physical::runtime_filter::JoinExecutionMode;
pub use crate::sql::planner::physical::vocab::{
    AggMode, HashSource, JoinDistribution, TopNPhase,
};

/// Read-only SQL table facts used by plan encoders.
pub mod table {
    pub use crate::sql::planner::table::{
        ScanSource, SqlMetadataTableKind, SqlMvTargetStatePartitionConstraint,
        SqlMvTargetStateRowFilter, SqlScanKind, SqlScanSource, TableDef,
    };
}

/// Read-only runtime-filter planning facts used by plan encoders.
pub mod runtime_filter {
    pub use crate::runtime_filter::model::contract::{
        ArtifactCapability, CompletionFenceKind, CompletionRequirement, ConsumerActivation,
        ContributionKind, LateApplyGranularity,
    };
    pub use crate::sql::planner::runtime_filter::graph::RuntimeFilterGraph;
    pub use crate::sql::planner::runtime_filter::graph::{
        ApplyPoint, ConsumerBindingTarget, ProducerBindingTarget,
    };
}
