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

mod activation_decision;
#[cfg(test)]
pub(crate) use crate::sql::planner::runtime_filter::activation::{
    ActivationConstraint, ActivationFallback, RequiredLiveReason,
};
#[cfg(test)]
pub(crate) use activation_decision::DraftRuntimeFilterGraph;
pub(crate) mod boundary;
pub(crate) mod build;
mod fragment;
mod node;
pub(crate) mod output;
pub(crate) mod runtime_filter_progress;
mod seal;
pub(crate) mod topology;
mod validation;
pub(crate) mod write;

#[cfg(test)]
pub(crate) mod test_support;

pub use boundary::{BoundaryCatalog, ExecutionColumnIdAllocator};
pub(crate) use boundary::{BoundaryColumn, BoundaryContract, BoundaryKind, ExecutionColumnId};
pub use fragment::{DataPartition, FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind};
pub use fragment::{DataSink, PartitionKind, PlanFragment};
pub(crate) use node::distributed_kind_from_physical;
pub use node::{
    DistributedNode, DistributedNodeKind, ExchangeFlavor, ExchangeReceiver,
    distributed_kind_to_physical,
};
pub use output::{
    ConnectorWriteOutputContract, FinalizedAggregateLayout, FragmentEdgeOutputCatalog,
    NodeExecutionColumn, NodeExecutionOutput, NodeOutputCatalog, WriteContractCatalog,
};
pub(crate) use runtime_filter_progress::{
    FrontierEdge, FrontierSkip, JoinBuildProgressCatalog, JoinBuildProgressProof,
    JoinBuildProgressSkip,
};
pub use seal::DistributedPlan;
pub(crate) use seal::native_encoder_test_fixture_plan;
pub use topology::TopologyContract;
