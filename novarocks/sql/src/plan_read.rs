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
// Design: ADR-0050 (docs/adr/ADR-0050-sealed-plan-logical-mutation-effects-and-opaque-routes.md)
//!
//! This is the single public SQL plan reading surface. Construction, draft
//! mutation, sealing, and validation remain private to the SQL compiler.

pub use crate::analysis::{ExprKind, SortItem, TypedExpr};
pub use crate::column_id::ColumnId;
pub use crate::common::CteId;
pub use crate::common::expr::{
    BinOp, JoinKind, LiteralValue, UnOp, WindowBound, WindowFrame, WindowFrameType,
};
pub use crate::common::plan_hints::{ScanVariantColumn, SqlTopNType};
pub use crate::common::schema::OutputColumn;
pub use crate::planner::distributed::write::{
    ChangeStreamRouterSink, ConnectorWriteFragmentSink, ConnectorWriteInputBinding,
};
pub use crate::planner::distributed::{
    BoundaryColumn, BoundaryContract, BoundaryKind, DataPartition, DataSink, DistributedNode,
    DistributedNodeKind, DistributedPlan, ExchangeFlavor, ExchangeReceiver, ExecutionColumnId,
    FragmentEdge, FragmentEdgeKind, FragmentEdgeOutputCatalog, FragmentId, FragmentStreamKind,
    NodeExecutionColumn, NodeExecutionOutput, NodeOutputCatalog, PartitionKind, PlanFragment,
    WriteContractCatalog, distributed_kind_to_physical,
};
pub use crate::planner::payload::{PlanRowCountAssertion, PlanScanNode};
pub use crate::planner::physical::node::{PhysicalPlanKind, PlanSetOpKind, RedistributeMode};
pub use crate::planner::physical::runtime_filter::JoinExecutionMode;
pub use crate::planner::physical::vocab::{AggMode, HashSource, JoinDistribution, TopNPhase};
pub use novarocks_spi::connector::{
    ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteRouteId,
};

impl DistributedNode {
    /// Project sealed runtime-filter binding identifiers for protocol encoding.
    /// The typed SQL binding identifiers remain internal to the planner.
    pub fn runtime_filter_binding_ids(&self) -> impl ExactSizeIterator<Item = u32> + '_ {
        self.runtime_filter_binding_ids
            .iter()
            .map(|binding_id| binding_id.get())
    }
}

/// Read-only access to one sealed connector writer sink. The opaque provider
/// payload remains inside the signed SPI handle; plan readers can only encode
/// its immutable envelope.
impl ConnectorWriteFragmentSink {
    pub fn handle(&self) -> Option<&novarocks_spi::connector::ConnectorWriterHandle> {
        self.handle.as_ref()
    }

    pub fn input(&self) -> &ConnectorWriteInputBinding {
        &self.input
    }

    pub fn has_output_contract(&self) -> bool {
        self.output_contract.is_some()
    }
}

/// Borrowed, read-only projection of one sealed change-stream router route.
/// Route construction and validation remain private to SQL planning.
pub struct ChangeStreamRouterRouteRead<'a>(
    &'a crate::planner::distributed::write::change_stream::ChangeStreamRoute,
);

impl ChangeStreamRouterRouteRead<'_> {
    pub fn route_id(&self) -> ConnectorWriteRouteId {
        self.0.route_id
    }

    pub fn cohort_id(&self) -> novarocks_spi::connector::ConnectorWriteCohortId {
        self.0.cohort_id
    }

    pub fn accepted_effects(&self) -> &[ConnectorRowMutationEffect] {
        &self.0.accepted_effects
    }

    pub fn input_ordinals(&self) -> &[ConnectorMutationRouteInput] {
        &self.0.input_ordinals
    }

    pub const fn target_fragment_id(&self) -> FragmentId {
        self.0.target_fragment_id
    }

    pub const fn target_exchange_node_id(&self) -> i32 {
        self.0.target_exchange_node_id
    }

    pub fn output_partition_ordinals(&self) -> &[usize] {
        &self.0.output_partition_ordinals
    }
}

impl ChangeStreamRouterSink {
    pub const fn group_id(&self) -> i32 {
        self.group_id
    }

    pub const fn effect_output_ordinal(&self) -> usize {
        self.effect_output_ordinal
    }

    pub fn routes(&self) -> impl ExactSizeIterator<Item = ChangeStreamRouterRouteRead<'_>> + '_ {
        self.routes.iter().map(ChangeStreamRouterRouteRead)
    }
}

/// Borrowed read-only projection of one sealed change-event assignment.
pub struct DistributedChangeEventOutputExprRead<'a>(
    &'a crate::planner::physical::node::DistributedChangeEventOutputExpr,
);

impl DistributedChangeEventOutputExprRead<'_> {
    pub const fn output_column_id(&self) -> ColumnId {
        self.0.output_column_id
    }

    pub fn expr(&self) -> Option<&TypedExpr> {
        self.0.expr.as_ref()
    }
}

/// Borrowed read-only projection of one sealed change-event specification.
pub struct DistributedChangeEventSpecRead<'a>(
    &'a crate::planner::physical::node::DistributedChangeEventSpec,
);

impl DistributedChangeEventSpecRead<'_> {
    pub fn predicate(&self) -> Option<&TypedExpr> {
        self.0.predicate.as_ref()
    }

    pub const fn effect(&self) -> ConnectorRowMutationEffect {
        self.0.effect
    }

    pub fn assignments(
        &self,
    ) -> impl ExactSizeIterator<Item = DistributedChangeEventOutputExprRead<'_>> + '_ {
        self.0
            .assignments
            .iter()
            .map(DistributedChangeEventOutputExprRead)
    }
}

impl crate::planner::physical::node::DistributedChangeEventExpandNode {
    pub fn output_columns(&self) -> &[OutputColumn] {
        &self.output_columns
    }

    pub const fn effect_column_id(&self) -> ColumnId {
        self.effect_column_id
    }

    pub fn events(&self) -> impl ExactSizeIterator<Item = DistributedChangeEventSpecRead<'_>> + '_ {
        self.events.iter().map(DistributedChangeEventSpecRead)
    }
}

impl crate::planner::physical::node::PhysicalHashAggregateNode {
    /// Return the visible aggregate output when present, otherwise the sealed
    /// full group-key plus aggregate-state layout used by bare-node encoders.
    pub fn output_columns_or_layout(&self) -> Vec<OutputColumn> {
        if self.output_columns.is_empty() {
            self.output_layout.full_output_columns()
        } else {
            self.output_columns.clone()
        }
    }

    pub fn group_key_columns(&self) -> &[OutputColumn] {
        &self.output_layout.group_key_columns
    }

    pub fn aggregate_state_columns(&self) -> &[OutputColumn] {
        &self.output_layout.aggregate_columns
    }
}

/// Read-only SQL table facts used by plan encoders.
pub mod table {
    pub use crate::planner::table::{
        ScanSource, SqlMetadataTableKind, SqlMvTargetLocatorScan,
        SqlMvTargetStatePartitionConstraint, SqlMvTargetStateRowFilter, SqlMvTargetStateScan,
        SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };

    impl SqlMvTargetStateScan {
        /// Return the target row-id column captured by SQL admission.
        pub fn row_id_column_name(&self) -> &str {
            &self.row_id_column_name
        }

        /// Return the target group-key columns captured by SQL admission.
        pub fn group_key_names(&self) -> &[String] {
            &self.group_key_names
        }

        /// Return the aggregate-state columns captured by SQL admission.
        pub fn aggregate_state_names(&self) -> &[String] {
            &self.aggregate_state_names
        }

        /// Return the optional branch-id column needed by this target-state scan.
        pub fn branch_id_column_name(&self) -> Option<&str> {
            match &self.row_filter {
                SqlMvTargetStateRowFilter::DeltaInputRowIds {
                    branch_scope: Some(scope),
                    ..
                } => Some(&scope.branch_id_column_name),
                SqlMvTargetStateRowFilter::DeltaInputRowIds {
                    branch_scope: None, ..
                } => None,
            }
        }
    }

    impl SqlMvTargetLocatorScan {
        /// Return the physical apply-key column captured by SQL admission.
        pub fn apply_key_column(&self) -> &str {
            &self.apply_key_column
        }

        /// Return the optional branch-id column captured by SQL admission.
        pub fn branch_id_column(&self) -> Option<&str> {
            self.branch_id_column.as_deref()
        }
    }

    impl SqlScanSource {
        /// Return the sealed scan kind selected by SQL planning. The binding
        /// token and table identity remain private to the scan source.
        pub fn kind(&self) -> &SqlScanKind {
            &self.kind
        }
    }
}

/// Read-only runtime-filter planning facts used by plan encoders.
pub mod runtime_filter {
    pub use crate::planner::runtime_filter::contract::{
        ArtifactCapability, CompletionFenceKind, CompletionRequirement, ConsumerActivation,
        ContributionKind, LateApplyGranularity,
    };
    pub use crate::planner::runtime_filter::graph::RuntimeFilterGraph;
    pub use crate::planner::runtime_filter::graph::{
        ApplyPoint, ConsumerBindingTarget, ProducerBindingTarget,
    };
    pub use crate::planner::runtime_filter::progress::JoinBuildProgressCatalog;
    pub use crate::planner::runtime_filter::sealed::SealedRuntimeFilterPlan;
}
