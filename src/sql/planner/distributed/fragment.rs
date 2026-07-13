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

use crate::runtime_filter::model::graph::RuntimeFilterGraph;
use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;

use super::node::DistributedNode;

pub(crate) type FragmentId = u32;

#[derive(Clone, Copy, Debug)]
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

    pub(crate) fn random() -> Self {
        Self {
            kind: PartitionKind::Random,
            exprs: Vec::new(),
        }
    }

    pub(crate) fn hash(exprs: Vec<TypedExpr>) -> Self {
        Self {
            kind: PartitionKind::Hash,
            exprs,
        }
    }

    pub(crate) fn explain_label(&self) -> String {
        match self.kind {
            PartitionKind::Unpartitioned => "UNPARTITIONED".to_string(),
            PartitionKind::Random => "RANDOM".to_string(),
            PartitionKind::Hash => {
                if self.exprs.is_empty() {
                    "HASH_PARTITIONED".to_string()
                } else {
                    let exprs = self
                        .exprs
                        .iter()
                        .map(crate::sql::explain::format_expr)
                        .collect::<Vec<_>>();
                    format!("HASH_PARTITIONED ({})", exprs.join(", "))
                }
            }
        }
    }
}

/// Planner-owned fragment sink intent lowered by codegen.
#[derive(Clone, Debug)]
pub(crate) enum DataSink {
    Result,
    Noop,
    IcebergWrite(super::write::sink::IcebergWriteFragmentSink),
    IcebergChangeStreamRouter(super::write::change_stream::IcebergChangeStreamRouterSink),
}

#[derive(Clone, Debug)]
pub(crate) struct PlanFragment {
    pub fragment_id: FragmentId,
    pub root: DistributedNode,
    pub data_partition: DataPartition,
    pub output_partition: DataPartition,
    pub sink: DataSink,
    pub output_exprs: Option<Vec<TypedExpr>>,
    pub output_columns: Vec<OutputColumn>,
    pub cte_id: Option<CteId>,
    pub cte_exchange_nodes: Vec<(CteId, i32, Vec<ColumnId>)>,
}

/// Result of emitting a multi-fragment plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FragmentEdgeKind {
    Stream,
    CteMulticast {
        cte_id: CteId,
        receive_producer_column_ids: Vec<ColumnId>,
    },
    IcebergChangeStreamRouter {
        router_group_id: i32,
        branch_id: i32,
        branch_kind: crate::sql::common::ChangeStreamBranchKind,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FragmentStreamKind {
    Gather,
    Broadcast,
    Partitioned,
    Other,
}

#[derive(Clone, Debug)]
pub(crate) struct FragmentEdge {
    pub source_fragment_id: FragmentId,
    pub target_fragment_id: FragmentId,
    pub target_exchange_node_id: i32,
    pub output_partition: DataPartition,
    pub stream_kind: FragmentStreamKind,
    pub edge_kind: FragmentEdgeKind,
    pub output_slot_ids: Vec<i32>,
}

#[derive(Clone, Debug)]
pub(crate) struct DistributedPlan {
    pub fragments: Vec<PlanFragment>,
    pub root_fragment_id: FragmentId,
    pub edges: Vec<FragmentEdge>,
    // RFD-5A will populate and consume this slot; remove the allowance at that cutover.
    #[allow(dead_code)]
    pub runtime_filter_graph: RuntimeFilterGraph,
}
