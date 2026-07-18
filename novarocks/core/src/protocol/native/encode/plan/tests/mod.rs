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

use super::*;
use crate::proto::expr::expr;
use crate::runtime_filter::model::graph::RuntimeFilterGraph;
use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::common::{ChangeStreamBranchKind, JoinKind};
use crate::sql::planner::distributed::write::change_stream::{
    IcebergChangeStreamBranchRoute, IcebergChangeStreamRouterSink,
};
use crate::sql::planner::physical::{
    JoinDistribution, PhysicalPlanStats, PlannerConfidence, TopNPhase,
};
use arrow::datatypes::DataType;

mod output;
mod relational;
mod runtime_filter;
mod topology;

fn empty_scan_bindings() -> &'static ScanExecutionBindings {
    Box::leak(Box::new(ScanExecutionBindings::default()))
}

fn output_column(id: u32, name: &str, data_type: DataType) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId::new_for_test(id),
        name: name.to_string(),
        data_type,
        nullable: false,
        is_internal: false,
    }
}

fn stats() -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: 1.0,
        row_count_confidence: PlannerConfidence::Exact,
        column_statistics: Default::default(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

pub(super) fn two_fragment_stream_plan_for_test() -> DistributedPlan {
    let source_columns = vec![
        output_column(1, "old", DataType::Int64),
        output_column(2, "delta", DataType::Int64),
    ];
    let receiver_columns = vec![source_columns[1].clone(), source_columns[0].clone()];
    crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![
            PlanFragment {
                fragment_id: 1,
                root: DistributedNode {
                    node_id: 10,
                    fragment_id: 1,
                    tuple_ids: vec![10],
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
        runtime_filter_binding_ids: Vec::new(),
                    children: Vec::new(),
                    stats: stats(),
                    payload: DistributedNodeKind::Values(
                        crate::sql::planner::payload::PlanValuesNode {
                            rows: Vec::new(),
                            columns: source_columns.clone(),
                        },
                    ),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Noop,
                output_exprs: None,
                output_columns: source_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            },
            PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 20,
                    fragment_id: 0,
                    tuple_ids: vec![20],
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
        runtime_filter_binding_ids: Vec::new(),
                    children: Vec::new(),
                    stats: stats(),
                    payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                        partition: DataPartition::unpartitioned(),
                        source_fragment_id: 1,
                        output_columns: receiver_columns,
                        output_qualifier: None,
                        flavor: ExchangeFlavor::Distribution,
                    }),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: Vec::new(),
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            },
        ],
        root_fragment_id: 0,
        runtime_filter_graph: RuntimeFilterGraph::default(),
        edges: vec![FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id: 0,
            target_exchange_node_id: 20,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: vec![2, 1],
        }],
    }
}
