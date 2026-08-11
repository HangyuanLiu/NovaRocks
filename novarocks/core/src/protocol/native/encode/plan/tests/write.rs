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

use arrow::datatypes::DataType;
use prost::Message;

use super::super::write::encode_change_stream_router_sink;
use super::*;
use crate::sql::planner::distributed::write::change_stream::{
    ChangeStreamRoute, ChangeStreamRouterSink,
};
use novarocks_spi::connector::{
    ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteCohortId,
    ConnectorWriteFieldToken, ConnectorWriteRouteId,
};

#[test]
fn change_stream_router_encoder_materializes_partition_exprs() {
    let plan = single_fragment_router_plan_for_test();
    let fragment = plan.fragments().first().expect("router fragment");
    let DataSink::ChangeStreamRouter(sink) = &fragment.sink else {
        panic!("expected Iceberg change-stream router sink");
    };
    let router = encode_change_stream_router_sink(
        sink,
        fragment.fragment_id,
        &NativePlanEncodeContext {
            scan_bindings: None,
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: Some(plan.write_contracts()),
        },
    )
    .expect("encode change-stream router sink");

    let route = router.routes.first().expect("router route");
    assert_eq!(route.output_partition_ordinals, vec![1]);
    assert_eq!(route.route_id, vec![7; 32]);
    assert_eq!(
        route.accepted_effects,
        vec![novarocks_protocol::plan::RowMutationEffect::Delete as i32]
    );
    let partition = route
        .output_partition
        .as_ref()
        .expect("route output partition");
    assert_eq!(
        partition.kind,
        novarocks_protocol::plan::PartitionKind::Hash as i32
    );
    let [expr] = partition.exprs.as_slice() else {
        panic!("expected one materialized partition expr");
    };
    let Some(novarocks_protocol::expr::expr::Kind::ColumnRef(column_ref)) = expr.kind.as_ref()
    else {
        panic!("expected partition expr to be a column ref");
    };
    assert_eq!(column_ref.column_id, 3);
}

#[test]
fn change_stream_router_encoder_preserves_wire_bytes() {
    let plan = single_fragment_router_plan_for_test();
    let fragment = plan.fragments().first().expect("router fragment");
    let DataSink::ChangeStreamRouter(sink) = &fragment.sink else {
        panic!("expected Iceberg change-stream router sink");
    };
    let router = encode_change_stream_router_sink(
        sink,
        fragment.fragment_id,
        &NativePlanEncodeContext {
            scan_bindings: None,
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: Some(plan.write_contracts()),
        },
    )
    .expect("encode change-stream router sink");

    let encoded = router.encode_to_vec();
    assert!(!encoded.is_empty());
    let decoded = novarocks_protocol::plan::ChangeStreamRouterSink::decode(encoded.as_slice())
        .expect("router wire round trip");
    assert_eq!(decoded.routes.len(), 1);
    assert_eq!(
        decoded.routes[0].accepted_effects,
        router.routes[0].accepted_effects
    );
}

fn single_fragment_router_plan_for_test() -> DistributedPlan {
    let output_columns = vec![
        output_column(1, "__row_mutation_effect", DataType::Int8),
        output_column(3, "bucket", DataType::Int32),
    ];
    crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root: DistributedNode {
                node_id: 10,
                fragment_id: 0,
                tuple_ids: vec![10],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Values(
                    crate::sql::planner::payload::PlanValuesNode {
                        rows: Vec::new(),
                        columns: output_columns.clone(),
                    },
                ),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::ChangeStreamRouter(ChangeStreamRouterSink {
                group_id: 0,
                effect_output_ordinal: 0,
                routes: vec![ChangeStreamRoute {
                    route_id: ConnectorWriteRouteId::from_bytes([7; 32]),
                    cohort_id: ConnectorWriteCohortId::from_bytes([8; 32]),
                    accepted_effects: vec![ConnectorRowMutationEffect::Delete],
                    input_ordinals: vec![ConnectorMutationRouteInput::new(
                        ConnectorWriteFieldToken::from_bytes([9; 32]),
                        1,
                    )],
                    target_fragment_id: 1,
                    target_exchange_node_id: 20,
                    output_partition_ordinals: vec![1],
                }],
            }),
            output_exprs: None,
            output_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    }
}

fn values_distributed_node(
    fragment_id: crate::sql::planner::distributed::FragmentId,
    node_id: i32,
    output: Vec<crate::sql::analysis::OutputColumn>,
) -> crate::sql::planner::distributed::DistributedNode {
    crate::sql::planner::distributed::DistributedNode {
        node_id,
        fragment_id,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: stats(),
        payload: crate::sql::planner::distributed::DistributedNodeKind::Values(
            crate::sql::planner::payload::PlanValuesNode {
                rows: Vec::new(),
                columns: output,
            },
        ),
    }
}
