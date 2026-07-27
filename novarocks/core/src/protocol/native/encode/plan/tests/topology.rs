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

#[test]
fn stream_sink_projection_and_receiver_schema_follow_edge_output_slots() {
    let plan = two_fragment_stream_plan_for_test();

    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");

    let source = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 1)
        .expect("source fragment");
    let Some(plan::data_sink::Kind::DataStream(sink)) =
        source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
    else {
        panic!("expected DataStream sink");
    };
    assert_eq!(sink.output_columns, vec![2, 1]);

    let target = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 0)
        .expect("target fragment");
    let receiver = target.root.as_ref().expect("target root");
    let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
    else {
        panic!("expected Exchange receiver");
    };
    assert_eq!(
        exchange
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(2, "delta"), (1, "old")]
    );
}

#[test]
fn stream_sink_uses_source_slots_while_receiver_schema_uses_exchange_columns() {
    let plan = two_fragment_stream_plan_with_lowered_slots_for_test();

    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");

    let source = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 1)
        .expect("source fragment");
    let Some(plan::data_sink::Kind::DataStream(sink)) =
        source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
    else {
        panic!("expected DataStream sink");
    };
    assert_eq!(sink.output_columns, vec![10, 20]);

    let target = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 0)
        .expect("target fragment");
    let receiver = target.root.as_ref().expect("target root");
    let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
    else {
        panic!("expected Exchange receiver");
    };
    assert_eq!(
        exchange
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(10, "employee_id"), (20, "name")]
    );
}

#[test]
fn stream_sink_allows_zero_column_values_source() {
    let plan = two_fragment_zero_column_stream_plan_for_test();

    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");

    let source = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 1)
        .expect("source fragment");
    let Some(plan::data_sink::Kind::DataStream(sink)) =
        source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
    else {
        panic!("expected DataStream sink");
    };
    assert!(sink.output_columns.is_empty());

    let target = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 0)
        .expect("target fragment");
    let receiver = target.root.as_ref().expect("target root");
    let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
    else {
        panic!("expected Exchange receiver");
    };
    assert!(exchange.output_columns.is_empty());
}

fn two_fragment_stream_plan_with_lowered_slots_for_test() -> DistributedPlan {
    let source_columns = vec![
        output_column(10, "employee_id", DataType::Int64),
        output_column(20, "name", DataType::Utf8),
        output_column(30, "title", DataType::Utf8),
    ];
    let receiver_columns = source_columns[..2].to_vec();
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
        runtime_filter_graph: Default::default(),
        edges: vec![FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id: 0,
            target_exchange_node_id: 20,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: vec![43, 44],
        }],
    }
}

fn two_fragment_zero_column_stream_plan_for_test() -> DistributedPlan {
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
                            rows: vec![Vec::new()],
                            columns: Vec::new(),
                        },
                    ),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Noop,
                output_exprs: None,
                output_columns: Vec::new(),
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
                        output_columns: Vec::new(),
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
        runtime_filter_graph: Default::default(),
        edges: vec![FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id: 0,
            target_exchange_node_id: 20,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }],
    }
}

#[test]
fn stream_sink_derives_generate_series_source_schema() {
    let plan = two_fragment_generate_series_stream_plan_for_test();

    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");

    let source = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 1)
        .expect("source fragment");
    let Some(plan::data_sink::Kind::DataStream(sink)) =
        source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
    else {
        panic!("expected DataStream sink");
    };
    assert_eq!(sink.output_columns, vec![7]);

    let target = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 0)
        .expect("target fragment");
    let receiver = target.root.as_ref().expect("target root");
    let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
    else {
        panic!("expected Exchange receiver");
    };
    assert_eq!(
        exchange
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str(), column.nullable))
            .collect::<Vec<_>>(),
        vec![(7, "generate_series", false)]
    );
}

fn two_fragment_generate_series_stream_plan_for_test() -> DistributedPlan {
    let output_columns = vec![output_column(7, "generate_series", DataType::Int64)];
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
                    payload: DistributedNodeKind::GenerateSeries(
                        crate::sql::planner::payload::PlanGenerateSeriesNode {
                            start: 1,
                            end: 3,
                            step: 1,
                            column_name: "generate_series".to_string(),
                            alias: None,
                            output_column_id: ColumnId::new_for_test(7),
                        },
                    ),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Noop,
                output_exprs: None,
                output_columns: Vec::new(),
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
                        output_columns,
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
        runtime_filter_graph: Default::default(),
        edges: vec![FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id: 0,
            target_exchange_node_id: 20,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: vec![7],
        }],
    }
}
