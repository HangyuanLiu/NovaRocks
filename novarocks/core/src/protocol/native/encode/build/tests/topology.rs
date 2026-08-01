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
fn column_ref(id: u32, name: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId::new_for_test(id),
            qualifier: Some("t".to_string()),
            column: name.to_string(),
        },
        data_type: DataType::Int64,
        nullable: false,
    }
}

#[test]
fn planner_broadcast_edge_remains_broadcast_through_builder() {
    use crate::sql::planner::physical::{
        PhysicalPlanKind, PhysicalPlanNode, RedistributeMode, RedistributeNode,
    };

    let columns = vec![output_col(1, "k")];
    let values = PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: Vec::new(),
            columns: columns.clone(),
        }),
        children: Vec::new(),
        output_columns: columns.clone(),
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    };
    let broadcast = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Broadcast,
            partition_exprs: Vec::new(),
            output_columns: columns.clone(),
        }),
        children: vec![values],
        output_columns: columns,
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    };
    let planned = crate::sql::planner::distributed::build::build_distributed_plan(&broadcast)
        .expect("planner broadcast DistributedPlan");
    assert_eq!(
        planned.edges()[0].stream_kind,
        FragmentStreamKind::Broadcast
    );
    assert!(matches!(
        planned.edges()[0].output_partition.kind,
        PartitionKind::Unpartitioned
    ));

    let result = build_for_test(TestBuildRequest::result(
        &planned,
        &EmptyCatalog,
        &ConnectorRegistry::new(),
        None,
    ))
    .expect("native fragment build");
    assert_eq!(
        result.0.scheduling_view().edges()[0].stream_kind,
        FragmentStreamKind::Broadcast
    );
    assert!(matches!(
        result.0.scheduling_view().edges()[0].output_partition.kind,
        PartitionKind::Unpartitioned
    ));
}

#[test]
fn random_partition_with_other_stream_kind_remains_other() {
    let plan = crate::sql::planner::distributed::test_support::rebuild_test_plan(
        stream_exchange_plan(ExchangeFlavor::Distribution),
        Default::default(),
        |draft| {
            let partition = DataPartition {
                kind: PartitionKind::Random,
                exprs: Vec::new(),
            };
            let DistributedNodeKind::Exchange(exchange) =
                &mut draft.fragments_mut()[1].root.payload
            else {
                panic!("target must be exchange");
            };
            exchange.partition = partition.clone();
            draft.edges_mut()[0].output_partition = partition.clone();
            draft.fragments_mut()[0].output_partition = partition;
            draft.edges_mut()[0].stream_kind = FragmentStreamKind::Other;
        },
    );

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &ConnectorRegistry::new(),
        None,
    ))
    .expect("build random stream");

    assert_eq!(
        result.0.scheduling_view().edges()[0].stream_kind,
        FragmentStreamKind::Other
    );
    assert!(matches!(
        result.0.scheduling_view().edges()[0].output_partition.kind,
        PartitionKind::Random
    ));
}

#[test]
fn legal_stream_partition_kind_combinations_remain_unchanged() {
    let cases = [
        (DataPartition::unpartitioned(), FragmentStreamKind::Gather),
        (
            DataPartition::unpartitioned(),
            FragmentStreamKind::Broadcast,
        ),
        (
            DataPartition {
                kind: PartitionKind::Random,
                exprs: Vec::new(),
            },
            FragmentStreamKind::Other,
        ),
        (
            DataPartition {
                kind: PartitionKind::Hash,
                exprs: vec![column_ref(1, "k")],
            },
            FragmentStreamKind::Partitioned,
        ),
    ];

    for (partition, stream_kind) in cases {
        let plan = crate::sql::planner::distributed::test_support::rebuild_test_plan(
            stream_exchange_plan(ExchangeFlavor::Distribution),
            Default::default(),
            |draft| {
                let DistributedNodeKind::Exchange(exchange) =
                    &mut draft.fragments_mut()[1].root.payload
                else {
                    panic!("target must be exchange");
                };
                exchange.partition = partition.clone();
                draft.edges_mut()[0].output_partition = partition.clone();
                draft.fragments_mut()[0].output_partition = partition.clone();
                draft.edges_mut()[0].stream_kind = stream_kind;
            },
        );

        let result = build_for_test(TestBuildRequest::result(
            &plan,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        ))
        .unwrap_or_else(|err| {
            panic!(
                "legal stream combination {:?}+{stream_kind:?} must lower: {err}",
                partition.kind
            )
        });

        assert_eq!(
            result.0.scheduling_view().edges()[0].stream_kind,
            stream_kind
        );
        assert_eq!(
            std::mem::discriminant(&result.0.scheduling_view().edges()[0].output_partition.kind),
            std::mem::discriminant(&partition.kind)
        );
    }
}

#[test]
fn lower_distributed_plan_accepts_stream_limit_and_topn_exchange_flavors() {
    let cases = vec![
        (
            "limit_offset",
            ExchangeFlavor::LimitOffset {
                limit: Some(1),
                offset: Some(0),
            },
        ),
        (
            "topn_split",
            ExchangeFlavor::TopNSplit {
                items: Vec::new(),
                limit: Some(1),
                offset: Some(0),
            },
        ),
    ];

    for (label, flavor) in cases {
        let dp = stream_exchange_plan(flavor);
        build_for_test(TestBuildRequest::result(
            &dp,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        ))
        .unwrap_or_else(|err| panic!("{label} stream exchange should lower: {err}"));
    }
}

#[test]
fn fragment_build_preserves_finalized_edges_and_input_plan() {
    let dp = stream_exchange_plan(ExchangeFlavor::Distribution);
    let before = format!("{dp:#?}");
    let planned_edges = format!("{:#?}", dp.edges());

    let result = build_for_test(TestBuildRequest::result(
        &dp,
        &EmptyCatalog,
        &ConnectorRegistry::new(),
        None,
    ))
    .expect("native fragment build");

    assert_eq!(format!("{dp:#?}"), before);
    assert_eq!(
        format!("{:#?}", result.0.scheduling_view().edges()),
        planned_edges
    );
}

#[test]
fn fragment_build_preserves_finalized_router_edge() {
    let planned = finalized_router_plan();
    let before = format!("{planned:#?}");
    let planned_edges = format!("{:#?}", planned.edges());

    let result = build_for_test(TestBuildRequest::result(
        &planned,
        &EmptyCatalog,
        &ConnectorRegistry::new(),
        None,
    ))
    .expect("native fragment build");

    assert_eq!(format!("{planned:#?}"), before);
    assert_eq!(
        format!("{:#?}", result.0.scheduling_view().edges()),
        planned_edges
    );

    let edge = &result.0.scheduling_view().edges()[0];
    assert!(matches!(edge.output_partition.kind, PartitionKind::Hash));
    assert_eq!(edge.output_partition.exprs.len(), 1);
    let ExprKind::ColumnRef {
        column_id, column, ..
    } = &edge.output_partition.exprs[0].kind
    else {
        panic!("expected router HASH partition column ref");
    };
    assert_eq!(*column_id, ColumnId::new_for_test(3));
    assert_eq!(column, "delete_id");
    assert_eq!(edge.stream_kind, FragmentStreamKind::Partitioned);
    let source = result
        .1
        .get(edge.source_fragment_id)
        .expect("router source fragment");
    let route_partition = match source
        .sink
        .as_ref()
        .and_then(|sink| sink.kind.as_ref())
        .expect("router sink")
    {
        crate::proto::plan::data_sink::Kind::ChangeStreamRouter(router) => router.branches[0]
            .output_partition
            .as_ref()
            .expect("router route partition"),
        other => panic!("expected router sink, got {other:?}"),
    };
    assert_eq!(
        route_partition.kind,
        crate::proto::plan::PartitionKind::Hash as i32
    );
    assert_eq!(route_partition.exprs.len(), 1);

    let target = result
        .1
        .get(edge.target_fragment_id)
        .expect("router target fragment");
    let receiver = match target
        .root
        .as_ref()
        .and_then(|root| root.payload.as_ref())
        .expect("router target exchange receiver")
    {
        crate::proto::plan::distributed_node::Payload::Exchange(exchange) => exchange,
        other => panic!("expected router exchange receiver, got {other:?}"),
    };
    assert_eq!(receiver.partition_type, route_partition.kind);
    assert_eq!(receiver.partition_exprs, route_partition.exprs);
}

#[test]
fn lower_distributed_plan_owns_native_fragments_matching_schedules_and_root() {
    let dp = stream_exchange_plan(ExchangeFlavor::LimitOffset {
        limit: Some(1),
        offset: Some(0),
    });

    let result = build_for_test(TestBuildRequest::result(
        &dp,
        &EmptyCatalog,
        &ConnectorRegistry::new(),
        None,
    ))
    .expect("native fragment build");
    let fragment_ids = result.1.fragment_ids().collect::<BTreeSet<_>>();
    let prepared_ids = result.0.fragment_ids();

    assert_eq!(fragment_ids, prepared_ids);
    assert!(fragment_ids.contains(&dp.root_fragment_id()));
}

#[test]
fn fragment_build_preserves_finalized_cte_multicast_edge_output_slots() {
    let cte_id: CteId = 7;
    let producer_columns = vec![
        output_col(1, "k"),
        output_col(2, "v"),
        output_col(3, "payload"),
    ];
    let receive_columns = vec![producer_columns[0].clone(), producer_columns[2].clone()];
    let receive_producer_column_ids =
        vec![producer_columns[0].column_id, producer_columns[2].column_id];

    let producer_fragment_id = 1;
    let consumer_fragment_id = 0;
    let exchange_node_id = 20;
    let producer_fragment = PlanFragment {
        fragment_id: producer_fragment_id,
        root: physical_values_node(producer_fragment_id, 10, producer_columns.clone()),
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Noop,
        output_exprs: None,
        output_columns: producer_columns,
        cte_id: Some(cte_id),
        cte_exchange_nodes: Vec::new(),
    };
    let consumer_fragment = PlanFragment {
        fragment_id: consumer_fragment_id,
        root: DistributedNode {
            node_id: exchange_node_id,
            fragment_id: consumer_fragment_id,
            tuple_ids: vec![exchange_node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                partition: DataPartition::unpartitioned(),
                source_fragment_id: producer_fragment_id,
                output_columns: receive_columns.clone(),
                output_qualifier: Some("c".to_string()),
                flavor: ExchangeFlavor::CteMulticast {
                    cte_id,
                    receive_producer_column_ids: receive_producer_column_ids.clone(),
                },
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Result,
        output_exprs: None,
        output_columns: receive_columns,
        cte_id: None,
        cte_exchange_nodes: vec![(
            cte_id,
            exchange_node_id,
            receive_producer_column_ids.clone(),
        )],
    };
    let dp = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![producer_fragment, consumer_fragment],
        root_fragment_id: consumer_fragment_id,
        runtime_filter_graph: Default::default(),
        edges: vec![FragmentEdge {
            source_fragment_id: producer_fragment_id,
            target_fragment_id: consumer_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            },
            output_slot_ids: vec![1, 3],
        }],
    };
    let before = format!("{dp:#?}");

    let result = build_for_test(TestBuildRequest::result(
        &dp,
        &EmptyCatalog,
        &ConnectorRegistry::new(),
        None,
    ))
    .expect("native lower plan");

    assert_eq!(format!("{dp:#?}"), before);
    assert_eq!(
        result.0.scheduling_view().edges()[0].output_slot_ids,
        vec![1, 3]
    );
    let native_consumer = result
        .1
        .get(consumer_fragment_id)
        .expect("encoded native consumer");
    assert_eq!(native_consumer.cte_exchange_nodes[0].column_ids, vec![1, 3]);
}
