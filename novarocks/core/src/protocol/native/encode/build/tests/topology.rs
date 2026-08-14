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

use std::collections::BTreeSet;

use novarocks_sql::plan_read::{ColumnId, ExprKind, FragmentStreamKind, PartitionKind};
use novarocks_sql::test_support::{NativeBuildFixture, native_build_plan};

use super::*;

macro_rules! build_fixture {
    ($fixture:expr) => {{
        let plan = native_build_plan($fixture).expect("sealed build fixture");
        build_for_test(TestBuildRequest::result(
            &plan,
            &ConnectorRegistry::new(),
            None,
        ))
        .expect("native fragment build")
    }};
}

#[test]
fn planner_broadcast_edge_remains_broadcast_through_builder() {
    let built = build_fixture!(NativeBuildFixture::BroadcastStream);
    assert_eq!(
        built.0.scheduling_view().edges()[0].stream_kind,
        FragmentStreamKind::Broadcast
    );
    assert!(matches!(
        built.0.scheduling_view().edges()[0].output_partition.kind,
        PartitionKind::Unpartitioned
    ));
}

#[test]
fn random_partition_with_other_stream_kind_remains_other() {
    let built = build_fixture!(NativeBuildFixture::RandomOtherStream);
    assert_eq!(
        built.0.scheduling_view().edges()[0].stream_kind,
        FragmentStreamKind::Other
    );
    assert!(matches!(
        built.0.scheduling_view().edges()[0].output_partition.kind,
        PartitionKind::Random
    ));
}

#[test]
fn legal_stream_partition_kind_combinations_remain_unchanged() {
    let cases = [
        (
            NativeBuildFixture::LimitOffsetStream,
            FragmentStreamKind::Gather,
        ),
        (
            NativeBuildFixture::BroadcastStream,
            FragmentStreamKind::Broadcast,
        ),
        (
            NativeBuildFixture::RandomOtherStream,
            FragmentStreamKind::Other,
        ),
        (
            NativeBuildFixture::HashPartitionedStream,
            FragmentStreamKind::Partitioned,
        ),
    ];

    for (fixture, stream_kind) in cases {
        let built = build_fixture!(fixture);
        let edge = &built.0.scheduling_view().edges()[0];
        assert_eq!(edge.stream_kind, stream_kind);
        match stream_kind {
            FragmentStreamKind::Gather | FragmentStreamKind::Broadcast => {
                assert!(matches!(
                    edge.output_partition.kind,
                    PartitionKind::Unpartitioned
                ));
            }
            FragmentStreamKind::Other => {
                assert!(matches!(edge.output_partition.kind, PartitionKind::Random));
            }
            FragmentStreamKind::Partitioned => {
                assert!(matches!(edge.output_partition.kind, PartitionKind::Hash));
            }
        }
    }
}

#[test]
fn lower_distributed_plan_accepts_stream_limit_and_topn_exchange_flavors() {
    for fixture in [
        NativeBuildFixture::LimitOffsetStream,
        NativeBuildFixture::TopNSplitStream,
    ] {
        build_fixture!(fixture);
    }
}

#[test]
fn fragment_build_preserves_finalized_edges_and_input_plan() {
    let plan = native_build_plan(NativeBuildFixture::LimitOffsetStream).expect("sealed fixture");
    let before = format!("{plan:#?}");
    let planned_edges = format!("{:#?}", plan.edges());
    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &ConnectorRegistry::new(),
        None,
    ))
    .expect("native fragment build");
    assert_eq!(format!("{plan:#?}"), before);
    assert_eq!(
        format!("{:#?}", result.0.scheduling_view().edges()),
        planned_edges
    );
}

#[test]
fn fragment_build_preserves_finalized_router_edge() {
    let plan = native_build_plan(NativeBuildFixture::RouterStream).expect("sealed router fixture");
    let before = format!("{plan:#?}");
    let planned_edges = format!("{:#?}", plan.edges());
    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &ConnectorRegistry::new(),
        None,
    ))
    .expect("native fragment build");

    assert_eq!(format!("{plan:#?}"), before);
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
    assert_eq!(*column_id, ColumnId(3));
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
        novarocks_protocol::plan::data_sink::Kind::ChangeStreamRouter(router) => router.routes[0]
            .output_partition
            .as_ref()
            .expect("router route partition"),
        other => panic!("expected router sink, got {other:?}"),
    };
    assert_eq!(
        route_partition.kind,
        novarocks_protocol::plan::PartitionKind::Hash as i32
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
        novarocks_protocol::plan::distributed_node::Payload::Exchange(exchange) => exchange,
        other => panic!("expected router exchange receiver, got {other:?}"),
    };
    assert_eq!(receiver.partition_type, route_partition.kind);
    assert_eq!(receiver.partition_exprs, route_partition.exprs);
}

#[test]
fn lower_distributed_plan_owns_native_fragments_matching_schedules_and_root() {
    let plan = native_build_plan(NativeBuildFixture::LimitOffsetStream).expect("sealed fixture");
    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &ConnectorRegistry::new(),
        None,
    ))
    .expect("native fragment build");
    let fragment_ids = result.1.fragment_ids().collect::<BTreeSet<_>>();
    let prepared_ids = result.0.fragment_ids();
    assert_eq!(fragment_ids, prepared_ids);
    assert!(fragment_ids.contains(&plan.root_fragment_id()));
}

#[test]
fn fragment_build_preserves_finalized_cte_multicast_edge_output_slots() {
    let plan = native_build_plan(NativeBuildFixture::CteMulticastStream)
        .expect("sealed CTE multicast fixture");
    let before = format!("{plan:#?}");
    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &ConnectorRegistry::new(),
        None,
    ))
    .expect("native lower plan");
    assert_eq!(format!("{plan:#?}"), before);
    assert_eq!(
        result.0.scheduling_view().edges()[0].output_slot_ids,
        vec![1, 3]
    );
    let native_consumer = result.1.get(0).expect("encoded native consumer");
    assert_eq!(native_consumer.cte_exchange_nodes[0].column_ids, vec![1, 3]);
}
