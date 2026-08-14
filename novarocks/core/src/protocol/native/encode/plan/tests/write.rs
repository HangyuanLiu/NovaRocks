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

use prost::Message;

use super::super::write::encode_change_stream_router_sink;
use super::*;
use novarocks_sql::test_support::{NativeEncoderPlanFixture, native_encoder_plan};

fn change_stream_router_plan() -> DistributedPlan {
    native_encoder_plan(NativeEncoderPlanFixture::ChangeStreamRouter)
        .expect("change-stream router fixture must seal")
}

#[test]
fn change_stream_router_encoder_materializes_partition_exprs() {
    let plan = change_stream_router_plan();
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
    let plan = change_stream_router_plan();
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
