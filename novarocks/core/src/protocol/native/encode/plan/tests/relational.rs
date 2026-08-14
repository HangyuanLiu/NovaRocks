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

use super::super::relational::encoded_physical_variant_names_for_test;
use super::*;
use novarocks_sql::test_support::{
    NativeEncoderPlanFixture, native_encoder_plan, native_physical_plan_variant_names,
};

#[test]
fn physical_plan_encoder_variant_guard_tracks_rust_enum_not_proto_arms() {
    assert_eq!(
        encoded_physical_variant_names_for_test(),
        native_physical_plan_variant_names()
    );
    assert!(
        !encoded_physical_variant_names_for_test().contains(&"Decode"),
        "Decode exists only as a proto arm; Rust physical plans are the source of truth"
    );
}

#[test]
fn hash_aggregate_payload_maps_group_layout_and_mode() {
    let distributed = native_encoder_plan(NativeEncoderPlanFixture::HashAggregate)
        .expect("aggregate fixture must seal");

    let encoded =
        encode_distributed_plan(&distributed, empty_scan_bindings()).expect("encode aggregate");
    let root = encoded.fragments[0].root.as_ref().expect("aggregate root");
    let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref() else {
        panic!("expected physical aggregate payload");
    };
    let Some(plan::plan_node::Kind::HashAggregate(aggregate)) = physical.kind.as_ref() else {
        panic!("expected HashAggregate payload");
    };
    assert_eq!(aggregate.mode, i32::from(plan::AggMode::Local));
    assert_eq!(aggregate.group_by.len(), 1);
    assert_eq!(
        aggregate
            .output_layout
            .as_ref()
            .expect("aggregate output layout")
            .group_key_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(
        aggregate
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[test]
fn encoded_join_output_maps_reconciled_children_not_stale_payload() {
    let plan = native_encoder_plan(NativeEncoderPlanFixture::ReconciledHashJoin)
        .expect("reconciled join fixture must seal");

    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");
    let root = encoded.fragments[0].root.as_ref().expect("root");
    let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref() else {
        panic!("expected physical join root");
    };
    assert_eq!(
        physical
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "l_k"), (2, "r_k")],
        "the encoder maps the reconciled contract, dropping the stale id 999"
    );
}
