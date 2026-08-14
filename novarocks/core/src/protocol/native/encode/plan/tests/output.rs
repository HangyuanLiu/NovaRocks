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
use novarocks_sql::test_support::{NativeEncoderPlanFixture, native_encoder_plan};

fn encode_fixture(fixture: NativeEncoderPlanFixture) -> plan::DistributedPlan {
    let plan = native_encoder_plan(fixture).expect("output fixture must seal");
    encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan")
}

fn output_ids(encoded: &plan::DistributedPlan) -> Vec<u32> {
    encoded.fragments[0]
        .output_columns
        .iter()
        .map(|column| column.column_id)
        .collect()
}

#[test]
fn result_fragment_output_columns_map_finalized_project_root_unique_ids() {
    let encoded = encode_fixture(NativeEncoderPlanFixture::DuplicateProject);
    assert_eq!(output_ids(&encoded), vec![1, 3]);
}

#[test]
fn topn_root_fragment_output_columns_map_finalized_child_unique_ids() {
    let encoded = encode_fixture(NativeEncoderPlanFixture::TopNDuplicateProject);
    assert_eq!(output_ids(&encoded), vec![1, 3]);
}

#[test]
fn encoder_maps_sealed_join_output_columns_from_the_node_output_contract() {
    let encoded = encode_fixture(NativeEncoderPlanFixture::ReconciledHashJoin);
    let root = encoded.fragments[0].root.as_ref().expect("encoded root");
    let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref() else {
        panic!("expected physical join root");
    };
    assert_eq!(
        physical
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "l_k"), (2, "r_k")]
    );
}

#[test]
fn encoder_maps_sealed_nest_loop_join_output_columns_from_the_node_output_contract() {
    let encoded = encode_fixture(NativeEncoderPlanFixture::NestLoopJoin);
    let root = encoded.fragments[0].root.as_ref().expect("encoded root");
    let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref() else {
        panic!("expected physical nest-loop root");
    };
    assert_eq!(
        physical
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "l_k"), (2, "r_k")]
    );
}

#[test]
fn assert_one_row_root_fragment_output_columns_follow_finalized_child_schema() {
    let encoded = encode_fixture(NativeEncoderPlanFixture::AssertOneRow);
    assert_eq!(output_ids(&encoded), vec![1]);
}

#[test]
fn sort_root_fragment_output_columns_follow_finalized_child_schema() {
    let encoded = encode_fixture(NativeEncoderPlanFixture::Sort);
    assert_eq!(output_ids(&encoded), vec![4, 1]);
}
