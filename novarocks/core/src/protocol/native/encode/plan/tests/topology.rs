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
use novarocks_sql::test_support::{NativePlanEncodingFixture, native_plan_encoding_plan};

fn encode_fixture(fixture: NativePlanEncodingFixture) -> plan::DistributedPlan {
    let plan = native_plan_encoding_plan(fixture).expect("sealed plan topology fixture");
    encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan")
}

fn stream_nodes(encoded: &plan::DistributedPlan) -> (&plan::DataStreamSink, &plan::ExchangeNode) {
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
    (sink, exchange)
}

#[test]
fn stream_sink_projection_and_receiver_schema_follow_edge_output_slots() {
    let encoded = encode_fixture(NativePlanEncodingFixture::ReorderedSlots);
    let (sink, exchange) = stream_nodes(&encoded);
    assert_eq!(sink.output_columns, vec![2, 1]);
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
    let encoded = encode_fixture(NativePlanEncodingFixture::LoweredSlots);
    let (sink, exchange) = stream_nodes(&encoded);
    assert_eq!(sink.output_columns, vec![10, 20]);
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
    let encoded = encode_fixture(NativePlanEncodingFixture::ZeroColumns);
    let (sink, exchange) = stream_nodes(&encoded);
    assert!(sink.output_columns.is_empty());
    assert!(exchange.output_columns.is_empty());
}

#[test]
fn stream_sink_derives_generate_series_source_schema() {
    let encoded = encode_fixture(NativePlanEncodingFixture::GenerateSeries);
    let (sink, exchange) = stream_nodes(&encoded);
    assert_eq!(sink.output_columns, vec![7]);
    assert_eq!(
        exchange
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str(), column.nullable))
            .collect::<Vec<_>>(),
        vec![(7, "generate_series", false)]
    );
}
