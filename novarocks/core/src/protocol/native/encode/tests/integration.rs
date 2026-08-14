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

use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::datatypes::DataType;
use bytes::Bytes;
use novarocks_sql::plan_read::{ColumnId, FragmentId, OutputColumn};
use novarocks_sql::test_support::{NativeEncoderPlanFixture, native_encoder_plan};
use prost::Message;

use super::super::plan;
use crate::protocol::native::type_mapping::decode_type;
use crate::query_execution::preparation::scan::ScanExecutionBindings;

fn empty_scan_bindings() -> &'static ScanExecutionBindings {
    Box::leak(Box::new(ScanExecutionBindings::default()))
}

fn planner_output_column(id: u32, name: &str, data_type: DataType) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId(id),
        name: name.to_string(),
        data_type,
        nullable: false,
        is_internal: false,
    }
}

fn prepared_connector_scan_bindings(
    fragment_id: FragmentId,
    node_id: i32,
    columns: &[OutputColumn],
    required_columns: &[&str],
) -> ScanExecutionBindings {
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse("ice")
        .expect("fixture connector instance ID");
    let mut bindings = ScanExecutionBindings::default();
    let physical_columns = columns
        .iter()
        .map(|planner| crate::query_execution::preparation::scan::ResolvedScanColumn {
            planner: planner.clone(),
            source: novarocks_catalog::schema::ColumnDef {
                name: planner.name.clone(),
                data_type: planner.data_type.clone(),
                nullable: planner.nullable,
                write_default: None,
                logical_type: None,
            },
            kind: crate::query_execution::preparation::scan::ResolvedScanColumnKind::PhysicalTableColumn,
        })
        .collect::<Vec<_>>();
    let required_reads = physical_columns
        .iter()
        .filter(|column| {
            required_columns
                .iter()
                .any(|required| column.source.name.eq_ignore_ascii_case(required))
        })
        .map(|column| crate::query_execution::preparation::scan::ResolvedReadColumn {
            planner_column_id: Some(column.planner.column_id),
            source: column.source.clone(),
            reason: crate::query_execution::preparation::scan::ResolvedReadReason::PlannerRequiredOrOutput,
        })
        .collect::<Vec<_>>();
    bindings
        .insert_binding(
            crate::query_execution::preparation::scan::ResolvedScanBinding {
                node_id,
                execution:
                    crate::query_execution::preparation::scan::ResolvedScanExecution::ConnectorRead,
                physical_columns,
                required_reads,
            },
        )
        .expect("insert prepared connector scan binding");
    let declaration = novarocks_spi::connector::ConnectorExecutionDeclaration::try_new(
        novarocks_spi::connector::ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("fixture connector provider ID"),
            instance_id: instance_id.clone(),
        },
        novarocks_spi::connector::ConnectorInstanceIncarnation::from_bytes([7; 16]),
        Bytes::from_static(b"integration-binding"),
    )
    .expect("fixture connector declaration");
    let scan = novarocks_spi::connector::ConnectorScan::try_new_snapshot(
        novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id: instance_id.clone(),
            incarnation: declaration.incarnation(),
        },
        novarocks_spi::connector::ConnectorReadSelector::Current,
        novarocks_spi::connector::ConnectorScanHandle::try_new(
            instance_id,
            Bytes::from_static(b"integration-scan"),
        )
        .expect("fixture connector scan handle"),
        Arc::new(arrow::datatypes::Schema::new(
            columns
                .iter()
                .map(|column| {
                    arrow::datatypes::Field::new(
                        &column.name,
                        column.data_type.clone(),
                        column.nullable,
                    )
                })
                .collect::<Vec<_>>(),
        )),
        Vec::new(),
    )
    .expect("sealed fixture connector scan");
    bindings
        .insert_connector_read(
            fragment_id,
            node_id,
            crate::query_execution::preparation::scan::PlannedConnectorRead {
                declaration,
                scan,
                splits: Vec::new(),
                provider_field_ordinals: (0..columns.len() as u32).collect(),
                planning_metrics: novarocks_spi::connector::ConnectorSplitPlanningMetrics::default(
                ),
                static_predicates: Vec::new(),
                predicate_dispositions: Vec::new(),
                residual_predicates: Vec::new(),
                batch: novarocks_spi::connector::ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero row budget"),
                    max_bytes: NonZeroUsize::new(1024).expect("nonzero byte budget"),
                },
                planning_lease: crate::query_execution::preparation::scan::fixture_planning_lease(
                    "ice",
                ),
                read_session: None,
            },
        )
        .expect("insert prepared connector read");
    bindings
}

fn encode_stream_fixture(fixture: NativeEncoderPlanFixture) -> plan::DistributedPlan {
    let plan = native_encoder_plan(fixture).expect("sealed stream-edge fixture");
    plan::encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode distributed plan")
}

fn encoded_exchange(encoded: &plan::DistributedPlan) -> &plan::ExchangeNode {
    let target_fragment = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 1)
        .expect("target fragment");
    let root = target_fragment.root.as_ref().expect("target root");
    let Some(plan::distributed_node::Payload::Exchange(exchange)) = root.payload.as_ref() else {
        panic!("expected exchange receiver payload");
    };
    exchange
}

#[test]
fn distributed_plan_encoder_round_trips_fragments_edges_partitions_and_exchange() {
    let plan = native_encoder_plan(NativeEncoderPlanFixture::HashExchange)
        .expect("native hash exchange fixture must seal");
    let encoded = plan::encode_distributed_plan(&plan, empty_scan_bindings())
        .expect("encode distributed plan");
    let decoded =
        novarocks_protocol::plan::DistributedPlan::decode(encoded.encode_to_vec().as_slice())
            .expect("decode proto message");

    assert_eq!(decoded.root_fragment_id, 1);
    assert_eq!(decoded.fragments.len(), 2);
    assert_eq!(decoded.edges.len(), 1);
    assert_eq!(decoded.edges[0].target_exchange_node_id, 42);
    assert_eq!(
        decoded.edges[0].output_partition,
        novarocks_protocol::plan::PartitionType::Hash as i32
    );
    assert_eq!(
        decoded.edges[0]
            .edge_kind
            .as_ref()
            .and_then(|kind| kind.kind.as_ref()),
        Some(&novarocks_protocol::plan::fragment_edge_kind::Kind::Stream(
            true
        ))
    );

    let root_fragment = decoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 1)
        .expect("root fragment");
    let root = root_fragment.root.as_ref().expect("root node");
    let Some(novarocks_protocol::plan::distributed_node::Payload::Exchange(exchange)) =
        root.payload.as_ref()
    else {
        panic!("expected exchange receiver payload");
    };
    assert_eq!(exchange.source_fragment_id, 0);
    assert_eq!(exchange.output_qualifier.as_deref(), Some("recv"));
    assert_eq!(
        exchange.partition_type,
        novarocks_protocol::plan::PartitionType::Hash as i32
    );
    assert_eq!(exchange.output_columns.len(), 1);
    assert_eq!(exchange.output_columns[0].column_id, 10);
    assert_eq!(exchange.output_columns[0].name, "v");
}

#[test]
fn stream_edge_projects_pruned_scan_columns_by_column_id() {
    let all_scan_columns = vec![
        planner_output_column(1, "v1", DataType::Int64),
        planner_output_column(2, "s2", DataType::Utf8),
        planner_output_column(3, "array1", DataType::Int64),
    ];
    let plan = native_encoder_plan(NativeEncoderPlanFixture::PrunedConnectorScanStreamEdge)
        .expect("sealed pruned scan fixture");
    let scan_bindings =
        prepared_connector_scan_bindings(0, 11, &all_scan_columns, &["s2", "array1"]);
    let encoded = plan::encode_distributed_plan(&plan, &scan_bindings).expect("encode plan");

    let patched = encoded_exchange(&encoded)
        .output_columns
        .iter()
        .map(|column| (column.column_id, column.name.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(patched, vec![(2, "s2"), (3, "array1")]);
}

#[test]
fn stream_edge_patches_exchange_columns_from_aggregate_layout_when_fragment_output_is_empty() {
    let encoded = encode_stream_fixture(NativeEncoderPlanFixture::AggregateLayoutStreamEdge);
    let exchange = encoded_exchange(&encoded);
    assert_eq!(exchange.output_columns.len(), 1);
    assert_eq!(exchange.output_columns[0].column_id, 2);
    assert_eq!(exchange.output_columns[0].name, "c1");
}

#[test]
fn stream_edge_patches_local_avg_exchange_schema_to_intermediate_type() {
    let encoded = encode_stream_fixture(NativeEncoderPlanFixture::LocalAverageStreamEdge);
    let exchange = encoded_exchange(&encoded);
    assert_eq!(exchange.output_columns.len(), 2);
    assert_eq!(exchange.output_columns[1].column_id, 15);
    let avg_type = decode_type(
        exchange.output_columns[1]
            .r#type
            .as_ref()
            .expect("avg column type"),
    )
    .expect("decode avg column type");
    assert_eq!(avg_type, DataType::Utf8);
}

#[test]
fn stream_edge_allows_zero_column_source_when_no_slots_are_requested() {
    let encoded = encode_stream_fixture(NativeEncoderPlanFixture::ZeroColumnStreamEdge);
    assert!(encoded_exchange(&encoded).output_columns.is_empty());
}
