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
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorExecutionDeclaration, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorProviderId, ConnectorScan,
    ConnectorScanHandle,
};

use super::*;
use crate::protocol::native::encode::plan as native_plan;
use crate::query_execution::preparation::scan::{
    ResolvedReadColumn, ResolvedReadReason, ResolvedScanBinding, ResolvedScanColumn,
    ResolvedScanColumnKind, ResolvedScanExecution, ScanExecutionBindings,
};
use novarocks_sql::plan_read::ColumnId;
use novarocks_sql::test_support::{NativeScanFixture, native_scan_plan};

#[test]
fn iceberg_delta_table_encoder_requires_prepared_connector_read() {
    let plan = novarocks_sql::test_support::native_scan_plan(
        novarocks_sql::test_support::NativeScanFixture::DeltaWithStaleUnprojectedColumn,
    )
    .expect("sealed delta scan fixture");
    let source_column = novarocks_catalog::schema::ColumnDef {
        name: "physical_order_id".to_string(),
        data_type: DataType::Int64,
        nullable: false,
        write_default: None,
        logical_type: None,
    };
    let hidden_equality_column = column_def_for_test("tenant_id", DataType::Int64, false);
    let mut bindings = ScanExecutionBindings::default();
    bindings
        .insert_binding(ResolvedScanBinding {
            node_id: 10,
            execution: ResolvedScanExecution::SealedConnectorScan(
                crate::query_execution::preparation::scan::fixture_sealed_change_scan("ice", 6, 7),
            ),
            physical_columns: vec![ResolvedScanColumn {
                planner: output_column(1, "bound_order_id", DataType::Int64),
                source: source_column.clone(),
                kind: ResolvedScanColumnKind::PhysicalTableColumn,
            }],
            required_reads: vec![
                ResolvedReadColumn {
                    planner_column_id: Some(ColumnId(1)),
                    source: source_column,
                    reason: ResolvedReadReason::PlannerRequiredOrOutput,
                },
                ResolvedReadColumn {
                    planner_column_id: None,
                    source: hidden_equality_column,
                    reason: ResolvedReadReason::EqualityDeleteKey,
                },
            ],
        })
        .expect("insert prepared delta binding");
    bindings
        .insert_connector_read(0, 10, planned_connector_read_for_test())
        .expect("insert planned delta connector read");

    let encoded = native_plan::encode_distributed_plan_with_context(
        &plan,
        native_plan::NativePlanEncodeContext {
            scan_bindings: Some(&bindings),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
        },
    )
    .expect("encode prepared delta binding");

    let root = encoded.fragments[0].root.as_ref().expect("encoded root");
    let Some(novarocks_protocol::plan::distributed_node::Payload::Physical(physical)) =
        root.payload.as_ref()
    else {
        panic!("expected physical root");
    };
    let Some(novarocks_protocol::plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
        panic!("expected scan root");
    };
    assert_eq!(scan.columns[0].name, "physical_order_id");
    assert_eq!(
        scan.required_columns,
        vec!["physical_order_id", "tenant_id"]
    );
    let table = scan.table.as_ref().expect("bound table");
    assert_eq!(
        table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["physical_order_id", "tenant_id"]
    );
    assert!(table.iceberg_row_lineage_metadata_columns.is_empty());
    let Some(novarocks_protocol::plan::scan_source::Kind::ConnectorRead(connector)) = table
        .source
        .as_ref()
        .and_then(|source| source.kind.as_ref())
    else {
        panic!("expected encoded connector source");
    };
    assert_eq!(connector.instance_id, "ice");
    assert_eq!(connector.scan_payload, b"delta-scan".to_vec());
    assert!(connector.splits.is_empty());
}

fn planned_connector_read_for_test()
-> crate::query_execution::preparation::scan::PlannedConnectorRead {
    let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
    let declaration = ConnectorExecutionDeclaration::try_new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: instance_id.clone(),
        },
        ConnectorInstanceIncarnation::from_bytes([7; 16]),
        Bytes::from_static(b"binding"),
    )
    .expect("declaration");
    let scan = ConnectorScan::try_new_snapshot(
        novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id: instance_id.clone(),
            incarnation: declaration.incarnation(),
        },
        novarocks_spi::connector::ConnectorReadSelector::Current,
        ConnectorScanHandle::try_new(instance_id, Bytes::from_static(b"delta-scan"))
            .expect("scan handle"),
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("physical_order_id", DataType::Int64, false),
            arrow::datatypes::Field::new("tenant_id", DataType::Int64, false),
        ])),
        Vec::new(),
    )
    .expect("sealed scan");
    crate::query_execution::preparation::scan::PlannedConnectorRead {
        declaration,
        scan,
        splits: Vec::new(),
        provider_field_ordinals: vec![0, 1],
        planning_metrics: novarocks_spi::connector::ConnectorSplitPlanningMetrics::default(),
        static_predicates: Vec::new(),
        predicate_dispositions: Vec::new(),
        residual_predicates: Vec::new(),
        batch: ConnectorBatchBudget {
            max_rows: NonZeroUsize::new(1024).expect("nonzero rows"),
            max_bytes: NonZeroUsize::new(1024).expect("nonzero bytes"),
        },
        planning_lease: crate::query_execution::preparation::scan::fixture_planning_lease("ice"),
        read_session: None,
    }
}

#[test]
fn ordinary_iceberg_binding_preserves_existing_encoding() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergWithUnprojectedPayload)
        .expect("sealed ordinary Iceberg fixture");

    let missing = encode_distributed_plan(&plan, empty_scan_bindings())
        .expect_err("tokenized SQL scans require a prepared binding");
    assert!(missing.contains("SqlData"), "{missing}");
    let mut bindings = ScanExecutionBindings::default();
    bindings
        .insert_binding(file_binding_for_test(
            10,
            vec![bound_column_for_test(
                1,
                "order_id",
                "order_id",
                ResolvedScanColumnKind::PhysicalTableColumn,
            )],
            vec![bound_read_for_test(Some(1), "order_id")],
        ))
        .expect("insert ordinary Iceberg binding");
    bindings
        .insert_connector_read(0, 10, planned_connector_read_for_test())
        .expect("materialize ordinary connector read");
    let with_binding = encode_distributed_plan_with_context(
        &plan,
        NativePlanEncodeContext {
            scan_bindings: Some(&bindings),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
        },
    )
    .expect("encode ordinary Iceberg binding");

    let scan = encoded_root_scan_for_test(&with_binding);
    let table = scan.table.as_ref().expect("bound table");
    let Some(novarocks_protocol::plan::scan_source::Kind::ConnectorRead(connector)) = table
        .source
        .as_ref()
        .and_then(|source| source.kind.as_ref())
    else {
        panic!("ordinary source must encode as ConnectorReadSource");
    };
    assert_eq!(connector.instance_id, "ice");
    assert_eq!(connector.scan_payload, b"delta-scan");
}

#[test]
fn refresh_file_bindings_drive_source_projection_metadata_and_hidden_reads() {
    for fixture in [
        NativeScanFixture::RefreshSnapshot,
        NativeScanFixture::RefreshMvTargetLocator,
        NativeScanFixture::RefreshMvTargetState,
    ] {
        let plan = native_scan_plan(fixture).expect("sealed refresh-source fixture");

        let mut bindings = ScanExecutionBindings::default();
        bindings
            .insert_binding(file_binding_for_test(
                10,
                vec![
                    ResolvedScanColumn {
                        planner: output_column(1, "bound_order_id", DataType::Int64),
                        source: column_def_for_test("physical_order_id", DataType::Int64, false),
                        kind: ResolvedScanColumnKind::PhysicalTableColumn,
                    },
                    ResolvedScanColumn {
                        planner: output_column(2, "bound_file", DataType::Utf8),
                        source: column_def_for_test("_file", DataType::Utf8, false),
                        kind: ResolvedScanColumnKind::IcebergMetadataColumn,
                    },
                ],
                vec![
                    bound_read_for_test(Some(1), "physical_order_id"),
                    ResolvedReadColumn {
                        planner_column_id: None,
                        source: column_def_for_test("tenant_id", DataType::Int64, false),
                        reason: ResolvedReadReason::EqualityDeleteKey,
                    },
                ],
            ))
            .expect("insert refresh file binding");
        bindings
            .insert_connector_read(0, 10, planned_connector_read_for_test())
            .expect("materialize refresh connector read");

        let encoded = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&bindings),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
            },
        )
        .expect("encode refresh binding");
        let scan = encoded_root_scan_for_test(&encoded);
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["physical_order_id", "_file"]
        );
        assert_eq!(
            scan.required_columns,
            vec!["physical_order_id", "tenant_id"]
        );
        let table = scan.table.as_ref().expect("bound table");
        assert_eq!(
            table
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["physical_order_id", "tenant_id"],
            "resolver-required sources must encode only binding-owned physical columns and hidden reads"
        );
        assert_eq!(
            table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["_file"]
        );
        let encoded_source = table
            .source
            .as_ref()
            .and_then(|source| source.kind.as_ref())
            .expect("encoded refresh source");
        assert!(
            matches!(
                encoded_source,
                novarocks_protocol::plan::scan_source::Kind::ConnectorRead(_)
            ),
            "prepared refresh scans must cross the native boundary as ConnectorReadSource"
        );
    }
}

#[test]
fn mv_target_sources_require_prepared_connector_reads() {
    for (source_name, fixture) in [
        ("SqlMvTargetLocator", NativeScanFixture::MvTargetLocator),
        ("SqlMvTargetState", NativeScanFixture::MvTargetState),
    ] {
        let plan = native_scan_plan(fixture).expect("sealed MV target source fixture");

        let error = encode_distributed_plan(&plan, empty_scan_bindings())
            .expect_err("unprepared MV target source must fail native submission");
        assert!(error.contains(source_name), "{error}");
    }
}

#[test]
fn required_bindings_reject_missing_node_and_execution_variant_mismatch() {
    let plan = native_scan_plan(NativeScanFixture::DeltaWithStaleUnprojectedColumn)
        .expect("sealed delta scan fixture");
    let missing = encode_distributed_plan_with_context(
        &plan,
        NativePlanEncodeContext {
            scan_bindings: Some(&ScanExecutionBindings::default()),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
        },
    )
    .expect_err("delta source without prepared binding must fail");
    assert!(missing.contains("node_id=10"), "{missing}");
    assert!(missing.contains("SqlDelta"), "{missing}");
    assert!(missing.contains("from_snapshot_id=1"), "{missing}");
    assert!(missing.contains("to_snapshot_id=2"), "{missing}");

    let mut wrong_node = ScanExecutionBindings::default();
    wrong_node
        .insert_binding(delta_binding_for_test(11))
        .expect("insert binding for wrong node");
    let err = encode_distributed_plan_with_context(
        &plan,
        NativePlanEncodeContext {
            scan_bindings: Some(&wrong_node),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
        },
    )
    .expect_err("binding at another node id must not be reused");
    assert!(err.contains("node_id=10"), "{err}");

    let mut wrong_execution = ScanExecutionBindings::default();
    wrong_execution
        .insert_binding(file_binding_for_test(
            10,
            vec![bound_column_for_test(
                1,
                "order_id",
                "order_id",
                ResolvedScanColumnKind::PhysicalTableColumn,
            )],
            vec![bound_read_for_test(Some(1), "order_id")],
        ))
        .expect("insert wrong execution variant");
    let err = encode_distributed_plan_with_context(
        &plan,
        NativePlanEncodeContext {
            scan_bindings: Some(&wrong_execution),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
        },
    )
    .expect_err("delta source with admitted connector binding must fail");
    assert!(err.contains("execution variant mismatch"), "{err}");
    assert!(err.contains("AdmittedConnectorRead"), "{err}");
}

#[test]
fn binding_encoder_preserves_variant_synthetic_output_and_required_name() {
    let plan = native_scan_plan(NativeScanFixture::VariantProjection)
        .expect("sealed VARIANT projection fixture");
    let mut bindings = ScanExecutionBindings::default();
    bindings
        .insert_binding(file_binding_for_test(
            10,
            vec![ResolvedScanColumn {
                planner: output_column(1, "v", DataType::LargeBinary),
                source: column_def_for_test("v", DataType::LargeBinary, false),
                kind: ResolvedScanColumnKind::PhysicalTableColumn,
            }],
            Vec::new(),
        ))
        .expect("insert variant binding");
    bindings
        .insert_connector_read(0, 10, planned_connector_read_for_test())
        .expect("materialize VARIANT connector read");

    let encoded = encode_distributed_plan_with_context(
        &plan,
        NativePlanEncodeContext {
            scan_bindings: Some(&bindings),
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
        },
    )
    .expect("encode bound VARIANT scan");
    let scan = encoded_root_scan_for_test(&encoded);
    assert_eq!(
        scan.columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "v"), (2, "__nr_var_v_0")]
    );
    assert_eq!(scan.required_columns, vec!["__nr_var_v_0"]);
    assert_eq!(scan.variant_columns[0].synthetic_column_id, 2);
    let table = scan.table.as_ref().expect("bound table");
    let Some(novarocks_protocol::plan::scan_source::Kind::ConnectorRead(connector)) = table
        .source
        .as_ref()
        .and_then(|source| source.kind.as_ref())
    else {
        panic!("variant binding must encode as ConnectorReadSource");
    };
    assert_eq!(connector.instance_id, "ice");
}

fn encoded_root_scan_for_test(plan: &plan::DistributedPlan) -> &plan::ScanNode {
    let root = plan.fragments[0].root.as_ref().expect("encoded root");
    let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref() else {
        panic!("expected physical root");
    };
    let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
        panic!("expected scan root");
    };
    scan
}

fn file_binding_for_test(
    node_id: i32,
    physical_columns: Vec<ResolvedScanColumn>,
    required_reads: Vec<ResolvedReadColumn>,
) -> ResolvedScanBinding {
    ResolvedScanBinding {
        node_id,
        execution: ResolvedScanExecution::AdmittedConnectorRead(
            crate::query_execution::preparation::scan::fixture_query_scan_materialization("ice"),
        ),
        physical_columns,
        required_reads,
    }
}

fn delta_binding_for_test(node_id: i32) -> ResolvedScanBinding {
    ResolvedScanBinding {
        node_id,
        execution: ResolvedScanExecution::SealedConnectorScan(
            crate::query_execution::preparation::scan::fixture_sealed_change_scan("ice", 6, 7),
        ),
        physical_columns: vec![bound_column_for_test(
            1,
            "order_id",
            "order_id",
            ResolvedScanColumnKind::PhysicalTableColumn,
        )],
        required_reads: vec![bound_read_for_test(Some(1), "order_id")],
    }
}

fn bound_column_for_test(
    id: u32,
    planner_name: &str,
    source_name: &str,
    kind: ResolvedScanColumnKind,
) -> ResolvedScanColumn {
    ResolvedScanColumn {
        planner: output_column(id, planner_name, DataType::Int64),
        source: column_def_for_test(source_name, DataType::Int64, false),
        kind,
    }
}

fn bound_read_for_test(planner_id: Option<u32>, source_name: &str) -> ResolvedReadColumn {
    ResolvedReadColumn {
        planner_column_id: planner_id.map(ColumnId),
        source: column_def_for_test(source_name, DataType::Int64, false),
        reason: if planner_id.is_some() {
            ResolvedReadReason::PlannerRequiredOrOutput
        } else {
            ResolvedReadReason::EqualityDeleteKey
        },
    }
}

fn column_def_for_test(
    name: &str,
    data_type: DataType,
    nullable: bool,
) -> novarocks_catalog::schema::ColumnDef {
    novarocks_catalog::schema::ColumnDef {
        name: name.to_string(),
        data_type,
        nullable,
        write_default: None,
        logical_type: None,
    }
}
