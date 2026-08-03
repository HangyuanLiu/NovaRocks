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

use super::super::collect_scan_bindings;
use super::*;
use novarocks_spi::connector::{ConnectorControlResolver, ConnectorInstanceId};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

struct RejectResolver;

impl ScanBindingResolver for RejectResolver {
    fn resolve_scan(
        &self,
        node_id: i32,
        _scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        panic!("ordinary Iceberg scan unexpectedly invoked resolver for node {node_id}")
    }
}

struct ErrorResolver;

impl ScanBindingResolver for ErrorResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        _scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        Err("boom".to_string())
    }
}

struct EmptyResolver;

impl ScanBindingResolver for EmptyResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        _scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        Ok(None)
    }
}

#[test]
fn scan_preparation_propagates_caller_cancellation() {
    let context =
        crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(true)))
            .expect("cancelled request context");
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(registry.clone());
    let err = match super::super::prepare_scan_bindings(
        &plan(scan_node(10, IcebergDataFileBinding::CurrentSnapshot)),
        &controls,
        &context,
        None,
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
    ) {
        Ok(_) => panic!("caller cancellation must reach the connector provider"),
        Err(err) => err,
    };

    assert!(
        err.contains("Cancelled: connector request was cancelled"),
        "{err}"
    );
}

#[test]
fn sqlx1_preparation_uses_the_query_binding_lease_without_reacquiring_current() {
    let root = scan_node(10, IcebergDataFileBinding::CurrentSnapshot);
    let DistributedNodeKind::Scan(scan) = &root.payload else {
        panic!("fixture root must be a scan");
    };
    let ScanSource::IcebergDataFiles { table, .. } = &scan.table.source else {
        panic!("fixture scan must use IcebergDataFiles");
    };
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let lease = controls
        .acquire_current(
            &ConnectorInstanceId::parse(&table.catalog).expect("fixture catalog instance"),
        )
        .expect("fixture planning lease");
    let bindings = crate::sql::catalog::provider::QueryTableBindingStore::default();
    bindings.insert_strict_base_binding_for_test(
        &table.catalog,
        &table.namespace,
        &table.table,
        crate::sql::catalog::provider::QueryTableBinding {
            resolved: crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
                Some(&table.catalog),
                "default",
                scan.table.clone(),
            ),
            statistics_pin: None,
            planning_lease: Some(lease.clone()),
        },
    );

    let prepared = super::super::prepare_scan_bindings(
        &plan(root),
        &controls,
        &crate::connector::test_request_context(),
        Some(&bindings),
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
    )
    .expect("exact query binding must plan the scan");
    let retained = prepared
        .connector_read(0, 10)
        .expect("prepared connector read")
        .planning_lease
        .as_ref()
        .expect("prepared read must retain planning lease");
    assert_eq!(
        retained.binding().incarnation(),
        lease.binding().incarnation(),
        "preparation must retain the query binding generation"
    );
}

#[test]
fn sqlx1_preparation_rejects_missing_binding_instead_of_reacquiring_current() {
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let bindings = crate::sql::catalog::provider::QueryTableBindingStore::default();
    let error = match super::super::prepare_scan_bindings(
        &plan(scan_node(10, IcebergDataFileBinding::CurrentSnapshot)),
        &controls,
        &crate::connector::test_request_context(),
        Some(&bindings),
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
    ) {
        Ok(_) => panic!("missing binding must fail before a current-generation acquire"),
        Err(error) => error,
    };
    assert!(error.contains("has no exact query binding"), "{error}");
}

#[test]
fn ordinary_current_snapshot_is_immutable_and_does_not_invoke_resolver() {
    let plan = plan(scan_node(10, IcebergDataFileBinding::CurrentSnapshot));
    let before = format!("{plan:#?}");
    let bindings = prepare_scan_bindings(
        &plan,
        &registry(vec![data_file("s3://bucket/current.parquet")]),
        Some(&RejectResolver),
    )
    .expect("prepare current-snapshot scan");

    assert_eq!(format!("{plan:#?}"), before);
    assert!(bindings.binding(10).is_some());
    assert!(bindings.scan_ranges(0, 10).expect("ranges").is_empty());
    assert_eq!(
        bindings
            .connector_read(0, 10)
            .expect("opaque connector read")
            .splits
            .len(),
        1
    );
}

#[test]
fn duplicate_scan_node_defense_reports_exact_error() {
    let root = scan_node(10, IcebergDataFileBinding::ExplicitFiles);
    let registry = registry(vec![data_file("s3://bucket/explicit.parquet")]);
    let mut seen_scan_node_ids = std::collections::BTreeSet::new();
    let mut bindings = crate::query_execution::preparation::scan::ScanExecutionBindings::default();
    let context = crate::connector::test_request_context();
    let controls = crate::connector::FixtureControlResolver::new(registry.clone());

    collect_scan_bindings(
        0,
        &root,
        &controls,
        &context,
        None,
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
        &mut seen_scan_node_ids,
        &mut bindings,
    )
    .expect("first scan preparation");
    let err = collect_scan_bindings(
        0,
        &root,
        &controls,
        &context,
        None,
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
        &mut seen_scan_node_ids,
        &mut bindings,
    )
    .expect_err("duplicate scan node must fail before re-planning");

    assert_eq!(err, "duplicate scan node_id=10");
}

#[test]
fn refresh_only_sources_require_resolver_with_kind_and_node_id() {
    for (source, expected_kind) in [
        (
            ScanSource::IcebergVersionTable {
                table: iceberg_table(),
                snapshot_id: 6,
            },
            "IcebergVersionTable",
        ),
        (
            ScanSource::IcebergDeltaTable {
                table: iceberg_table(),
                from_snapshot_id: 6,
                to_snapshot_id: 7,
            },
            "IcebergDeltaTable",
        ),
    ] {
        let mut root = scan_node(37, IcebergDataFileBinding::ExplicitFiles);
        replace_scan_source(&mut root, source);

        let err = match prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), None) {
            Ok(_) => panic!("{expected_kind} without resolver must fail"),
            Err(err) => err,
        };

        assert!(err.contains("requires scan binding resolver"), "{err}");
        assert!(err.contains(expected_kind), "{err}");
        assert!(err.contains("node_id=37"), "{err}");
    }
}

#[test]
fn resolver_error_reports_source_kind_node_id_and_cause() {
    let mut root = scan_node(47, IcebergDataFileBinding::ExplicitFiles);
    replace_scan_source(
        &mut root,
        ScanSource::IcebergVersionTable {
            table: iceberg_table(),
            snapshot_id: 6,
        },
    );

    let err =
        match prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), Some(&ErrorResolver)) {
            Ok(_) => panic!("resolver error must fail preparation"),
            Err(err) => err,
        };

    assert_eq!(
        err,
        "scan binding resolver failed for required source IcebergVersionTable node_id=47: boom"
    );
}

#[test]
fn resolver_ok_none_reports_exact_required_source_error() {
    let mut root = scan_node(48, IcebergDataFileBinding::ExplicitFiles);
    replace_scan_source(
        &mut root,
        ScanSource::IcebergVersionTable {
            table: iceberg_table(),
            snapshot_id: 6,
        },
    );

    let err =
        match prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), Some(&EmptyResolver)) {
            Ok(_) => panic!("empty resolver result must fail preparation"),
            Err(err) => err,
        };

    assert_eq!(
        err,
        "scan binding resolver returned no binding for required source IcebergVersionTable node_id=48"
    );
}

#[test]
fn resolver_failure_precedes_invalid_physical_projection() {
    let mut root = scan_node(49, IcebergDataFileBinding::ExplicitFiles);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.columns[0].name = "missing".to_string();
    scan.table.source = ScanSource::IcebergVersionTable {
        table: iceberg_table(),
        snapshot_id: 6,
    };

    let err =
        match prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), Some(&ErrorResolver)) {
            Ok(_) => panic!("resolver error must win over physical projection error"),
            Err(err) => err,
        };

    assert_eq!(
        err,
        "scan binding resolver failed for required source IcebergVersionTable node_id=49: boom"
    );
}

#[test]
fn target_state_and_locator_reject_equality_deletes() {
    use crate::exec::row_position::{
        ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
        ICEBERG_ROW_POS_COL,
    };

    let sources = [
        (
            ScanSource::IcebergMvTargetLocator(
                crate::sql::planner::table::IcebergMvTargetLocatorScan {
                    catalog: "test_catalog".to_string(),
                    database: "test_db".to_string(),
                    table: "test_table".to_string(),
                    target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                    target_snapshot_id: Some(6),
                    apply_key_column: "id".to_string(),
                    branch_id_column: None,
                },
            ),
            "target-locator",
        ),
        (
            ScanSource::IcebergMvTargetState(crate::sql::planner::table::IcebergMvTargetStateScan {
                catalog: "test_catalog".to_string(),
                database: "test_db".to_string(),
                table: "test_table".to_string(),
                target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                target_snapshot_id: Some(6),
                aggregate_state_layout_version: 1,
                columns: vec![source_column("id", DataType::Int32, false)],
                group_key_names: vec!["id".to_string()],
                aggregate_state_names: Vec::new(),
                physical_column_names: vec!["id".to_string()],
                row_id_column_name: ICEBERG_ROW_ID_COL.to_string(),
                row_filter:
                    crate::sql::planner::table::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                        row_id_column_name: ICEBERG_ROW_ID_COL.to_string(),
                        branch_scope: None,
                    },
                partition_constraint:
                    crate::sql::planner::table::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
            }),
            "target-state",
        ),
    ];

    for (source, expected_kind) in sources {
        let mut root = scan_node(39, IcebergDataFileBinding::ExplicitFiles);
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.table
            .columns
            .push(source_column("category", DataType::Utf8, true));
        scan.table.iceberg_row_lineage_metadata_columns = vec![
            source_column(ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
            source_column(ICEBERG_ROW_POS_COL, DataType::Int64, false),
            source_column(ICEBERG_ROW_ID_COL, DataType::Int64, false),
            source_column(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
        ];
        scan.columns.extend([
            column(11, ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
            column(12, ICEBERG_ROW_POS_COL, DataType::Int64, false),
            column(13, ICEBERG_ROW_ID_COL, DataType::Int64, false),
            column(14, ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
        ]);
        scan.table.source = source;
        let mut file = data_file("s3://bucket/target-data.parquet");
        file.delete_files = vec![equality_delete_file(Vec::new(), vec![3])];
        let resolver = StaticResolver {
            execution: resolved_files(vec![file.clone()]),
        };

        let err = match prepare_scan_bindings(&plan(root), &registry(vec![file]), Some(&resolver)) {
            Ok(_) => panic!("{expected_kind} equality-delete scan must fail"),
            Err(err) => err,
        };

        assert!(err.contains(expected_kind), "{err}");
        assert!(err.contains("does not support equality deletes"), "{err}");
    }
}

#[test]
fn resolver_execution_kind_must_match_semantic_source() {
    let mut version = scan_node(41, IcebergDataFileBinding::ExplicitFiles);
    replace_scan_source(
        &mut version,
        ScanSource::IcebergVersionTable {
            table: iceberg_table(),
            snapshot_id: 6,
        },
    );
    let resolver = StaticResolver {
        execution: resolved_delta(),
    };

    let err =
        match prepare_scan_bindings(&plan(version), &ConnectorRegistry::new(), Some(&resolver)) {
            Ok(_) => panic!("version scan must reject delta execution"),
            Err(err) => err,
        };

    assert!(err.contains("IcebergVersionTable"), "{err}");
    assert!(err.contains("requires IcebergFiles execution"), "{err}");
    assert!(err.contains("node_id=41"), "{err}");
}
