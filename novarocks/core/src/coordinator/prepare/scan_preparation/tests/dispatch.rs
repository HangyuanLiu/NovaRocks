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

use super::super::{collect_scan_bindings, store_planned_starrocks_scan};
use super::*;

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

fn data_file_with_i32_stats(path: &str, min: i32, max: i32) -> IcebergDataFileInfo {
    let mut file = data_file(path);
    file.column_stats = Some(HashMap::from([(
        "id".to_string(),
        crate::connector::iceberg::scan_model::IcebergColumnStats {
            null_count: Some(0),
            value_count: Some(10),
            column_size: None,
            lower_bound: Some(min.to_le_bytes().to_vec()),
            upper_bound: Some(max.to_le_bytes().to_vec()),
        },
    )]));
    file
}

fn id_eq(value: i64) -> crate::sql::analysis::TypedExpr {
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};

    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId::new_for_test(1),
                    qualifier: Some("ice_t".to_string()),
                    column: "id".to_string(),
                },
                data_type: DataType::Int32,
                nullable: false,
            }),
            op: BinOp::Eq,
            right: Box::new(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(value)),
                data_type: DataType::Int32,
                nullable: false,
            }),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn resolved_delta() -> ResolvedScanExecution {
    ResolvedScanExecution::IcebergDelta(
        crate::coordinator::prepare::scan::ResolvedIcebergDeltaScan {
            runtime_plan: crate::coordinator::prepare::scan::IcebergDeltaScanRuntimePlan {
                table_location: "s3://bucket/test_table".to_string(),
                data_columns: Vec::new(),
                cloud_properties: BTreeMap::new(),
                change_files: Vec::new(),
                delete_side: None,
            },
        },
    )
}

fn replace_scan_source(root: &mut DistributedNode, source: ScanSource) {
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.table.source = source;
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
    assert_eq!(bindings.scan_ranges(0, 10).expect("ranges").len(), 1);
}

#[test]
fn explicit_files_preserve_native_split_ranges() {
    let plan = plan(scan_node(10, IcebergDataFileBinding::ExplicitFiles));
    let bindings = prepare_scan_bindings(
        &plan,
        &registry(vec![data_file("s3://bucket/explicit.parquet")]),
        None,
    )
    .expect("prepare explicit scan");
    let ranges = bindings.scan_ranges(0, 10).expect("ranges");

    assert_eq!(ranges.len(), 1);
    let crate::runtime::scan_range::ScanRange::File(file) = &ranges[0].range else {
        panic!("expected file range");
    };
    assert_eq!(
        file.full_path.as_deref(),
        Some("s3://bucket/explicit.parquet")
    );
    assert_eq!(file.offset, 0);
    assert_eq!(file.length, 128);
}

#[test]
fn duplicate_scan_node_defense_reports_exact_error() {
    let root = scan_node(10, IcebergDataFileBinding::ExplicitFiles);
    let registry = registry(vec![data_file("s3://bucket/explicit.parquet")]);
    let mut seen_scan_node_ids = std::collections::BTreeSet::new();
    let mut bindings = crate::coordinator::prepare::scan::ScanExecutionBindings::default();

    collect_scan_bindings(
        0,
        &root,
        &registry,
        None,
        &mut seen_scan_node_ids,
        &mut bindings,
    )
    .expect("first scan preparation");
    let err = collect_scan_bindings(
        0,
        &root,
        &registry,
        None,
        &mut seen_scan_node_ids,
        &mut bindings,
    )
    .expect_err("duplicate scan node must fail before re-planning");

    assert_eq!(err, "duplicate scan node_id=10");
}

#[test]
fn metadata_scan_uses_native_sentinel_range() {
    let mut root = scan_node(10, IcebergDataFileBinding::ExplicitFiles);
    replace_scan_source(
        &mut root,
        ScanSource::IcebergMetadataTable {
            table: iceberg_table(),
            metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType::Snapshots,
            serialized_table: "{}".to_string(),
            cloud_properties: BTreeMap::new(),
            metadata_payload: None,
        },
    );

    let bindings = prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), None)
        .expect("prepare metadata scan");
    let ranges = bindings.scan_ranges(0, 10).expect("metadata ranges");

    assert_eq!(ranges.len(), 1);
    let crate::runtime::scan_range::ScanRange::File(file) = &ranges[0].range else {
        panic!("expected metadata file range");
    };
    assert_eq!(file.full_path.as_deref(), Some("iceberg-metadata"));
    assert!(file.use_iceberg_jni_metadata_reader);
    assert!(bindings.binding(10).is_none());
}

#[test]
fn ordinary_iceberg_scan_preserves_min_max_pruning() {
    let mut root = scan_node(10, IcebergDataFileBinding::CurrentSnapshot);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.predicates = vec![id_eq(12)];
    let bindings = prepare_scan_bindings(
        &plan(root),
        &registry(vec![
            data_file_with_i32_stats("s3://bucket/id-1-5.parquet", 1, 5),
            data_file_with_i32_stats("s3://bucket/id-10-20.parquet", 10, 20),
        ]),
        None,
    )
    .expect("prepare pruned scan");
    let ranges = bindings.scan_ranges(0, 10).expect("ranges");

    assert_eq!(ranges.len(), 1);
    let crate::runtime::scan_range::ScanRange::File(file) = &ranges[0].range else {
        panic!("expected file range");
    };
    assert_eq!(
        file.full_path.as_deref(),
        Some("s3://bucket/id-10-20.parquet")
    );
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
fn delta_scan_uses_resolved_payload_and_sentinel_range() {
    let mut root = scan_node(40, IcebergDataFileBinding::ExplicitFiles);
    replace_scan_source(
        &mut root,
        ScanSource::IcebergDeltaTable {
            table: iceberg_table(),
            from_snapshot_id: 6,
            to_snapshot_id: 7,
        },
    );
    let resolver = StaticResolver {
        execution: resolved_delta(),
    };

    let bindings = prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), Some(&resolver))
        .expect("prepare delta scan");

    assert!(matches!(
        bindings.binding(40).expect("binding").execution,
        ResolvedScanExecution::IcebergDelta(_)
    ));
    let ranges = bindings.scan_ranges(0, 40).expect("delta ranges");
    assert_eq!(ranges.len(), 1);
    let crate::runtime::scan_range::ScanRange::File(file) = &ranges[0].range else {
        panic!("expected delta sentinel range");
    };
    assert_eq!(file.full_path.as_deref(), Some("iceberg-metadata"));
    assert!(file.use_iceberg_jni_metadata_reader);
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

#[test]
fn starrocks_planning_result_stores_ranges_and_source_descriptor() {
    use crate::connector::scan_model::starrocks::{
        PlannedNativeStarRocksScan, StarRocksScanSourceDescriptor,
        StarRocksStorageColumnDescriptor, test_starrocks_tablet_schema_descriptor,
    };

    let storage_columns = vec![StarRocksStorageColumnDescriptor {
        name: "id".to_string(),
        unique_id: 1,
        default_value: None,
    }];
    let planned = PlannedNativeStarRocksScan {
        ranges: vec![
            crate::runtime::scan_range::ScanRangeParams::starrocks_tablet(300, 100, 7)
                .expect("tablet range"),
        ],
        source: StarRocksScanSourceDescriptor {
            catalog_name: "default_catalog".to_string(),
            db_id: 10,
            table_id: 20,
            schema_id: 30,
            storage_columns: storage_columns.clone(),
            tablet_schema: test_starrocks_tablet_schema_descriptor(30, &storage_columns),
        },
    };
    let mut bindings = crate::coordinator::prepare::scan::ScanExecutionBindings::default();

    store_planned_starrocks_scan(0, 42, planned, &mut bindings)
        .expect("store StarRocks planning result");

    let ranges = bindings.scan_ranges(0, 42).expect("ranges");
    assert_eq!(ranges.len(), 1);
    let crate::runtime::scan_range::ScanRange::StarRocksTablet(range) = &ranges[0].range else {
        panic!("expected tablet range");
    };
    assert_eq!(range.tablet_id, 300);
    let source = bindings.starrocks_source(42).expect("source descriptor");
    assert_eq!(source.db_id, 10);
    assert_eq!(source.table_id, 20);
    assert_eq!(source.schema_id, 30);
    assert!(bindings.binding(42).is_none());

    let duplicate = PlannedNativeStarRocksScan {
        ranges: Vec::new(),
        source: StarRocksScanSourceDescriptor {
            catalog_name: "other_catalog".to_string(),
            db_id: 11,
            table_id: 21,
            schema_id: 31,
            storage_columns: Vec::new(),
            tablet_schema: test_starrocks_tablet_schema_descriptor(31, &[]),
        },
    };
    let err = store_planned_starrocks_scan(0, 42, duplicate, &mut bindings)
        .expect_err("duplicate StarRocks planning must fail before partial insertion");
    assert_eq!(
        err,
        "duplicate StarRocks scan planning fragment_id=0 node_id=42"
    );
}
