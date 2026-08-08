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

fn data_file_with_i32_stats(path: &str, min: i32, max: i32) -> IcebergDataFileInfo {
    let mut file = data_file(path);
    file.column_stats = Some(HashMap::from([(
        "id".to_string(),
        novarocks_connector_iceberg::scan_model::IcebergColumnStats {
            null_count: Some(0),
            value_count: Some(10),
            column_size: None,
            lower_bound: Some(min.to_le_bytes().to_vec()),
            upper_bound: Some(max.to_le_bytes().to_vec()),
        },
    )]));
    file
}

fn identity_partition_file(path: &str, id: i32) -> IcebergDataFileInfo {
    let mut file = data_file(path);
    file.partition_key = Some(format!("Struct([{id}])"));
    file.partition_values = vec![
        novarocks_connector_iceberg::scan_model::IcebergPartitionFieldValue {
            source_column: "id".to_string(),
            field_name: "id".to_string(),
            transform: "identity".to_string(),
            value: Some(novarocks_connector_iceberg::scan_model::IcebergPartitionValue::Int32(id)),
        },
    ];
    file
}

fn position_delete_file(
    path: &str,
) -> novarocks_connector_iceberg::scan_model::IcebergDeleteFileInfo {
    novarocks_connector_iceberg::scan_model::IcebergDeleteFileInfo {
        path: path.to_string(),
        file_format: novarocks_connector_iceberg::scan_model::IcebergDeleteFileFormat::Parquet,
        file_content: novarocks_connector_iceberg::scan_model::IcebergDeleteFileContent::Position,
        length: Some(1),
        content_offset: None,
        content_size_in_bytes: None,
        sequence_number: Some(2),
        partition_spec_id: Some(0),
        partition_key: Some("Struct([])".to_string()),
        equality_column_names: Vec::new(),
        equality_field_ids: Vec::new(),
    }
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

fn unsupported_id_predicate() -> crate::sql::analysis::TypedExpr {
    use crate::sql::analysis::{ExprKind, TypedExpr};

    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "abs".to_string(),
            args: vec![id_eq(12)],
            distinct: false,
            volatility: crate::sql::functions::FunctionVolatility::Immutable,
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn planned_data_files(
    bindings: &crate::query_execution::preparation::scan::ScanExecutionBindings,
    node_id: i32,
) -> Vec<IcebergDataFileInfo> {
    let planned = bindings
        .connector_read(0, node_id)
        .expect("opaque connector read");
    planned
        .splits
        .iter()
        .map(|split| {
            crate::connector::iceberg::provider::planned_split_data_file_for_test(split)
                .expect("decode test Iceberg split")
        })
        .collect()
}

#[test]
fn ordinary_iceberg_scan_uses_opaque_connector_read_and_preserves_residual() {
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
    assert!(
        bindings
            .scan_ranges(0, 10)
            .is_some_and(|ranges| ranges.is_empty())
    );
    let read = bindings
        .connector_read(0, 10)
        .expect("opaque connector read");
    assert_eq!(read.splits.len(), 1);
    assert_eq!(
        planned_data_files(&bindings, 10)[0].path,
        "s3://bucket/id-10-20.parquet"
    );
    assert_eq!(read.static_predicates.len(), 1);
    assert_eq!(
        format!("{:?}", read.residual_predicates),
        format!("{:?}", vec![id_eq(12)])
    );
    assert!(read.predicate_dispositions.iter().all(|disposition| {
        disposition.kind == novarocks_spi::connector::ConnectorPredicateDispositionKind::PruningOnly
    }));
}

#[test]
fn delta_scan_uses_opaque_connector_read() {
    let mut root = scan_node(40, IcebergDataFileBinding::ExplicitFiles);
    replace_scan_source(
        &mut root,
        crate::sql::planner::table::test_sql_scan_source(
            crate::sql::planner::table::SqlScanKind::Delta {
                from_snapshot_id: 6,
                to_snapshot_id: 7,
            },
        ),
    );
    let resolver = StaticResolver {
        execution: resolved_data_delta(),
    };

    let bindings = prepare_scan_bindings(&plan(root), &registry(Vec::new()), Some(&resolver))
        .expect("prepare delta scan");

    assert!(matches!(
        bindings.binding(40).expect("binding").execution,
        ResolvedScanExecution::IcebergDelta(_)
    ));
    assert!(
        bindings
            .scan_ranges(0, 40)
            .expect("delta ranges")
            .is_empty()
    );
    let planned = bindings
        .connector_read(0, 40)
        .expect("delta connector read");
    assert_eq!(
        planned.declaration.descriptor().provider_id.as_str(),
        "iceberg"
    );
    assert_eq!(planned.splits.len(), 1);
    assert_eq!(planned.splits[0].split_id(), "delta-0");
}

#[test]
fn explicit_files_plan_opaque_connector_splits() {
    let plan = plan(scan_node(10, IcebergDataFileBinding::ExplicitFiles));
    let bindings = prepare_scan_bindings(
        &plan,
        &registry(vec![data_file("s3://bucket/explicit.parquet")]),
        None,
    )
    .expect("prepare explicit scan");
    let ranges = bindings.scan_ranges(0, 10).expect("ranges");
    assert!(ranges.is_empty());
    let planned = bindings.connector_read(0, 10).expect("connector read");
    assert_eq!(
        planned.declaration.descriptor().provider_id.as_str(),
        "iceberg"
    );
    assert_eq!(
        planned.declaration.descriptor().instance_id.as_str(),
        "test_catalog"
    );
    assert_eq!(planned.splits.len(), 1);
    assert_eq!(planned.splits[0].split_id(), "fixture-0");
    assert_eq!(planned.splits[0].owner().as_str(), "test_catalog");
}

#[test]
fn sqlx2_frozen_snapshot_scan_uses_its_exact_admitted_file_set() {
    let mut root = scan_node(10, IcebergDataFileBinding::ExplicitFiles);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("fixture root must be a scan");
    };
    let ScanSource::Sql(source) = &mut scan.table.source;
    source.kind = crate::sql::planner::table::SqlScanKind::FrozenInputSet {
        version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(11),
    };
    let plan = plan(root);
    let controls = crate::connector::FixtureControlResolver::new(registry(vec![data_file(
        "s3://bucket/current.parquet",
    )]));
    let store = fixture_query_table_bindings_with_materialized_files(
        &plan,
        &controls,
        vec![data_file("s3://bucket/snapshot-11.parquet")],
    );
    let DistributedNodeKind::Scan(scan) = &plan.fragments()[0].root.payload else {
        panic!("fixture root must remain a scan");
    };
    let ScanSource::Sql(source) = &scan.table.source;
    let selected = store
        .frozen_snapshot_materialization(source.binding, 11)
        .expect("select admitted snapshot files");
    let crate::engine::query_planning::bindings::QueryScanMaterialization { selector, .. } =
        selected
    else {
        panic!("frozen snapshot must retain neutral connector materialization");
    };

    assert_eq!(
        selector,
        novarocks_spi::connector::ConnectorReadSelector::SnapshotId(11),
        "FrozenInputSet must retain its admitted snapshot selector"
    );
    super::super::prepare_scan_bindings(
        &plan,
        &controls,
        &crate::connector::test_request_context(),
        Some(&store),
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
    )
    .expect("prepare selected frozen snapshot scan");
}

#[test]
fn sqlx2_frozen_snapshot_scan_rejects_a_selector_without_admitted_files() {
    let mut root = scan_node(10, IcebergDataFileBinding::ExplicitFiles);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("fixture root must be a scan");
    };
    let ScanSource::Sql(source) = &mut scan.table.source;
    source.kind = crate::sql::planner::table::SqlScanKind::FrozenInputSet {
        version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(11),
    };
    let controls = crate::connector::FixtureControlResolver::new(registry(vec![data_file(
        "s3://bucket/current.parquet",
    )]));
    let store = fixture_query_table_bindings_with_materialized_files(
        &plan(root.clone()),
        &controls,
        vec![data_file("s3://bucket/snapshot-11.parquet")],
    );
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("fixture root must remain a scan");
    };
    let ScanSource::Sql(source) = &mut scan.table.source;
    source.kind = crate::sql::planner::table::SqlScanKind::FrozenInputSet {
        version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(12),
    };
    let plan = plan(root);

    let error = match super::super::prepare_scan_bindings(
        &plan,
        &controls,
        &crate::connector::test_request_context(),
        Some(&store),
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
    ) {
        Ok(_) => panic!("unadmitted frozen snapshot must fail before split planning"),
        Err(error) => error,
    };
    assert!(
        error.contains("snapshot 12 has no admitted connector materialization"),
        "{error}"
    );
}

#[test]
fn identity_partition_predicate_stays_on_opaque_connector_path() {
    let mut root = scan_node(10, IcebergDataFileBinding::CurrentSnapshot);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.predicates = vec![id_eq(12)];
    let bindings = prepare_scan_bindings(
        &plan(root),
        &registry(vec![
            identity_partition_file("s3://bucket/id-1.parquet", 1),
            identity_partition_file("s3://bucket/id-12.parquet", 12),
        ]),
        None,
    )
    .expect("prepare connector scan");
    let read = bindings
        .connector_read(0, 10)
        .expect("opaque connector read");
    assert_eq!(read.splits.len(), 1);
    assert_eq!(
        planned_data_files(&bindings, 10)[0].path,
        "s3://bucket/id-12.parquet"
    );
    assert_eq!(
        format!("{:?}", read.residual_predicates),
        format!("{:?}", vec![id_eq(12)])
    );
}

#[test]
fn large_plain_file_preserves_provider_owned_split_and_byte_estimate() {
    let plan = plan(scan_node(10, IcebergDataFileBinding::ExplicitFiles));
    let mut file = data_file("s3://bucket/large.parquet");
    file.size = 300 * 1024 * 1024;
    let bindings =
        prepare_scan_bindings(&plan, &registry(vec![file]), None).expect("prepare large-file scan");
    assert!(bindings.scan_ranges(0, 10).expect("ranges").is_empty());
    let planned = bindings.connector_read(0, 10).expect("connector read");

    assert_eq!(planned.splits.len(), 1);
    assert_eq!(planned.splits[0].estimated_bytes(), Some(300 * 1024 * 1024));
    let file =
        crate::connector::iceberg::provider::planned_split_data_file_for_test(&planned.splits[0])
            .expect("decode test Iceberg split");
    assert_eq!(file.path, "s3://bucket/large.parquet");
    assert_eq!(file.size, 300 * 1024 * 1024);
}

#[test]
fn excessive_delete_apply_cost_preserves_exact_planning_error() {
    let plan = plan(scan_node(10, IcebergDataFileBinding::ExplicitFiles));
    let mut file = data_file("s3://bucket/data.parquet");
    file.delete_files = (0..1025)
        .map(|idx| position_delete_file(&format!("s3://bucket/delete-{idx}.parquet")))
        .collect();

    let err = match prepare_scan_bindings(&plan, &registry(vec![file]), None) {
        Ok(_) => panic!("delete-heavy scan must fail"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        "scan preparation node_id=10: ResourceExhausted: too many Iceberg delete files attached to s3://bucket/data.parquet: count=1025 max=1024"
    );
}

#[test]
fn unsupported_predicate_does_not_guess_pruning() {
    let mut root = scan_node(10, IcebergDataFileBinding::CurrentSnapshot);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.predicates = vec![unsupported_id_predicate()];
    let bindings = prepare_scan_bindings(
        &plan(root),
        &registry(vec![
            data_file_with_i32_stats("s3://bucket/id-1-5.parquet", 1, 5),
            data_file_with_i32_stats("s3://bucket/id-10-20.parquet", 10, 20),
        ]),
        None,
    )
    .expect("unsupported pruning predicate must preserve scan semantics");

    let read = bindings
        .connector_read(0, 10)
        .expect("opaque connector read");
    assert!(read.static_predicates.is_empty());
    assert_eq!(
        format!("{:?}", read.residual_predicates),
        format!("{:?}", vec![unsupported_id_predicate()])
    );
    assert_eq!(read.splits.len(), 2);
}

#[test]
fn sqlx2_mv_target_state_uses_only_frozen_allow_list_files() {
    use std::collections::BTreeSet;

    use crate::mv::model::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};
    use crate::mv::persistence::schema::{
        MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract,
    };

    let mut selected = identity_partition_file("s3://bucket/selected.parquet", 7);
    selected.partition_spec_id = Some(3);
    let mut skipped = identity_partition_file("s3://bucket/skipped.parquet", 9);
    skipped.partition_spec_id = Some(3);
    let allow_key = MvPartitionKey::new(
        3,
        vec![MvPartitionKeyField::new(
            "id".to_string(),
            MvPartitionValue::String("7".to_string()),
        )],
    );
    let contract = MvPartitionContract {
        target_spec_id: 3,
        fields: vec![MvPartitionFieldContract {
            partition_field_id: 100,
            partition_field_name: "id".to_string(),
            source_target_field_id: 1,
            source_column_name: "id".to_string(),
            transform: MvPartitionTransformContract::Identity,
        }],
    };

    let files = super::super::filter_frozen_mv_target_state_files(
        vec![selected, skipped],
        &crate::mv::model::TargetPartitionFilter::AllowList(BTreeSet::from([allow_key])),
        Some(&contract),
        42,
    )
    .expect("frozen target-state files should be deterministically pruned");

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "s3://bucket/selected.parquet");
}

#[test]
fn sqlx2_mv_target_state_empty_allow_list_reads_no_frozen_files() {
    use std::collections::BTreeSet;

    let files = super::super::filter_frozen_mv_target_state_files(
        vec![data_file("s3://bucket/target.parquet")],
        &crate::mv::model::TargetPartitionFilter::AllowList(BTreeSet::new()),
        None,
        43,
    )
    .expect("an empty admitted allow-list is a zero-file scan");

    assert!(files.is_empty());
}
