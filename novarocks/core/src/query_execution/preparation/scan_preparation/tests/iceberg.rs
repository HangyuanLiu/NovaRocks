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

fn identity_partition_file(path: &str, id: i32) -> IcebergDataFileInfo {
    let mut file = data_file(path);
    file.partition_key = Some(format!("Struct([{id}])"));
    file.partition_values = vec![
        crate::connector::iceberg::scan_model::IcebergPartitionFieldValue {
            source_column: "id".to_string(),
            field_name: "id".to_string(),
            transform: "identity".to_string(),
            value: Some(crate::connector::iceberg::scan_model::IcebergPartitionValue::Int32(id)),
        },
    ];
    file
}

fn position_delete_file(
    path: &str,
) -> crate::connector::iceberg::scan_model::IcebergDeleteFileInfo {
    crate::connector::iceberg::scan_model::IcebergDeleteFileInfo {
        path: path.to_string(),
        file_format: crate::connector::iceberg::scan_model::IcebergDeleteFileFormat::Parquet,
        file_content: crate::connector::iceberg::scan_model::IcebergDeleteFileContent::Position,
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
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

#[test]
fn metadata_scan_uses_native_sentinel_range() {
    let direct = super::super::iceberg::build_iceberg_metadata_scan_range_params();
    let crate::runtime::scan_range::ScanRange::File(file) = direct.range else {
        panic!("expected metadata file range");
    };
    assert_eq!(file.full_path.as_deref(), Some("iceberg-metadata"));
    assert!(file.use_iceberg_jni_metadata_reader);

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
    assert_eq!(
        read.splits.len(),
        2,
        "fixture provider does not claim predicate execution or file pruning"
    );
    assert_eq!(read.static_predicates.len(), 1);
    assert_eq!(
        format!("{:?}", read.residual_predicates),
        format!("{:?}", vec![id_eq(12)])
    );
    assert!(read.predicate_dispositions.iter().all(|disposition| {
        disposition.kind == novarocks_spi::connector::ConnectorPredicateDispositionKind::Unsupported
    }));
}

#[test]
fn delta_scan_uses_opaque_connector_read() {
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
    assert_eq!(read.splits.len(), 2);
    assert_eq!(
        format!("{:?}", read.residual_predicates),
        format!("{:?}", vec![id_eq(12)])
    );
}

#[test]
fn large_plain_file_stays_an_opaque_connector_split() {
    let plan = plan(scan_node(10, IcebergDataFileBinding::ExplicitFiles));
    let mut file = data_file("s3://bucket/large.parquet");
    file.size = 300 * 1024 * 1024;
    let bindings =
        prepare_scan_bindings(&plan, &registry(vec![file]), None).expect("prepare large-file scan");
    let read = bindings
        .connector_read(0, 10)
        .expect("opaque connector read");
    assert_eq!(read.splits.len(), 1);
    assert_eq!(read.splits[0].split_id(), "fixture-0");
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
        "scan preparation node_id=10: too many Iceberg delete files attached to data file s3://bucket/data.parquet: count=1025 max=1024"
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
