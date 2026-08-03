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

fn equality_delete_file(
    equality_column_names: Vec<&str>,
    equality_field_ids: Vec<i32>,
) -> crate::connector::iceberg::scan_model::IcebergDeleteFileInfo {
    crate::connector::iceberg::scan_model::IcebergDeleteFileInfo {
        path: "s3://bucket/eq-delete.parquet".to_string(),
        file_format: crate::connector::iceberg::scan_model::IcebergDeleteFileFormat::Parquet,
        file_content: crate::connector::iceberg::scan_model::IcebergDeleteFileContent::Equality,
        length: Some(1),
        content_offset: None,
        content_size_in_bytes: None,
        sequence_number: Some(2),
        partition_spec_id: Some(0),
        partition_key: Some("Struct([])".to_string()),
        equality_column_names: equality_column_names
            .into_iter()
            .map(str::to_string)
            .collect(),
        equality_field_ids,
    }
}

fn iceberg_data_file(
    delete_files: Vec<crate::connector::iceberg::scan_model::IcebergDeleteFileInfo>,
) -> crate::connector::iceberg::scan_model::IcebergDataFileInfo {
    crate::connector::iceberg::scan_model::IcebergDataFileInfo {
        path: "s3://bucket/data.parquet".to_string(),
        size: 128,
        row_count: Some(10),
        column_stats: None,
        partition_spec_id: Some(0),
        partition_key: Some("Struct([])".to_string()),
        first_row_id: None,
        data_sequence_number: Some(1),
        ivm_change_op: None,
        included_positions: None,
        delete_files,
        manifest_path: None,
        partition_values: Vec::new(),
    }
}

fn iceberg_i32_stats_file(
    path: &str,
    min: i32,
    max: i32,
) -> crate::connector::iceberg::scan_model::IcebergDataFileInfo {
    let mut file = iceberg_data_file(Vec::new());
    file.path = path.to_string();
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

fn iceberg_identity_partition_file(
    path: &str,
    id: i32,
) -> crate::connector::iceberg::scan_model::IcebergDataFileInfo {
    let mut file = iceberg_data_file(Vec::new());
    file.path = path.to_string();
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

fn set_iceberg_scan_predicates(
    plan: DistributedPlan,
    predicates: Vec<TypedExpr>,
) -> DistributedPlan {
    crate::sql::planner::distributed::test_support::rebuild_test_plan(
        plan,
        Default::default(),
        |draft| {
            let DistributedNodeKind::Scan(scan) = &mut draft.fragments_mut()[0].root.payload else {
                panic!("root must be scan");
            };
            scan.predicates = predicates;
        },
    )
}

fn id_eq_literal(value: i64) -> TypedExpr {
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
            op: crate::sql::analysis::BinOp::Eq,
            right: Box::new(TypedExpr {
                kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(value)),
                data_type: DataType::Int32,
                nullable: false,
            }),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn iceberg_registry(
    files: Vec<crate::connector::iceberg::scan_model::IcebergDataFileInfo>,
) -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    crate::connector::iceberg::provider::register_planned_files_fixture(
        &registry,
        "test_catalog",
        files,
        None,
    );
    registry
}

fn native_root_scan(
    result: &(
        PreparedFragmentSet,
        NativeFragmentBundle,
        Vec<BoundarySchemaReport>,
    ),
) -> &novarocks_protocol::plan::ScanNode {
    let root_fragment_id = result.0.scheduling_view().execution_anchor();
    let root = result
        .1
        .get(root_fragment_id)
        .expect("native root fragment")
        .root
        .as_ref()
        .expect("root node");
    let novarocks_protocol::plan::distributed_node::Payload::Physical(physical) =
        root.payload.as_ref().expect("root payload")
    else {
        panic!("root must be physical");
    };
    let novarocks_protocol::plan::plan_node::Kind::Scan(scan) =
        physical.kind.as_ref().expect("physical kind")
    else {
        panic!("root must be scan");
    };
    scan
}

fn native_connector_splits(
    result: &(
        PreparedFragmentSet,
        NativeFragmentBundle,
        Vec<BoundarySchemaReport>,
    ),
) -> &[novarocks_spi::connector::ConnectorSplit] {
    result
        .0
        .scheduling_view()
        .connector_read(0, 10)
        .expect("opaque connector read")
        .splits
        .as_slice()
}

fn native_planned_data_files(
    result: &(
        PreparedFragmentSet,
        NativeFragmentBundle,
        Vec<BoundarySchemaReport>,
    ),
) -> Vec<crate::connector::iceberg::scan_model::IcebergDataFileInfo> {
    native_connector_splits(result)
        .iter()
        .map(|split| {
            crate::connector::iceberg::provider::planned_split_data_file_for_test(split)
                .expect("decode test Iceberg split")
        })
        .collect()
}

#[test]
fn equality_delete_field_ids_remain_provider_owned() {
    let plan = iceberg_scan_plan(Some(vec!["id"]));
    let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
        Vec::new(),
        vec![3],
    )])]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build native Iceberg scan");

    assert_eq!(native_root_scan(&result).required_columns, vec!["id"]);
}

#[test]
fn equality_delete_column_names_remain_provider_owned() {
    let plan = iceberg_scan_plan(Some(vec!["id"]));
    let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
        vec!["category"],
        Vec::new(),
    )])]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build native Iceberg scan");

    assert_eq!(native_root_scan(&result).required_columns, vec!["id"]);
}

#[test]
fn equality_delete_key_from_planned_splits_is_hidden_from_query_projection() {
    let plan = iceberg_scan_plan(Some(vec!["id"]));
    let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
        Vec::new(),
        vec![3],
    )])]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build native Iceberg scan");
    let scan = native_root_scan(&result);

    assert_eq!(scan.required_columns, vec!["id"]);
    assert_eq!(
        scan.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id"]
    );
}

#[test]
fn equality_delete_with_non_key_projection_keeps_provider_hidden_layout_private() {
    let plan = iceberg_scan_plan_with_outputs(None, &["id"]);
    let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
        Vec::new(),
        vec![3],
    )])]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build unrestricted native Iceberg scan");
    let scan = native_root_scan(&result);

    assert_eq!(scan.required_columns, vec!["id"]);
    assert_eq!(
        scan.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id"]
    );
    let table = scan.table.as_ref().expect("native scan table");
    assert!(
        table.columns.iter().any(|column| column.name == "category"),
        "hidden equality key must be materializable from the table schema"
    );
}

#[test]
fn equality_delete_with_unrestricted_select_all_preserves_all_query_outputs() {
    let plan = iceberg_scan_plan_with_outputs(None, &["id", "category"]);
    let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
        Vec::new(),
        vec![3],
    )])]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build unrestricted SELECT * Iceberg scan");
    let scan = native_root_scan(&result);

    assert_eq!(scan.required_columns, vec!["id", "category"]);
    assert_eq!(
        scan.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "category"]
    );
}

#[test]
fn equality_delete_unknown_field_id_is_native_planning_error() {
    let plan = iceberg_scan_plan(Some(vec!["id"]));
    let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
        Vec::new(),
        vec![99],
    )])]);

    let err = match build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    )) {
        Ok(_) => panic!("unknown equality field id must fail"),
        Err(err) => err,
    };

    assert!(err.contains("unknown field id 99"), "{err}");
}

#[test]
fn equality_delete_duplicate_identity_is_native_planning_error() {
    for delete_file in [
        equality_delete_file(Vec::new(), vec![3, 3]),
        equality_delete_file(vec!["category", "CATEGORY"], Vec::new()),
    ] {
        let plan = iceberg_scan_plan(Some(vec!["id"]));
        let registry = iceberg_registry(vec![iceberg_data_file(vec![delete_file])]);

        let err = match build_for_test(TestBuildRequest::result(
            &plan,
            &EmptyCatalog,
            &registry,
            None,
        )) {
            Ok(_) => panic!("duplicate equality identity must fail"),
            Err(err) => err,
        };
        assert!(err.contains("duplicate equality"), "{err}");
    }
}

#[test]
fn equality_delete_field_id_and_name_mismatch_is_native_planning_error() {
    let plan = iceberg_scan_plan(Some(vec!["id"]));
    let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
        vec!["id"],
        vec![3],
    )])]);

    let err = match build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    )) {
        Ok(_) => panic!("equality id/name mismatch must fail"),
        Err(err) => err,
    };

    assert!(err.contains("field id/name mismatch"), "{err}");
}

#[test]
fn native_iceberg_scan_predicate_prunes_file_stats_for_id_12() {
    let plan = set_iceberg_scan_predicates(iceberg_scan_plan(None), vec![id_eq_literal(12)]);
    let registry = iceberg_registry(vec![
        iceberg_i32_stats_file("s3://bucket/id-1-5.parquet", 1, 5),
        iceberg_i32_stats_file("s3://bucket/id-10-20.parquet", 10, 20),
    ]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build native Iceberg scan");
    let files = native_planned_data_files(&result);

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "s3://bucket/id-10-20.parquet");
}

#[test]
fn native_iceberg_scan_predicate_prunes_identity_partition_for_id_12() {
    let plan = set_iceberg_scan_predicates(iceberg_scan_plan(None), vec![id_eq_literal(12)]);
    let registry = iceberg_registry(vec![
        iceberg_identity_partition_file("s3://bucket/id-1.parquet", 1),
        iceberg_identity_partition_file("s3://bucket/id-12.parquet", 12),
    ]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build native Iceberg scan");
    let files = native_planned_data_files(&result);

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "s3://bucket/id-12.parquet");
}

#[test]
fn native_iceberg_scan_keeps_large_file_in_provider_owned_split() {
    let plan = iceberg_scan_plan(None);
    let mut file = iceberg_data_file(Vec::new());
    file.path = "s3://bucket/large.parquet".to_string();
    file.size = 300 * 1024 * 1024;
    let registry = iceberg_registry(vec![file]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build native Iceberg scan");
    let splits = native_connector_splits(&result);

    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].estimated_bytes(), Some(300 * 1024 * 1024));
    let file = crate::connector::iceberg::provider::planned_split_data_file_for_test(&splits[0])
        .expect("decode test Iceberg split");
    assert_eq!(file.path, "s3://bucket/large.parquet");
    assert_eq!(file.size, 300 * 1024 * 1024);
}

#[test]
fn native_iceberg_scan_rejects_excessive_delete_apply_cost() {
    let plan = iceberg_scan_plan(None);
    let delete_files = (0..1025)
        .map(|idx| position_delete_file(&format!("s3://bucket/delete-{idx}.parquet")))
        .collect();
    let registry = iceberg_registry(vec![iceberg_data_file(delete_files)]);

    let err = match build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    )) {
        Ok(_) => panic!("delete-heavy scan must fail"),
        Err(err) => err,
    };

    assert!(err.contains("too many Iceberg delete files"), "{err}");
}

#[test]
fn native_iceberg_scan_unsupported_predicate_does_not_guess_pruning() {
    let plan = set_iceberg_scan_predicates(
        iceberg_scan_plan(None),
        vec![TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "abs".to_string(),
                args: vec![id_eq_literal(12)],
                distinct: false,
                volatility: crate::sql::functions::FunctionVolatility::Immutable,
            },
            data_type: DataType::Boolean,
            nullable: false,
        }],
    );
    let registry = iceberg_registry(vec![
        iceberg_i32_stats_file("s3://bucket/id-1-5.parquet", 1, 5),
        iceberg_i32_stats_file("s3://bucket/id-10-20.parquet", 10, 20),
    ]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("unsupported pruning predicate must preserve scan semantics");

    assert_eq!(native_planned_data_files(&result).len(), 2);
}
