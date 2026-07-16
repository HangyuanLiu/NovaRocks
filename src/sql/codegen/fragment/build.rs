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

use super::NativeFragmentBundle;
use super::boundary_schema::{BoundarySchemaReport, project_boundary_reports};
use crate::connector::ConnectorRegistry;
use crate::coordinator::prepare::scan::ScanBindingResolver;
use crate::coordinator::prepare::{PreparedFragmentSet, prepare_fragments};
use crate::sql::catalog::CatalogProvider;
use crate::sql::planner::distributed::DistributedPlan;

struct TestBuildRequest<'a> {
    distributed_plan: &'a DistributedPlan,
    catalog: &'a dyn CatalogProvider,
    connectors: &'a ConnectorRegistry,
    scan_binding_resolver: Option<&'a dyn ScanBindingResolver>,
}

impl<'a> TestBuildRequest<'a> {
    fn result(
        distributed_plan: &'a DistributedPlan,
        catalog: &'a dyn CatalogProvider,
        connectors: &'a ConnectorRegistry,
        scan_binding_resolver: Option<&'a dyn ScanBindingResolver>,
    ) -> Self {
        Self {
            distributed_plan,
            catalog,
            connectors,
            scan_binding_resolver,
        }
    }
}

fn build_for_test(
    request: TestBuildRequest<'_>,
) -> Result<
    (
        PreparedFragmentSet,
        NativeFragmentBundle,
        Vec<BoundarySchemaReport>,
    ),
    String,
> {
    let _ = request.catalog;
    let prepared = prepare_fragments(
        request.distributed_plan,
        request.connectors,
        request.scan_binding_resolver,
    )?;
    let native_bundle = super::encode_native_fragment_bundle(request.distributed_plan, &prepared)?;
    Ok((
        prepared,
        native_bundle,
        project_boundary_reports(request.distributed_plan),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::DataType;

    use super::super::boundary_schema::{
        BoundaryKind, BoundarySchemaColumn, project_boundary_reports,
    };
    use super::*;
    use crate::connector::ConnectorRegistry;
    use crate::connector::iceberg::scan_model::IcebergDataFileBinding;
    use crate::coordinator::prepare::build_iceberg_metadata_scan_range_params;
    use crate::runtime_filter::model::graph::RuntimeFilterGraph;
    use crate::sql::analysis::cte::CteId;
    use crate::sql::analysis::{ExprKind, OutputColumn as AnalysisOutputColumn, TypedExpr};
    use crate::sql::catalog::CatalogProvider;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::{
        BoundaryContract, BoundaryKind as PlannerBoundaryKind, DataPartition, DistributedNode,
        DistributedNodeKind, ExchangeFlavor, ExchangeReceiver, FragmentEdge, FragmentEdgeKind,
        FragmentId, FragmentStreamKind, PartitionKind, PlanFragment,
    };
    use crate::sql::planner::payload::{PlanScanNode, PlanValuesNode};
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
    use crate::sql::planner::table::{ScanSource, TableDef};

    struct EmptyCatalog;

    impl CatalogProvider for EmptyCatalog {
        fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
            Err(format!("unexpected table lookup {database}.{table}"))
        }
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn output_col(id: u32, name: &str) -> AnalysisOutputColumn {
        AnalysisOutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn column_ref(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn physical_values_node(
        fragment_id: FragmentId,
        node_id: i32,
        columns: Vec<AnalysisOutputColumn>,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: vec![node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns,
            }),
        }
    }

    #[derive(Debug)]
    struct PlannedIcebergFiles {
        files: Vec<crate::connector::iceberg::scan_model::IcebergDataFileInfo>,
    }

    impl crate::connector::scan_planning::ConnectorScanPlanner for PlannedIcebergFiles {
        fn name(&self) -> &'static str {
            "iceberg"
        }

        fn begin_scan(
            &self,
            table: crate::connector::scan_planning::TableHandle,
            _ctx: crate::connector::scan_planning::BeginScanContext,
        ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
            let table = table
                .downcast_ref::<crate::connector::iceberg::scan_planner::IcebergTableHandle>()
                .ok_or_else(|| "PlannedIcebergFiles expected IcebergTableHandle".to_string())?
                .clone();
            Ok(crate::connector::scan_planning::ScanHandle::new(
                "iceberg",
                crate::connector::iceberg::scan_planner::IcebergScanHandle { table },
            ))
        }

        fn plan_splits(
            &self,
            _scan: &crate::connector::scan_planning::ScanHandle,
            _ctx: crate::connector::scan_planning::SplitPlanningContext,
        ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
            Ok(self
                .files
                .iter()
                .cloned()
                .map(|data_file| {
                    crate::connector::scan_planning::Split::new(
                        "iceberg",
                        crate::connector::iceberg::scan_planner::IcebergSplit { data_file },
                    )
                })
                .collect())
        }
    }

    fn iceberg_schema_field(
        field_id: i32,
        name: &str,
    ) -> crate::connector::iceberg::scan_model::IcebergSchemaFieldDef {
        crate::connector::iceberg::scan_model::IcebergSchemaFieldDef {
            field_id,
            name: name.to_string(),
            initial_default: None,
            write_default: None,
            initial_default_json: None,
            write_default_json: None,
            children: Vec::new(),
        }
    }

    fn iceberg_table_info() -> crate::connector::iceberg::scan_model::IcebergTableInfo {
        crate::connector::iceberg::scan_model::IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "s3://bucket/test_table".to_string(),
            schema: crate::connector::iceberg::scan_model::IcebergSchemaDef {
                fields: vec![
                    iceberg_schema_field(1, "id"),
                    iceberg_schema_field(3, "category"),
                ],
            },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

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
                value: Some(
                    crate::connector::iceberg::scan_model::IcebergPartitionValue::Int32(id),
                ),
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

    fn iceberg_scan_plan(required_columns: Option<Vec<&str>>) -> DistributedPlan {
        iceberg_scan_plan_with_outputs(required_columns, &["id"])
    }

    fn iceberg_scan_plan_with_outputs(
        required_columns: Option<Vec<&str>>,
        output_names: &[&str],
    ) -> DistributedPlan {
        let id = AnalysisOutputColumn {
            column_id: ColumnId::new_for_test(1),
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        };
        let category = AnalysisOutputColumn {
            column_id: ColumnId::new_for_test(3),
            name: "category".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            is_internal: false,
        };
        let all_outputs = [id, category];
        let output_columns = output_names
            .iter()
            .map(|name| {
                all_outputs
                    .iter()
                    .find(|column| column.name == *name)
                    .unwrap_or_else(|| panic!("unknown Iceberg scan test output {name}"))
                    .clone()
            })
            .collect::<Vec<_>>();
        let table = TableDef {
            name: "ice_t".to_string(),
            columns: vec![
                crate::catalog::schema::ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                crate::catalog::schema::ColumnDef {
                    name: "category".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::IcebergDataFiles {
                table: iceberg_table_info(),
                files: Vec::new(),
                cloud_properties: BTreeMap::new(),
                binding: IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        let scan = DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: vec![10],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Scan(PlanScanNode {
                database: "default".to_string(),
                table,
                alias: None,
                columns: output_columns.clone(),
                predicates: Vec::new(),
                required_columns: required_columns
                    .map(|columns| columns.into_iter().map(str::to_string).collect()),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
        };
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: scan,
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: crate::sql::planner::distributed::DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        }
    }

    fn set_iceberg_scan_predicates(
        plan: DistributedPlan,
        predicates: Vec<TypedExpr>,
    ) -> DistributedPlan {
        crate::sql::planner::distributed::test_support::rebuild_test_plan(plan, |draft| {
            let DistributedNodeKind::Scan(scan) = &mut draft.fragments_mut()[0].root.payload else {
                panic!("root must be scan");
            };
            scan.predicates = predicates;
        })
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
        let mut registry = ConnectorRegistry::new();
        registry.register_scan_planner(std::sync::Arc::new(PlannedIcebergFiles { files }));
        registry
    }

    fn native_root_scan(
        result: &(
            PreparedFragmentSet,
            NativeFragmentBundle,
            Vec<BoundarySchemaReport>,
        ),
    ) -> &crate::proto::plan::ScanNode {
        let root_fragment_id = result.0.scheduling_view().execution_anchor();
        let root = result
            .1
            .get(root_fragment_id)
            .expect("native root fragment")
            .root
            .as_ref()
            .expect("root node");
        let crate::proto::plan::distributed_node::Payload::Physical(physical) =
            root.payload.as_ref().expect("root payload")
        else {
            panic!("root must be physical");
        };
        let crate::proto::plan::plan_node::Kind::Scan(scan) =
            physical.kind.as_ref().expect("physical kind")
        else {
            panic!("root must be scan");
        };
        scan
    }

    fn native_file_ranges(
        result: &(
            PreparedFragmentSet,
            NativeFragmentBundle,
            Vec<BoundarySchemaReport>,
        ),
    ) -> &[crate::runtime::scan_range::ScanRangeParams] {
        result
            .0
            .scheduling_view()
            .scan_ranges(0, 10)
            .expect("scan node ranges")
    }

    fn native_file_range(
        range: &crate::runtime::scan_range::ScanRangeParams,
    ) -> &crate::runtime::scan_range::FileScanRange {
        match &range.range {
            crate::runtime::scan_range::ScanRange::File(file) => file,
            crate::runtime::scan_range::ScanRange::StarRocksTablet(_) => {
                panic!("expected file range")
            }
        }
    }

    struct SentinelDeltaResolver {
        calls: AtomicUsize,
    }

    impl crate::coordinator::prepare::scan::ScanBindingResolver for SentinelDeltaResolver {
        fn resolve_scan(
            &self,
            node_id: i32,
            scan: &PlanScanNode,
        ) -> Result<Option<crate::coordinator::prepare::scan::ResolvedScanExecution>, String>
        {
            self.calls.fetch_add(1, Ordering::Relaxed);
            assert_eq!(node_id, 10);
            assert!(matches!(
                scan.table.source,
                ScanSource::IcebergDeltaTable {
                    from_snapshot_id: 6,
                    to_snapshot_id: 7,
                    ..
                }
            ));
            Ok(Some(
                crate::coordinator::prepare::scan::ResolvedScanExecution::IcebergDelta(
                    crate::coordinator::prepare::scan::ResolvedIcebergDeltaScan {
                        runtime_plan:
                            crate::coordinator::prepare::scan::IcebergDeltaScanRuntimePlan {
                                table_location: "s3://bucket/test_table".to_string(),
                                data_columns: Vec::new(),
                                cloud_properties: BTreeMap::new(),
                                change_files: Vec::new(),
                                delete_side: None,
                            },
                    },
                ),
            ))
        }
    }

    #[test]
    fn fragment_build_prepares_delta_once_without_mutating_input_plan() {
        let plan = crate::sql::planner::distributed::test_support::rebuild_test_plan(
            iceberg_scan_plan(Some(vec!["id"])),
            |draft| {
                let DistributedNodeKind::Scan(scan) = &mut draft.fragments_mut()[0].root.payload
                else {
                    panic!("root must be scan");
                };
                scan.table.source = ScanSource::IcebergDeltaTable {
                    table: iceberg_table_info(),
                    from_snapshot_id: 6,
                    to_snapshot_id: 7,
                };
            },
        );
        let before = format!("{plan:#?}");
        let resolver = SentinelDeltaResolver {
            calls: AtomicUsize::new(0),
        };

        let result = build_for_test(TestBuildRequest {
            distributed_plan: &plan,
            catalog: &EmptyCatalog,
            connectors: &ConnectorRegistry::new(),
            scan_binding_resolver: Some(&resolver),
        })
        .expect("build prepared delta fragment");

        assert_eq!(
            resolver.calls.load(Ordering::Relaxed),
            1,
            "delta binding must resolve once"
        );
        assert_eq!(format!("{plan:#?}"), before);
        let ranges = result
            .0
            .scheduling_view()
            .scan_ranges(0, 10)
            .expect("delta sentinel range by original node id");
        assert_eq!(ranges.len(), 1);
        let file = native_file_range(&ranges[0]);
        assert_eq!(file.full_path.as_deref(), Some("iceberg-metadata"));
        assert!(file.use_iceberg_jni_metadata_reader);
    }

    #[test]
    fn fragment_build_reports_missing_delta_resolver_before_encoding() {
        let plan = crate::sql::planner::distributed::test_support::rebuild_test_plan(
            iceberg_scan_plan(Some(vec!["id"])),
            |draft| {
                let DistributedNodeKind::Scan(scan) = &mut draft.fragments_mut()[0].root.payload
                else {
                    panic!("root must be scan");
                };
                scan.table.source = ScanSource::IcebergDeltaTable {
                    table: iceberg_table_info(),
                    from_snapshot_id: 6,
                    to_snapshot_id: 7,
                };
            },
        );

        let err = match build_for_test(TestBuildRequest::result(
            &plan,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        )) {
            Ok(_) => panic!("delta scan without resolver must fail during preparation"),
            Err(err) => err,
        };

        assert!(err.contains("IcebergDeltaTable"), "{err}");
        assert!(err.contains("node_id=10"), "{err}");
        assert!(err.contains("from_snapshot_id=6"), "{err}");
        assert!(err.contains("to_snapshot_id=7"), "{err}");
        assert!(err.contains("requires scan binding resolver"), "{err}");
    }

    #[test]
    fn equality_delete_field_ids_are_merged_into_native_required_columns() {
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

        assert_eq!(
            native_root_scan(&result).required_columns,
            vec!["id", "category"]
        );
    }

    #[test]
    fn equality_delete_column_names_are_merged_into_native_required_columns() {
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

        assert_eq!(
            native_root_scan(&result).required_columns,
            vec!["id", "category"]
        );
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

        assert_eq!(scan.required_columns, vec!["id", "category"]);
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id"]
        );
    }

    #[test]
    fn equality_delete_with_unrestricted_non_key_projection_preserves_full_read_layout() {
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

        assert_eq!(scan.required_columns, vec!["id", "category"]);
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
        let ranges = native_file_ranges(&result);

        assert_eq!(ranges.len(), 1);
        assert_eq!(
            native_file_range(&ranges[0]).full_path.as_deref(),
            Some("s3://bucket/id-10-20.parquet")
        );
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
        let ranges = native_file_ranges(&result);

        assert_eq!(ranges.len(), 1);
        assert_eq!(
            native_file_range(&ranges[0]).full_path.as_deref(),
            Some("s3://bucket/id-12.parquet")
        );
    }

    #[test]
    fn native_iceberg_scan_splits_large_plain_file() {
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
        let ranges = native_file_ranges(&result);

        assert_eq!(ranges.len(), 3);
        assert_eq!(native_file_range(&ranges[0]).offset, 0);
        assert_eq!(native_file_range(&ranges[1]).offset, 128 * 1024 * 1024);
        assert_eq!(native_file_range(&ranges[2]).offset, 256 * 1024 * 1024);
        assert_eq!(native_file_range(&ranges[2]).length, 44 * 1024 * 1024);
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

        assert_eq!(native_file_ranges(&result).len(), 2);
    }

    #[test]
    fn planner_broadcast_edge_remains_broadcast_through_builder_and_scheduling() {
        use crate::sql::planner::physical::{
            PhysicalPlanKind, PhysicalPlanNode, RedistributeMode, RedistributeNode,
        };

        let columns = vec![output_col(1, "k")];
        let values = PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: columns.clone(),
            }),
            children: Vec::new(),
            output_columns: columns.clone(),
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let broadcast = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Broadcast,
                partition_exprs: Vec::new(),
                output_columns: columns.clone(),
            }),
            children: vec![values],
            output_columns: columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let planned = crate::sql::planner::distributed::build::build_distributed_plan(&broadcast)
            .expect("planner broadcast DistributedPlan");
        assert_eq!(
            planned.edges()[0].stream_kind,
            FragmentStreamKind::Broadcast
        );
        assert!(matches!(
            planned.edges()[0].output_partition.kind,
            PartitionKind::Unpartitioned
        ));

        let result = build_for_test(TestBuildRequest::result(
            &planned,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        ))
        .expect("native fragment build");
        assert_eq!(
            result.0.scheduling_view().edges()[0].stream_kind,
            FragmentStreamKind::Broadcast
        );
        assert!(matches!(
            result.0.scheduling_view().edges()[0].output_partition.kind,
            PartitionKind::Unpartitioned
        ));

        let target_fragment_id = result.0.scheduling_view().edges()[0].target_fragment_id;
        let prepared = crate::coordinator::prepare::prepared_fragment_set_for_test(
            planned
                .fragments()
                .iter()
                .map(|fragment| {
                    let is_target = fragment.fragment_id == target_fragment_id;
                    (
                        fragment.fragment_id,
                        if is_target {
                            crate::coordinator::prepare::PreparedFragmentRole::TerminalWrite
                        } else {
                            crate::coordinator::prepare::PreparedFragmentRole::NonTerminal
                        },
                        if is_target {
                            vec![(
                                99,
                                vec![
                                    build_iceberg_metadata_scan_range_params(),
                                    build_iceberg_metadata_scan_range_params(),
                                    build_iceberg_metadata_scan_range_params(),
                                ],
                            )]
                        } else {
                            Vec::new()
                        },
                    )
                })
                .collect(),
            planned.topology().topological_fragment_order().to_vec(),
            planned.topology().execution_anchor_fragment_id(),
            planned.edges().to_vec(),
        );
        let scheduler = crate::coordinator::scheduler::FragmentScheduler::new(vec![
            "127.0.0.1:19001".parse().unwrap(),
            "127.0.0.1:19002".parse().unwrap(),
            "127.0.0.1:19003".parse().unwrap(),
        ]);
        let scheduling = scheduler
            .schedule(
                prepared.scheduling_view(),
                crate::common::types::UniqueId { hi: 1, lo: 7 },
                None,
            )
            .expect("schedule broadcast plan");
        assert_eq!(
            scheduling.by_fragment[&target_fragment_id].len(),
            3,
            "broadcast input must not collapse a target with three scan ranges to Gather"
        );
    }

    #[test]
    fn random_partition_with_other_stream_kind_remains_other() {
        let plan = crate::sql::planner::distributed::test_support::rebuild_test_plan(
            stream_exchange_plan(ExchangeFlavor::Distribution),
            |draft| {
                let partition = DataPartition {
                    kind: PartitionKind::Random,
                    exprs: Vec::new(),
                };
                let DistributedNodeKind::Exchange(exchange) =
                    &mut draft.fragments_mut()[1].root.payload
                else {
                    panic!("target must be exchange");
                };
                exchange.partition = partition.clone();
                draft.edges_mut()[0].output_partition = partition.clone();
                draft.fragments_mut()[0].output_partition = partition;
                draft.edges_mut()[0].stream_kind = FragmentStreamKind::Other;
            },
        );

        let result = build_for_test(TestBuildRequest::result(
            &plan,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        ))
        .expect("build random stream");

        assert_eq!(
            result.0.scheduling_view().edges()[0].stream_kind,
            FragmentStreamKind::Other
        );
        assert!(matches!(
            result.0.scheduling_view().edges()[0].output_partition.kind,
            PartitionKind::Random
        ));
    }

    #[test]
    fn legal_stream_partition_kind_combinations_remain_unchanged() {
        let cases = [
            (DataPartition::unpartitioned(), FragmentStreamKind::Gather),
            (
                DataPartition::unpartitioned(),
                FragmentStreamKind::Broadcast,
            ),
            (
                DataPartition {
                    kind: PartitionKind::Random,
                    exprs: Vec::new(),
                },
                FragmentStreamKind::Other,
            ),
            (
                DataPartition {
                    kind: PartitionKind::Hash,
                    exprs: vec![column_ref(1, "k")],
                },
                FragmentStreamKind::Partitioned,
            ),
        ];

        for (partition, stream_kind) in cases {
            let plan = crate::sql::planner::distributed::test_support::rebuild_test_plan(
                stream_exchange_plan(ExchangeFlavor::Distribution),
                |draft| {
                    let DistributedNodeKind::Exchange(exchange) =
                        &mut draft.fragments_mut()[1].root.payload
                    else {
                        panic!("target must be exchange");
                    };
                    exchange.partition = partition.clone();
                    draft.edges_mut()[0].output_partition = partition.clone();
                    draft.fragments_mut()[0].output_partition = partition.clone();
                    draft.edges_mut()[0].stream_kind = stream_kind;
                },
            );

            let result = build_for_test(TestBuildRequest::result(
                &plan,
                &EmptyCatalog,
                &ConnectorRegistry::new(),
                None,
            ))
            .unwrap_or_else(|err| {
                panic!(
                    "legal stream combination {:?}+{stream_kind:?} must lower: {err}",
                    partition.kind
                )
            });

            assert_eq!(
                result.0.scheduling_view().edges()[0].stream_kind,
                stream_kind
            );
            assert_eq!(
                std::mem::discriminant(
                    &result.0.scheduling_view().edges()[0].output_partition.kind
                ),
                std::mem::discriminant(&partition.kind)
            );
        }
    }

    fn stream_exchange_plan(flavor: ExchangeFlavor) -> DistributedPlan {
        let columns = vec![output_col(1, "k")];
        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer_fragment = PlanFragment {
            fragment_id: producer_fragment_id,
            root: physical_values_node(producer_fragment_id, 10, columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Noop,
            output_exprs: None,
            output_columns: columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let consumer_fragment = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: columns.clone(),
                    output_qualifier: None,
                    flavor,
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Result,
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![producer_fragment, consumer_fragment],
            root_fragment_id: consumer_fragment_id,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![1],
            }],
        }
    }

    #[test]
    fn lower_distributed_plan_accepts_stream_limit_and_topn_exchange_flavors() {
        let cases = vec![
            (
                "limit_offset",
                ExchangeFlavor::LimitOffset {
                    limit: Some(1),
                    offset: Some(0),
                },
            ),
            (
                "topn_split",
                ExchangeFlavor::TopNSplit {
                    items: Vec::new(),
                    limit: Some(1),
                    offset: Some(0),
                },
            ),
        ];

        for (label, flavor) in cases {
            let dp = stream_exchange_plan(flavor);
            build_for_test(TestBuildRequest::result(
                &dp,
                &EmptyCatalog,
                &ConnectorRegistry::new(),
                None,
            ))
            .unwrap_or_else(|err| panic!("{label} stream exchange should lower: {err}"));
        }
    }

    #[test]
    fn fragment_build_preserves_finalized_edges_and_input_plan() {
        let dp = stream_exchange_plan(ExchangeFlavor::Distribution);
        let before = format!("{dp:#?}");
        let planned_edges = format!("{:#?}", dp.edges());

        let result = build_for_test(TestBuildRequest::result(
            &dp,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        ))
        .expect("native fragment build");

        assert_eq!(format!("{dp:#?}"), before);
        assert_eq!(
            format!("{:#?}", result.0.scheduling_view().edges()),
            planned_edges
        );
    }

    fn finalized_router_plan() -> DistributedPlan {
        let output_columns = vec![
            output_col(1, "op"),
            output_col(2, "route"),
            output_col(3, "delete_id"),
        ];
        let dp = crate::sql::planner::distributed::test_support::distributed_plan_draft_builder_for_test! {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: physical_values_node(0, 10, output_columns.clone()),
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: crate::sql::planner::distributed::DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        };
        let mut branch =
            crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![2]);
        branch.output_partition_ordinals = vec![2];
        branch.sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::distributed::write::sink::test_support::unpartitioned_metadata_json(),
        );
        let dag =
            crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec::for_test(Some(0), None, vec![branch]);
        crate::sql::planner::distributed::write::plan::finalize_iceberg_change_stream_test_plan(
            dp, "test_db", dag,
        )
        .expect("plan change-stream write")
    }

    #[test]
    fn fragment_build_preserves_finalized_router_edge() {
        let planned = finalized_router_plan();
        let before = format!("{planned:#?}");
        let planned_edges = format!("{:#?}", planned.edges());

        let result = build_for_test(TestBuildRequest::result(
            &planned,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        ))
        .expect("native fragment build");

        assert_eq!(format!("{planned:#?}"), before);
        assert_eq!(
            format!("{:#?}", result.0.scheduling_view().edges()),
            planned_edges
        );

        let edge = &result.0.scheduling_view().edges()[0];
        assert!(matches!(edge.output_partition.kind, PartitionKind::Hash));
        assert_eq!(edge.output_partition.exprs.len(), 1);
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &edge.output_partition.exprs[0].kind
        else {
            panic!("expected router HASH partition column ref");
        };
        assert_eq!(*column_id, ColumnId::new_for_test(3));
        assert_eq!(column, "delete_id");
        assert_eq!(edge.stream_kind, FragmentStreamKind::Partitioned);
        let source = result
            .1
            .get(edge.source_fragment_id)
            .expect("router source fragment");
        let route_partition = match source
            .sink
            .as_ref()
            .and_then(|sink| sink.kind.as_ref())
            .expect("router sink")
        {
            crate::proto::plan::data_sink::Kind::IcebergChangeStreamRouter(router) => router
                .branches[0]
                .output_partition
                .as_ref()
                .expect("router route partition"),
            other => panic!("expected router sink, got {other:?}"),
        };
        assert_eq!(
            route_partition.kind,
            crate::proto::plan::PartitionKind::Hash as i32
        );
        assert_eq!(route_partition.exprs.len(), 1);

        let target = result
            .1
            .get(edge.target_fragment_id)
            .expect("router target fragment");
        let receiver = match target
            .root
            .as_ref()
            .and_then(|root| root.payload.as_ref())
            .expect("router target exchange receiver")
        {
            crate::proto::plan::distributed_node::Payload::Exchange(exchange) => exchange,
            other => panic!("expected router exchange receiver, got {other:?}"),
        };
        assert_eq!(receiver.partition_type, route_partition.kind);
        assert_eq!(receiver.partition_exprs, route_partition.exprs);
    }

    #[test]
    fn lower_distributed_plan_owns_native_fragments_matching_schedules_and_root() {
        let dp = stream_exchange_plan(ExchangeFlavor::LimitOffset {
            limit: Some(1),
            offset: Some(0),
        });

        let result = build_for_test(TestBuildRequest::result(
            &dp,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        ))
        .expect("native fragment build");
        let fragment_ids = result.1.fragment_ids().collect::<BTreeSet<_>>();
        let prepared_ids = result.0.fragment_ids();

        assert_eq!(fragment_ids, prepared_ids);
        assert!(fragment_ids.contains(&dp.root_fragment_id()));
    }

    #[test]
    fn lower_distributed_plan_owns_native_write_sink_shape() {
        let columns = vec![output_col(1, "id")];
        let mut sink_spec =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        sink_spec.target_table.columns[0].data_type = DataType::Int64;
        sink_spec.target_columns[0].data_type = DataType::Int64;
        let fragment = PlanFragment {
            fragment_id: 0,
            root: physical_values_node(0, 10, columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::IcebergWrite(
                crate::sql::planner::distributed::write::sink::IcebergWriteFragmentSink {
                    descriptor_database: "default".to_string(),
                    spec: sink_spec,
                    input: crate::sql::planner::distributed::write::sink::IcebergWriteInputBinding::RootOutputByOrdinal,
                },
            ),
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![fragment],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        };

        let result = build_for_test(TestBuildRequest::result(
            &plan,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        ))
        .expect("native write fragment build");
        assert_eq!(
            result
                .0
                .fragment(0)
                .expect("prepared write fragment")
                .execution_role(),
            crate::coordinator::prepare::PreparedFragmentRole::TerminalWrite
        );
        assert!(
            result
                .0
                .fragment(0)
                .expect("prepared write fragment")
                .boundary_projection()
                .output_columns()
                .is_empty(),
            "Iceberg target schema belongs to WriteContractCatalog, not prepared query output"
        );
        let sink = result
            .1
            .get(0)
            .expect("native fragment")
            .sink
            .as_ref()
            .expect("native sink");
        assert!(matches!(
            sink.kind,
            Some(crate::proto::plan::data_sink::Kind::IcebergWrite(_))
        ));
    }

    #[test]
    fn fragment_build_preserves_finalized_cte_multicast_edge_output_slots() {
        let cte_id: CteId = 7;
        let producer_columns = vec![
            output_col(1, "k"),
            output_col(2, "v"),
            output_col(3, "payload"),
        ];
        let receive_columns = vec![producer_columns[0].clone(), producer_columns[2].clone()];
        let receive_producer_column_ids =
            vec![producer_columns[0].column_id, producer_columns[2].column_id];

        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer_fragment = PlanFragment {
            fragment_id: producer_fragment_id,
            root: physical_values_node(producer_fragment_id, 10, producer_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Noop,
            output_exprs: None,
            output_columns: producer_columns,
            cte_id: Some(cte_id),
            cte_exchange_nodes: Vec::new(),
        };
        let consumer_fragment = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: receive_columns.clone(),
                    output_qualifier: Some("c".to_string()),
                    flavor: ExchangeFlavor::CteMulticast {
                        cte_id,
                        receive_producer_column_ids: receive_producer_column_ids.clone(),
                    },
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Result,
            output_exprs: None,
            output_columns: receive_columns,
            cte_id: None,
            cte_exchange_nodes: vec![(
                cte_id,
                exchange_node_id,
                receive_producer_column_ids.clone(),
            )],
        };
        let dp = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![producer_fragment, consumer_fragment],
            root_fragment_id: consumer_fragment_id,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids,
                },
                output_slot_ids: vec![1, 3],
            }],
        };
        let before = format!("{dp:#?}");

        let result = build_for_test(TestBuildRequest::result(
            &dp,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        ))
        .expect("native lower plan");

        assert_eq!(format!("{dp:#?}"), before);
        assert_eq!(
            result.0.scheduling_view().edges()[0].output_slot_ids,
            vec![1, 3]
        );
        let native_consumer = result
            .1
            .get(consumer_fragment_id)
            .expect("encoded native consumer");
        assert_eq!(native_consumer.cte_exchange_nodes[0].column_ids, vec![1, 3]);
    }

    // ------------------------------------------------------------------
    // CGO-9B Task 3: codegen boundary reports are a read-only projection of
    // the planner's sealed boundary catalog. These tests pin that the
    // projection preserves ExecutionColumnId occurrence order and ColumnId
    // provenance, and that codegen cannot select a different logical schema
    // than the planner catalog.
    // ------------------------------------------------------------------

    fn cte_multicast_plan() -> DistributedPlan {
        let cte_id: CteId = 7;
        let producer_columns = vec![
            output_col(1, "k"),
            output_col(2, "v"),
            output_col(3, "payload"),
        ];
        let receive_columns = vec![producer_columns[0].clone(), producer_columns[2].clone()];
        let receive_producer_column_ids =
            vec![producer_columns[0].column_id, producer_columns[2].column_id];
        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer_fragment = PlanFragment {
            fragment_id: producer_fragment_id,
            root: physical_values_node(producer_fragment_id, 10, producer_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Noop,
            output_exprs: None,
            output_columns: producer_columns,
            cte_id: Some(cte_id),
            cte_exchange_nodes: Vec::new(),
        };
        let consumer_fragment = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: receive_columns.clone(),
                    output_qualifier: Some("c".to_string()),
                    flavor: ExchangeFlavor::CteMulticast {
                        cte_id,
                        receive_producer_column_ids: receive_producer_column_ids.clone(),
                    },
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Result,
            output_exprs: None,
            output_columns: receive_columns,
            cte_id: None,
            cte_exchange_nodes: vec![(
                cte_id,
                exchange_node_id,
                receive_producer_column_ids.clone(),
            )],
        };
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![producer_fragment, consumer_fragment],
            root_fragment_id: consumer_fragment_id,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids,
                },
                output_slot_ids: vec![1, 3],
            }],
        }
    }

    /// Assert the projected reports are a faithful, order-preserving projection
    /// of the sealed boundary catalog: one report per contract, kind mapped, and
    /// every column copied verbatim (occurrence identity, logical provenance, and
    /// descriptive schema). This is the structural proof that codegen cannot
    /// select a different logical schema: it cannot fabricate the planner's
    /// per-occurrence `ExecutionColumnId`s, so matching them means they were
    /// copied, not re-derived.
    fn assert_reports_mirror_catalog(plan: &DistributedPlan) {
        let contracts = plan.boundaries().contracts();
        let reports = project_boundary_reports(plan);
        assert_eq!(
            reports.len(),
            contracts.len(),
            "exactly one report per sealed boundary contract"
        );
        for (report, contract) in reports.iter().zip(contracts) {
            assert_eq!(report.fragment_id, Some(contract.fragment_id as i32));
            assert_eq!(report.node_id, contract.node_id);
            assert_eq!(
                report.boundary_kind,
                BoundaryKind::from_planner(contract.kind)
            );
            assert_eq!(
                report.columns.len(),
                contract.columns.len(),
                "column count preserved for {:?}",
                contract.kind
            );
            for (ordinal, (column, source)) in
                report.columns.iter().zip(&contract.columns).enumerate()
            {
                // Occurrence identity and logical provenance copied verbatim.
                assert_eq!(column.execution_column_id, source.execution_column_id);
                assert_eq!(column.column_id, source.column_id);
                // Descriptive schema copied verbatim, never re-selected.
                assert_eq!(column.name, source.name);
                assert_eq!(column.arrow_type, source.data_type);
                assert_eq!(column.nullable, source.nullable);
                // slot_id is the 1-based boundary-local occurrence position.
                assert_eq!(column.slot_id, ordinal as i32 + 1);
                assert_eq!(column.slot_id, source.output_ordinal as i32 + 1);
            }
        }
    }

    #[test]
    fn codegen_projects_stream_boundaries_from_planner_catalog() {
        assert_reports_mirror_catalog(&stream_exchange_plan(ExchangeFlavor::Distribution));
    }

    #[test]
    fn codegen_projects_cte_multicast_boundaries_from_planner_catalog() {
        // The receiver projects producer columns [k, payload] out of [k, v,
        // payload]; the send/receive boundaries must carry exactly that
        // planner-owned two-column schema, never the producer's full output.
        assert_reports_mirror_catalog(&cte_multicast_plan());
    }

    #[test]
    fn codegen_projects_router_and_write_boundaries_from_planner_catalog() {
        assert_reports_mirror_catalog(&finalized_router_plan());
    }

    #[test]
    fn codegen_projects_single_fragment_result_boundary_from_planner_catalog() {
        assert_reports_mirror_catalog(&iceberg_scan_plan(None));
    }

    #[test]
    fn codegen_preserves_execution_column_id_occurrence_order_across_send_and_receive() {
        let plan = stream_exchange_plan(ExchangeFlavor::Distribution);
        let reports = project_boundary_reports(&plan);
        let send = reports
            .iter()
            .find(|report| report.boundary_kind == BoundaryKind::ExchangeSender)
            .expect("stream plan has an exchange sender boundary");
        let receive = reports
            .iter()
            .find(|report| report.boundary_kind == BoundaryKind::ExchangeReceiver)
            .expect("stream plan has an exchange receiver boundary");

        // Same logical column at both seams ...
        assert_eq!(send.columns[0].column_id, receive.columns[0].column_id);
        // ... but distinct query-scoped occurrence identity, and the planner
        // numbers the send occurrence before the receive occurrence per edge.
        assert_ne!(
            send.columns[0].execution_column_id,
            receive.columns[0].execution_column_id
        );
        assert!(
            send.columns[0].execution_column_id.value()
                < receive.columns[0].execution_column_id.value()
        );
    }

    #[test]
    fn codegen_projection_drops_per_fragment_root_and_gains_sink_boundaries() {
        // A change-stream router/write plan has no result sink. The projection
        // emits exactly the planner's four seams -- no ResultRoot, and the sink
        // inputs the planner owns (router input + Iceberg write input) that the
        // pre-Task-3 codegen enum had no variant for.
        let reports = project_boundary_reports(&finalized_router_plan());
        let has = |kind: BoundaryKind| reports.iter().any(|report| report.boundary_kind == kind);
        assert!(
            !has(BoundaryKind::ResultRoot),
            "a router/write plan projects no ResultRoot boundary"
        );
        assert!(has(BoundaryKind::ChangeStreamRouterInput));
        assert!(has(BoundaryKind::IcebergWriteInput));
        assert!(has(BoundaryKind::ExchangeSender));
        assert!(has(BoundaryKind::ExchangeReceiver));
    }

    #[test]
    fn native_fragment_build_boundary_schemas_are_the_planner_catalog_projection() {
        let plan = stream_exchange_plan(ExchangeFlavor::Distribution);
        let result = build_for_test(TestBuildRequest::result(
            &plan,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        ))
        .expect("native fragment build");

        // build_for_test() wires the query-level reports straight from the projection,
        // in canonical order, without reordering or dropping any boundary.
        assert_eq!(result.2, project_boundary_reports(&plan));
    }

    #[test]
    fn boundary_schema_columns_carry_planner_provenance() {
        // Regression pin for the provenance the pre-Task-3 generator dropped: the
        // projected column carries both the query-scoped occurrence id and the
        // logical ColumnId, not just a re-numbered slot.
        let plan = cte_multicast_plan();
        let reports = project_boundary_reports(&plan);
        let result_root = reports
            .iter()
            .find(|report| report.boundary_kind == BoundaryKind::ResultRoot)
            .expect("cte plan has a result-root boundary");
        let column_ids: Vec<ColumnId> = result_root
            .columns
            .iter()
            .map(|column: &BoundarySchemaColumn| column.column_id)
            .collect();
        assert_eq!(
            column_ids,
            vec![ColumnId::new_for_test(1), ColumnId::new_for_test(3)],
            "result-root boundary preserves the planner ColumnId provenance"
        );
        // Occurrence ids are dense and ordered within the boundary.
        assert!(
            result_root.columns[0].execution_column_id.value()
                < result_root.columns[1].execution_column_id.value()
        );

        // Anchor the provenance to the concrete planner contract it projects, so
        // the pin fails if codegen ever re-derives instead of copying.
        let contract: &BoundaryContract = plan
            .boundaries()
            .contracts()
            .iter()
            .find(|contract| contract.kind == PlannerBoundaryKind::ResultOutput)
            .expect("cte plan has a result-output contract");
        assert_eq!(contract.columns.len(), result_root.columns.len());
        for (projected, source) in result_root.columns.iter().zip(&contract.columns) {
            assert_eq!(projected.column_id, source.column_id);
            assert_eq!(projected.execution_column_id, source.execution_column_id);
        }
    }
}
