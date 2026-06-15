//! Test-only IR equivalence helpers.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::connector::iceberg::IcebergMetadataTableType;
    use crate::connector::{ConnectorRegistry, iceberg::IcebergConnectorScanPlanner};
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{
        CatalogProvider, ColumnDef, IcebergDataFileBinding, IcebergDataFileInfo, IcebergSchemaDef,
        IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::codegen::fragment_builder::PlanFragmentBuilder;
    use crate::sql::codegen::{
        FragmentBuildResult, FragmentEdge, FragmentEdgeKind, MultiFragmentBuildResult,
        OutputColumn as CodegenOutputColumn, RuntimeFilterPlanResult,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        Operator, PhysicalDistributionOp, PhysicalFilterOp, PhysicalProjectOp, PhysicalScanOp,
    };
    use crate::sql::optimizer::physical_plan::{PhysicalPlanNode, PlanExecutionProps};
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::statistics::Statistics;

    #[test]
    fn scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("scan", scan_plan());
    }

    #[test]
    fn scan_filter_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("scan_filter", filter_plan(scan_plan()));
    }

    #[test]
    fn scan_project_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("scan_project", project_plan(scan_plan()));
    }

    #[test]
    fn scan_filter_project_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "scan_filter_project",
            project_plan(filter_plan(scan_plan())),
        );
    }

    #[test]
    fn root_gather_scan_filter_project_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "root_gather_scan_filter_project",
            root_gather_plan(project_plan(filter_plan(scan_plan()))),
        );
    }

    #[test]
    fn iceberg_data_file_scan_ranges_match_direct_fragment_builder() {
        let mut connectors = ConnectorRegistry::new();
        connectors.register_scan_planner(Arc::new(IcebergConnectorScanPlanner::new()));
        let (direct, distributed) = build_both_paths(
            "iceberg_data_file_scan_ranges",
            iceberg_data_file_scan_plan(),
            &connectors,
        );

        assert_non_empty_scan_ranges("iceberg_data_file_scan_ranges direct", &direct);
        assert_non_empty_scan_ranges(
            "iceberg_data_file_scan_ranges DistributedPlan",
            &distributed,
        );
        assert_multi_fragment_equivalent("iceberg_data_file_scan_ranges", &direct, &distributed);
    }

    fn assert_distributed_plan_equivalent(case_name: &str, plan: PhysicalPlanNode) {
        let connectors = ConnectorRegistry::new();
        let (direct, distributed) = build_both_paths(case_name, plan, &connectors);
        assert_multi_fragment_equivalent(case_name, &direct, &distributed);
    }

    fn build_both_paths(
        case_name: &str,
        plan: PhysicalPlanNode,
        connectors: &ConnectorRegistry,
    ) -> (MultiFragmentBuildResult, MultiFragmentBuildResult) {
        let catalog = DummyCatalog;
        let direct = PlanFragmentBuilder::build(&plan, &catalog, &connectors, "test_db")
            .unwrap_or_else(|err| panic!("{case_name}: direct build failed: {err}"));
        let distributed = PlanFragmentBuilder::build_via_distributed_plan(
            &plan,
            &catalog,
            &connectors,
            "test_db",
        )
        .unwrap_or_else(|err| panic!("{case_name}: DistributedPlan build failed: {err}"));

        (direct, distributed)
    }

    fn assert_multi_fragment_equivalent(
        case_name: &str,
        direct: &MultiFragmentBuildResult,
        distributed: &MultiFragmentBuildResult,
    ) {
        assert_eq!(
            direct.root_fragment_id, distributed.root_fragment_id,
            "{case_name}: root_fragment_id"
        );
        assert_eq!(
            direct.fragment_results.len(),
            distributed.fragment_results.len(),
            "{case_name}: fragment count"
        );
        assert_edges_eq(case_name, &direct.edges, &distributed.edges);
        assert_eq!(
            direct.boundary_schemas, distributed.boundary_schemas,
            "{case_name}: multi-fragment boundary schemas"
        );
        assert_runtime_filter_plan_eq(case_name, &direct.rf_plan, &distributed.rf_plan);

        for direct_fragment in &direct.fragment_results {
            let distributed_fragment =
                fragment_by_id(case_name, distributed, direct_fragment.fragment_id);
            assert_fragment_equivalent(case_name, direct_fragment, distributed_fragment);
        }
    }

    fn fragment_by_id<'a>(
        case_name: &str,
        result: &'a MultiFragmentBuildResult,
        fragment_id: crate::sql::codegen::FragmentId,
    ) -> &'a FragmentBuildResult {
        result
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == fragment_id)
            .unwrap_or_else(|| panic!("{case_name}: fragment {fragment_id} not found"))
    }

    fn assert_fragment_equivalent(
        case_name: &str,
        direct: &FragmentBuildResult,
        distributed: &FragmentBuildResult,
    ) {
        assert_eq!(
            direct.fragment_id, distributed.fragment_id,
            "{case_name}: fragment_id"
        );
        assert_eq!(direct.plan, distributed.plan, "{case_name}: fragment plan");
        assert_eq!(
            direct.desc_tbl, distributed.desc_tbl,
            "{case_name}: descriptor table"
        );
        assert_eq!(
            direct.exec_params, distributed.exec_params,
            "{case_name}: exec params"
        );
        assert_eq!(
            direct.output_sink, distributed.output_sink,
            "{case_name}: output sink"
        );
        assert_eq!(
            direct.output_exprs, distributed.output_exprs,
            "{case_name}: output exprs"
        );
        assert_output_columns_eq(
            case_name,
            &direct.output_columns,
            &distributed.output_columns,
        );
        assert!(
            direct.direct_exec.is_none() && distributed.direct_exec.is_none(),
            "{case_name}: scan/filter/project root fragments should not use direct exec"
        );
        assert_eq!(
            direct.boundary_schemas, distributed.boundary_schemas,
            "{case_name}: fragment boundary schemas"
        );
        assert_eq!(direct.cte_id, distributed.cte_id, "{case_name}: cte_id");
        assert_eq!(
            direct.cte_exchange_nodes, distributed.cte_exchange_nodes,
            "{case_name}: cte exchange nodes"
        );
        assert_eq!(
            direct.query_global_dicts, distributed.query_global_dicts,
            "{case_name}: query global dicts"
        );
        assert_eq!(
            direct.query_global_dict_exprs, distributed.query_global_dict_exprs,
            "{case_name}: query global dict exprs"
        );
    }

    fn assert_output_columns_eq(
        case_name: &str,
        direct: &[CodegenOutputColumn],
        distributed: &[CodegenOutputColumn],
    ) {
        assert_eq!(
            direct.len(),
            distributed.len(),
            "{case_name}: output column count"
        );
        for (idx, (direct, distributed)) in direct.iter().zip(distributed.iter()).enumerate() {
            assert_eq!(
                direct.name, distributed.name,
                "{case_name}: output column {idx} name"
            );
            assert_eq!(
                direct.data_type, distributed.data_type,
                "{case_name}: output column {idx} type"
            );
            assert_eq!(
                direct.nullable, distributed.nullable,
                "{case_name}: output column {idx} nullability"
            );
        }
    }

    fn assert_edges_eq(case_name: &str, direct: &[FragmentEdge], distributed: &[FragmentEdge]) {
        assert_eq!(direct.len(), distributed.len(), "{case_name}: edge count");
        for (idx, (direct, distributed)) in direct.iter().zip(distributed.iter()).enumerate() {
            assert_eq!(
                direct.source_fragment_id, distributed.source_fragment_id,
                "{case_name}: edge {idx} source fragment"
            );
            assert_eq!(
                direct.target_fragment_id, distributed.target_fragment_id,
                "{case_name}: edge {idx} target fragment"
            );
            assert_eq!(
                direct.target_exchange_node_id, distributed.target_exchange_node_id,
                "{case_name}: edge {idx} target exchange node"
            );
            assert_eq!(
                direct.output_partition, distributed.output_partition,
                "{case_name}: edge {idx} output partition"
            );
            assert_eq!(
                direct.stream_kind, distributed.stream_kind,
                "{case_name}: edge {idx} stream kind"
            );
            assert_fragment_edge_kind_eq(case_name, idx, &direct.edge_kind, &distributed.edge_kind);
        }
    }

    fn assert_fragment_edge_kind_eq(
        case_name: &str,
        idx: usize,
        direct: &FragmentEdgeKind,
        distributed: &FragmentEdgeKind,
    ) {
        match (direct, distributed) {
            (FragmentEdgeKind::Stream, FragmentEdgeKind::Stream) => {}
            (
                FragmentEdgeKind::CteMulticast { cte_id: direct_id },
                FragmentEdgeKind::CteMulticast {
                    cte_id: distributed_id,
                },
            ) => assert_eq!(
                direct_id, distributed_id,
                "{case_name}: edge {idx} CTE multicast id"
            ),
            _ => panic!("{case_name}: edge {idx} kind mismatch: direct and DistributedPlan differ"),
        }
    }

    fn assert_runtime_filter_plan_eq(
        case_name: &str,
        direct: &Option<RuntimeFilterPlanResult>,
        distributed: &Option<RuntimeFilterPlanResult>,
    ) {
        match (direct, distributed) {
            (None, None) => {}
            (Some(direct), Some(distributed)) => {
                assert_eq!(
                    direct.all_filters, distributed.all_filters,
                    "{case_name}: runtime filter descriptors"
                );
                assert_eq!(
                    direct.build_side_filters, distributed.build_side_filters,
                    "{case_name}: runtime filter build-side map"
                );
                assert_eq!(
                    direct.probe_side_filters, distributed.probe_side_filters,
                    "{case_name}: runtime filter probe-side map"
                );
            }
            _ => panic!("{case_name}: runtime filter plan presence mismatch"),
        }
    }

    fn assert_non_empty_scan_ranges(case_name: &str, result: &MultiFragmentBuildResult) {
        let root = fragment_by_id(case_name, result, result.root_fragment_id);
        let ranges = &root.exec_params.per_node_scan_ranges;
        assert!(
            !ranges.is_empty() && ranges.values().any(|node_ranges| !node_ranges.is_empty()),
            "{case_name}: expected non-empty scan ranges"
        );
    }

    struct DummyCatalog;

    impl CatalogProvider for DummyCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("equivalence tests use fully resolved metadata-table scans".to_string())
        }
    }

    fn scan_plan() -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: metadata_table_def(),
                alias: Some("t".to_string()),
                columns: vec![k.clone(), v.clone()],
                predicates: vec![cmp_expr(
                    column_ref_expr(1, "k", DataType::Int64, false),
                    BinOp::Eq,
                    int_lit(7),
                )],
                required_columns: Some(vec!["k".to_string(), "v".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
        )
    }

    fn iceberg_data_file_scan_plan() -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: iceberg_data_table_def(),
                alias: Some("t".to_string()),
                columns: vec![k.clone(), v.clone()],
                predicates: vec![cmp_expr(
                    column_ref_expr(1, "k", DataType::Int64, false),
                    BinOp::Eq,
                    int_lit(7),
                )],
                required_columns: Some(vec!["k".to_string(), "v".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
        )
    }

    fn filter_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalFilter(PhysicalFilterOp {
                predicate: and_expr(
                    cmp_expr(
                        column_ref_expr(1, "k", DataType::Int64, false),
                        BinOp::Gt,
                        int_lit(10),
                    ),
                    cmp_expr(
                        column_ref_expr(2, "v", DataType::Int64, true),
                        BinOp::Lt,
                        int_lit(20),
                    ),
                ),
            }),
            vec![child],
            output_columns,
        )
    }

    fn project_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = vec![output_col(101, "k_plus_one", DataType::Int64, false)];
        physical_node(
            Operator::PhysicalProject(PhysicalProjectOp {
                items: vec![ProjectItem {
                    expr: add_expr(column_ref_expr(1, "k", DataType::Int64, false), int_lit(1)),
                    output_name: "k_plus_one".to_string(),
                    output_column_id: ColumnId::new_for_test(101),
                }],
                output_qualifier: None,
            }),
            vec![child],
            output_columns,
        )
    }

    fn root_gather_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Gather,
            }),
            vec![child],
            output_columns,
        )
    }

    fn physical_node(
        op: Operator,
        children: Vec<PhysicalPlanNode>,
        output_columns: Vec<OutputColumn>,
    ) -> PhysicalPlanNode {
        PhysicalPlanNode {
            op,
            children,
            stats: Statistics::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    fn metadata_table_def() -> TableDef {
        TableDef {
            name: "t$snapshots".to_string(),
            columns: vec![
                column_def("k", DataType::Int64, false),
                column_def("v", DataType::Int64, true),
                column_def("unused", DataType::Int64, true),
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergMetadataTable {
                table: iceberg_table_info(),
                metadata_table_type: IcebergMetadataTableType::Snapshots,
                serialized_table: "{}".to_string(),
                cloud_properties: Default::default(),
                metadata_payload: None,
            },
        }
    }

    fn iceberg_data_table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![
                column_def("k", DataType::Int64, false),
                column_def("v", DataType::Int64, true),
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg_table_info(),
                files: vec![iceberg_data_file("s3://bucket/t/data-1.parquet")],
                cloud_properties: Default::default(),
                binding: IcebergDataFileBinding::ExplicitFiles,
            },
        }
    }

    fn iceberg_data_file(path: &str) -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: path.to_string(),
            size: 128,
            row_count: Some(10),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        }
    }

    fn iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "t".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///warehouse/t".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn column_def(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn output_col(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn column_ref_expr(id: u32, column: &str, data_type: DataType, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: column.to_string(),
            },
            data_type,
            nullable,
        }
    }

    fn int_lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn cmp_expr(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn add_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Add,
                right: Box::new(right),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }
}
