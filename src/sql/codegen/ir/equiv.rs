//! Test-only IR equivalence helpers.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::connector::iceberg::IcebergMetadataTableType;
    use crate::connector::{ConnectorRegistry, iceberg::IcebergConnectorScanPlanner};
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, SortItem, TypedExpr,
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
        AggMode, JoinDistribution, Operator, PhysicalAssertOneRowOp, PhysicalDecodeOp,
        PhysicalDistributionOp, PhysicalExceptOp, PhysicalFilterOp, PhysicalGenerateSeriesOp,
        PhysicalHashAggregateOp, PhysicalHashJoinEqCondition, PhysicalHashJoinOp,
        PhysicalIntersectOp, PhysicalLimitOp, PhysicalNestLoopJoinOp, PhysicalProjectOp,
        PhysicalRepeatOp, PhysicalScanOp, PhysicalSortOp, PhysicalTableFunctionOp, PhysicalTopNOp,
        PhysicalUnionOp, PhysicalValuesOp, PhysicalWindowOp, ScanDictionaryColumn, TopNPhase,
    };
    use crate::sql::optimizer::physical_plan::{PhysicalPlanNode, PlanExecutionProps};
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::runtime_filter_pass::{RuntimeFilterDesc, RuntimeFilterProbe};
    use crate::sql::optimizer::statistics::Statistics;
    use crate::sql::planner::plan::{AggregateCall, DecodeMapping, WindowExpr};

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
    fn sort_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("sort_over_scan", sort_plan(scan_plan()));
    }

    #[test]
    fn limit_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "limit_over_scan",
            limit_plan(scan_plan(), Some(5), None),
        );
    }

    #[test]
    fn limit_over_sort_with_offset_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "limit_over_sort_with_offset",
            limit_plan(sort_plan(scan_plan()), Some(5), Some(2)),
        );
    }

    #[test]
    fn limit_over_top_n_overrides_top_n_limit_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "limit_over_top_n_overrides_top_n_limit",
            limit_plan(
                top_n_plan(scan_plan(), TopNPhase::Final, false, Some(10), Some(0)),
                Some(5),
                Some(2),
            ),
        );
    }

    #[test]
    fn limit_over_aggregate_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "limit_over_aggregate",
            limit_plan(aggregate_count_plan(scan_plan()), Some(3), None),
        );
    }

    #[test]
    fn limit_over_hash_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "limit_over_hash_join",
            limit_plan(inner_hash_join_two_scans_plan(), Some(4), None),
        );
    }

    #[test]
    fn top_n_final_single_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "top_n_final_single_over_scan",
            top_n_plan(scan_plan(), TopNPhase::Final, false, Some(5), Some(1)),
        );
    }

    #[test]
    fn top_n_partial_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "top_n_partial_over_scan",
            top_n_plan(scan_plan(), TopNPhase::Partial, false, Some(7), Some(0)),
        );
    }

    #[test]
    fn top_n_split_fails_fast_in_distributed_plan() {
        assert_distributed_plan_error_contains(
            "top_n_split",
            top_n_plan(scan_plan(), TopNPhase::Final, true, Some(5), Some(0)),
            "TopN split is Phase 2",
        );
    }

    #[test]
    fn limit_offset_without_sort_child_fails_fast_in_distributed_plan() {
        assert_distributed_plan_error_contains(
            "limit_offset_without_sort_child",
            limit_plan(project_plan(scan_plan()), Some(5), Some(1)),
            "LIMIT/OFFSET without a local SORT/TOPN child is not supported",
        );
    }

    #[test]
    fn gather_over_limit_fails_fast_until_limit_exchange() {
        assert_distributed_plan_error_contains(
            "gather_over_limit",
            sort_plan(distribution_plan(
                limit_plan(scan_plan(), Some(5), None),
                DistributionSpec::Gather,
            )),
            "Gather over Limit is Task A4",
        );
    }

    #[test]
    fn aggregate_single_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "aggregate_single_over_scan",
            aggregate_group_by_plan(scan_plan()),
        );
    }

    #[test]
    fn aggregate_with_count_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "aggregate_with_count",
            aggregate_count_plan(scan_plan()),
        );
    }

    #[test]
    fn shuffle_agg_exchange_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "shuffle_agg_exchange",
            aggregate_group_by_plan(distribution_plan(
                scan_plan(),
                DistributionSpec::shuffle_agg([ColumnId::new_for_test(1)]),
            )),
        );
    }

    #[test]
    fn nested_gather_exchange_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "nested_gather_exchange",
            sort_plan(distribution_plan(scan_plan(), DistributionSpec::Gather)),
        );
    }

    #[test]
    fn sort_over_project_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "sort_over_project_over_scan",
            sort_plan(project_plan(scan_plan())),
        );
    }

    #[test]
    fn inner_hash_join_two_scans_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "inner_hash_join_two_scans",
            inner_hash_join_two_scans_plan(),
        );
    }

    #[test]
    fn broadcast_join_exchange_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "broadcast_join_exchange",
            broadcast_join_exchange_plan(),
        );
    }

    #[test]
    fn two_sided_shuffle_join_exchange_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "two_sided_shuffle_join_exchange",
            two_sided_shuffle_join_exchange_plan(),
        );
    }

    #[test]
    fn gather_root_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "gather_root",
            root_gather_plan(project_plan(filter_plan(scan_plan()))),
        );
    }

    #[test]
    fn left_outer_hash_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("left_outer_hash_join", left_outer_hash_join_plan());
    }

    #[test]
    fn hash_join_other_condition_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "hash_join_other_condition",
            hash_join_other_condition_plan(),
        );
    }

    #[test]
    fn left_semi_hash_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "left_semi_hash_join",
            hash_join_surviving_side_plan(JoinKind::LeftSemi),
        );
    }

    #[test]
    fn right_anti_hash_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "right_anti_hash_join",
            hash_join_surviving_side_plan(JoinKind::RightAnti),
        );
    }

    #[test]
    fn null_aware_left_anti_hash_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "null_aware_left_anti_hash_join",
            hash_join_surviving_side_plan(JoinKind::NullAwareLeftAnti),
        );
    }

    #[test]
    fn nest_loop_cross_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("nest_loop_cross_join", nest_loop_cross_join_plan());
    }

    #[test]
    fn nest_loop_inner_condition_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "nest_loop_inner_condition",
            nest_loop_condition_plan(JoinKind::Inner),
        );
    }

    #[test]
    fn nest_loop_left_outer_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "nest_loop_left_outer",
            nest_loop_condition_plan(JoinKind::LeftOuter),
        );
    }

    #[test]
    fn nest_loop_left_anti_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "nest_loop_left_anti",
            nest_loop_surviving_side_plan(JoinKind::LeftAnti),
        );
    }

    #[test]
    fn nest_loop_null_aware_left_anti_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "nest_loop_null_aware_left_anti",
            nest_loop_surviving_side_plan(JoinKind::NullAwareLeftAnti),
        );
    }

    #[test]
    fn union_all_two_scans_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "union_all_two_scans",
            union_plan(
                true,
                aliased_scan_plan("l", 1, 2),
                aliased_scan_plan("r", 3, 4),
            ),
        );
    }

    #[test]
    fn union_distinct_fails_fast_in_distributed_plan() {
        assert_distributed_plan_error_contains(
            "union_distinct_fails_fast",
            union_plan(
                false,
                aliased_scan_plan("l", 1, 2),
                aliased_scan_plan("r", 3, 4),
            ),
            "UNION DISTINCT is Phase 2",
        );
    }

    #[test]
    fn intersect_two_scans_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "intersect_two_scans",
            intersect_plan(aliased_scan_plan("l", 1, 2), aliased_scan_plan("r", 3, 4)),
        );
    }

    #[test]
    fn except_two_scans_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "except_two_scans",
            except_plan(aliased_scan_plan("l", 1, 2), aliased_scan_plan("r", 3, 4)),
        );
    }

    #[test]
    fn values_rows_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("values_rows", values_rows_plan());
    }

    #[test]
    fn assert_one_row_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "assert_one_row_over_scan",
            assert_one_row_plan(scan_plan()),
        );
    }

    #[test]
    fn decode_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("decode_over_scan", decode_over_scan_plan());
    }

    #[test]
    fn repeat_grouping_sets_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("repeat_grouping_sets", repeat_grouping_sets_plan());
    }

    #[test]
    fn window_row_number_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "window_row_number_over_scan",
            window_row_number_over_scan_plan(),
        );
    }

    #[test]
    fn generate_series_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("generate_series", generate_series_plan());
    }

    #[test]
    fn unnest_table_function_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "unnest_table_function_over_scan",
            unnest_table_function_over_scan_plan(),
        );
    }

    #[test]
    fn decode_output_expr_uses_materialized_string_slot() {
        let build = build_distributed_plan_only("decode_output_expr_slot", decode_over_scan_plan());
        let root = fragment_by_id("decode_output_expr_slot", &build, build.root_fragment_id);
        let decode = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == crate::plan_nodes::TPlanNodeType::DECODE_NODE)
            .expect("decode node");
        let decode_payload = decode.decode_node.as_ref().expect("decode payload");
        let mapping = decode_payload
            .dict_id_to_string_ids
            .as_ref()
            .expect("decode mapping");
        let string_slot_id = *mapping.values().next().expect("one decode mapping");
        let output_exprs = root.output_exprs.as_ref().expect("root output exprs");
        let slot_ref = output_exprs[0].nodes[0]
            .slot_ref
            .as_ref()
            .expect("decode output slot ref");
        assert_eq!(
            slot_ref.slot_id, string_slot_id,
            "decode result sink must read the materialized string slot"
        );
    }

    #[test]
    fn set_op_uses_declared_child_output_order() {
        let build =
            build_distributed_plan_only("set_op_child_output_order", reordered_union_values_plan());
        let root = fragment_by_id("set_op_child_output_order", &build, build.root_fragment_id);
        let union = root.plan.nodes.first().expect("set op root");
        assert_eq!(
            union.node_type,
            crate::plan_nodes::TPlanNodeType::UNION_NODE
        );
        assert_eq!(union.num_children, 2);
        let first_expr = &union
            .union_node
            .as_ref()
            .expect("union payload")
            .result_expr_lists[0][0];
        assert_eq!(
            first_expr.nodes[0].node_type,
            crate::exprs::TExprNodeType::SLOT_REF,
            "first set-op expression should read the declared string child column directly"
        );
    }

    #[test]
    fn set_op_child_arity_mismatch_fails_fast() {
        assert_distributed_plan_error_contains(
            "set_op_child_arity_mismatch",
            union_plan(
                true,
                aliased_scan_plan("l", 1, 2),
                single_column_scan_plan(output_col(901, "k", DataType::Int64, false)),
            ),
            "set operation child 1 column count mismatch",
        );
    }

    #[test]
    fn values_row_length_mismatch_fails_fast() {
        assert_distributed_plan_error_contains(
            "values_row_length_mismatch",
            bad_values_row_length_plan(),
            "VALUES row column count mismatch",
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

    fn build_distributed_plan_only(
        case_name: &str,
        plan: PhysicalPlanNode,
    ) -> MultiFragmentBuildResult {
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        PlanFragmentBuilder::build_via_distributed_plan(&plan, &catalog, &connectors, "test_db")
            .unwrap_or_else(|err| panic!("{case_name}: DistributedPlan build failed: {err}"))
    }

    fn assert_distributed_plan_error_contains(
        case_name: &str,
        plan: PhysicalPlanNode,
        expected: &str,
    ) {
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        let err = match PlanFragmentBuilder::build_via_distributed_plan(
            &plan,
            &catalog,
            &connectors,
            "test_db",
        ) {
            Ok(_) => panic!("{case_name}: DistributedPlan build unexpectedly succeeded"),
            Err(err) => err,
        };
        assert!(
            err.contains(expected),
            "{case_name}: expected error to contain `{expected}`, got `{err}`"
        );
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

    fn sort_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let sort_col = child.output_columns[0].clone();
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalSort(PhysicalSortOp {
                items: vec![SortItem {
                    expr: column_ref_expr(
                        sort_col.column_id.0,
                        &sort_col.name,
                        sort_col.data_type.clone(),
                        sort_col.nullable,
                    ),
                    asc: true,
                    nulls_first: false,
                }],
                analytic_partition_exprs: vec![],
                partition_limit: None,
                topn_type: None,
            }),
            vec![child],
            output_columns,
        )
    }

    fn limit_plan(
        child: PhysicalPlanNode,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalLimit(PhysicalLimitOp { limit, offset }),
            vec![child],
            output_columns,
        )
    }

    fn top_n_plan(
        child: PhysicalPlanNode,
        phase: TopNPhase,
        is_split: bool,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> PhysicalPlanNode {
        let sort_col = child.output_columns[0].clone();
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalTopN(PhysicalTopNOp {
                items: vec![SortItem {
                    expr: column_ref_expr(
                        sort_col.column_id.0,
                        &sort_col.name,
                        sort_col.data_type.clone(),
                        sort_col.nullable,
                    ),
                    asc: true,
                    nulls_first: false,
                }],
                limit,
                offset,
                phase,
                is_split,
            }),
            vec![child],
            output_columns,
        )
    }

    fn aggregate_group_by_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        physical_node(
            Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: vec![column_ref_expr(1, "k", DataType::Int64, false)],
                aggregates: vec![],
                output_columns: vec![k.clone()],
                is_merge: vec![],
            }),
            vec![child],
            vec![k],
        )
    }

    fn aggregate_count_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        let count = output_col(201, "count(*)", DataType::Int64, true);
        physical_node(
            Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: vec![column_ref_expr(1, "k", DataType::Int64, false)],
                aggregates: vec![AggregateCall {
                    name: "count".to_string(),
                    args: vec![],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: ColumnId::new_for_test(201),
                }],
                output_columns: vec![k.clone(), count.clone()],
                is_merge: vec![false],
            }),
            vec![child],
            vec![k, count],
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

    fn distribution_plan(child: PhysicalPlanNode, spec: DistributionSpec) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalDistribution(PhysicalDistributionOp { spec }),
            vec![child],
            output_columns,
        )
    }

    fn inner_hash_join_two_scans_plan() -> PhysicalPlanNode {
        let (mut join, left_key, right_key) = hash_join_plan(JoinKind::Inner);
        join.children[0].probe_runtime_filters = vec![RuntimeFilterProbe {
            filter_id: 7,
            probe_expr: left_key.clone(),
        }];
        join.build_runtime_filters = vec![RuntimeFilterDesc {
            filter_id: 7,
            build_expr: right_key,
            probe_expr: left_key,
            expr_order: 0,
            distribution: JoinDistribution::Broadcast,
        }];
        join
    }

    fn broadcast_join_exchange_plan() -> PhysicalPlanNode {
        let left = aliased_scan_plan("l", 1, 2);
        let right = distribution_plan(aliased_scan_plan("r", 3, 4), DistributionSpec::Broadcast);
        let left_key = column_ref_expr_with_qualifier(1, "l", "k", DataType::Int64, false);
        let right_key = column_ref_expr_with_qualifier(3, "r", "k", DataType::Int64, false);
        let output_columns = join_output_columns(&left, &right, JoinOutput::Both);
        physical_node(
            Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn two_sided_shuffle_join_exchange_plan() -> PhysicalPlanNode {
        let left = distribution_plan(
            aliased_scan_plan("l", 1, 2),
            DistributionSpec::shuffle_agg([ColumnId::new_for_test(1)]),
        );
        let right = distribution_plan(
            aliased_scan_plan("r", 3, 4),
            DistributionSpec::shuffle_agg([ColumnId::new_for_test(3)]),
        );
        let left_key = column_ref_expr_with_qualifier(1, "l", "k", DataType::Int64, false);
        let right_key = column_ref_expr_with_qualifier(3, "r", "k", DataType::Int64, false);
        let output_columns = join_output_columns(&left, &right, JoinOutput::Both);
        physical_node(
            Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn left_outer_hash_join_plan() -> PhysicalPlanNode {
        let (join, _, _) = hash_join_plan(JoinKind::LeftOuter);
        join
    }

    fn hash_join_plan(join_type: JoinKind) -> (PhysicalPlanNode, TypedExpr, TypedExpr) {
        hash_join_plan_with_options(join_type, None, JoinOutput::Both)
    }

    fn hash_join_other_condition_plan() -> PhysicalPlanNode {
        let left_value = column_ref_expr_with_qualifier(2, "l", "v", DataType::Int64, true);
        let right_value = column_ref_expr_with_qualifier(4, "r", "v", DataType::Int64, true);
        let other_condition = cmp_expr(left_value, BinOp::Gt, right_value);
        let (join, _, _) =
            hash_join_plan_with_options(JoinKind::Inner, Some(other_condition), JoinOutput::Both);
        join
    }

    fn hash_join_surviving_side_plan(join_type: JoinKind) -> PhysicalPlanNode {
        let output = match join_type {
            JoinKind::RightSemi | JoinKind::RightAnti => JoinOutput::RightOnly,
            _ => JoinOutput::LeftOnly,
        };
        let (join, _, _) = hash_join_plan_with_options(join_type, None, output);
        join
    }

    #[derive(Clone, Copy)]
    enum JoinOutput {
        Both,
        LeftOnly,
        RightOnly,
    }

    fn hash_join_plan_with_options(
        join_type: JoinKind,
        other_condition: Option<TypedExpr>,
        output: JoinOutput,
    ) -> (PhysicalPlanNode, TypedExpr, TypedExpr) {
        let left = aliased_scan_plan("l", 1, 2);
        let right = aliased_scan_plan("r", 3, 4);
        let left_key = column_ref_expr_with_qualifier(1, "l", "k", DataType::Int64, false);
        let right_key = column_ref_expr_with_qualifier(3, "r", "k", DataType::Int64, false);
        let output_columns = join_output_columns(&left, &right, output);
        let node = physical_node(
            Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_key.clone(),
                    right: right_key.clone(),
                    null_safe: false,
                }],
                other_condition,
                distribution: JoinDistribution::Broadcast,
            }),
            vec![left, right],
            output_columns,
        );
        (node, left_key, right_key)
    }

    fn join_output_columns(
        left: &PhysicalPlanNode,
        right: &PhysicalPlanNode,
        output: JoinOutput,
    ) -> Vec<OutputColumn> {
        match output {
            JoinOutput::Both => {
                let mut output_columns = left.output_columns.clone();
                output_columns.extend(right.output_columns.clone());
                output_columns
            }
            JoinOutput::LeftOnly => left.output_columns.clone(),
            JoinOutput::RightOnly => right.output_columns.clone(),
        }
    }

    fn nest_loop_cross_join_plan() -> PhysicalPlanNode {
        nest_loop_plan(JoinKind::Cross, None, JoinOutput::Both)
    }

    fn nest_loop_condition_plan(join_type: JoinKind) -> PhysicalPlanNode {
        let left_value = column_ref_expr_with_qualifier(2, "l", "v", DataType::Int64, true);
        let right_value = column_ref_expr_with_qualifier(4, "r", "v", DataType::Int64, true);
        nest_loop_plan(
            join_type,
            Some(cmp_expr(left_value, BinOp::Gt, right_value)),
            JoinOutput::Both,
        )
    }

    fn nest_loop_surviving_side_plan(join_type: JoinKind) -> PhysicalPlanNode {
        let output = match join_type {
            JoinKind::RightSemi | JoinKind::RightAnti => JoinOutput::RightOnly,
            _ => JoinOutput::LeftOnly,
        };
        let left_value = column_ref_expr_with_qualifier(2, "l", "v", DataType::Int64, true);
        let right_value = column_ref_expr_with_qualifier(4, "r", "v", DataType::Int64, true);
        nest_loop_plan(
            join_type,
            Some(cmp_expr(left_value, BinOp::Gt, right_value)),
            output,
        )
    }

    fn nest_loop_plan(
        join_type: JoinKind,
        condition: Option<TypedExpr>,
        output: JoinOutput,
    ) -> PhysicalPlanNode {
        let left = aliased_scan_plan("l", 1, 2);
        let right = aliased_scan_plan("r", 3, 4);
        let output_columns = join_output_columns(&left, &right, output);
        physical_node(
            Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type,
                condition,
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn union_plan(all: bool, left: PhysicalPlanNode, right: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = set_op_output_columns();
        physical_node(
            Operator::PhysicalUnion(PhysicalUnionOp {
                all,
                output_columns: output_columns.clone(),
                child_output_columns: vec![
                    left.output_columns.clone(),
                    right.output_columns.clone(),
                ],
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn intersect_plan(left: PhysicalPlanNode, right: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = set_op_output_columns();
        physical_node(
            Operator::PhysicalIntersect(PhysicalIntersectOp {
                output_columns: output_columns.clone(),
                child_output_columns: vec![
                    left.output_columns.clone(),
                    right.output_columns.clone(),
                ],
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn except_plan(left: PhysicalPlanNode, right: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = set_op_output_columns();
        physical_node(
            Operator::PhysicalExcept(PhysicalExceptOp {
                output_columns: output_columns.clone(),
                child_output_columns: vec![
                    left.output_columns.clone(),
                    right.output_columns.clone(),
                ],
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn set_op_output_columns() -> Vec<OutputColumn> {
        vec![
            output_col(501, "k", DataType::Int64, false),
            output_col(502, "v", DataType::Int64, true),
        ]
    }

    fn values_rows_plan() -> PhysicalPlanNode {
        let k = output_col(601, "k", DataType::Int64, false);
        let v = output_col(602, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalValues(PhysicalValuesOp {
                rows: vec![
                    vec![int_lit(1), int_lit(10)],
                    vec![int_lit(2), null_int_lit()],
                ],
                columns: vec![k.clone(), v.clone()],
            }),
            vec![],
            vec![k, v],
        )
    }

    fn bad_values_row_length_plan() -> PhysicalPlanNode {
        let k = output_col(611, "k", DataType::Int64, false);
        let v = output_col(612, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalValues(PhysicalValuesOp {
                rows: vec![vec![int_lit(1)]],
                columns: vec![k.clone(), v.clone()],
            }),
            vec![],
            vec![k, v],
        )
    }

    fn reordered_union_values_plan() -> PhysicalPlanNode {
        let left = two_column_values_plan(621, 622);
        let right = two_column_values_plan(623, 624);
        let output_columns = vec![
            output_col(625, "s", DataType::Utf8, true),
            output_col(626, "k", DataType::Int64, false),
        ];
        let child_output_columns = vec![
            vec![
                left.output_columns[1].clone(),
                left.output_columns[0].clone(),
            ],
            vec![
                right.output_columns[1].clone(),
                right.output_columns[0].clone(),
            ],
        ];
        physical_node(
            Operator::PhysicalUnion(PhysicalUnionOp {
                all: true,
                output_columns: output_columns.clone(),
                child_output_columns,
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn two_column_values_plan(k_id: u32, s_id: u32) -> PhysicalPlanNode {
        let k = output_col(k_id, "k", DataType::Int64, false);
        let s = output_col(s_id, "s", DataType::Utf8, true);
        physical_node(
            Operator::PhysicalValues(PhysicalValuesOp {
                rows: vec![],
                columns: vec![k.clone(), s.clone()],
            }),
            vec![],
            vec![k, s],
        )
    }

    fn assert_one_row_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalAssertOneRow(PhysicalAssertOneRowOp {
                subquery_text: "select k from t".to_string(),
            }),
            vec![child],
            output_columns,
        )
    }

    fn decode_over_scan_plan() -> PhysicalPlanNode {
        let source_id = ColumnId::new_for_test(701);
        let output_id = ColumnId::new_for_test(702);
        let scan = physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: dict_metadata_table_def(),
                alias: Some("t".to_string()),
                columns: vec![OutputColumn {
                    column_id: source_id,
                    name: "s".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                }],
                predicates: vec![],
                required_columns: Some(vec!["s_dict".to_string()]),
                dict_columns: vec![ScanDictionaryColumn {
                    source_column: "s".to_string(),
                    dict_column: "s_dict".to_string(),
                    dictionary: dict_snapshot_a_b(),
                }],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![OutputColumn {
                column_id: source_id,
                name: "s_dict".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
        );
        let decoded = output_col(output_id.0, "s", DataType::Utf8, false);
        physical_node(
            Operator::PhysicalDecode(PhysicalDecodeOp {
                mappings: vec![DecodeMapping {
                    source_column_id: source_id,
                    output_column_id: output_id,
                    dict_column: "s_dict".to_string(),
                    string_column: "s".to_string(),
                }],
                output_columns: vec![decoded.clone()],
            }),
            vec![scan],
            vec![decoded],
        )
    }

    fn repeat_grouping_sets_plan() -> PhysicalPlanNode {
        let k = output_col(801, "k", DataType::Int64, false);
        let grouping_col = output_col(802, "__grouping_fn_0", DataType::Int64, false);
        physical_node(
            Operator::PhysicalRepeat(PhysicalRepeatOp {
                repeat_column_ref_list: vec![vec!["k".to_string()], vec![]],
                repeat_column_ref_ids: vec![vec![k.column_id], vec![]],
                grouping_ids: vec![0, 1],
                all_rollup_columns: vec!["k".to_string()],
                all_rollup_column_ids: vec![k.column_id],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![("__grouping_fn_0".to_string(), vec!["k".to_string()])],
                grouping_fn_arg_ids: vec![vec![k.column_id]],
                grouping_fn_ids: vec![("__grouping_fn_0".to_string(), grouping_col.column_id)],
            }),
            vec![single_column_scan_plan(k.clone())],
            vec![k, grouping_col],
        )
    }

    fn window_row_number_over_scan_plan() -> PhysicalPlanNode {
        let k = output_col(901, "k", DataType::Int64, false);
        let row_number = output_col(902, "rn", DataType::Int64, false);
        physical_node(
            Operator::PhysicalWindow(PhysicalWindowOp {
                window_exprs: vec![WindowExpr {
                    name: "row_number".to_string(),
                    args: vec![],
                    distinct: false,
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: None,
                    result_type: DataType::Int64,
                    output_name: row_number.name.clone(),
                    output_column_id: row_number.column_id,
                    ignore_nulls: false,
                }],
                output_columns: vec![k.clone(), row_number.clone()],
            }),
            vec![single_column_scan_plan(k.clone())],
            vec![k, row_number],
        )
    }

    fn generate_series_plan() -> PhysicalPlanNode {
        let value = output_col(911, "value", DataType::Int64, false);
        physical_node(
            Operator::PhysicalGenerateSeries(PhysicalGenerateSeriesOp {
                start: 1,
                end: 3,
                step: 1,
                column_name: value.name.clone(),
                alias: Some("gs".to_string()),
                output_column_id: value.column_id,
            }),
            vec![],
            vec![value],
        )
    }

    fn unnest_table_function_over_scan_plan() -> PhysicalPlanNode {
        let array_type = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int64,
            true,
        )));
        let arr = output_col(921, "arr", array_type.clone(), true);
        let item = output_col(922, "item", DataType::Int64, true);
        physical_node(
            Operator::PhysicalTableFunction(PhysicalTableFunctionOp {
                function_name: "unnest".to_string(),
                args: vec![column_ref_expr(921, "arr", array_type, true)],
                output_columns: vec![item.clone()],
                alias: Some("u".to_string()),
                is_left_join: false,
            }),
            vec![single_column_scan_plan(arr.clone())],
            vec![arr, item],
        )
    }

    fn aliased_scan_plan(alias: &str, key_id: u32, value_id: u32) -> PhysicalPlanNode {
        let k = output_col(key_id, "k", DataType::Int64, false);
        let v = output_col(value_id, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: metadata_table_def(),
                alias: Some(alias.to_string()),
                columns: vec![k.clone(), v.clone()],
                predicates: vec![],
                required_columns: Some(vec!["k".to_string(), "v".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
        )
    }

    fn single_column_scan_plan(column: OutputColumn) -> PhysicalPlanNode {
        physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: single_column_metadata_table_def(&column),
                alias: Some("t".to_string()),
                columns: vec![column.clone()],
                predicates: vec![],
                required_columns: Some(vec![column.name.clone()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![column],
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

    fn single_column_metadata_table_def(column: &OutputColumn) -> TableDef {
        TableDef {
            name: "t$snapshots".to_string(),
            columns: vec![column_def(
                &column.name,
                column.data_type.clone(),
                column.nullable,
            )],
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

    fn dict_metadata_table_def() -> TableDef {
        TableDef {
            name: "dict_t$snapshots".to_string(),
            columns: vec![column_def("s", DataType::Utf8, false)],
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
        column_ref_expr_with_qualifier(id, "t", column, data_type, nullable)
    }

    fn column_ref_expr_with_qualifier(
        id: u32,
        qualifier: &str,
        column: &str,
        data_type: DataType,
        nullable: bool,
    ) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(qualifier.to_string()),
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

    fn null_int_lit() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Null),
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn dict_snapshot_a_b() -> Arc<crate::engine::dictionary::model::DictionarySnapshot> {
        use crate::engine::dictionary::model::{
            DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue,
            DictionaryWatermark,
        };

        Arc::new(DictionarySnapshot {
            dictionary_id: 1,
            owner: DictionaryOwner::IcebergTable {
                catalog: "test_catalog".to_string(),
                namespace: "test_db".to_string(),
                table: "dict_t".to_string(),
                table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            },
            column_id: None,
            column_name: "s".to_string(),
            data_type: DataType::Int32,
            version: 1,
            watermark: DictionaryWatermark::Iceberg {
                snapshot_id: Some(7),
                schema_id: 1,
            },
            values: vec![
                DictionaryValue {
                    id: 1,
                    bytes: b"a".to_vec(),
                },
                DictionaryValue {
                    id: 2,
                    bytes: b"b".to_vec(),
                },
            ],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving: true,
        })
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
