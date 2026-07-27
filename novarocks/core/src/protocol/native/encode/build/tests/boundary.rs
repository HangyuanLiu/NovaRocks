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
        runtime_filter_graph: Default::default(),
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
        for (ordinal, (column, source)) in report.columns.iter().zip(&contract.columns).enumerate()
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
