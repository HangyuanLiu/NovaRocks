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
use novarocks_sql::plan_read::{ColumnId, SqlBoundaryKindRead, boundary_contract_reads};
use novarocks_sql::test_support::{
    NativeBuildFixture, NativeEncoderPlanFixture, NativeScanFixture, native_build_plan,
    native_encoder_plan, native_scan_plan,
};

// ------------------------------------------------------------------
// CGO-9B Task 3: codegen boundary reports are a read-only projection of
// the planner's sealed boundary catalog. These tests pin that the
// projection preserves ExecutionColumnId occurrence order and ColumnId
// provenance, and that codegen cannot select a different logical schema
// than the planner catalog.
// ------------------------------------------------------------------

/// Assert the projected reports are a faithful, order-preserving projection
/// of the sealed boundary catalog: one report per contract, kind mapped, and
/// every column copied verbatim (occurrence identity, logical provenance, and
/// descriptive schema). This is the structural proof that codegen cannot
/// select a different logical schema: it cannot fabricate the planner's
/// per-occurrence `ExecutionColumnId`s, so matching them means they were
/// copied, not re-derived.
fn assert_reports_mirror_catalog(plan: &DistributedPlan) {
    let contracts = boundary_contract_reads(plan);
    let reports = project_boundary_reports(plan);
    assert_eq!(
        reports.len(),
        contracts.len(),
        "exactly one report per sealed boundary contract"
    );
    for (report, contract) in reports.iter().zip(&contracts) {
        assert_eq!(report.fragment_id, Some(contract.fragment_id as i32));
        assert_eq!(report.node_id, contract.node_id);
        assert_eq!(report.boundary_kind, BoundaryKind::from_sql(contract.kind));
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
    let plan = native_encoder_plan(NativeEncoderPlanFixture::HashExchange)
        .expect("sealed stream boundary fixture");
    assert_reports_mirror_catalog(&plan);
}

#[test]
fn codegen_projects_cte_multicast_boundaries_from_planner_catalog() {
    // The receiver projects producer columns [k, payload] out of [k, v,
    // payload]; the send/receive boundaries must carry exactly that
    // planner-owned two-column schema, never the producer's full output.
    let plan = native_build_plan(NativeBuildFixture::CteMulticastStream)
        .expect("sealed CTE boundary fixture");
    assert_reports_mirror_catalog(&plan);
}

#[test]
fn codegen_projects_router_and_write_boundaries_from_planner_catalog() {
    let plan = native_build_plan(NativeBuildFixture::RouterStream)
        .expect("sealed router boundary fixture");
    assert_reports_mirror_catalog(&plan);
}

#[test]
fn codegen_projects_single_fragment_result_boundary_from_planner_catalog() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergUnrestricted)
        .expect("sealed result boundary fixture");
    assert_reports_mirror_catalog(&plan);
}

#[test]
fn codegen_preserves_execution_column_id_occurrence_order_across_send_and_receive() {
    let plan = native_encoder_plan(NativeEncoderPlanFixture::HashExchange)
        .expect("sealed stream boundary fixture");
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
    let plan = native_build_plan(NativeBuildFixture::RouterStream)
        .expect("sealed router boundary fixture");
    let reports = project_boundary_reports(&plan);
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
    let plan = native_encoder_plan(NativeEncoderPlanFixture::HashExchange)
        .expect("sealed stream boundary fixture");
    let result = build_for_test(TestBuildRequest::result(
        &plan,
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
    let plan = native_build_plan(NativeBuildFixture::CteMulticastStream)
        .expect("sealed CTE boundary fixture");
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
        vec![ColumnId(1), ColumnId(3)],
        "result-root boundary preserves the planner ColumnId provenance"
    );
    // Occurrence ids are dense and ordered within the boundary.
    assert!(
        result_root.columns[0].execution_column_id.value()
            < result_root.columns[1].execution_column_id.value()
    );

    // Anchor the provenance to the concrete planner contract it projects, so
    // the pin fails if codegen ever re-derives instead of copying.
    let contracts = boundary_contract_reads(&plan);
    let contract = contracts
        .iter()
        .find(|contract| contract.kind == SqlBoundaryKindRead::ResultOutput)
        .expect("cte plan has a result-output contract");
    assert_eq!(contract.columns.len(), result_root.columns.len());
    for (projected, source) in result_root.columns.iter().zip(&contract.columns) {
        assert_eq!(projected.column_id, source.column_id);
        assert_eq!(projected.execution_column_id, source.execution_column_id);
    }
}
