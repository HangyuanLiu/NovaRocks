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
use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::physical::runtime_filter::{
    RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};
use crate::sql::planner::physical::{JoinExecutionMode, PhysicalPlanKind};

// Runtime-filter semantic DTO mapping is intentionally covered by the Frontend
// owner in `novarocks-frontend::runtime_filter::plan_encoder`. Core only freezes
// the carrier-neutral fragment shells and their binding-id attachment points.

#[test]
fn native_encoder_leaves_nonempty_runtime_filter_plan_unbound() {
    let probe_expr = TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId::new_for_test(1),
            qualifier: Some("probe".to_string()),
            column: "k".to_string(),
        },
        data_type: DataType::Int64,
        nullable: false,
    };
    let build_expr = TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId::new_for_test(2),
            qualifier: Some("build".to_string()),
            column: "k".to_string(),
        },
        data_type: DataType::Int64,
        nullable: false,
    };
    let probe_output = vec![output_column(1, "probe", DataType::Int64)];
    let build_output = vec![output_column(2, "build", DataType::Int64)];
    let probe = crate::sql::planner::physical::PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(crate::sql::planner::payload::PlanValuesNode {
            rows: Vec::new(),
            columns: probe_output.clone(),
        }),
        children: Vec::new(),
        output_columns: probe_output,
        stats: stats(),
        probe_runtime_filters: vec![RuntimeFilterProbeIntent {
            filter_id: 41,
            probe_expr: probe_expr.clone(),
        }],
    };
    let build = crate::sql::planner::physical::PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(crate::sql::planner::payload::PlanValuesNode {
            rows: Vec::new(),
            columns: build_output.clone(),
        }),
        children: Vec::new(),
        output_columns: build_output,
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    };
    let physical = crate::sql::planner::physical::PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(
            crate::sql::planner::physical::PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: vec![crate::sql::planner::physical::PhysicalHashJoinEqCondition {
                    left: probe_expr.clone(),
                    right: build_expr.clone(),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
                execution_mode: Some(JoinExecutionMode::Broadcast),
                build_runtime_filters: vec![RuntimeFilterBuildIntent {
                    filter_id: 41,
                    build_expr,
                    probe_expr,
                    expr_order: 0,
                    execution_mode: JoinExecutionMode::Broadcast,
                }],
                output_columns: vec![
                    output_column(1, "probe", DataType::Int64),
                    output_column(2, "build", DataType::Int64),
                ],
            },
        )),
        children: vec![probe, build],
        output_columns: vec![
            output_column(1, "probe", DataType::Int64),
            output_column(2, "build", DataType::Int64),
        ],
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    };

    let distributed = crate::sql::planner::distributed::build::build_distributed_plan(&physical)
        .expect("build Graph-owned RF plan");
    assert_eq!(distributed.runtime_filter_graph().channel_count(), 1);
    assert!(distributed.runtime_filter_graph().binding_count() > 0);

    let registry = crate::connector::ConnectorRegistry::new();
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        &controls,
        &crate::connector::test_request_context(),
        None,
        None,
        crate::query_execution::preparation::ScanPreparationOptions::default(),
    )
    .expect("prepare Graph-owned RF projection");
    let bundle = crate::protocol::native::encode::bundle::encode_native_fragment_bundle(
        &distributed,
        &prepared,
    )
    .expect("encode RF-unbound fragment shells");

    fn collect_binding_ids(node: &plan::DistributedNode, ids: &mut Vec<u32>) {
        ids.extend_from_slice(&node.runtime_filter_binding_ids);
        for child in &node.children {
            collect_binding_ids(child, ids);
        }
    }

    let mut binding_ids = Vec::new();
    for (_, fragment) in bundle.fragments_in_id_order() {
        assert!(
            fragment.runtime_filter_bindings.is_none(),
            "Core must leave runtime-filter semantic tables unbound"
        );
        collect_binding_ids(
            fragment.root.as_ref().expect("fragment root"),
            &mut binding_ids,
        );
    }
    binding_ids.sort_unstable();
    binding_ids.dedup();
    assert_eq!(
        binding_ids.len(),
        distributed.runtime_filter_graph().binding_count(),
        "generic shells must retain every binding-id attachment point"
    );
}

#[test]
fn native_encoder_leaves_empty_runtime_filter_plan_unbound() {
    let distributed = two_fragment_stream_plan_for_test();
    assert!(distributed.runtime_filter_graph().is_empty());
    let registry = crate::connector::ConnectorRegistry::new();
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        &controls,
        &crate::connector::test_request_context(),
        None,
        None,
        crate::query_execution::preparation::ScanPreparationOptions::default(),
    )
    .expect("prepare plan without runtime filters");
    let bundle = crate::protocol::native::encode::bundle::encode_native_fragment_bundle(
        &distributed,
        &prepared,
    )
    .expect("encode RF-unbound fragment shells");

    for (_, fragment) in bundle.fragments_in_id_order() {
        assert!(
            fragment.runtime_filter_bindings.is_none(),
            "Frontend must attach even an explicit empty binding table"
        );
    }
}
