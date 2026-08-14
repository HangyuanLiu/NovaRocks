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

//! Feature-gated fixtures for consumers that need a complete, sealed SQL plan.
//!
//! This module is intentionally absent from the default public surface.  Its
//! fixtures are sealed through the same planner validation path as production
//! plans; it exposes neither a draft graph nor a mutation or sealing entrypoint.

use arrow::datatypes::DataType;

use crate::analysis::{ExprKind, OutputColumn, SubqueryKind, TypedExpr};
use crate::column_id::ColumnId;
use crate::common::{
    BinOp, JoinKind, LambdaParam, LiteralValue, ScanVariantColumn, UnOp, WindowBound,
    WindowFrame, WindowFrameType,
};
use crate::functions::FunctionVolatility;
use crate::plan_read::{DistributedPlan, SortItem};
use crate::planner::distributed::write::change_stream::{
    ChangeStreamRoute, ChangeStreamRouterSink,
};
use crate::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, ExchangeFlavor,
    ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentStreamKind, PartitionKind,
    PlanFragment,
};
use crate::planner::payload::PlanScanNode;
use crate::planner::payload::{
    PlanAssertOneRowNode, PlanProjectNode, PlanSortNode, PlanValuesNode,
};
use crate::planner::physical::{
    AggMode, AggregateOutputLayout, JoinDistribution, PhysicalHashAggregateNode,
    PhysicalHashJoinNode, PhysicalNestLoopJoinNode, PhysicalPlanKind, PhysicalPlanStats,
    PhysicalTopNNode, PlannerConfidence, TopNPhase,
};
use crate::planner::table::{
    SqlMvTargetLocatorScan, SqlMvTargetStatePartitionConstraint, SqlMvTargetStateRowFilter,
    SqlMvTargetStateScan, SqlScanKind, SqlTableVersionSelector, TableDef, test_sql_scan_source,
};
use novarocks_spi::connector::{
    ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteCohortId,
    ConnectorWriteFieldToken, ConnectorWriteRouteId,
};

/// Closed fixture catalog for native encoder consumers.
///
/// New cases belong here only when an encoder assertion needs a distinct sealed
/// SQL shape. There is deliberately no caller-supplied draft or mutation hook.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeEncoderPlanFixture {
    Minimal,
    HashExchange,
    HashAggregate,
    ReconciledHashJoin,
    ChangeStreamRouter,
    DuplicateProject,
    TopNDuplicateProject,
    NestLoopJoin,
    AssertOneRow,
    Sort,
}

/// Closed sealed scan shapes used by native encoder binding tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeScanFixture {
    DeltaWithStaleUnprojectedColumn,
    OrdinaryIcebergWithUnprojectedPayload,
    RefreshSnapshot,
    RefreshMvTargetLocator,
    RefreshMvTargetState,
    MvTargetLocator,
    MvTargetState,
    VariantProjection,
}

/// Build a sealed scan fixture without exporting a mutable planner draft.
pub fn native_scan_plan(fixture: NativeScanFixture) -> Result<DistributedPlan, String> {
    match fixture {
        NativeScanFixture::DeltaWithStaleUnprojectedColumn => native_delta_scan_plan(),
        NativeScanFixture::OrdinaryIcebergWithUnprojectedPayload => {
            native_ordinary_iceberg_scan_plan()
        }
        NativeScanFixture::RefreshSnapshot => native_refresh_scan_plan(refresh_snapshot_source()),
        NativeScanFixture::RefreshMvTargetLocator => {
            native_refresh_scan_plan(mv_target_locator_source("bound_order_id"))
        }
        NativeScanFixture::RefreshMvTargetState => {
            native_refresh_scan_plan(mv_target_state_source("bound_order_id"))
        }
        NativeScanFixture::MvTargetLocator => {
            native_basic_scan_plan(mv_target_locator_source("order_id"))
        }
        NativeScanFixture::MvTargetState => {
            native_basic_scan_plan(mv_target_state_source("order_id"))
        }
        NativeScanFixture::VariantProjection => native_variant_projection_scan_plan(),
    }
}

/// Build one complete plan used by native-encoder integration fixtures.
///
/// The returned plan has passed the production distributed-plan seal. Consumers
/// may inspect it only through [`crate::plan_read`].
pub fn native_encoder_plan(fixture: NativeEncoderPlanFixture) -> Result<DistributedPlan, String> {
    match fixture {
        NativeEncoderPlanFixture::Minimal => {
            crate::planner::distributed::native_encoder_test_fixture_plan()
        }
        NativeEncoderPlanFixture::HashExchange => native_hash_exchange_plan(),
        NativeEncoderPlanFixture::HashAggregate => native_hash_aggregate_plan(),
        NativeEncoderPlanFixture::ReconciledHashJoin => native_reconciled_hash_join_plan(),
        NativeEncoderPlanFixture::ChangeStreamRouter => native_change_stream_router_plan(),
        NativeEncoderPlanFixture::DuplicateProject => native_duplicate_project_plan(false),
        NativeEncoderPlanFixture::TopNDuplicateProject => native_duplicate_project_plan(true),
        NativeEncoderPlanFixture::NestLoopJoin => native_nest_loop_join_plan(),
        NativeEncoderPlanFixture::AssertOneRow => native_assert_one_row_plan(),
        NativeEncoderPlanFixture::Sort => native_sort_plan(),
    }
}

/// Rust physical-plan variants expected by the native encoder. This avoids
/// exposing the internal physical-plan enum merely for a test-only guard.
pub fn native_physical_plan_variant_names() -> &'static [&'static str] {
    PhysicalPlanKind::variant_names_for_test()
}

/// A sealed two-fragment hash exchange used by native wire round-trip tests.
fn native_hash_exchange_plan() -> Result<DistributedPlan, String> {
    let output = vec![output_column(10, "v", DataType::Int64)];
    let source = PlanFragment {
        fragment_id: 0,
        root: values_node(0, 11, output.clone()),
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition {
            kind: PartitionKind::Hash,
            exprs: vec![column_expr(10, "v", DataType::Int64)],
        },
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: output.clone(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let receiver = DistributedNode {
        node_id: 42,
        fragment_id: 1,
        tuple_ids: vec![42],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: DistributedNodeKind::Exchange(ExchangeReceiver {
            partition: DataPartition::hash(vec![column_expr(10, "v", DataType::Int64)]),
            source_fragment_id: 0,
            output_columns: output.clone(),
            output_qualifier: Some("recv".to_string()),
            flavor: ExchangeFlavor::Distribution,
        }),
    };
    let target = PlanFragment {
        fragment_id: 1,
        root: receiver,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: output,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let mut builder = crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        vec![source, target],
        Some(1),
        vec![FragmentEdge {
            source_fragment_id: 0,
            target_fragment_id: 1,
            target_exchange_node_id: 42,
            output_partition: DataPartition {
                kind: PartitionKind::Hash,
                exprs: vec![column_expr(10, "v", DataType::Int64)],
            },
            stream_kind: FragmentStreamKind::Partitioned,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: vec![10],
        }],
        Default::default(),
    );
    builder
        .seal()
        .map_err(|error| format!("native hash exchange fixture must seal: {error}"))
}

fn native_hash_aggregate_plan() -> Result<DistributedPlan, String> {
    let group = output_column(1, "group_key", DataType::Int64);
    let aggregate = DistributedNode {
        node_id: 7,
        fragment_id: 0,
        tuple_ids: vec![7],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![values_node(0, 8, vec![group.clone()])],
        stats: physical_stats(),
        payload: DistributedNodeKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Local,
            group_by: vec![TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(1),
                    qualifier: None,
                    column: "group_key".to_string(),
                },
                data_type: DataType::Int64,
                nullable: false,
            }],
            aggregates: Vec::new(),
            is_merge: Vec::new(),
            output_layout: AggregateOutputLayout::new(vec![group.clone()], Vec::new()),
            output_columns: vec![group.clone()],
            topn_runtime_filter_builds: Vec::new(),
        })),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root: aggregate,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: vec![group],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_reconciled_hash_join_plan() -> Result<DistributedPlan, String> {
    let left = output_column(1, "l_k", DataType::Int64);
    let right = output_column(2, "r_k", DataType::Int64);
    let root = DistributedNode {
        node_id: 1,
        fragment_id: 0,
        tuple_ids: vec![1],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![
            values_node(0, 2, vec![left.clone()]),
            values_node(0, 3, vec![right.clone()]),
        ],
        stats: physical_stats(),
        payload: DistributedNodeKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: Vec::new(),
            other_condition: None,
            distribution: JoinDistribution::Unknown,
            execution_mode: None,
            build_runtime_filters: Vec::new(),
            output_columns: vec![
                left.clone(),
                right.clone(),
                output_column(999, "stale", DataType::Int64),
            ],
        })),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: vec![left, right],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_change_stream_router_plan() -> Result<DistributedPlan, String> {
    let output_columns = vec![
        output_column(1, "__row_mutation_effect", DataType::Int8),
        output_column(3, "bucket", DataType::Int32),
    ];
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root: values_node(0, 10, output_columns.clone()),
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::ChangeStreamRouter(ChangeStreamRouterSink {
            group_id: 0,
            effect_output_ordinal: 0,
            routes: vec![ChangeStreamRoute {
                route_id: ConnectorWriteRouteId::from_bytes([7; 32]),
                cohort_id: ConnectorWriteCohortId::from_bytes([8; 32]),
                accepted_effects: vec![ConnectorRowMutationEffect::Delete],
                input_ordinals: vec![ConnectorMutationRouteInput::new(
                    ConnectorWriteFieldToken::from_bytes([9; 32]),
                    1,
                )],
                target_fragment_id: 1,
                target_exchange_node_id: 20,
                output_partition_ordinals: vec![1],
            }],
        }),
        output_exprs: None,
        output_columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_duplicate_project_plan(topn: bool) -> Result<DistributedPlan, String> {
    let child_columns = vec![
        output_column(1, "c1", DataType::Int64),
        output_column(2, "c2", DataType::Int64),
    ];
    let duplicate_output = vec![
        output_column(1, "c1", DataType::Int64),
        output_column(1, "c1", DataType::Int64),
    ];
    let project = DistributedNode {
        node_id: 30,
        fragment_id: 0,
        tuple_ids: vec![30],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![values_node(0, 29, child_columns)],
        stats: physical_stats(),
        payload: DistributedNodeKind::Project(PlanProjectNode {
            items: duplicate_output
                .iter()
                .map(|column| crate::analysis::ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: column.column_id,
                            qualifier: None,
                            column: column.name.clone(),
                        },
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                    },
                    output_name: column.name.clone(),
                    output_column_id: column.column_id,
                })
                .collect(),
            output_qualifier: None,
        }),
    };
    let root = if topn {
        DistributedNode {
            node_id: 32,
            fragment_id: 0,
            tuple_ids: vec![32],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: vec![project],
            stats: physical_stats(),
            payload: DistributedNodeKind::TopN(PhysicalTopNNode {
                items: Vec::new(),
                limit: Some(10),
                offset: None,
                phase: TopNPhase::Final,
                is_split: false,
            }),
        }
    } else {
        project
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: duplicate_output,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_nest_loop_join_plan() -> Result<DistributedPlan, String> {
    let left = output_column(1, "l_k", DataType::Int64);
    let right = output_column(2, "r_k", DataType::Int64);
    let root = DistributedNode {
        node_id: 41,
        fragment_id: 0,
        tuple_ids: vec![1, 2],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![
            values_node(0, 42, vec![left.clone()]),
            values_node(0, 43, vec![right.clone()]),
        ],
        stats: physical_stats(),
        payload: DistributedNodeKind::NestLoopJoin(PhysicalNestLoopJoinNode {
            join_type: JoinKind::Inner,
            condition: None,
            output_columns: vec![left.clone(), right.clone()],
        }),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: vec![left, right],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_assert_one_row_plan() -> Result<DistributedPlan, String> {
    let column = output_column(1, "only_row", DataType::Int64);
    let root = DistributedNode {
        node_id: 42,
        fragment_id: 0,
        tuple_ids: vec![1],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![values_node(0, 43, vec![column.clone()])],
        stats: physical_stats(),
        payload: DistributedNodeKind::AssertOneRow(PlanAssertOneRowNode::global_at_most_one(
            "select 1",
        )),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: vec![column],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_sort_plan() -> Result<DistributedPlan, String> {
    let columns = vec![
        output_column(4, "l_shipdate", DataType::Date32),
        output_column(1, "l_orderkey", DataType::Int64),
    ];
    let root = DistributedNode {
        node_id: 42,
        fragment_id: 0,
        tuple_ids: vec![1],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![values_node(0, 41, columns.clone())],
        stats: physical_stats(),
        payload: DistributedNodeKind::Sort(PlanSortNode {
            items: Vec::new(),
            analytic_partition_by: Vec::new(),
            output_columns: columns.clone(),
            offset: None,
            partition_limit: None,
            topn_type: None,
        }),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_delta_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Delta {
            from_snapshot_id: 1,
            to_snapshot_id: 2,
        },
        vec![
            column_def("order_id", DataType::Int64, false),
            column_def("stale_unprojected", DataType::Utf8, true),
        ],
        vec![output_column(1, "order_id", DataType::Int64)],
        None,
        Vec::new(),
    )
}

fn native_ordinary_iceberg_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        vec![
            column_def("order_id", DataType::Int64, false),
            column_def("unprojected_payload", DataType::Utf8, true),
        ],
        vec![output_column(1, "order_id", DataType::Int64)],
        Some(vec!["order_id".to_string()]),
        Vec::new(),
    )
}

fn native_refresh_scan_plan(source: SqlScanKind) -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        source,
        vec![
            column_def("stale", DataType::Utf8, true),
            column_def("stale_unprojected", DataType::Utf8, true),
        ],
        vec![
            output_column(1, "stale", DataType::Utf8),
            output_column(2, "stale_meta", DataType::Int64),
        ],
        None,
        Vec::new(),
    )
}

fn native_basic_scan_plan(source: SqlScanKind) -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        source,
        vec![column_def("order_id", DataType::Int64, false)],
        vec![output_column(1, "order_id", DataType::Int64)],
        None,
        Vec::new(),
    )
}

fn native_variant_projection_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        vec![column_def("v", DataType::LargeBinary, false)],
        vec![
            output_column(1, "v", DataType::LargeBinary),
            OutputColumn {
                column_id: ColumnId(2),
                name: "__nr_var_v_0".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: true,
            },
        ],
        Some(vec!["__nr_var_v_0".to_string()]),
        vec![ScanVariantColumn {
            source_column_id: ColumnId(1),
            source_column: "v".to_string(),
            synthetic_column_id: ColumnId(2),
            synthetic_column: "__nr_var_v_0".to_string(),
            canonical_path: "$.a.b".to_string(),
            requested_type: DataType::Int64,
            strict: true,
        }],
    )
}

fn refresh_snapshot_source() -> SqlScanKind {
    SqlScanKind::Data {
        version: SqlTableVersionSelector::Snapshot(1),
    }
}

fn mv_target_locator_source(apply_key_column: &str) -> SqlScanKind {
    SqlScanKind::MvTargetLocator {
        facts: SqlMvTargetLocatorScan {
            target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
            target_snapshot_id: Some(1),
            apply_key_column: apply_key_column.to_string(),
            branch_id_column: None,
        },
    }
}

fn mv_target_state_source(row_id_column_name: &str) -> SqlScanKind {
    SqlScanKind::MvTargetState {
        facts: SqlMvTargetStateScan {
            target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
            target_snapshot_id: Some(1),
            aggregate_state_layout_version: 1,
            columns: Vec::new(),
            group_key_names: vec![row_id_column_name.to_string()],
            aggregate_state_names: Vec::new(),
            physical_column_names: vec![row_id_column_name.to_string()],
            row_id_column_name: row_id_column_name.to_string(),
            row_filter: SqlMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: row_id_column_name.to_string(),
                branch_scope: None,
            },
            partition_constraint: SqlMvTargetStatePartitionConstraint::Unpartitioned,
        },
    }
}

fn native_scan_fixture_plan(
    source: SqlScanKind,
    table_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    output_columns: Vec<OutputColumn>,
    required_columns: Option<Vec<String>>,
    variant_columns: Vec<ScanVariantColumn>,
) -> Result<DistributedPlan, String> {
    let table = TableDef {
        name: "orders".to_string(),
        columns: table_columns,
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: test_sql_scan_source(source),
    };
    let root = DistributedNode {
        node_id: 10,
        fragment_id: 0,
        tuple_ids: vec![10],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: DistributedNodeKind::Scan(PlanScanNode {
            database: "db".to_string(),
            table,
            alias: None,
            columns: output_columns.clone(),
            predicates: Vec::new(),
            required_columns,
            variant_columns,
            mv_rewritten_from: None,
        }),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

/// A column reference suitable for native expression-encoding assertions.
pub fn column_expr(id: u32, name: &str, data_type: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId(id),
            qualifier: Some("t".to_string()),
            column: name.to_string(),
        },
        data_type,
        nullable: true,
    }
}

/// An integer literal suitable for native expression-encoding assertions.
pub fn int_expr(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(value)),
        data_type: DataType::Int64,
        nullable: false,
    }
}

/// The expression variants covered by the native protobuf encoder contract.
pub fn native_expression_variants() -> Vec<TypedExpr> {
    let column = column_expr(1, "c1", DataType::Int64);
    let literal = int_expr(2);
    let lambda_body = TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(TypedExpr {
                kind: ExprKind::LambdaParamRef {
                    name: "x".to_string(),
                    slot_id: 3,
                },
                data_type: DataType::Int64,
                nullable: true,
            }),
            op: BinOp::Add,
            right: Box::new(int_expr(1)),
        },
        data_type: DataType::Int64,
        nullable: true,
    };
    let sort_item = SortItem {
        expr: column.clone(),
        asc: false,
        nulls_first: true,
    };

    vec![
        column_expr(1, "c1", DataType::Int64),
        TypedExpr {
            kind: ExprKind::LambdaParamRef {
                name: "x".to_string(),
                slot_id: 3,
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        literal.clone(),
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(column.clone()),
                op: BinOp::Gt,
                right: Box::new(literal.clone()),
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::UnaryOp {
                op: UnOp::Not,
                expr: Box::new(bool_expr(true)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "abs".to_string(),
                args: vec![column.clone()],
                distinct: false,
                volatility: FunctionVolatility::Immutable,
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        lambda_expr(lambda_body.clone()),
        TypedExpr {
            kind: ExprKind::AggregateCall {
                name: "sum".to_string(),
                args: vec![column.clone()],
                distinct: true,
                order_by: vec![sort_item.clone()],
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(column.clone()),
                target: DataType::Float64,
            },
            data_type: DataType::Float64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(column.clone()),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(string_expr("x")),
                list: vec![string_expr("a"), string_expr("b")],
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Between {
                expr: Box::new(column.clone()),
                low: Box::new(int_expr(1)),
                high: Box::new(int_expr(9)),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Like {
                expr: Box::new(string_expr("abc")),
                pattern: Box::new(string_expr("a%")),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Case {
                operand: None,
                when_then: vec![(bool_expr(true), int_expr(1))],
                else_expr: Some(Box::new(int_expr(0))),
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::IsTruthValue {
                expr: Box::new(bool_expr(false)),
                value: false,
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Nested(Box::new(column.clone())),
            data_type: DataType::Int64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::WindowCall {
                name: "rank".to_string(),
                args: vec![],
                distinct: false,
                partition_by: vec![column],
                order_by: vec![sort_item],
                window_frame: Some(WindowFrame {
                    frame_type: WindowFrameType::Rows,
                    start: WindowBound::UnboundedPreceding,
                    end: WindowBound::CurrentRow,
                }),
                ignore_nulls: false,
            },
            data_type: DataType::Int64,
            nullable: false,
        },
    ]
}

/// A lambda expression with its private parameter vocabulary already sealed in
/// the returned public expression carrier.
pub fn native_lambda_expression() -> TypedExpr {
    lambda_expr(int_expr(1))
}

/// An immutable scalar call representative of the function-call wire arm.
pub fn native_immutable_function_expression() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "abs".to_string(),
            args: vec![column_expr(1, "c1", DataType::Int64)],
            distinct: false,
            volatility: FunctionVolatility::Immutable,
        },
        data_type: DataType::Int64,
        nullable: true,
    }
}

/// A deliberately unsupported placeholder used to assert the encoder's
/// fail-fast branch without exposing the private subquery-kind enum.
pub fn subquery_placeholder_expr(id: u32, data_type: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::SubqueryPlaceholder {
            id: id as usize,
            kind: SubqueryKind::Scalar,
            data_type: data_type.clone(),
        },
        data_type,
        nullable: true,
    }
}

fn literal_expr(value: LiteralValue, data_type: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(value),
        data_type,
        nullable: false,
    }
}

fn bool_expr(value: bool) -> TypedExpr {
    literal_expr(LiteralValue::Bool(value), DataType::Boolean)
}

fn string_expr(value: &str) -> TypedExpr {
    literal_expr(LiteralValue::String(value.to_string()), DataType::Utf8)
}

fn lambda_expr(body: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::LambdaFunction {
            params: vec![LambdaParam {
                name: "x".to_string(),
                slot_id: 3,
                data_type: DataType::Int64,
                nullable: true,
            }],
            body: Box::new(body),
        },
        data_type: DataType::Int64,
        nullable: true,
    }
}

fn output_column(id: u32, name: &str, data_type: DataType) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId(id),
        name: name.to_string(),
        data_type,
        nullable: false,
        is_internal: false,
    }
}

fn column_def(
    name: &str,
    data_type: DataType,
    nullable: bool,
) -> novarocks_catalog::schema::ColumnDef {
    novarocks_catalog::schema::ColumnDef {
        name: name.to_string(),
        data_type,
        nullable,
        write_default: None,
        logical_type: None,
    }
}

fn physical_stats() -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: 0.0,
        row_count_confidence: PlannerConfidence::Fallback,
        column_statistics: Default::default(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn values_node(fragment_id: u32, node_id: i32, columns: Vec<OutputColumn>) -> DistributedNode {
    DistributedNode {
        node_id,
        fragment_id,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: DistributedNodeKind::Values(PlanValuesNode {
            rows: Vec::new(),
            columns,
        }),
    }
}

fn seal_fixture_plan(fragments: Vec<PlanFragment>) -> Result<DistributedPlan, String> {
    let root_fragment_id = fragments
        .last()
        .map(|fragment| fragment.fragment_id)
        .ok_or_else(|| "native encoder fixture requires one fragment".to_string())?;
    crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        fragments,
        Some(root_fragment_id),
        Vec::new(),
        Default::default(),
    )
    .seal()
    .map_err(|error| format!("native encoder fixture must seal: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{NativeEncoderPlanFixture, native_encoder_plan};
    use super::{NativeScanFixture, native_scan_plan};

    #[test]
    fn native_encoder_fixtures_return_complete_sealed_plans() {
        for fixture in [
            NativeEncoderPlanFixture::Minimal,
            NativeEncoderPlanFixture::HashExchange,
            NativeEncoderPlanFixture::HashAggregate,
            NativeEncoderPlanFixture::ReconciledHashJoin,
            NativeEncoderPlanFixture::ChangeStreamRouter,
            NativeEncoderPlanFixture::DuplicateProject,
            NativeEncoderPlanFixture::TopNDuplicateProject,
            NativeEncoderPlanFixture::NestLoopJoin,
            NativeEncoderPlanFixture::AssertOneRow,
            NativeEncoderPlanFixture::Sort,
        ] {
            let plan = native_encoder_plan(fixture).expect("fixture must seal");
            assert!(
                plan.fragments()
                    .iter()
                    .any(|fragment| fragment.fragment_id == plan.root_fragment_id())
            );
        }
        let scan = native_scan_plan(NativeScanFixture::DeltaWithStaleUnprojectedColumn)
            .expect("scan fixture must seal");
        assert_eq!(scan.fragments().len(), 1);
    }
}
