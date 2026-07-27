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

fn duplicate_projection_fragment_for_test(sink: DataSink) -> PlanFragment {
    let child_columns = vec![
        output_column(1, "c1", DataType::Int64),
        output_column(2, "c2", DataType::Int64),
    ];
    let duplicate_output = vec![
        output_column(1, "c1", DataType::Int64),
        output_column(1, "c1", DataType::Int64),
    ];
    let root = DistributedNode {
        node_id: 30,
        fragment_id: 0,
        tuple_ids: vec![30],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![DistributedNode {
            node_id: 29,
            fragment_id: 0,
            tuple_ids: vec![29],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Values(crate::sql::planner::payload::PlanValuesNode {
                rows: Vec::new(),
                columns: child_columns,
            }),
        }],
        stats: stats(),
        payload: DistributedNodeKind::Project(crate::sql::planner::payload::PlanProjectNode {
            items: duplicate_output
                .iter()
                .map(|column| crate::sql::analysis::ProjectItem {
                    expr: crate::sql::analysis::TypedExpr {
                        kind: crate::sql::analysis::ExprKind::ColumnRef {
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
    PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink,
        output_exprs: None,
        output_columns: duplicate_output,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }
}

#[test]
fn result_fragment_output_columns_map_finalized_project_root_unique_ids() {
    // The encoder maps the fragment's finalized output columns from the
    // sealed contract: a `SELECT c1, c1` project root's repeated id is made
    // unique (1, then a synthetic 3) by planner finalization, and the encoder
    // emits that 1:1.
    let fragment = duplicate_projection_fragment_for_test(DataSink::Result);
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![fragment],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    };

    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");
    let fragment = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 0)
        .expect("fragment 0");
    assert_eq!(
        fragment
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "c1"), (3, "c1")]
    );
}

#[test]
fn topn_root_fragment_output_columns_map_finalized_child_unique_ids() {
    let mut fragment = duplicate_projection_fragment_for_test(DataSink::Result);
    let child = fragment.root;
    fragment.root = DistributedNode {
        node_id: 32,
        fragment_id: 0,
        tuple_ids: vec![32],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![child],
        stats: stats(),
        payload: DistributedNodeKind::TopN(crate::sql::planner::physical::PhysicalTopNNode {
            items: Vec::new(),
            limit: Some(10),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        }),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![fragment],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    };

    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");
    let fragment = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 0)
        .expect("fragment 0");
    assert_eq!(
        fragment
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![1, 3],
        "a TopN root forwards its child's finalized unique-id output"
    );
}

#[test]
fn encoder_maps_sealed_join_output_columns_from_the_node_output_contract() {
    let output_columns = vec![
        output_column(1, "l_k", DataType::Int64),
        output_column(2, "r_k", DataType::Int64),
    ];
    let child = |node_id: i32, column: OutputColumn| DistributedNode {
        node_id,
        fragment_id: 0,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: stats(),
        payload: DistributedNodeKind::Values(crate::sql::planner::payload::PlanValuesNode {
            rows: Vec::new(),
            columns: vec![column],
        }),
    };
    let join = DistributedNode {
        node_id: 40,
        fragment_id: 0,
        tuple_ids: vec![1, 2],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![
            child(41, output_column(1, "l_k", DataType::Int64)),
            child(42, output_column(2, "r_k", DataType::Int64)),
        ],
        stats: stats(),
        payload: DistributedNodeKind::HashJoin(Box::new(
            crate::sql::planner::physical::PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: Vec::new(),
                other_condition: None,
                distribution: JoinDistribution::Unknown,
                execution_mode: None,
                build_runtime_filters: Vec::new(),
                output_columns: output_columns.clone(),
            },
        )),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root: join,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: output_columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    };

    // The sealed node-output contract is the authoritative source of the
    // join's execution output.
    let sealed = plan
        .node_outputs()
        .output_for(0, 40)
        .expect("sealed join output");
    let sealed_columns: Vec<(u32, &str)> = sealed
        .columns
        .iter()
        .map(|column| (column.column_id.0, column.name.as_str()))
        .collect();
    assert_eq!(sealed_columns, vec![(1, "l_k"), (2, "r_k")]);

    // The encoder maps that sealed contract 1:1 onto the wire, never
    // re-deriving from the children or join type.
    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");
    let root = encoded.fragments[0].root.as_ref().expect("encoded root");
    let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref() else {
        panic!("expected physical join root");
    };
    assert_eq!(
        physical
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        sealed_columns
    );
}

#[test]
fn encoder_maps_sealed_nest_loop_join_output_columns_from_the_node_output_contract() {
    let output_columns = vec![
        output_column(1, "l_k", DataType::Int64),
        output_column(2, "r_k", DataType::Int64),
    ];
    let child = |node_id: i32, column: OutputColumn| DistributedNode {
        node_id,
        fragment_id: 0,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: stats(),
        payload: DistributedNodeKind::Values(crate::sql::planner::payload::PlanValuesNode {
            rows: Vec::new(),
            columns: vec![column],
        }),
    };
    let join = DistributedNode {
        node_id: 41,
        fragment_id: 0,
        tuple_ids: vec![1, 2],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![
            child(42, output_column(1, "l_k", DataType::Int64)),
            child(43, output_column(2, "r_k", DataType::Int64)),
        ],
        stats: stats(),
        payload: DistributedNodeKind::NestLoopJoin(
            crate::sql::planner::physical::PhysicalNestLoopJoinNode {
                join_type: JoinKind::Inner,
                condition: None,
                output_columns: output_columns.clone(),
            },
        ),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root: join,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: output_columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    };

    // The sealed node-output contract is the authoritative source of the
    // nest-loop join's execution output.
    let sealed = plan
        .node_outputs()
        .output_for(0, 41)
        .expect("sealed nest loop join output");
    let sealed_columns: Vec<(u32, &str)> = sealed
        .columns
        .iter()
        .map(|column| (column.column_id.0, column.name.as_str()))
        .collect();
    assert_eq!(sealed_columns, vec![(1, "l_k"), (2, "r_k")]);

    // The encoder maps that sealed contract 1:1 onto the wire, never
    // re-deriving from the children or join type.
    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");
    let root = encoded.fragments[0].root.as_ref().expect("encoded root");
    let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref() else {
        panic!("expected physical nest loop join root");
    };
    assert_eq!(
        physical
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        sealed_columns
    );
}

#[test]
fn assert_one_row_root_fragment_output_columns_follow_finalized_child_schema() {
    // An AssertOneRow passthrough root has no independent output: the planner
    // seal finalizes the fragment output from its child, and the encoder maps
    // that sealed contract 1:1 (no re-derivation from the encoded tree).
    let child_column = output_column(1, "only_row", DataType::Int64);
    let node = DistributedNode {
        node_id: 42,
        fragment_id: 0,
        tuple_ids: vec![1],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![DistributedNode {
            node_id: 43,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Values(crate::sql::planner::payload::PlanValuesNode {
                rows: Vec::new(),
                columns: vec![child_column.clone()],
            }),
        }],
        stats: stats(),
        payload: DistributedNodeKind::AssertOneRow(
            crate::sql::planner::payload::PlanAssertOneRowNode::global_at_most_one("select 1"),
        ),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root: node,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: vec![child_column],
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    };

    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");
    let fragment = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 0)
        .expect("fragment 0");
    assert_eq!(
        fragment
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "only_row")]
    );
}

#[test]
fn sort_root_fragment_output_columns_follow_finalized_child_schema() {
    // A Sort passthrough root forwards its child's finalized execution
    // output. The planner seal owns that finalization (there is no stale
    // physical output to "repair" anymore), and the encoder maps the sealed
    // fragment output 1:1 rather than re-walking the encoded tree.
    let child_columns = vec![
        output_column(4, "l_shipdate", DataType::Date32),
        output_column(1, "l_orderkey", DataType::Int64),
    ];
    let sort = DistributedNode {
        node_id: 42,
        fragment_id: 0,
        tuple_ids: vec![1],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![DistributedNode {
            node_id: 41,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Values(crate::sql::planner::payload::PlanValuesNode {
                rows: Vec::new(),
                columns: child_columns.clone(),
            }),
        }],
        stats: stats(),
        payload: DistributedNodeKind::Sort(crate::sql::planner::payload::PlanSortNode {
            items: Vec::new(),
            analytic_partition_by: Vec::new(),
            output_columns: child_columns.clone(),
            offset: None,
            partition_limit: None,
            topn_type: None,
        }),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root: sort,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: child_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    };

    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");
    let fragment = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 0)
        .expect("fragment 0");
    assert_eq!(
        fragment
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![4, 1]
    );
}
