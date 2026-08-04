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

use super::change_stream::{
    ChangeStreamBranchRoute, ChangeStreamRouterSink, ChangeStreamWriteDagSpec,
    SqlChangeStreamWriteTopology, SqlChangeStreamWriterBranch,
};
use super::contract::{ConnectorWriteInputBinding, SqlWritePlanInput};
use super::sink::{ConnectorWriteFragmentSink, ConnectorWritePlanInput};
use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::sql::planner::distributed::fragment::DistributedPlanDraft;
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan, ExchangeFlavor,
    ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind,
    PartitionKind, PlanFragment,
};

#[derive(Clone, Debug)]
pub(crate) struct PlannedSqlChangeStreamDistributedPlan {
    pub(crate) distributed_plan: DistributedPlan,
    pub(crate) topology: SqlChangeStreamWriteTopology,
}

#[derive(Debug)]
pub(in crate::sql::planner::distributed) struct PlannedSqlChangeStreamDistributedPlanDraft {
    distributed_plan: DistributedPlanDraft,
    topology: SqlChangeStreamWriteTopology,
}

/// Build a distributed plan whose terminal is a provider-neutral connector
/// writer. The supplied Arrow/output contract is the complete SQL-owned
/// boundary; provider payload is attached only after placement has frozen a
/// writer identity.
pub(crate) fn build_connector_write_distributed_plan(
    physical: &crate::sql::planner::physical::PhysicalPlanNode,
    sink: ConnectorWritePlanInput,
) -> Result<DistributedPlan, String> {
    let draft = crate::sql::planner::distributed::build::build_distributed_plan_draft(physical)?;
    let draft = with_connector_write_sink(draft, sink)?;
    crate::sql::planner::distributed::seal::seal_draft(draft).map_err(|error| error.to_string())
}

/// Build a terminal writer from the compiler-owned write contract. This is
/// the SQLX-2 path: table metadata and provider-specific options are resolved
/// by the application from `contract.target.binding` only after the
/// distributed plan is sealed.
pub(crate) fn build_sql_write_distributed_plan(
    physical: &crate::sql::planner::physical::PhysicalPlanNode,
    sink: SqlWritePlanInput,
) -> Result<DistributedPlan, String> {
    let draft = crate::sql::planner::distributed::build::build_distributed_plan_draft(physical)?;
    let draft = with_sql_write_sink(draft, sink)?;
    crate::sql::planner::distributed::seal::seal_draft(draft).map_err(|error| error.to_string())
}

/// Attach a compiler-owned write contract to an unsealed physical plan.  This
/// is deliberately the same generic connector terminal used after native
/// writer placement: SQL does not construct, inspect, or serialize a
/// provider-specific sink here.
pub(in crate::sql::planner::distributed) fn with_sql_write_sink(
    plan: DistributedPlanDraft,
    sink: SqlWritePlanInput,
) -> Result<DistributedPlanDraft, String> {
    with_connector_write_sink(
        plan,
        ConnectorWritePlanInput::from_sql_write_plan_input(sink),
    )
}

pub(crate) fn build_sql_change_stream_distributed_plan(
    physical: &crate::sql::planner::physical::PhysicalPlanNode,
    dag: ChangeStreamWriteDagSpec,
) -> Result<PlannedSqlChangeStreamDistributedPlan, String> {
    let draft = crate::sql::planner::distributed::build::build_distributed_plan_draft(physical)?;
    let planned = with_sql_change_stream_write(draft, dag)?;
    let distributed_plan =
        crate::sql::planner::distributed::seal::seal_draft(planned.distributed_plan)
            .map_err(|error| error.to_string())?;
    Ok(PlannedSqlChangeStreamDistributedPlan {
        distributed_plan,
        topology: planned.topology,
    })
}

pub(in crate::sql::planner::distributed) fn with_connector_write_sink(
    mut plan: DistributedPlanDraft,
    sink: ConnectorWritePlanInput,
) -> Result<DistributedPlanDraft, String> {
    let root_fragment_id = plan
        .root_fragment_id
        .ok_or_else(|| "connector write sink requires a draft root fragment id".to_string())?;
    let root = plan
        .fragments
        .iter_mut()
        .find(|fragment| fragment.fragment_id == root_fragment_id)
        .ok_or_else(|| {
            format!("connector write sink cannot find root fragment id={root_fragment_id}")
        })?;
    if !matches!(root.sink, DataSink::Result) {
        return Err(format!(
            "connector write sink expected root fragment id={} to use result sink",
            root.fragment_id
        ));
    }
    let output_contract =
        crate::sql::planner::distributed::output::finalize_connector_write_output(root, &sink)
            .map_err(|error| error.to_string())?;
    root.sink = DataSink::ConnectorWrite(ConnectorWriteFragmentSink {
        handle: None,
        input: sink.input,
        output_contract: Some(output_contract),
    });
    Ok(plan)
}

pub(in crate::sql::planner::distributed) fn with_sql_change_stream_write(
    mut plan: DistributedPlanDraft,
    dag: ChangeStreamWriteDagSpec,
) -> Result<PlannedSqlChangeStreamDistributedPlanDraft, String> {
    dag.validate()?;
    if dag.branches.is_empty() {
        return Err("Iceberg change-stream write DAG requires at least one branch".to_string());
    }
    let change_op_output_ordinal = dag.change_op_output_ordinal.ok_or_else(|| {
        "Iceberg change-stream write DAG requires change_op_output_ordinal".to_string()
    })?;

    let root_fragment_id = plan.root_fragment_id.ok_or_else(|| {
        "Iceberg change-stream write requires a draft root fragment id".to_string()
    })?;
    let root_index = plan
        .fragments
        .iter()
        .position(|fragment| fragment.fragment_id == root_fragment_id)
        .ok_or_else(|| {
            format!("Iceberg change-stream write cannot find root fragment id={root_fragment_id}")
        })?;
    if !matches!(plan.fragments[root_index].sink, DataSink::Result) {
        return Err(format!(
            "Iceberg change-stream write expected root fragment id={} to use result sink",
            root_fragment_id
        ));
    }

    let source_fragment = plan.fragments[root_index].clone();
    validate_output_ordinal(
        &source_fragment.output_columns,
        change_op_output_ordinal,
        "change_op",
    )?;
    if let Some(data_route_ordinal) = dag.data_route_output_ordinal {
        validate_output_ordinal(
            &source_fragment.output_columns,
            data_route_ordinal,
            "data_route",
        )?;
    }

    let mut next_fragment_id = next_fragment_id(&plan);
    let mut next_node_id = next_node_id(&plan);
    let mut next_tuple_id = next_tuple_id(&plan);
    let mut routes = Vec::with_capacity(dag.branches.len());
    let mut writer_branches = Vec::with_capacity(dag.branches.len());
    let mut writer_fragments = Vec::with_capacity(dag.branches.len());
    let mut writer_edges = Vec::with_capacity(dag.branches.len());

    for branch in dag.branches {
        validate_output_ordinals(
            &source_fragment.output_columns,
            &branch.stream_output_ordinals,
            &format!("branch {:?} output", branch.branch_kind),
        )?;
        validate_output_ordinals(
            &source_fragment.output_columns,
            &branch.output_partition_ordinals,
            &format!("branch {:?} partition", branch.branch_kind),
        )?;

        let sink = branch.sink;

        let writer_columns = output_columns_by_ordinals(
            &source_fragment.output_columns,
            &branch.stream_output_ordinals,
        )?;
        let output_slot_ids = output_slot_ids_for_ordinals(
            &source_fragment.output_columns,
            &branch.stream_output_ordinals,
            &format!("branch {:?} output", branch.branch_kind),
        )?;
        if !matches!(sink.input, ConnectorWriteInputBinding::RootOutputByOrdinal) {
            return Err("SQL change-stream writer requires root output input binding".to_string());
        }
        if writer_columns.len() != sink.contract.input_columns.len() {
            return Err(format!(
                "SQL change-stream branch {:?} output column count {} does not match write input column count {}",
                branch.branch_kind,
                writer_columns.len(),
                sink.contract.input_columns.len()
            ));
        }

        let writer_fragment_id = next_fragment_id;
        next_fragment_id += 1;
        let exchange_node_id = next_node_id;
        next_node_id += 1;
        let exchange_tuple_id = next_tuple_id;
        next_tuple_id += 1;
        let output_partition = data_partition_for_ordinals(
            &source_fragment.output_columns,
            &branch.output_partition_ordinals,
            &format!("branch {:?} partition", branch.branch_kind),
        )?;
        let stream_kind = stream_kind_for_data_partition(&output_partition);

        let sink_template = ConnectorWritePlanInput::from_sql_write_plan_input(sink.clone());
        let mut writer_fragment = PlanFragment {
            fragment_id: writer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: writer_fragment_id,
                tuple_ids: vec![exchange_tuple_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: source_fragment.root.stats.clone(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: output_partition.clone(),
                    source_fragment_id: root_fragment_id,
                    output_columns: writer_columns.clone(),
                    output_qualifier: None,
                    flavor: ExchangeFlavor::Distribution,
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: writer_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let output_contract =
            crate::sql::planner::distributed::output::finalize_connector_write_output(
                &writer_fragment,
                &sink_template,
            )
            .map_err(|error| error.to_string())?;
        writer_fragment.sink = DataSink::ConnectorWrite(ConnectorWriteFragmentSink {
            handle: None,
            input: sink_template.input,
            output_contract: Some(output_contract),
        });
        writer_fragments.push(writer_fragment);

        writer_edges.push(FragmentEdge {
            source_fragment_id: root_fragment_id,
            target_fragment_id: writer_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition,
            stream_kind,
            edge_kind: FragmentEdgeKind::ChangeStreamRouter {
                router_group_id: 0,
                branch_id: branch.branch_id,
                branch_kind: branch.branch_kind,
            },
            output_slot_ids,
        });

        routes.push(ChangeStreamBranchRoute {
            branch_id: branch.branch_id,
            branch_kind: branch.branch_kind,
            target_fragment_id: writer_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_ordinals: branch.stream_output_ordinals,
            output_partition_ordinals: branch.output_partition_ordinals,
        });
        writer_branches.push(SqlChangeStreamWriterBranch {
            branch_id: branch.branch_id,
            branch_kind: branch.branch_kind,
            writer_fragment_id,
            sink,
        });
    }

    plan.fragments[root_index].sink = DataSink::ChangeStreamRouter(ChangeStreamRouterSink {
        group_id: 0,
        change_op_output_ordinal,
        data_route_output_ordinal: dag.data_route_output_ordinal,
        branches: routes,
    });
    plan.fragments.extend(writer_fragments);
    plan.edges.extend(writer_edges);

    Ok(PlannedSqlChangeStreamDistributedPlanDraft {
        distributed_plan: plan,
        topology: SqlChangeStreamWriteTopology { writer_branches },
    })
}

#[cfg(test)]
pub(crate) fn finalize_sql_change_stream_test_plan(
    builder: crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder,
    dag: ChangeStreamWriteDagSpec,
) -> Result<DistributedPlan, String> {
    let planned = with_sql_change_stream_write(builder.into_draft(), dag)?;
    crate::sql::planner::distributed::seal::seal_draft(planned.distributed_plan)
        .map_err(|error| error.to_string())
}

fn next_fragment_id(plan: &DistributedPlanDraft) -> FragmentId {
    plan.fragments
        .iter()
        .map(|fragment| fragment.fragment_id)
        .max()
        .unwrap_or_default()
        + 1
}

fn next_node_id(plan: &DistributedPlanDraft) -> i32 {
    plan.fragments
        .iter()
        .flat_map(|fragment| node_ids(&fragment.root))
        .max()
        .unwrap_or_default()
        + 1
}

fn next_tuple_id(plan: &DistributedPlanDraft) -> i32 {
    plan.fragments
        .iter()
        .flat_map(|fragment| node_tuple_ids(&fragment.root))
        .max()
        .unwrap_or_default()
        + 1
}

fn node_ids(node: &DistributedNode) -> Vec<i32> {
    let mut ids = vec![node.node_id];
    for child in &node.children {
        ids.extend(node_ids(child));
    }
    ids
}

fn node_tuple_ids(node: &DistributedNode) -> Vec<i32> {
    let mut ids = node.tuple_ids.clone();
    ids.extend_from_slice(&node.nullable_tuple_ids);
    for child in &node.children {
        ids.extend(node_tuple_ids(child));
    }
    ids
}

fn validate_output_ordinal(
    output_columns: &[crate::sql::analysis::OutputColumn],
    ordinal: usize,
    label: &str,
) -> Result<(), String> {
    if output_columns.get(ordinal).is_none() {
        return Err(format!(
            "Iceberg change-stream {label} output ordinal {ordinal} is out of range"
        ));
    }
    Ok(())
}

fn validate_output_ordinals(
    output_columns: &[crate::sql::analysis::OutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<(), String> {
    for ordinal in ordinals {
        validate_output_ordinal(output_columns, *ordinal, label)?;
    }
    Ok(())
}

fn output_columns_by_ordinals(
    output_columns: &[crate::sql::analysis::OutputColumn],
    ordinals: &[usize],
) -> Result<Vec<crate::sql::analysis::OutputColumn>, String> {
    ordinals
        .iter()
        .copied()
        .map(|ordinal| {
            output_columns.get(ordinal).cloned().ok_or_else(|| {
                format!("Iceberg change-stream branch output ordinal {ordinal} is out of range")
            })
        })
        .collect()
}

fn output_slot_ids_for_ordinals(
    output_columns: &[OutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<Vec<i32>, String> {
    ordinals
        .iter()
        .copied()
        .map(|ordinal| {
            let column = output_columns.get(ordinal).ok_or_else(|| {
                format!("Iceberg change-stream {label} output ordinal {ordinal} is out of range")
            })?;
            i32::try_from(column.column_id.0).map_err(|_| {
                format!(
                    "Iceberg change-stream {label} column id {} cannot be encoded as stream output slot id",
                    column.column_id
                )
            })
        })
        .collect()
}

fn data_partition_for_ordinals(
    output_columns: &[OutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<DataPartition, String> {
    if ordinals.is_empty() {
        return Ok(DataPartition::unpartitioned());
    }

    let exprs = ordinals
        .iter()
        .copied()
        .map(|ordinal| {
            let column = output_columns.get(ordinal).ok_or_else(|| {
                format!("Iceberg change-stream {label} output ordinal {ordinal} is out of range")
            })?;
            Ok(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: column.column_id,
                    qualifier: None,
                    column: column.name.clone(),
                },
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(DataPartition::hash(exprs))
}

fn stream_kind_for_data_partition(partition: &DataPartition) -> FragmentStreamKind {
    match partition.kind {
        PartitionKind::Unpartitioned => FragmentStreamKind::Gather,
        PartitionKind::Random => FragmentStreamKind::Other,
        PartitionKind::Hash => FragmentStreamKind::Partitioned,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::change_stream::{ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec};
    use super::super::contract::{ConnectorWriteInputBinding, test_support};
    use super::super::sink::ConnectorWritePlanInput;
    use crate::sql::analysis::{ExprKind, OutputColumn};
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::ChangeStreamBranchKind;
    use crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder;
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
    };
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    use super::{with_connector_write_sink, with_sql_change_stream_write, with_sql_write_sink};

    #[test]
    fn sqlx2_write_sink_uses_only_the_sql_contract() {
        let plan = single_fragment_plan_for_test();
        let planned = with_sql_write_sink(
            plan.into_draft(),
            test_support::simple_sql_write_plan_input(
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            ),
        )
        .expect("attach SQL write sink");
        let planned = crate::sql::planner::distributed::seal::seal_draft(planned)
            .expect("SQL write draft seals");
        let root = planned
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == planned.root_fragment_id())
            .expect("root fragment");
        assert!(matches!(root.sink, DataSink::ConnectorWrite(_)));
    }

    #[test]
    fn sqlx2_write_sink_replaces_root_result_sink() {
        let plan = single_fragment_plan_for_test();
        let sink = test_support::simple_sql_write_plan_input(
            ConnectorWriteInputBinding::RootOutputByOrdinal,
        );

        let planned = with_sql_write_sink(plan.into_draft(), sink).expect("plan write sink");
        let planned = crate::sql::planner::distributed::seal::seal_draft(planned)
            .expect("decorated write draft seals");

        let root = planned
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == planned.root_fragment_id())
            .expect("root fragment");
        assert!(matches!(root.sink, DataSink::ConnectorWrite(_)));
    }

    #[test]
    fn sqlx2_write_sink_rejects_out_of_range_output_ordinal() {
        let plan = single_fragment_plan_for_test();
        let sink = test_support::simple_sql_write_plan_input(
            ConnectorWriteInputBinding::OutputOrdinals(vec![7]),
        );

        let err = with_sql_write_sink(plan.into_draft(), sink).expect_err("out-of-range ordinal");

        assert!(
            err.contains("sink input references output ordinal 7")
                && err.contains("output columns exist"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn generic_connector_sink_replaces_result_without_provider_input() {
        let plan = single_fragment_plan_for_test();
        let sink = ConnectorWritePlanInput::from_target_columns(
            &test_support::simple_sql_write_plan_input(
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            )
            .contract
            .input_columns,
            ConnectorWriteInputBinding::RootOutputByOrdinal,
            None,
        );

        let planned =
            with_connector_write_sink(plan.into_draft(), sink).expect("generic write sink");
        let planned = crate::sql::planner::distributed::seal::seal_draft(planned)
            .expect("generic write draft seals");
        let root = planned
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == planned.root_fragment_id())
            .expect("root fragment");
        assert!(matches!(root.sink, DataSink::ConnectorWrite(_)));
    }

    #[test]
    fn change_stream_expander_adds_router_and_writer_fragments() {
        let plan = single_fragment_plan_for_test_with_columns(vec![
            ("op", DataType::Int32),
            ("route", DataType::Int32),
            ("delete_id", DataType::Int32),
            ("reuse_id", DataType::Int32),
        ]);
        let mut delete_branch = ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![3, 2]);
        let repeated_column = delete_branch.sink.contract.input_columns[0].clone();
        delete_branch
            .sink
            .contract
            .input_columns
            .push(repeated_column);
        delete_branch.output_partition_ordinals = vec![1];
        let reuse_branch = ChangeStreamWriteBranchSpec::reuse_data_for_test(vec![3]);
        let dag =
            ChangeStreamWriteDagSpec::for_test(Some(0), Some(1), vec![delete_branch, reuse_branch]);

        let planned =
            with_sql_change_stream_write(plan.into_draft(), dag).expect("plan change stream");
        let topology = planned.topology;
        let distributed_plan =
            crate::sql::planner::distributed::seal::seal_draft(planned.distributed_plan)
                .expect("decorated change-stream draft seals");

        assert_eq!(distributed_plan.fragments().len(), 3);
        let root = distributed_plan
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == distributed_plan.root_fragment_id())
            .expect("root fragment");
        let DataSink::ChangeStreamRouter(router) = &root.sink else {
            panic!("expected router sink");
        };
        assert_eq!(router.group_id, 0);
        assert_eq!(router.change_op_output_ordinal, 0);
        assert_eq!(router.data_route_output_ordinal, Some(1));
        assert_eq!(router.branches.len(), 2);
        assert_eq!(router.branches[0].output_ordinals, vec![3, 2]);
        assert_eq!(router.branches[0].output_partition_ordinals, vec![1]);

        assert_eq!(distributed_plan.edges().len(), 2);
        let first_edge = &distributed_plan.edges()[0];
        assert_eq!(first_edge.source_fragment_id, 0);
        assert_eq!(first_edge.target_fragment_id, 1);
        assert_eq!(first_edge.output_slot_ids, vec![4, 3]);
        assert_eq!(distributed_plan.edges()[1].output_slot_ids, vec![4]);
        assert_eq!(
            first_edge.stream_kind,
            crate::sql::planner::distributed::FragmentStreamKind::Partitioned
        );
        assert!(matches!(
            first_edge.output_partition.kind,
            crate::sql::planner::distributed::PartitionKind::Hash
        ));
        let [partition_expr] = first_edge.output_partition.exprs.as_slice() else {
            panic!("expected native hash partition expr");
        };
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &partition_expr.kind
        else {
            panic!("expected native partition expr to be a column ref");
        };
        assert_eq!(*column_id, ColumnId::new_for_test(2));
        assert_eq!(column, "route");
        assert!(matches!(
            first_edge.edge_kind,
            crate::sql::planner::distributed::FragmentEdgeKind::ChangeStreamRouter {
                branch_kind: ChangeStreamBranchKind::DeleteDv,
                ..
            }
        ));

        let writer = distributed_plan
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == first_edge.target_fragment_id)
            .expect("writer fragment");
        assert!(matches!(writer.sink, DataSink::ConnectorWrite(_)));
        assert_eq!(writer.output_columns.len(), 2);
        assert_eq!(writer.output_columns[0].name, "reuse_id");
        assert_eq!(writer.output_columns[1].name, "delete_id");
        assert_eq!(topology.writer_branches.len(), 2);
        assert_eq!(
            topology.writer_branches[0].sink.contract.target.binding,
            topology.writer_branches[1].sink.contract.target.binding
        );
        assert_eq!(
            topology.writer_branches[0].sink.contract.target.table.table,
            "orders"
        );
    }

    #[test]
    fn change_stream_expander_rejects_missing_change_op_ordinal() {
        let plan = single_fragment_plan_for_test();
        let dag = ChangeStreamWriteDagSpec::for_test(
            None,
            None,
            vec![ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![0])],
        );

        let err =
            with_sql_change_stream_write(plan.into_draft(), dag).expect_err("missing change_op");

        assert!(err.contains("requires change_op_output_ordinal"));
    }

    #[test]
    fn change_stream_expander_rejects_out_of_range_branch_output_ordinal() {
        let plan = single_fragment_plan_for_test();
        let dag = ChangeStreamWriteDagSpec::for_test(
            Some(0),
            None,
            vec![ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![7])],
        );

        let err = with_sql_change_stream_write(plan.into_draft(), dag)
            .expect_err("out-of-range branch output ordinal");

        assert!(
            err.contains("output ordinal 7 is out of range"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn change_stream_expander_rejects_branch_output_slot_id_overflow() {
        let mut plan = single_fragment_plan_for_test();
        let overflow_column_id = ColumnId::new_for_test(i32::MAX as u32 + 1);
        plan.fragments_mut()[0].output_columns[0].column_id = overflow_column_id;
        let DistributedNodeKind::Values(values) = &mut plan.fragments_mut()[0].root.payload else {
            panic!("expected values root");
        };
        values.columns[0].column_id = overflow_column_id;
        let dag = ChangeStreamWriteDagSpec::for_test(
            Some(0),
            None,
            vec![ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![0])],
        );

        let err = with_sql_change_stream_write(plan.into_draft(), dag)
            .expect_err("branch output slot id overflow");

        assert!(
            err.contains("column id c2147483648 cannot be encoded as stream output slot id"),
            "unexpected error: {err}"
        );
    }

    fn single_fragment_plan_for_test() -> DistributedPlanDraftBuilder {
        single_fragment_plan_for_test_with_columns(vec![("id", DataType::Int32)])
    }

    fn single_fragment_plan_for_test_with_columns(
        columns: Vec<(&str, DataType)>,
    ) -> DistributedPlanDraftBuilder {
        let output_columns = columns
            .into_iter()
            .enumerate()
            .map(|(idx, (name, data_type))| OutputColumn {
                column_id: ColumnId::new_for_test((idx + 1) as u32),
                name: name.to_string(),
                data_type,
                nullable: false,
                is_internal: false,
            })
            .collect::<Vec<_>>();
        DistributedPlanDraftBuilder::new(
            vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 10,
                    fragment_id: 0,
                    tuple_ids: vec![10],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    runtime_filter_binding_ids: Vec::new(),
                    children: vec![],
                    stats: stats(),
                    payload: DistributedNodeKind::Values(
                        crate::sql::planner::payload::PlanValuesNode {
                            rows: vec![],
                            columns: output_columns.clone(),
                        },
                    ),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            Some(0),
            Vec::new(),
            Default::default(),
        )
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }
}
