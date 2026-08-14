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
    ChangeStreamRoute, ChangeStreamRouterSink, ChangeStreamWriteDagSpec,
    SqlChangeStreamWriteTopology, SqlChangeStreamWriterRoute, route_output_ordinals,
};
use super::contract::{ConnectorWriteInputBinding, SqlWritePlanInput};
use super::sink::{ConnectorWriteFragmentSink, ConnectorWritePlanInput};
use crate::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::planner::distributed::fragment::DistributedPlanDraft;
use crate::planner::distributed::{
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
pub(in crate::planner::distributed) struct PlannedSqlChangeStreamDistributedPlanDraft {
    distributed_plan: DistributedPlanDraft,
    topology: SqlChangeStreamWriteTopology,
}

/// Build a distributed plan whose terminal is a provider-neutral connector
/// writer. The supplied Arrow/output contract is the complete SQL-owned
/// boundary; provider payload is attached only after placement has frozen a
/// writer identity.
pub(crate) fn build_connector_write_distributed_plan(
    physical: &crate::planner::physical::PhysicalPlanNode,
    sink: ConnectorWritePlanInput,
) -> Result<DistributedPlan, String> {
    let draft = crate::planner::distributed::build::build_distributed_plan_draft(physical)?;
    let draft = with_connector_write_sink(draft, sink)?;
    crate::planner::distributed::seal::seal_draft(draft).map_err(|error| error.to_string())
}

/// Build a terminal writer from the compiler-owned write contract. This is
/// the SQLX-2 path: table metadata and provider-specific options are resolved
/// by the application from `contract.target.binding` only after the
/// distributed plan is sealed.
pub(crate) fn build_sql_write_distributed_plan(
    physical: &crate::planner::physical::PhysicalPlanNode,
    sink: SqlWritePlanInput,
) -> Result<DistributedPlan, String> {
    let draft = crate::planner::distributed::build::build_distributed_plan_draft(physical)?;
    let draft = with_sql_write_sink(draft, sink)?;
    crate::planner::distributed::seal::seal_draft(draft).map_err(|error| error.to_string())
}

/// Attach a compiler-owned write contract to an unsealed physical plan.  This
/// is deliberately the same generic connector terminal used after native
/// writer placement: SQL does not construct, inspect, or serialize a
/// provider-specific sink here.
pub(in crate::planner::distributed) fn with_sql_write_sink(
    plan: DistributedPlanDraft,
    sink: SqlWritePlanInput,
) -> Result<DistributedPlanDraft, String> {
    with_connector_write_sink(
        plan,
        ConnectorWritePlanInput::from_sql_write_plan_input(sink),
    )
}

pub(crate) fn build_sql_change_stream_distributed_plan(
    physical: &crate::planner::physical::PhysicalPlanNode,
    dag: ChangeStreamWriteDagSpec,
) -> Result<PlannedSqlChangeStreamDistributedPlan, String> {
    let draft = crate::planner::distributed::build::build_distributed_plan_draft(physical)?;
    let planned = with_sql_change_stream_write(draft, dag)?;
    let distributed_plan = crate::planner::distributed::seal::seal_draft(planned.distributed_plan)
        .map_err(|error| error.to_string())?;
    Ok(PlannedSqlChangeStreamDistributedPlan {
        distributed_plan,
        topology: planned.topology,
    })
}

pub(in crate::planner::distributed) fn with_connector_write_sink(
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
        crate::planner::distributed::output::finalize_connector_write_output(root, &sink)
            .map_err(|error| error.to_string())?;
    root.sink = DataSink::ConnectorWrite(ConnectorWriteFragmentSink {
        handle: None,
        input: sink.input,
        output_contract: Some(output_contract),
    });
    Ok(plan)
}

pub(in crate::planner::distributed) fn with_sql_change_stream_write(
    mut plan: DistributedPlanDraft,
    dag: ChangeStreamWriteDagSpec,
) -> Result<PlannedSqlChangeStreamDistributedPlanDraft, String> {
    dag.validate()?;
    if dag.routes.is_empty() {
        return Err("row-mutation router requires at least one route".to_string());
    }

    let root_fragment_id = plan
        .root_fragment_id
        .ok_or_else(|| "row-mutation write requires a draft root fragment id".to_string())?;
    let root_index = plan
        .fragments
        .iter()
        .position(|fragment| fragment.fragment_id == root_fragment_id)
        .ok_or_else(|| {
            format!("row-mutation write cannot find root fragment id={root_fragment_id}")
        })?;
    if !matches!(plan.fragments[root_index].sink, DataSink::Result) {
        return Err(format!(
            "row-mutation write expected root fragment id={} to use result sink",
            root_fragment_id
        ));
    }

    let source_fragment = plan.fragments[root_index].clone();
    validate_output_ordinal(
        &source_fragment.output_columns,
        dag.effect_output_ordinal,
        "effect",
    )?;

    let mut next_fragment_id = next_fragment_id(&plan);
    let mut next_node_id = next_node_id(&plan);
    let mut next_tuple_id = next_tuple_id(&plan);
    let mut routes = Vec::with_capacity(dag.routes.len());
    let mut writer_routes = Vec::with_capacity(dag.routes.len());
    let mut writer_fragments = Vec::with_capacity(dag.routes.len());
    let mut writer_edges = Vec::with_capacity(dag.routes.len());

    for route in dag.routes {
        let stream_output_ordinals = route_output_ordinals(&route);
        validate_output_ordinals(
            &source_fragment.output_columns,
            &stream_output_ordinals,
            "route input",
        )?;
        validate_output_ordinals(
            &source_fragment.output_columns,
            &route.output_partition_ordinals,
            "route partition",
        )?;

        let sink = route.sink;

        let writer_columns =
            output_columns_by_ordinals(&source_fragment.output_columns, &stream_output_ordinals)?;
        let output_slot_ids = output_slot_ids_for_ordinals(
            &source_fragment.output_columns,
            &stream_output_ordinals,
            "route input",
        )?;
        if !matches!(sink.input, ConnectorWriteInputBinding::RootOutputByOrdinal) {
            return Err("SQL change-stream writer requires root output input binding".to_string());
        }
        if writer_columns.len() != sink.contract.input_columns.len() {
            return Err(format!(
                "SQL row-mutation route output column count {} does not match write input column count {}",
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
            &route.output_partition_ordinals,
            "route partition",
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
        let output_contract = crate::planner::distributed::output::finalize_connector_write_output(
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
                route_id: route.route_id,
            },
            output_slot_ids,
        });

        routes.push(ChangeStreamRoute {
            route_id: route.route_id,
            cohort_id: route.cohort_id,
            accepted_effects: route.accepted_effects.clone(),
            input_ordinals: route.input_ordinals,
            target_fragment_id: writer_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition_ordinals: route.output_partition_ordinals,
        });
        writer_routes.push(SqlChangeStreamWriterRoute {
            route_id: route.route_id,
            cohort_id: route.cohort_id,
            accepted_effects: route.accepted_effects,
            writer_fragment_id,
            sink,
        });
    }

    plan.fragments[root_index].sink = DataSink::ChangeStreamRouter(ChangeStreamRouterSink {
        group_id: 0,
        effect_output_ordinal: dag.effect_output_ordinal,
        routes,
    });
    plan.fragments.extend(writer_fragments);
    plan.edges.extend(writer_edges);

    Ok(PlannedSqlChangeStreamDistributedPlanDraft {
        distributed_plan: plan,
        topology: SqlChangeStreamWriteTopology { writer_routes },
    })
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn finalize_sql_change_stream_test_plan(
    builder: crate::planner::distributed::test_support::DistributedPlanDraftBuilder,
    dag: ChangeStreamWriteDagSpec,
) -> Result<DistributedPlan, String> {
    let planned = with_sql_change_stream_write(builder.into_draft(), dag)?;
    crate::planner::distributed::seal::seal_draft(planned.distributed_plan)
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
    output_columns: &[crate::analysis::OutputColumn],
    ordinal: usize,
    label: &str,
) -> Result<(), String> {
    if output_columns.get(ordinal).is_none() {
        return Err(format!(
            "row-mutation {label} output ordinal {ordinal} is out of range"
        ));
    }
    Ok(())
}

fn validate_output_ordinals(
    output_columns: &[crate::analysis::OutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<(), String> {
    for ordinal in ordinals {
        validate_output_ordinal(output_columns, *ordinal, label)?;
    }
    Ok(())
}

fn output_columns_by_ordinals(
    output_columns: &[crate::analysis::OutputColumn],
    ordinals: &[usize],
) -> Result<Vec<crate::analysis::OutputColumn>, String> {
    ordinals
        .iter()
        .copied()
        .map(|ordinal| {
            output_columns.get(ordinal).cloned().ok_or_else(|| {
                format!("row-mutation route input ordinal {ordinal} is out of range")
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
                format!("row-mutation {label} output ordinal {ordinal} is out of range")
            })?;
            i32::try_from(column.column_id.0).map_err(|_| {
                format!(
                    "row-mutation {label} column id {} cannot be encoded as stream output slot id",
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
                format!("row-mutation {label} output ordinal {ordinal} is out of range")
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

    use super::super::change_stream::ChangeStreamWriteDagSpec;
    use super::super::contract::{ConnectorWriteInputBinding, test_support};
    use super::super::sink::ConnectorWritePlanInput;
    use crate::analysis::{ExprKind, OutputColumn};
    use crate::column_id::ColumnId;
    use crate::planner::distributed::test_support::DistributedPlanDraftBuilder;
    use crate::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
    };
    use crate::planner::physical::{PhysicalPlanStats, PlannerConfidence};

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
        let planned =
            crate::planner::distributed::seal::seal_draft(planned).expect("SQL write draft seals");
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
        let planned = crate::planner::distributed::seal::seal_draft(planned)
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

    fn test_route(
        route_byte: u8,
        effects: Vec<novarocks_spi::connector::ConnectorRowMutationEffect>,
        input_ordinal: u32,
        partition_ordinals: Vec<usize>,
    ) -> super::super::change_stream::ChangeStreamWriteRouteSpec {
        super::super::change_stream::ChangeStreamWriteRouteSpec {
            route_id: novarocks_spi::connector::ConnectorWriteRouteId::from_bytes([route_byte; 32]),
            cohort_id: novarocks_spi::connector::ConnectorWriteCohortId::from_bytes(
                [route_byte.wrapping_add(1); 32],
            ),
            accepted_effects: effects,
            input_ordinals: vec![novarocks_spi::connector::ConnectorMutationRouteInput::new(
                novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes(
                    [route_byte.wrapping_add(2); 32],
                ),
                input_ordinal,
            )],
            output_partition_ordinals: partition_ordinals,
            sink: test_support::simple_sql_write_plan_input(
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            ),
        }
    }

    #[test]
    fn row_mutation_expander_adds_opaque_routes_and_preserves_replace_fanout() {
        let plan = single_fragment_plan_for_test_with_columns(vec![
            ("__row_mutation_effect", DataType::Int8),
            ("delete_id", DataType::Int32),
            ("replacement", DataType::Int32),
        ]);
        let dag = ChangeStreamWriteDagSpec::for_test(
            0,
            vec![
                test_route(
                    7,
                    vec![
                        novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
                        novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
                    ],
                    1,
                    vec![1],
                ),
                test_route(
                    8,
                    vec![novarocks_spi::connector::ConnectorRowMutationEffect::Replace],
                    2,
                    Vec::new(),
                ),
            ],
        );

        let planned =
            with_sql_change_stream_write(plan.into_draft(), dag).expect("plan row mutation");
        let topology = planned.topology;
        let distributed_plan =
            crate::planner::distributed::seal::seal_draft(planned.distributed_plan)
                .expect("decorated row-mutation draft seals");

        let root = distributed_plan
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == distributed_plan.root_fragment_id())
            .expect("root fragment");
        let DataSink::ChangeStreamRouter(router) = &root.sink else {
            panic!("expected row-mutation router sink");
        };
        assert_eq!(router.effect_output_ordinal, 0);
        assert_eq!(router.routes.len(), 2);
        assert_eq!(
            router.routes[0].accepted_effects,
            vec![
                novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
                novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
            ]
        );
        assert_eq!(
            router.routes[1].accepted_effects,
            vec![novarocks_spi::connector::ConnectorRowMutationEffect::Replace]
        );
        assert_eq!(topology.writer_routes.len(), 2);
        assert_eq!(
            topology.writer_routes[0].route_id,
            router.routes[0].route_id
        );
    }

    #[test]
    fn row_mutation_expander_rejects_effect_ordinal_out_of_range() {
        let plan = single_fragment_plan_for_test();
        let dag = ChangeStreamWriteDagSpec::for_test(
            7,
            vec![test_route(
                7,
                vec![novarocks_spi::connector::ConnectorRowMutationEffect::Delete],
                0,
                Vec::new(),
            )],
        );

        let err = with_sql_change_stream_write(plan.into_draft(), dag)
            .expect_err("invalid effect ordinal");
        assert!(err.contains("effect output ordinal 7"), "{err}");
    }

    #[test]
    fn row_mutation_expander_rejects_route_output_slot_id_overflow() {
        let mut plan = single_fragment_plan_for_test();
        let overflow_column_id = ColumnId::new_for_test(i32::MAX as u32 + 1);
        plan.fragments_mut()[0].output_columns[0].column_id = overflow_column_id;
        let DistributedNodeKind::Values(values) = &mut plan.fragments_mut()[0].root.payload else {
            panic!("expected values root");
        };
        values.columns[0].column_id = overflow_column_id;
        let dag = ChangeStreamWriteDagSpec::for_test(
            0,
            vec![test_route(
                7,
                vec![novarocks_spi::connector::ConnectorRowMutationEffect::Delete],
                0,
                Vec::new(),
            )],
        );

        let err = with_sql_change_stream_write(plan.into_draft(), dag)
            .expect_err("route output slot id overflow");
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
                    payload: DistributedNodeKind::Values(crate::planner::payload::PlanValuesNode {
                        rows: vec![],
                        columns: output_columns.clone(),
                    }),
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
