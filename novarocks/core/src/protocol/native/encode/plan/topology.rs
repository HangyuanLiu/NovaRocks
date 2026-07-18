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

pub(super) fn attach_stream_sinks(
    src: &DistributedPlan,
    fragments: &mut [plan::PlanFragment],
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<(), String> {
    let fragment_index_by_id = fragments
        .iter()
        .enumerate()
        .map(|(idx, fragment)| (fragment.fragment_id, idx))
        .collect::<HashMap<_, _>>();

    for edge in src.edges() {
        if !matches!(edge.edge_kind, FragmentEdgeKind::Stream) {
            continue;
        }
        let idx = *fragment_index_by_id
            .get(&edge.source_fragment_id)
            .ok_or_else(|| {
                format!(
                    "native stream edge source fragment {} missing encoded fragment",
                    edge.source_fragment_id
                )
            })?;
        // The planner finalized this edge's projection at seal. The encoder maps
        // the sender's `DataStreamSink.output_columns` (source slot ids) from it
        // 1:1; the destination receiver's `output_columns` are set to the same
        // projection during node encoding (`encode_exchange_receiver`), so the two
        // sides stay equal without an after-the-fact receiver patch.
        let stream_output_slot_ids = finalized_stream_edge_slot_ids(ctx, edge)?;
        let fragment = &mut fragments[idx];
        if !matches!(
            fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref()),
            Some(plan::data_sink::Kind::Noop(true))
        ) {
            return Err(format!(
                "native stream edge source fragment {} must have a NOOP sink before stream attachment",
                edge.source_fragment_id
            ));
        }
        fragment.sink = Some(plan::DataSink {
            kind: Some(plan::data_sink::Kind::DataStream(plan::DataStreamSink {
                dest_node_id: edge.target_exchange_node_id,
                output_partition: Some(encode_data_partition(&edge.output_partition)?),
                output_columns: stream_output_slot_ids,
                limit: None,
            })),
        });
    }
    Ok(())
}

/// The finalized stream-edge projection for `edge`, as planner output columns
/// read from the sealed fragment/edge contract.
fn finalized_stream_edge_projection<'a>(
    ctx: &'a NativePlanEncodeContext<'_>,
    edge: &FragmentEdge,
) -> Result<&'a [AnalysisOutputColumn], String> {
    let catalog = required_context_ref(ctx.fragment_edge_outputs, || {
        format!(
            "native stream edge from fragment {} to exchange node {} has no sealed projection contract",
            edge.source_fragment_id, edge.target_exchange_node_id
        )
    })?;
    catalog
        .stream_edge_projection(edge.target_fragment_id, edge.target_exchange_node_id)
        .ok_or_else(|| {
            format!(
                "native stream edge from fragment {} to exchange node {} is missing from the sealed projection contract",
                edge.source_fragment_id, edge.target_exchange_node_id
            )
        })
}

/// The sender-side slot ids (= projection column ids) for a stream edge.
fn finalized_stream_edge_slot_ids(
    ctx: &NativePlanEncodeContext<'_>,
    edge: &FragmentEdge,
) -> Result<Vec<i32>, String> {
    finalized_stream_edge_projection(ctx, edge)?
        .iter()
        .map(|column| {
            i32::try_from(column.column_id.0).map_err(|_| {
                format!(
                    "native stream edge output column {} cannot convert to slot id",
                    column.column_id.0
                )
            })
        })
        .collect()
}

pub(super) fn encode_plan_fragment_with_context(
    src: &PlanFragment,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::PlanFragment, String> {
    let root = encode_node_with_context(&src.root, ctx)?;
    let (output_exprs, output_columns) = output::encode_fragment_output_contract(src, ctx)?;
    Ok(plan::PlanFragment {
        fragment_id: src.fragment_id,
        root: Some(root),
        data_partition: Some(encode_data_partition(&src.data_partition)?),
        output_partition: Some(encode_data_partition(&src.output_partition)?),
        sink: Some(encode_data_sink(&src.sink, src.fragment_id, ctx)?),
        output_exprs,
        output_columns,
        cte_id: src.cte_id,
        cte_exchange_nodes: src
            .cte_exchange_nodes
            .iter()
            .map(|(cte_id, node_id, column_ids)| plan::CteExchangeBinding {
                cte_id: *cte_id,
                node_id: *node_id,
                column_ids: column_ids.iter().map(|id| id.0).collect(),
            })
            .collect(),
        runtime_filter_bindings: Some({
            let prepared = required_context_ref(ctx.runtime_filter_bindings, || {
                format!(
                    "native fragment {} encoding requires prepared runtime filter binding tables",
                    src.fragment_id
                )
            })?;
            let prepared_fragment = prepared.fragment(src.fragment_id).ok_or_else(|| {
                format!(
                    "prepared fragment {} missing while encoding runtime filter bindings",
                    src.fragment_id
                )
            })?;
            encode_runtime_filter_binding_table(
                src.fragment_id,
                prepared_fragment.runtime_filter_bindings(),
            )?
        }),
    })
}

pub(super) fn encode_data_partition(src: &DataPartition) -> Result<plan::DataPartition, String> {
    Ok(plan::DataPartition {
        kind: match src.kind {
            PartitionKind::Unpartitioned => plan::PartitionKind::Unpartitioned as i32,
            PartitionKind::Random => plan::PartitionKind::Random as i32,
            PartitionKind::Hash => plan::PartitionKind::Hash as i32,
        },
        exprs: encode_exprs(&src.exprs)?,
    })
}

fn encode_data_sink(
    src: &DataSink,
    fragment_id: crate::sql::planner::distributed::FragmentId,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::DataSink, String> {
    use plan::data_sink::Kind;

    Ok(plan::DataSink {
        kind: Some(match src {
            DataSink::Result => Kind::Result(true),
            DataSink::Noop => Kind::Noop(true),
            DataSink::IcebergWrite(sink) => Kind::IcebergWrite(plan::IcebergWriteFragmentSink {
                descriptor_database: sink.descriptor_database.clone(),
                spec: Some(encode_iceberg_write_sink_spec(&sink.spec, ctx)?),
                input: Some(encode_iceberg_write_input_binding(&sink.input)),
            }),
            DataSink::IcebergChangeStreamRouter(sink) => {
                Kind::IcebergChangeStreamRouter(plan::IcebergChangeStreamRouterSink {
                    group_id: sink.group_id,
                    change_op_output_ordinal: usize_to_u64(sink.change_op_output_ordinal),
                    data_route_output_ordinal: sink.data_route_output_ordinal.map(usize_to_u64),
                    branches: sink
                        .branches
                        .iter()
                        .map(|branch| {
                            Ok(plan::IcebergChangeStreamBranchRoute {
                                branch_id: branch.branch_id,
                                branch_kind: encode_change_stream_branch_kind(branch.branch_kind),
                                target_fragment_id: branch.target_fragment_id,
                                target_exchange_node_id: branch.target_exchange_node_id,
                                output_ordinals: branch
                                    .output_ordinals
                                    .iter()
                                    .map(|value| usize_to_u64(*value))
                                    .collect(),
                                // The ordinals still travel on the wire 1:1, but
                                // the runtime consumes `output_partition` below;
                                // the encoder no longer reconstructs the partition
                                // expression from them (CGO-9C Task 3).
                                output_partition_ordinals: branch
                                    .output_partition_ordinals
                                    .iter()
                                    .map(|value| usize_to_u64(*value))
                                    .collect(),
                                output_partition: Some(encode_finalized_router_branch_partition(
                                    ctx,
                                    fragment_id,
                                    branch.branch_id,
                                )?),
                                destinations: None,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                })
            }
        }),
    })
}

/// Map a change-stream router branch's finalized partition from the sealed write
/// contract (CGO-9C Task 3). The planner already reconstructed the partition
/// expression from the branch's ordinals against the router fragment's output
/// columns at seal; the encoder maps the typed result 1:1.
fn encode_finalized_router_branch_partition(
    ctx: &NativePlanEncodeContext<'_>,
    fragment_id: crate::sql::planner::distributed::FragmentId,
    branch_id: i32,
) -> Result<plan::DataPartition, String> {
    let partition = required_context_ref(ctx.write_contracts, || {
            format!("native change-stream router fragment {fragment_id} has no sealed write contract")
        })?
        .router_branch_partition(fragment_id, branch_id)
        .ok_or_else(|| {
            format!(
                "native change-stream router fragment {fragment_id} branch {branch_id} is missing from the sealed write contract"
            )
        })?;
    encode_data_partition(partition)
}

pub(super) fn encode_fragment_edge(src: &FragmentEdge) -> Result<plan::FragmentEdge, String> {
    Ok(plan::FragmentEdge {
        source_fragment_id: src.source_fragment_id,
        target_fragment_id: src.target_fragment_id,
        target_exchange_node_id: src.target_exchange_node_id,
        output_partition: encode_edge_partition_type(&src.output_partition),
        stream_kind: match src.stream_kind {
            FragmentStreamKind::Gather => plan::FragmentStreamKind::Gather as i32,
            FragmentStreamKind::Broadcast => plan::FragmentStreamKind::Broadcast as i32,
            FragmentStreamKind::Partitioned => plan::FragmentStreamKind::Partitioned as i32,
            FragmentStreamKind::Other => plan::FragmentStreamKind::Other as i32,
        },
        edge_kind: Some(encode_fragment_edge_kind(&src.edge_kind)),
        output_slot_ids: src.output_slot_ids.clone(),
    })
}

fn encode_fragment_edge_kind(src: &FragmentEdgeKind) -> plan::FragmentEdgeKind {
    use plan::fragment_edge_kind::Kind;

    plan::FragmentEdgeKind {
        kind: Some(match src {
            FragmentEdgeKind::Stream => Kind::Stream(true),
            FragmentEdgeKind::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            } => Kind::CteMulticast(plan::CteMulticastEdge {
                cte_id: *cte_id,
                receive_producer_column_ids: receive_producer_column_ids
                    .iter()
                    .map(|id| id.0)
                    .collect(),
            }),
            FragmentEdgeKind::IcebergChangeStreamRouter {
                router_group_id,
                branch_id,
                branch_kind,
            } => Kind::IcebergChangeStreamRouter(plan::IcebergChangeStreamRouterEdge {
                router_group_id: *router_group_id,
                branch_id: *branch_id,
                branch_kind: encode_change_stream_branch_kind(*branch_kind),
            }),
        }),
    }
}
