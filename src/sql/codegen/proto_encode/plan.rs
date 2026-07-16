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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[cfg(test)]
use std::collections::BTreeMap;

use arrow::datatypes::{DataType, Field};
use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::expr::{encode_expr, encode_sort_items, encode_window_frame};
use crate::coordinator::prepare::scan::{
    ResolvedScanBinding, ResolvedScanColumnKind, ResolvedScanExecution, ScanExecutionBindings,
};
use crate::proto::{common, plan};
use crate::sql::analysis::OutputColumn as AnalysisOutputColumn;
// Consumed only by `#[cfg(test)]` encoder fixtures (the production write/router
// encoding reads finalized planner types, not these analysis constructors).
use crate::catalog::schema::{ColumnDefault, SqlType, validate_column_default};
#[cfg(test)]
use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::codegen::scan::connector::{
    StarRocksColumnSchemaDescriptor, StarRocksKeysTypeDescriptor, StarRocksScanSourceDescriptor,
    StarRocksTabletSchemaDescriptor,
};
use crate::sql::common::{ChangeStreamBranchKind, JoinKind};
use crate::sql::planner::distributed::runtime_filter::{
    GraphRuntimeFilterBuild, GraphRuntimeFilterProbe, RuntimeFilterGraphProjection,
    project_runtime_filters,
};
use crate::sql::planner::distributed::write::sink::IcebergWriteInputBinding;
use crate::sql::planner::distributed::write::sink::{
    IcebergWriteFileCompression, IcebergWriteSinkMode, IcebergWriteSinkSpec,
};
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan, ExchangeFlavor,
    ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentEdgeOutputCatalog,
    FragmentStreamKind, NodeExecutionColumn, NodeOutputCatalog, PartitionKind, PlanFragment,
    WriteContractCatalog,
};
use crate::sql::planner::payload::PlanRowCountAssertion;
use crate::sql::planner::physical::{
    AggMode, HashSource, JoinDistribution, JoinExecutionMode, PhysicalPlanKind, PlanSetOpKind,
    RedistributeMode, TopNPhase,
};
use crate::sql::planner::table as table_model;
use crate::types::native_proto::encode_type;

pub(crate) struct NativePlanEncodeContext<'a> {
    pub(crate) scan_bindings: Option<&'a ScanExecutionBindings>,
    /// The sealed node-output contract. The encoder reads each covered node's
    /// (join / scan / set-op / sort) execution output from here rather than
    /// re-deriving or repairing it. `None` only in bare-node encoder unit tests
    /// that have no sealed plan; those rely on the payload columns encoded by
    /// `encode_physical_node`, which is the same data the catalog is built from.
    pub(crate) node_outputs: Option<&'a NodeOutputCatalog>,
    /// The sealed fragment-output / stream-edge-projection contract (CGO-9C
    /// Task 2). The encoder maps each fragment's finalized output columns and each
    /// stream edge's finalized sender/receiver projection from here instead of
    /// re-deriving a stream schema or patching the exchange receiver. `None` only
    /// in bare-node/bare-fragment encoder unit tests that have no sealed plan.
    pub(crate) fragment_edge_outputs: Option<&'a FragmentEdgeOutputCatalog>,
    /// The sealed Iceberg write output / change-stream router partition contract
    /// (CGO-9C Task 3). The encoder maps each Iceberg write fragment's finalized
    /// output expressions and target output schema, and each change-stream router
    /// branch's finalized partition, from here instead of synthesizing the write
    /// output or reconstructing a partition from `output_partition_ordinals`.
    /// `None` only in bare-node/bare-fragment encoder unit tests, which never
    /// encode a write or router fragment.
    pub(crate) write_contracts: Option<&'a WriteContractCatalog>,
    pub(crate) runtime_filter_projection: Option<&'a RuntimeFilterGraphProjection>,
}

#[cfg(test)]
pub(crate) fn encode_distributed_plan(
    src: &DistributedPlan,
) -> Result<plan::DistributedPlan, String> {
    encode_distributed_plan_with_context(
        src,
        NativePlanEncodeContext {
            scan_bindings: None,
            // Both bound from the sealed plan inside encode_distributed_plan_with_context.
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_projection: None,
        },
    )
}

pub(crate) fn encode_distributed_plan_with_context(
    src: &DistributedPlan,
    ctx: NativePlanEncodeContext<'_>,
) -> Result<plan::DistributedPlan, String> {
    // The sealed node-output contract of `src` is authoritative for every covered
    // node, so bind it here: all fragment/node encoding then reads each covered
    // node's execution output from it instead of re-deriving or repairing it,
    // regardless of how the incoming context was constructed.
    let owned_runtime_filter_projection;
    let runtime_filter_projection = match ctx.runtime_filter_projection {
        Some(projection) => projection,
        None => {
            owned_runtime_filter_projection = project_runtime_filters(src)?;
            &owned_runtime_filter_projection
        }
    };
    let ctx = NativePlanEncodeContext {
        scan_bindings: ctx.scan_bindings,
        node_outputs: Some(src.node_outputs()),
        fragment_edge_outputs: Some(src.fragment_edge_outputs()),
        write_contracts: Some(src.write_contracts()),
        runtime_filter_projection: Some(runtime_filter_projection),
    };
    let mut fragments = src
        .fragments()
        .iter()
        .map(|fragment| encode_plan_fragment_with_context(fragment, &ctx))
        .collect::<Result<Vec<_>, _>>()?;
    attach_stream_sinks(src, &mut fragments, &ctx)?;
    Ok(plan::DistributedPlan {
        fragments,
        root_fragment_id: src.root_fragment_id(),
        edges: src
            .edges()
            .iter()
            .map(encode_fragment_edge)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn attach_stream_sinks(
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
    let catalog = ctx.fragment_edge_outputs.ok_or_else(|| {
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

fn encode_plan_fragment_with_context(
    src: &PlanFragment,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::PlanFragment, String> {
    let root = encode_node_with_context(&src.root, ctx)?;
    let (output_exprs, output_columns) = encode_fragment_output_contract(src, ctx)?;
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
    })
}

fn encode_fragment_output_contract(
    src: &PlanFragment,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<(Vec<crate::proto::expr::Expr>, Vec<common::OutputColumn>), String> {
    if matches!(src.sink, DataSink::IcebergWrite(_)) {
        // The planner finalized the write output expressions and target output
        // schema at seal (CGO-9C Task 3). The encoder maps them 1:1 instead of
        // synthesizing the target schema or falling back to the fragment's output
        // columns / input binding.
        let contract = ctx
            .write_contracts
            .ok_or_else(|| {
                format!(
                    "native Iceberg write fragment {} has no sealed write contract",
                    src.fragment_id
                )
            })?
            .iceberg_write_output(src.fragment_id)
            .ok_or_else(|| {
                format!(
                    "native Iceberg write fragment {} is missing from the sealed write contract",
                    src.fragment_id
                )
            })?;
        let output_exprs = encode_exprs(&contract.output_exprs)?;
        let output_columns = contract
            .target_schema
            .iter()
            .map(|column| {
                Ok(common::OutputColumn {
                    column_id: column.column_id,
                    name: column.name.clone(),
                    r#type: Some(encode_type(&column.data_type)?),
                    nullable: column.nullable,
                    is_internal: column.is_internal,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        return Ok((output_exprs, output_columns));
    }

    let output_exprs = src
        .output_exprs
        .as_ref()
        .map(|exprs| encode_exprs(exprs))
        .transpose()?
        .unwrap_or_default();
    let output_columns = encode_finalized_fragment_output_columns(src, ctx)?;
    Ok((output_exprs, output_columns))
}

/// Map a fragment's finalized output columns from the sealed fragment/edge
/// contract. The planner already reconciled the fragment's declared output with
/// its root's execution output (unique wire ids for re-materialized projections,
/// producer fragments forwarding their root wholesale); the encoder maps the
/// result 1:1 instead of re-walking the encoded tree or falling back.
fn encode_finalized_fragment_output_columns(
    src: &PlanFragment,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<Vec<common::OutputColumn>, String> {
    let catalog = ctx.fragment_edge_outputs.ok_or_else(|| {
        format!(
            "native fragment {} has no sealed output contract",
            src.fragment_id
        )
    })?;
    let columns = catalog
        .fragment_output_columns(src.fragment_id)
        .ok_or_else(|| {
            format!(
                "native fragment {} is missing from the sealed output contract",
                src.fragment_id
            )
        })?;
    encode_output_columns(columns)
}

#[cfg(test)]
pub(crate) fn encode_node(src: &DistributedNode) -> Result<plan::DistributedNode, String> {
    encode_node_with_context(
        src,
        &NativePlanEncodeContext {
            scan_bindings: None,
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_projection: None,
        },
    )
}

pub(crate) fn encode_node_with_context(
    src: &DistributedNode,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::DistributedNode, String> {
    let children = src
        .children
        .iter()
        .map(|child| encode_node_with_context(child, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let payload = match &src.payload {
        DistributedNodeKind::Exchange(exchange) => {
            // A stream-edge receiver carries exactly the edge's finalized
            // projection (the planner's authoritative reconciliation of this
            // receiver against what its source fragment sends); the encoder maps
            // it 1:1 here instead of a later exchange-receiver patch. A receiver
            // that is not a finalized stream-edge target (a CTE-multicast or
            // change-stream-router receiver) keeps its declared columns.
            let output_columns = ctx
                .fragment_edge_outputs
                .and_then(|catalog| catalog.stream_edge_projection(src.fragment_id, src.node_id))
                .unwrap_or(&exchange.output_columns);
            plan::distributed_node::Payload::Exchange(encode_exchange_receiver(
                exchange,
                output_columns,
            )?)
        }
        other => {
            let physical = crate::sql::planner::distributed::distributed_kind_to_physical(other);
            plan::distributed_node::Payload::Physical(encode_physical_node(
                &physical,
                src.node_id,
                ctx,
            )?)
        }
    };
    let mut node = plan::DistributedNode {
        node_id: src.node_id,
        fragment_id: src.fragment_id,
        tuple_ids: src.tuple_ids.clone(),
        nullable_tuple_ids: src.nullable_tuple_ids.clone(),
        limit: src.limit,
        build_runtime_filters: ctx
            .runtime_filter_projection
            .map(|projection| projection.builds_for(src.fragment_id, src.node_id))
            .unwrap_or_default()
            .iter()
            .map(encode_graph_runtime_filter_build)
            .collect::<Result<Vec<_>, _>>()?,
        probe_runtime_filters: ctx
            .runtime_filter_projection
            .map(|projection| projection.probes_for(src.fragment_id, src.node_id))
            .unwrap_or_default()
            .iter()
            .map(encode_graph_runtime_filter_probe)
            .collect::<Result<Vec<_>, _>>()?,
        children,
        payload: Some(payload),
    };
    apply_sealed_node_output_columns(&mut node, src, ctx)?;
    Ok(node)
}

/// Bind the encoded node's execution output columns from the sealed node-output
/// contract for the covered kinds (join / scan / set-op / sort / hash-aggregate).
/// The planner has already finalized and validated those outputs at seal time, so
/// the encoder maps them 1:1 here rather than re-deriving or repairing them.
///
/// A `HashAggregate` additionally carries a finalized group-key + aggregate-state
/// wire layout (with per-mode intermediate types applied); this maps that layout —
/// and the visible-or-full output columns — into the `HashAggregateNode` payload,
/// replacing the raw baseline `encode_physical_node` produced. The intermediate
/// aggregate-state type determination lives entirely in the planner.
///
/// `ctx.node_outputs` is `None` only in the bare-node encoder unit tests, which
/// have no sealed plan; there the payload columns encoded by `encode_physical_node`
/// already stand (the same data the catalog is built from).
fn apply_sealed_node_output_columns(
    node: &mut plan::DistributedNode,
    src: &DistributedNode,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<(), String> {
    let Some(catalog) = ctx.node_outputs else {
        return Ok(());
    };
    let Some(output) = catalog.output_for(src.fragment_id, src.node_id) else {
        // Not a covered kind; its output columns come straight from encoding.
        return Ok(());
    };
    let output_columns = output
        .columns
        .iter()
        .map(encode_node_execution_column)
        .collect::<Result<Vec<_>, _>>()?;
    let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut() else {
        return Err(format!(
            "native node {} carries a sealed execution output but is not a physical payload",
            src.node_id
        ));
    };
    physical.output_columns = output_columns;

    // A HashAggregate maps its finalized wire layout (and visible output columns)
    // 1:1 from the contract, overriding the raw baseline.
    if matches!(physical.kind, Some(plan::plan_node::Kind::HashAggregate(_))) {
        let layout = catalog
            .aggregate_layout(src.fragment_id, src.node_id)
            .ok_or_else(|| {
                format!(
                    "native HashAggregate node {} has a covered execution output but no sealed wire layout",
                    src.node_id
                )
            })?;
        let group_key_columns = encode_output_columns(&layout.group_key_columns)?;
        let aggregate_columns = encode_output_columns(&layout.aggregate_columns)?;
        let visible_output_columns = physical.output_columns.clone();
        let Some(plan::plan_node::Kind::HashAggregate(aggregate)) = physical.kind.as_mut() else {
            unreachable!("physical.kind was just matched as HashAggregate");
        };
        aggregate.output_layout = Some(plan::AggregateOutputLayout {
            group_key_columns,
            aggregate_columns,
        });
        aggregate.output_columns = visible_output_columns;
    }
    Ok(())
}

fn encode_node_execution_column(
    column: &NodeExecutionColumn,
) -> Result<common::OutputColumn, String> {
    Ok(common::OutputColumn {
        column_id: column.column_id.0,
        name: column.name.clone(),
        r#type: Some(encode_type(&column.data_type)?),
        nullable: column.nullable,
        is_internal: column.is_internal,
    })
}

#[cfg(test)]
pub(super) fn encoded_physical_variant_names_for_test() -> &'static [&'static str] {
    &[
        "Scan",
        "Filter",
        "Project",
        "Sort",
        "Limit",
        "Values",
        "Repeat",
        "Window",
        "GenerateSeries",
        "TableFunction",
        "AssertOneRow",
        "TopN",
        "HashAggregate",
        "HashJoin",
        "NestLoopJoin",
        "SetOp",
        "ChangeEventExpand",
        "CTEAnchor",
        "CTEProduce",
        "CTEConsume",
        "Redistribute",
    ]
}

fn encode_physical_node(
    src: &PhysicalPlanKind,
    node_id: i32,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::PlanNode, String> {
    use plan::plan_node::Kind;

    let (output_columns, kind) = match src {
        PhysicalPlanKind::Scan(node) => (
            encode_output_columns(&node.columns)?,
            Kind::Scan(encode_scan_node(node, node_id, ctx)?),
        ),
        PhysicalPlanKind::Filter(node) => (
            Vec::new(),
            Kind::Filter(plan::FilterNode {
                predicate: Some(encode_expr(&node.predicate)?),
            }),
        ),
        PhysicalPlanKind::Project(node) => (
            Vec::new(),
            Kind::Project(plan::ProjectNode {
                items: node
                    .items
                    .iter()
                    .map(|item| {
                        Ok(plan::ProjectItem {
                            expr: Some(encode_expr(&item.expr)?),
                            output_name: item.output_name.clone(),
                            output_column_id: item.output_column_id.0,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_qualifier: node.output_qualifier.clone(),
            }),
        ),
        PhysicalPlanKind::Sort(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Sort(plan::SortNode {
                items: encode_sort_items(&node.items)?,
                analytic_partition_by: encode_exprs(&node.analytic_partition_by)?,
                output_columns: encode_output_columns(&node.output_columns)?,
                offset: node.offset,
                partition_limit: node.partition_limit.map(usize_to_u64),
                topn_type: node.topn_type.map(encode_sort_topn_type),
            }),
        ),
        PhysicalPlanKind::Limit(node) => (
            Vec::new(),
            Kind::Limit(plan::LimitNode {
                limit: node.limit,
                offset: node.offset,
            }),
        ),
        PhysicalPlanKind::Values(node) => (
            encode_output_columns(&node.columns)?,
            Kind::Values(plan::ValuesNode {
                rows: node
                    .rows
                    .iter()
                    .map(|row| {
                        Ok(plan::ExprList {
                            values: encode_exprs(row)?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                columns: encode_output_columns(&node.columns)?,
            }),
        ),
        PhysicalPlanKind::Repeat(node) => (
            Vec::new(),
            Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: node
                    .repeat_column_ref_list
                    .iter()
                    .map(|values| plan::StringList {
                        values: values.clone(),
                    })
                    .collect(),
                repeat_column_ref_ids: node
                    .repeat_column_ref_ids
                    .iter()
                    .map(|values| plan::UInt32List {
                        values: values.iter().map(|id| id.0).collect(),
                    })
                    .collect(),
                grouping_ids: node.grouping_ids.clone(),
                all_rollup_columns: node.all_rollup_columns.clone(),
                all_rollup_column_ids: node.all_rollup_column_ids.iter().map(|id| id.0).collect(),
                grouping_key_aliases: node
                    .grouping_key_aliases
                    .iter()
                    .map(|(first, second)| plan::StringPair {
                        first: first.clone(),
                        second: second.clone(),
                    })
                    .collect(),
                grouping_fn_args: node
                    .grouping_fn_args
                    .iter()
                    .map(|(name, values)| plan::NamedStringList {
                        name: name.clone(),
                        values: values.clone(),
                    })
                    .collect(),
                grouping_fn_arg_ids: node
                    .grouping_fn_arg_ids
                    .iter()
                    .map(|values| plan::UInt32List {
                        values: values.iter().map(|id| id.0).collect(),
                    })
                    .collect(),
                grouping_fn_ids: node
                    .grouping_fn_ids
                    .iter()
                    .map(|(name, value)| plan::NamedUInt32 {
                        name: name.clone(),
                        value: value.0,
                    })
                    .collect(),
                virtual_tuple_id: node.virtual_tuple_id,
            }),
        ),
        PhysicalPlanKind::Window(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Window(plan::WindowNode {
                window_exprs: node
                    .window_exprs
                    .iter()
                    .map(|expr| {
                        Ok(plan::WindowExpr {
                            name: expr.name.clone(),
                            args: encode_exprs(&expr.args)?,
                            distinct: expr.distinct,
                            partition_by: encode_exprs(&expr.partition_by)?,
                            order_by: encode_sort_items(&expr.order_by)?,
                            window_frame: expr
                                .window_frame
                                .as_ref()
                                .map(encode_window_frame)
                                .transpose()?,
                            result_type: Some(encode_type(&expr.result_type)?),
                            output_name: expr.output_name.clone(),
                            output_column_id: expr.output_column_id.0,
                            ignore_nulls: expr.ignore_nulls,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
        PhysicalPlanKind::GenerateSeries(node) => (
            Vec::new(),
            Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: node.start,
                end: node.end,
                step: node.step,
                column_name: node.column_name.clone(),
                alias: node.alias.clone(),
                output_column_id: node.output_column_id.0,
            }),
        ),
        PhysicalPlanKind::TableFunction(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::TableFunction(plan::TableFunctionNode {
                function_name: node.function_name.clone(),
                args: encode_exprs(&node.args)?,
                output_columns: encode_output_columns(&node.output_columns)?,
                alias: node.alias.clone(),
                is_left_join: node.is_left_join,
            }),
        ),
        PhysicalPlanKind::AssertOneRow(node) => (
            Vec::new(),
            Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: node.subquery_text.clone(),
                desired_num_rows: node.desired_num_rows,
                assertion: encode_row_count_assertion(node.assertion),
                group_key_column_ids: node
                    .group_key_column_ids
                    .iter()
                    .map(|column_id| column_id.0)
                    .collect(),
                group_key_labels: node.group_key_labels.clone(),
                keyed_message_prefix: node.keyed_message_prefix.clone(),
            }),
        ),
        PhysicalPlanKind::TopN(node) => (
            Vec::new(),
            Kind::Topn(plan::TopNNode {
                items: encode_sort_items(&node.items)?,
                limit: node.limit,
                offset: node.offset,
                phase: encode_topn_phase(node.phase),
                is_split: node.is_split,
            }),
        ),
        PhysicalPlanKind::HashAggregate(node) => {
            // Baseline raw layout/output columns straight from the physical payload.
            // In a sealed plan `apply_sealed_node_output_columns` overwrites both the
            // node output columns and this `output_layout`/`output_columns` from the
            // finalized aggregate contract (which applies the per-mode intermediate
            // aggregate-state types). This raw form only stands in the bare-node
            // encoder unit tests that have no sealed plan; the intermediate-type
            // determination is owned by the planner (`finalize_hash_aggregate_wire`).
            let raw_output_columns = if node.output_columns.is_empty() {
                node.output_layout.full_output_columns()
            } else {
                node.output_columns.clone()
            };
            (
                encode_output_columns(&raw_output_columns)?,
                Kind::HashAggregate(plan::HashAggregateNode {
                    mode: encode_agg_mode(node.mode),
                    group_by: encode_exprs(&node.group_by)?,
                    aggregates: node
                        .aggregates
                        .iter()
                        .map(|call| {
                            Ok(plan::PlanAggregateCall {
                                name: call.name.clone(),
                                args: encode_exprs(&call.args)?,
                                distinct: call.distinct,
                                result_type: Some(encode_type(&call.result_type)?),
                                order_by: encode_sort_items(&call.order_by)?,
                                output_column_id: call.output_column_id.0,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                    is_merge: node.is_merge.clone(),
                    output_layout: Some(plan::AggregateOutputLayout {
                        group_key_columns: encode_output_columns(
                            &node.output_layout.group_key_columns,
                        )?,
                        aggregate_columns: encode_output_columns(
                            &node.output_layout.aggregate_columns,
                        )?,
                    }),
                    output_columns: encode_output_columns(&raw_output_columns)?,
                }),
            )
        }
        PhysicalPlanKind::HashJoin(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::HashJoin(plan::HashJoinNode {
                join_type: encode_join_kind(node.join_type),
                eq_conditions: node
                    .eq_conditions
                    .iter()
                    .map(|cond| {
                        Ok(plan::HashJoinEqCondition {
                            left: Some(encode_expr(&cond.left)?),
                            right: Some(encode_expr(&cond.right)?),
                            null_safe: cond.null_safe,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                other_condition: node.other_condition.as_ref().map(encode_expr).transpose()?,
                distribution: encode_join_distribution(&node.distribution),
                execution_mode: node.execution_mode.map(encode_join_execution_mode),
                build_runtime_filters: ctx
                    .runtime_filter_projection
                    .map(|projection| projection.builds_for_node(node_id))
                    .unwrap_or_default()
                    .iter()
                    .map(|rf| {
                        Ok(plan::RuntimeFilterBuildIntent {
                            filter_id: rf.filter_id,
                            build_expr: Some(encode_expr(&rf.build_expr)?),
                            probe_expr: Some(encode_expr(&rf.probe_expr)?),
                            expr_order: usize_to_u32(rf.expr_order)?,
                            execution_mode: encode_join_execution_mode(rf.execution_mode),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            }),
        ),
        PhysicalPlanKind::NestLoopJoin(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::NestLoopJoin(plan::NestLoopJoinNode {
                join_type: encode_join_kind(node.join_type),
                condition: node.condition.as_ref().map(encode_expr).transpose()?,
            }),
        ),
        PhysicalPlanKind::SetOp(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::SetOp(plan::SetOpNode {
                kind: encode_set_op_kind(node.kind),
                output_columns: encode_output_columns(&node.output_columns)?,
                child_output_columns: node
                    .child_output_columns
                    .iter()
                    .map(|columns| {
                        Ok(plan::OutputColumnList {
                            columns: encode_output_columns(columns)?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            }),
        ),
        PhysicalPlanKind::ChangeEventExpand(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: node
                    .events
                    .iter()
                    .map(|event| {
                        Ok(plan::DistributedChangeEventSpec {
                            predicate: event.predicate.as_ref().map(encode_expr).transpose()?,
                            branch_kind: encode_change_stream_branch_kind(event.branch_kind),
                            assignments: event
                                .assignments
                                .iter()
                                .map(|assignment| {
                                    Ok(plan::DistributedChangeEventOutputExpr {
                                        output_column_id: assignment.output_column_id.0,
                                        expr: assignment
                                            .expr
                                            .as_ref()
                                            .map(encode_expr)
                                            .transpose()?,
                                    })
                                })
                                .collect::<Result<Vec<_>, String>>()?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_columns: encode_output_columns(&node.output_columns)?,
                change_op_column_id: node.change_op_column_id.0,
                data_route_column_id: node.data_route_column_id.map(|id| id.0),
            }),
        ),
        PhysicalPlanKind::CTEAnchor(node) => (
            Vec::new(),
            Kind::CteAnchor(plan::CteAnchorNode {
                cte_id: node.cte_id,
            }),
        ),
        PhysicalPlanKind::CTEProduce(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::CteProduce(plan::CteProduceNode {
                cte_id: node.cte_id,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
        PhysicalPlanKind::CTEConsume(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::CteConsume(plan::CteConsumeNode {
                cte_id: node.cte_id,
                alias: node.alias.clone(),
                output_columns: encode_output_columns(&node.output_columns)?,
                producer_column_ids: node.producer_column_ids.iter().map(|id| id.0).collect(),
            }),
        ),
        PhysicalPlanKind::Redistribute(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Redistribute(plan::RedistributeNode {
                mode: Some(encode_redistribute_mode(&node.mode)),
                partition_exprs: encode_exprs(&node.partition_exprs)?,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
    };

    Ok(plan::PlanNode {
        output_columns,
        kind: Some(kind),
    })
}

fn encode_row_count_assertion(assertion: PlanRowCountAssertion) -> i32 {
    match assertion {
        PlanRowCountAssertion::Eq => plan::RowCountAssertion::Eq as i32,
        PlanRowCountAssertion::Ne => plan::RowCountAssertion::Ne as i32,
        PlanRowCountAssertion::Lt => plan::RowCountAssertion::Lt as i32,
        PlanRowCountAssertion::Le => plan::RowCountAssertion::Le as i32,
        PlanRowCountAssertion::Gt => plan::RowCountAssertion::Gt as i32,
        PlanRowCountAssertion::Ge => plan::RowCountAssertion::Ge as i32,
    }
}

fn encode_scan_node(
    src: &crate::sql::planner::payload::PlanScanNode,
    node_id: i32,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ScanNode, String> {
    let binding = scan_binding_for_source(node_id, &src.table.source, ctx)?;
    let columns = match binding {
        Some(binding) => encode_bound_scan_output_columns(src, binding)?,
        None => encode_output_columns(&src.columns)?,
    };
    let required_columns = binding.map_or_else(
        || src.required_columns.clone().unwrap_or_default(),
        |binding| encode_bound_required_columns(src, binding),
    );
    Ok(plan::ScanNode {
        database: src.database.clone(),
        table: Some(encode_table_def_with_context(
            &src.table,
            Some(node_id),
            Some(&src.columns),
            binding,
            ctx,
        )?),
        alias: src.alias.clone(),
        columns,
        predicates: encode_exprs(&src.predicates)?,
        required_columns,
        dict_columns: Vec::new(),
        variant_columns: src
            .variant_columns
            .iter()
            .map(|column| {
                Ok(plan::ScanVariantColumn {
                    source_column_id: column.source_column_id.0,
                    source_column: column.source_column.clone(),
                    synthetic_column_id: column.synthetic_column_id.0,
                    synthetic_column: column.synthetic_column.clone(),
                    canonical_path: column.canonical_path.clone(),
                    requested_type: Some(encode_type(&column.requested_type)?),
                    strict: column.strict,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        mv_rewritten_from: src.mv_rewritten_from.clone(),
    })
}

fn encode_bound_scan_output_columns(
    src: &crate::sql::planner::payload::PlanScanNode,
    binding: &ResolvedScanBinding,
) -> Result<Vec<common::OutputColumn>, String> {
    let physical_by_planner_id = binding
        .physical_columns
        .iter()
        .map(|column| (column.planner.column_id, column))
        .collect::<HashMap<_, _>>();
    let synthetic_ids = src
        .variant_columns
        .iter()
        .map(|column| column.synthetic_column_id)
        .collect::<HashSet<_>>();
    let mut encoded = Vec::with_capacity(src.columns.len());
    let mut seen_physical_ids = HashSet::new();
    for column in &src.columns {
        if let Some(bound) = physical_by_planner_id.get(&column.column_id) {
            encoded.push(encode_bound_scan_output_column(bound)?);
            seen_physical_ids.insert(column.column_id);
        } else if synthetic_ids.contains(&column.column_id) {
            encoded.push(encode_output_column(column)?);
        }
    }
    for bound in &binding.physical_columns {
        if seen_physical_ids.insert(bound.planner.column_id) {
            encoded.push(encode_bound_scan_output_column(bound)?);
        }
    }
    Ok(encoded)
}

fn encode_bound_required_columns(
    src: &crate::sql::planner::payload::PlanScanNode,
    binding: &ResolvedScanBinding,
) -> Vec<String> {
    let mut required = binding
        .required_reads
        .iter()
        .map(|read| read.source.name.clone())
        .collect::<Vec<_>>();
    for variant in &src.variant_columns {
        let required_by_planner = src.required_columns.as_ref().is_none_or(|columns| {
            columns
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&variant.synthetic_column))
        });
        if required_by_planner
            && !required
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&variant.synthetic_column))
        {
            required.push(variant.synthetic_column.clone());
        }
    }
    required
}

fn encode_bound_scan_output_column(
    column: &crate::coordinator::prepare::scan::ResolvedScanColumn,
) -> Result<common::OutputColumn, String> {
    Ok(common::OutputColumn {
        column_id: column.planner.column_id.0,
        name: column.source.name.clone(),
        r#type: Some(encode_type(&column.source.data_type)?),
        nullable: column.source.nullable,
        is_internal: column.planner.is_internal,
    })
}

/// Encode an exchange receiver. `output_columns` is the receiver's finalized
/// wire schema: for a stream-edge target it is the planner's reconciled edge
/// projection (kept equal to what the sender sends); otherwise it is the
/// receiver's own declared columns.
fn encode_exchange_receiver(
    src: &ExchangeReceiver,
    output_columns: &[AnalysisOutputColumn],
) -> Result<plan::ExchangeReceiver, String> {
    Ok(plan::ExchangeReceiver {
        partition_type: encode_edge_partition_type(&src.partition),
        partition_exprs: encode_exprs(&src.partition.exprs)?,
        source_fragment_id: src.source_fragment_id,
        output_columns: encode_output_columns(output_columns)?,
        output_qualifier: src.output_qualifier.clone(),
        flavor: Some(encode_exchange_flavor(&src.flavor)?),
    })
}

fn encode_exchange_flavor(src: &ExchangeFlavor) -> Result<plan::ExchangeFlavor, String> {
    use plan::exchange_flavor::Kind;

    Ok(plan::ExchangeFlavor {
        kind: Some(match src {
            ExchangeFlavor::Distribution => Kind::Distribution(true),
            ExchangeFlavor::LimitOffset { limit, offset } => {
                Kind::LimitOffset(plan::LimitOffsetFlavor {
                    limit: *limit,
                    offset: *offset,
                })
            }
            ExchangeFlavor::TopNSplit {
                items,
                limit,
                offset,
            } => Kind::TopnSplit(plan::TopNSplitFlavor {
                items: encode_sort_items(items)?,
                limit: *limit,
                offset: *offset,
            }),
            ExchangeFlavor::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            } => Kind::CteMulticast(plan::CteMulticastFlavor {
                cte_id: *cte_id,
                receive_producer_column_ids: receive_producer_column_ids
                    .iter()
                    .map(|id| id.0)
                    .collect(),
            }),
        }),
    })
}

pub(crate) fn encode_data_partition(src: &DataPartition) -> Result<plan::DataPartition, String> {
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
                spec: Some(encode_iceberg_write_sink_spec(&sink.spec)?),
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
    let partition = ctx
        .write_contracts
        .ok_or_else(|| {
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

fn encode_fragment_edge(src: &FragmentEdge) -> Result<plan::FragmentEdge, String> {
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

fn encode_graph_runtime_filter_build(
    src: &GraphRuntimeFilterBuild,
) -> Result<plan::RuntimeFilterBuild, String> {
    Ok(plan::RuntimeFilterBuild {
        filter_id: src.filter_id,
        build_expr: Some(encode_expr(&src.build_expr)?),
        probe_expr: Some(encode_expr(&src.probe_expr)?),
        expr_order: usize_to_u32(src.expr_order)?,
        execution_mode: encode_join_execution_mode(src.execution_mode),
        source_fragment_id: src.source_fragment_id,
        target_fragment_ids: src.target_fragment_ids.clone(),
    })
}

fn encode_graph_runtime_filter_probe(
    src: &GraphRuntimeFilterProbe,
) -> Result<plan::RuntimeFilterProbe, String> {
    Ok(plan::RuntimeFilterProbe {
        filter_id: src.filter_id,
        probe_expr: Some(encode_expr(&src.probe_expr)?),
        source_fragment_id: src.source_fragment_id,
    })
}

fn encode_table_def(src: &table_model::TableDef) -> Result<plan::TableDef, String> {
    encode_table_def_with_context(
        src,
        None,
        None,
        None,
        &NativePlanEncodeContext {
            scan_bindings: None,
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_projection: None,
        },
    )
}

fn encode_table_def_with_context(
    src: &table_model::TableDef,
    scan_node_id: Option<i32>,
    scan_columns: Option<&[AnalysisOutputColumn]>,
    binding: Option<&ResolvedScanBinding>,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::TableDef, String> {
    let (columns, metadata_columns) = match binding {
        Some(binding) if scan_source_requires_resolved_binding(&src.source) => {
            resolved_binding_table_columns(binding)
        }
        Some(binding) => merged_bound_table_columns(src, scan_columns.unwrap_or_default(), binding),
        None => (
            src.columns.clone(),
            src.iceberg_row_lineage_metadata_columns.clone(),
        ),
    };
    Ok(plan::TableDef {
        name: src.name.clone(),
        columns: columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        iceberg_row_lineage_metadata_columns: metadata_columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        source: Some(encode_scan_source(&src.source, scan_node_id, binding, ctx)?),
    })
}

fn scan_source_requires_resolved_binding(source: &table_model::ScanSource) -> bool {
    matches!(
        source,
        table_model::ScanSource::IcebergDeltaTable { .. }
            | table_model::ScanSource::IcebergVersionTable { .. }
            | table_model::ScanSource::IcebergMvTargetState(_)
            | table_model::ScanSource::IcebergMvTargetLocator(_)
    )
}

fn resolved_binding_table_columns(
    binding: &ResolvedScanBinding,
) -> (
    Vec<crate::catalog::schema::ColumnDef>,
    Vec<crate::catalog::schema::ColumnDef>,
) {
    let mut columns = Vec::new();
    let mut metadata_columns = Vec::new();
    let mut seen = HashSet::new();

    for bound in &binding.physical_columns {
        if !seen.insert(bound.source.name.to_ascii_lowercase()) {
            continue;
        }
        match bound.kind {
            ResolvedScanColumnKind::PhysicalTableColumn => columns.push(bound.source.clone()),
            ResolvedScanColumnKind::IcebergMetadataColumn => {
                metadata_columns.push(bound.source.clone())
            }
        }
    }
    for read in &binding.required_reads {
        if seen.insert(read.source.name.to_ascii_lowercase()) {
            columns.push(read.source.clone());
        }
    }

    (columns, metadata_columns)
}

fn merged_bound_table_columns(
    src: &table_model::TableDef,
    scan_columns: &[AnalysisOutputColumn],
    binding: &ResolvedScanBinding,
) -> (
    Vec<crate::catalog::schema::ColumnDef>,
    Vec<crate::catalog::schema::ColumnDef>,
) {
    let mut columns = src.columns.clone();
    let mut metadata_columns = src.iceberg_row_lineage_metadata_columns.clone();
    for bound in &binding.physical_columns {
        let target = match bound.kind {
            ResolvedScanColumnKind::PhysicalTableColumn => &mut columns,
            ResolvedScanColumnKind::IcebergMetadataColumn => &mut metadata_columns,
        };
        let planner_source_name = scan_columns
            .iter()
            .find(|column| column.column_id == bound.planner.column_id)
            .map(|column| column.name.as_str());
        overlay_bound_column(
            target,
            &bound.planner.name,
            planner_source_name,
            &bound.source,
        );
    }
    for read in &binding.required_reads {
        if replace_column_by_name(&mut columns, &read.source)
            || replace_column_by_name(&mut metadata_columns, &read.source)
        {
            continue;
        }
        columns.push(read.source.clone());
    }
    (columns, metadata_columns)
}

fn overlay_bound_column(
    columns: &mut Vec<crate::catalog::schema::ColumnDef>,
    planner_name: &str,
    planner_source_name: Option<&str>,
    source: &crate::catalog::schema::ColumnDef,
) {
    if let Some(index) = columns.iter().position(|column| {
        column.name.eq_ignore_ascii_case(planner_name)
            || planner_source_name.is_some_and(|name| column.name.eq_ignore_ascii_case(name))
            || column.name.eq_ignore_ascii_case(&source.name)
    }) {
        columns[index] = source.clone();
    } else {
        columns.push(source.clone());
    }
}

fn replace_column_by_name(
    columns: &mut [crate::catalog::schema::ColumnDef],
    source: &crate::catalog::schema::ColumnDef,
) -> bool {
    let Some(column) = columns
        .iter_mut()
        .find(|column| column.name.eq_ignore_ascii_case(&source.name))
    else {
        return false;
    };
    *column = source.clone();
    true
}

fn encode_column_def(src: &crate::catalog::schema::ColumnDef) -> Result<plan::ColumnDef, String> {
    Ok(plan::ColumnDef {
        name: src.name.clone(),
        data_type: Some(encode_type(&src.data_type)?),
        nullable: src.nullable,
        write_default_json: src
            .write_default
            .as_ref()
            .map(|literal| encode_column_write_default_json(src, literal))
            .transpose()?,
        logical_type: src.logical_type.as_ref().map(encode_sql_type).transpose()?,
    })
}

fn encode_column_write_default_json(
    column: &crate::catalog::schema::ColumnDef,
    value: &ColumnDefault,
) -> Result<String, String> {
    validate_column_default(value)?;
    let iceberg_type = iceberg_type_for_column_def(column)?;
    let normalized_value;
    let value = match (value, &iceberg_type) {
        (
            ColumnDefault::TimestamptzMicros { micros_since_epoch },
            Type::Primitive(PrimitiveType::Timestamp),
        ) => {
            normalized_value = ColumnDefault::TimestampMicros {
                micros_since_epoch: *micros_since_epoch,
            };
            &normalized_value
        }
        (
            ColumnDefault::TimestamptzNanos { nanos_since_epoch },
            Type::Primitive(PrimitiveType::TimestampNs),
        ) => {
            normalized_value = ColumnDefault::TimestampNanos {
                nanos_since_epoch: *nanos_since_epoch,
            };
            &normalized_value
        }
        _ => value,
    };
    crate::connector::iceberg::default_value::column_default_to_iceberg_literal(
        value,
        &iceberg_type,
    )
    .and_then(|literal| {
        literal
            .try_into_json(&iceberg_type)
            .map(|json| json.to_string())
            .map_err(|err| err.to_string())
    })
    .map_err(|err| {
        format!(
            "encode write_default_json for column `{}` as {:?}: {err}",
            column.name, iceberg_type
        )
    })
}

fn iceberg_type_for_column_def(column: &crate::catalog::schema::ColumnDef) -> Result<Type, String> {
    if let Some(logical_type) = column.logical_type.as_ref() {
        let mut next_field_id = 1;
        return crate::connector::iceberg::catalog::registry::iceberg_type_for_sql_type(
            logical_type,
            &mut next_field_id,
        );
    }
    iceberg_type_for_arrow_data_type(&column.data_type)
}

fn iceberg_type_for_arrow_data_type(data_type: &DataType) -> Result<Type, String> {
    if let Some(primitive) = iceberg_primitive_type_for_arrow_data_type(data_type)? {
        return Ok(Type::Primitive(primitive));
    }

    match data_type {
        DataType::Struct(fields) => Ok(Type::Struct(StructType::new(
            fields
                .iter()
                .map(|field| iceberg_nested_field_for_arrow_field(field.as_ref()))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::List(element) | DataType::LargeList(element) => Ok(Type::List(ListType::new(
            iceberg_nested_field_for_arrow_field(element.as_ref())?,
        ))),
        DataType::Map(entries, _sorted) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(format!(
                    "native plan MAP entries field must be Struct, got {:?}",
                    entries.data_type()
                ));
            };
            if fields.len() != 2 {
                return Err(format!(
                    "native plan MAP entries Struct must have 2 fields, got {}",
                    fields.len()
                ));
            }
            Ok(Type::Map(MapType::new(
                iceberg_nested_field_for_arrow_field(fields[0].as_ref())?,
                iceberg_nested_field_for_arrow_field(fields[1].as_ref())?,
            )))
        }
        other => Err(format!(
            "native plan cannot encode write_default_json for Arrow type {other:?} without a logical Iceberg type"
        )),
    }
}

fn iceberg_primitive_type_for_arrow_data_type(
    data_type: &DataType,
) -> Result<Option<PrimitiveType>, String> {
    Ok(Some(match data_type {
        DataType::Boolean => PrimitiveType::Boolean,
        DataType::Int8 | DataType::Int16 | DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        DataType::Float32 => PrimitiveType::Float,
        DataType::Float64 => PrimitiveType::Double,
        DataType::Utf8 | DataType::LargeUtf8 => PrimitiveType::String,
        DataType::Binary | DataType::LargeBinary => PrimitiveType::Binary,
        DataType::Date32 => PrimitiveType::Date,
        DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => PrimitiveType::Time,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            PrimitiveType::TimestampNs
        }
        DataType::Decimal128(precision, scale) => {
            let scale = u32::try_from(*scale).map_err(|_| {
                format!("Decimal128 negative scale {scale} is not supported by Iceberg defaults")
            })?;
            PrimitiveType::Decimal {
                precision: u32::from(*precision),
                scale,
            }
        }
        _ => return Ok(None),
    }))
}

fn iceberg_nested_field_for_arrow_field(
    field: &Field,
) -> Result<iceberg::spec::NestedFieldRef, String> {
    let field_id = arrow_field_id(field)?;
    let field_type = iceberg_type_for_arrow_data_type(field.data_type())?;
    Ok(Arc::new(NestedField::new(
        field_id,
        field.name(),
        field_type,
        !field.is_nullable(),
    )))
}

fn arrow_field_id(field: &Field) -> Result<i32, String> {
    let raw = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .ok_or_else(|| {
            format!(
                "native plan field {} is missing parquet field id metadata",
                field.name()
            )
        })?;
    raw.parse::<i32>().map_err(|err| {
        format!(
            "native plan field {} has invalid parquet field id {raw}: {err}",
            field.name()
        )
    })
}

fn scan_binding_for_source<'a>(
    node_id: i32,
    source: &table_model::ScanSource,
    ctx: &'a NativePlanEncodeContext<'_>,
) -> Result<Option<&'a ResolvedScanBinding>, String> {
    let binding = ctx
        .scan_bindings
        .and_then(|bindings| bindings.binding(node_id));
    let required = scan_source_requires_resolved_binding(source);
    if required && binding.is_none() {
        return Err(match source {
            table_model::ScanSource::IcebergDeltaTable {
                from_snapshot_id,
                to_snapshot_id,
                ..
            } => format!(
                "native scan encoder missing prepared binding for node_id={node_id} source={} from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}",
                scan_source_kind(source)
            ),
            _ => format!(
                "native scan encoder missing prepared binding for node_id={node_id} source={}",
                scan_source_kind(source)
            ),
        });
    }
    let Some(binding) = binding else {
        return Ok(None);
    };
    if binding.node_id != node_id {
        return Err(format!(
            "native scan encoder binding node mismatch: requested node_id={node_id}, binding node_id={}",
            binding.node_id
        ));
    }
    let valid_execution = match source {
        table_model::ScanSource::IcebergDeltaTable { .. } => {
            matches!(binding.execution, ResolvedScanExecution::IcebergDelta(_))
        }
        table_model::ScanSource::IcebergDataFiles { .. }
        | table_model::ScanSource::IcebergVersionTable { .. }
        | table_model::ScanSource::IcebergMvTargetState(_)
        | table_model::ScanSource::IcebergMvTargetLocator(_) => {
            matches!(binding.execution, ResolvedScanExecution::IcebergFiles(_))
        }
        table_model::ScanSource::IcebergMetadataTable { .. }
        | table_model::ScanSource::StarRocks { .. } => false,
    };
    if !valid_execution {
        return Err(format!(
            "native scan encoder execution variant mismatch for node_id={node_id} source={}: binding={}",
            scan_source_kind(source),
            resolved_execution_kind(&binding.execution)
        ));
    }
    Ok(Some(binding))
}

fn scan_source_kind(source: &table_model::ScanSource) -> &'static str {
    match source {
        table_model::ScanSource::StarRocks { .. } => "StarRocks",
        table_model::ScanSource::IcebergDataFiles { .. } => "IcebergDataFiles",
        table_model::ScanSource::IcebergMetadataTable { .. } => "IcebergMetadataTable",
        table_model::ScanSource::IcebergDeltaTable { .. } => "IcebergDeltaTable",
        table_model::ScanSource::IcebergVersionTable { .. } => "IcebergVersionTable",
        table_model::ScanSource::IcebergMvTargetState(_) => "IcebergMvTargetState",
        table_model::ScanSource::IcebergMvTargetLocator(_) => "IcebergMvTargetLocator",
    }
}

fn resolved_execution_kind(execution: &ResolvedScanExecution) -> &'static str {
    match execution {
        ResolvedScanExecution::IcebergFiles(_) => "IcebergFiles",
        ResolvedScanExecution::IcebergDelta(_) => "IcebergDelta",
    }
}

fn encode_scan_source(
    src: &table_model::ScanSource,
    scan_node_id: Option<i32>,
    binding: Option<&ResolvedScanBinding>,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ScanSource, String> {
    use plan::scan_source::Kind;

    if let Some(ResolvedScanExecution::IcebergFiles(files)) =
        binding.map(|binding| &binding.execution)
    {
        return Ok(plan::ScanSource {
            kind: Some(Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(encode_iceberg_table_info(&files.table)?),
                files: files
                    .files
                    .iter()
                    .map(encode_iceberg_data_file_info)
                    .collect::<Result<Vec<_>, _>>()?,
                cloud_properties: files.cloud_properties.clone().into_iter().collect(),
                binding: match files.binding {
                    table_model::IcebergDataFileBinding::CurrentSnapshot => {
                        plan::IcebergDataFileBinding::CurrentSnapshot as i32
                    }
                    table_model::IcebergDataFileBinding::ExplicitFiles => {
                        plan::IcebergDataFileBinding::ExplicitFiles as i32
                    }
                },
            })),
        });
    }

    Ok(plan::ScanSource {
        kind: Some(match src {
            table_model::ScanSource::StarRocks { db_id, table_id } => {
                let node_id = scan_node_id.ok_or_else(|| {
                    "StarRocks table source is only valid on a native ScanNode".to_string()
                })?;
                let descriptor = ctx
                    .scan_bindings
                    .and_then(|bindings| bindings.starrocks_source(node_id))
                    .ok_or_else(|| {
                        format!(
                            "StarRocks ScanNode node_id={node_id} missing native source descriptor"
                        )
                    })?;
                validate_starrocks_source_descriptor(node_id, *db_id, *table_id, descriptor)?;
                Kind::StarrocksTable(plan::StarRocksTableSource {
                    catalog_name: descriptor.catalog_name.clone(),
                    db_id: descriptor.db_id,
                    table_id: descriptor.table_id,
                    schema_id: descriptor.schema_id,
                    storage_columns: descriptor
                        .storage_columns
                        .iter()
                        .map(|column| plan::StarRocksColumnStorageMeta {
                            name: column.name.clone(),
                            unique_id: column.unique_id,
                            default_value: column.default_value.clone(),
                        })
                        .collect(),
                    current_schema: Some(encode_starrocks_tablet_schema(
                        &descriptor.tablet_schema,
                    )),
                })
            }
            table_model::ScanSource::IcebergDataFiles {
                table,
                files,
                cloud_properties,
                binding,
            } => Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(encode_iceberg_table_info(table)?),
                files: files
                    .iter()
                    .map(encode_iceberg_data_file_info)
                    .collect::<Result<Vec<_>, _>>()?,
                cloud_properties: cloud_properties.clone().into_iter().collect(),
                binding: match binding {
                    table_model::IcebergDataFileBinding::CurrentSnapshot => {
                        plan::IcebergDataFileBinding::CurrentSnapshot as i32
                    }
                    table_model::IcebergDataFileBinding::ExplicitFiles => {
                        plan::IcebergDataFileBinding::ExplicitFiles as i32
                    }
                },
            }),
            table_model::ScanSource::IcebergMetadataTable {
                table,
                metadata_table_type,
                serialized_table,
                cloud_properties,
                metadata_payload,
            } => Kind::IcebergMetadataTable(plan::IcebergMetadataTable {
                table: Some(encode_iceberg_table_info(table)?),
                metadata_table_type: encode_iceberg_metadata_table_type(metadata_table_type),
                serialized_table: serialized_table.clone(),
                cloud_properties: cloud_properties.clone().into_iter().collect(),
                metadata_payload: metadata_payload.clone(),
            }),
            table_model::ScanSource::IcebergDeltaTable {
                table,
                from_snapshot_id,
                to_snapshot_id,
            } => {
                let Some(ResolvedScanExecution::IcebergDelta(delta)) =
                    binding.map(|binding| &binding.execution)
                else {
                    return Err(format!(
                        "native scan encoder missing prepared IcebergDelta binding for node_id={}",
                        scan_node_id
                            .map(|node_id| node_id.to_string())
                            .unwrap_or_else(|| "<none>".to_string())
                    ));
                };

                Kind::IcebergDeltaTable(plan::IcebergDeltaTable {
                    table: Some(encode_iceberg_table_info(table)?),
                    from_snapshot_id: *from_snapshot_id,
                    to_snapshot_id: *to_snapshot_id,
                    delta_plan: Some(
                        super::iceberg_delta_scan::encode_iceberg_delta_scan_plan_native(
                            &delta.runtime_plan,
                        )?,
                    ),
                })
            }
            table_model::ScanSource::IcebergVersionTable { table, snapshot_id } => {
                Kind::IcebergVersionTable(plan::IcebergVersionTable {
                    table: Some(encode_iceberg_table_info(table)?),
                    snapshot_id: *snapshot_id,
                })
            }
            table_model::ScanSource::IcebergMvTargetState(scan) => {
                Kind::IcebergMvTargetState(plan::IcebergMvTargetState {
                    catalog: scan.catalog.clone(),
                    database: scan.database.clone(),
                    table: scan.table.clone(),
                    target_table_uuid: scan.target_table_uuid.clone(),
                    target_snapshot_id: scan.target_snapshot_id,
                    aggregate_state_layout_version: u32::from(scan.aggregate_state_layout_version),
                    columns: scan
                        .columns
                        .iter()
                        .map(encode_column_def)
                        .collect::<Result<Vec<_>, _>>()?,
                    group_key_names: scan.group_key_names.clone(),
                    aggregate_state_names: scan.aggregate_state_names.clone(),
                    physical_column_names: scan.physical_column_names.clone(),
                    row_id_column_name: scan.row_id_column_name.clone(),
                    row_filter: Some(encode_mv_target_state_row_filter(&scan.row_filter)),
                    partition_constraint: match scan.partition_constraint {
                        table_model::IcebergMvTargetStatePartitionConstraint::Unpartitioned => {
                            plan::IcebergMvTargetStatePartitionConstraint::Unpartitioned as i32
                        }
                        table_model::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired => {
                            plan::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired as i32
                        }
                    },
                })
            }
            table_model::ScanSource::IcebergMvTargetLocator(scan) => {
                Kind::IcebergMvTargetLocator(plan::IcebergMvTargetLocator {
                    catalog: scan.catalog.clone(),
                    database: scan.database.clone(),
                    table: scan.table.clone(),
                    target_table_uuid: scan.target_table_uuid.clone(),
                    target_snapshot_id: scan.target_snapshot_id,
                    apply_key_column: scan.apply_key_column.clone(),
                    branch_id_column: scan.branch_id_column.clone(),
                })
            }
        }),
    })
}

fn encode_starrocks_tablet_schema(
    schema: &StarRocksTabletSchemaDescriptor,
) -> plan::StarRocksTabletSchema {
    plan::StarRocksTabletSchema {
        schema_id: schema.schema_id,
        keys_type: match schema.keys_type {
            StarRocksKeysTypeDescriptor::Duplicate => {
                plan::StarRocksKeysType::StarrocksKeysTypeDuplicate as i32
            }
            StarRocksKeysTypeDescriptor::Unique => {
                plan::StarRocksKeysType::StarrocksKeysTypeUnique as i32
            }
            StarRocksKeysTypeDescriptor::Aggregate => {
                plan::StarRocksKeysType::StarrocksKeysTypeAggregate as i32
            }
            StarRocksKeysTypeDescriptor::Primary => {
                plan::StarRocksKeysType::StarrocksKeysTypePrimary as i32
            }
        },
        num_short_key_columns: schema.num_short_key_columns,
        sort_key_idxes: schema.sort_key_idxes.clone(),
        sort_key_unique_ids: schema.sort_key_unique_ids.clone(),
        columns: schema
            .columns
            .iter()
            .map(encode_starrocks_column_schema)
            .collect(),
    }
}

fn encode_starrocks_column_schema(
    column: &StarRocksColumnSchemaDescriptor,
) -> plan::StarRocksColumnSchema {
    plan::StarRocksColumnSchema {
        unique_id: column.unique_id,
        name: column.name.clone(),
        physical_type: column.physical_type.clone(),
        is_key: Some(column.is_key),
        aggregation: column.aggregation.clone(),
        nullable: Some(column.nullable),
        default_value: column.default_value.clone(),
        precision: column.precision,
        scale: column.scale,
        visible: Some(column.visible),
        children: column
            .children
            .iter()
            .map(encode_starrocks_column_schema)
            .collect(),
    }
}

fn validate_starrocks_source_descriptor(
    node_id: i32,
    expected_db_id: i64,
    expected_table_id: i64,
    descriptor: &StarRocksScanSourceDescriptor,
) -> Result<(), String> {
    if descriptor.db_id != expected_db_id || descriptor.table_id != expected_table_id {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native source identity mismatch: plan=({expected_db_id}, {expected_table_id}) descriptor=({}, {})",
            descriptor.db_id, descriptor.table_id
        ));
    }
    if descriptor.catalog_name.trim().is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native source catalog_name must not be empty"
        ));
    }
    for (field, value) in [
        ("db_id", descriptor.db_id),
        ("table_id", descriptor.table_id),
        ("schema_id", descriptor.schema_id),
    ] {
        if value <= 0 {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} native source {field} must be positive, got {value}"
            ));
        }
    }
    if descriptor.tablet_schema.schema_id != descriptor.schema_id {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native current schema id mismatch: source_schema_id={} current_schema_id={}",
            descriptor.schema_id, descriptor.tablet_schema.schema_id
        ));
    }
    if descriptor.tablet_schema.columns.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native current schema columns must not be empty"
        ));
    }
    let mut current_names = HashSet::new();
    let mut current_unique_ids = HashSet::new();
    for column in &descriptor.tablet_schema.columns {
        let name = column.name.as_deref().unwrap_or_default().trim();
        if !current_names.insert(name.to_ascii_lowercase()) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} native current schema contains duplicate column name {name}"
            ));
        }
        if !current_unique_ids.insert(column.unique_id) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} native current schema contains duplicate unique_id {}",
                column.unique_id
            ));
        }
    }
    if let Some(count) = descriptor.tablet_schema.num_short_key_columns
        && (count < 0 || count as usize > descriptor.tablet_schema.columns.len())
    {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native current schema num_short_key_columns out of range: {count}"
        ));
    }
    for unique_id in &descriptor.tablet_schema.sort_key_unique_ids {
        if !current_unique_ids.contains(&(*unique_id as i32)) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} native current schema sort_key_unique_ids references unknown unique_id {unique_id}"
            ));
        }
    }
    if descriptor
        .tablet_schema
        .sort_key_idxes
        .iter()
        .any(|index| *index as usize >= descriptor.tablet_schema.columns.len())
    {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native current schema sort_key_idxes contains out-of-range index"
        ));
    }
    if !descriptor.tablet_schema.sort_key_idxes.is_empty()
        && !descriptor.tablet_schema.sort_key_unique_ids.is_empty()
        && (descriptor.tablet_schema.sort_key_idxes.len()
            != descriptor.tablet_schema.sort_key_unique_ids.len()
            || descriptor
                .tablet_schema
                .sort_key_idxes
                .iter()
                .zip(&descriptor.tablet_schema.sort_key_unique_ids)
                .any(|(index, unique_id)| {
                    descriptor.tablet_schema.columns[*index as usize].unique_id != *unique_id as i32
                }))
    {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native current schema sort key indexes and unique ids are inconsistent"
        ));
    }
    for column in &descriptor.tablet_schema.columns {
        validate_starrocks_schema_column(node_id, column, true)?;
    }
    if descriptor.storage_columns.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native source storage_columns must not be empty"
        ));
    }
    let mut names = HashSet::new();
    let mut unique_ids = HashSet::new();
    for column in &descriptor.storage_columns {
        let name = column.name.trim();
        if name.is_empty() {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} storage column name must not be empty"
            ));
        }
        if column.unique_id < 0 {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} storage column {name} unique_id must be non-negative, got {}",
                column.unique_id
            ));
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} storage columns contain duplicate name {name}"
            ));
        }
        if !unique_ids.insert(column.unique_id) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} storage columns contain duplicate unique_id {}",
                column.unique_id
            ));
        }
    }
    let current_visible_columns = descriptor
        .tablet_schema
        .columns
        .iter()
        .filter(|column| column.visible)
        .map(|column| {
            (
                column
                    .name
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
                column.unique_id,
                column.default_value.as_deref(),
            )
        })
        .collect::<Vec<_>>();
    let storage_columns = descriptor
        .storage_columns
        .iter()
        .map(|column| {
            (
                column.name.to_ascii_lowercase(),
                column.unique_id,
                column.default_value.as_deref(),
            )
        })
        .collect::<Vec<_>>();
    if current_visible_columns != storage_columns {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native storage_columns do not match current schema visible columns"
        ));
    }
    Ok(())
}

fn validate_starrocks_schema_column(
    node_id: i32,
    column: &StarRocksColumnSchemaDescriptor,
    top_level: bool,
) -> Result<(), String> {
    let name = column.name.as_deref().map(str::trim).unwrap_or_default();
    if top_level && name.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema top-level column name must not be empty"
        ));
    }
    if top_level && column.unique_id < 0 {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema column {name} unique_id must be non-negative"
        ));
    }
    let physical_type = column.physical_type.trim().to_ascii_uppercase();
    if physical_type.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema column {name} physical_type must not be empty"
        ));
    }
    let expected_children = match physical_type.as_str() {
        "ARRAY" => Some(1),
        "MAP" => Some(2),
        "STRUCT" => None,
        _ => Some(0),
    };
    if let Some(expected) = expected_children
        && column.children.len() != expected
    {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema column {name} type {physical_type} requires {expected} children, got {}",
            column.children.len()
        ));
    }
    if physical_type == "STRUCT" && column.children.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema column {name} STRUCT requires at least one child"
        ));
    }
    if physical_type == "STRUCT" {
        let mut child_names = HashSet::new();
        let mut positive_child_ids = HashSet::new();
        for child in &column.children {
            let child_name = child.name.as_deref().map(str::trim).unwrap_or_default();
            if child_name.is_empty() {
                return Err(format!(
                    "StarRocks ScanNode node_id={node_id} current schema STRUCT column {name} child name must not be empty"
                ));
            }
            if !child_names.insert(child_name.to_ascii_lowercase()) {
                return Err(format!(
                    "StarRocks ScanNode node_id={node_id} current schema STRUCT column {name} contains duplicate child name {child_name}"
                ));
            }
            if child.unique_id >= 0 && !positive_child_ids.insert(child.unique_id) {
                return Err(format!(
                    "StarRocks ScanNode node_id={node_id} current schema STRUCT column {name} contains duplicate positive child unique_id {}",
                    child.unique_id
                ));
            }
        }
    }
    for child in &column.children {
        validate_starrocks_schema_column(node_id, child, false)?;
    }
    Ok(())
}

fn encode_mv_target_state_row_filter(
    src: &table_model::IcebergMvTargetStateRowFilter,
) -> plan::IcebergMvTargetStateRowFilter {
    use plan::iceberg_mv_target_state_row_filter::Kind;

    plan::IcebergMvTargetStateRowFilter {
        kind: Some(match src {
            table_model::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope,
            } => Kind::DeltaInputRowIds(plan::DeltaInputRowIdsFilter {
                row_id_column_name: row_id_column_name.clone(),
                branch_scope: branch_scope.as_ref().map(|scope| plan::BranchScope {
                    branch_id_column_name: scope.branch_id_column_name.clone(),
                    branch_id: scope.branch_id,
                }),
            }),
        }),
    }
}

fn encode_iceberg_table_info(
    src: &table_model::IcebergTableInfo,
) -> Result<plan::IcebergTableInfo, String> {
    Ok(plan::IcebergTableInfo {
        catalog: src.catalog.clone(),
        namespace: src.namespace.clone(),
        table: src.table.clone(),
        table_uuid: src.table_uuid.clone(),
        current_snapshot_id: src.current_snapshot_id,
        schema_id: src.schema_id,
        location: src.location.clone(),
        schema: Some(encode_iceberg_schema_def(&src.schema)?),
        serialized_metadata: src.serialized_metadata.clone(),
        serialized_metadata_rows: src.serialized_metadata_rows.clone(),
    })
}

fn encode_iceberg_schema_def(
    src: &table_model::IcebergSchemaDef,
) -> Result<plan::IcebergSchemaDef, String> {
    Ok(plan::IcebergSchemaDef {
        fields: src
            .fields
            .iter()
            .map(encode_iceberg_schema_field)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn encode_iceberg_schema_field(
    src: &table_model::IcebergSchemaFieldDef,
) -> Result<plan::IcebergSchemaFieldDef, String> {
    Ok(plan::IcebergSchemaFieldDef {
        field_id: src.field_id,
        name: src.name.clone(),
        initial_default_json: encode_iceberg_schema_default_json(
            "initial_default",
            src.initial_default_json.as_ref(),
            src.initial_default.as_ref(),
        )?,
        write_default_json: encode_iceberg_schema_default_json(
            "write_default",
            src.write_default_json.as_ref(),
            src.write_default.as_ref(),
        )?,
        children: src
            .children
            .iter()
            .map(encode_iceberg_schema_field)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn encode_iceberg_schema_default_json(
    label: &str,
    precomputed_json: Option<&String>,
    literal: Option<&iceberg::spec::Literal>,
) -> Result<Option<String>, String> {
    if let Some(json) = precomputed_json {
        return Ok(Some(json.clone()));
    }
    literal
        .map(super::iceberg_literal_json::serialize_iceberg_literal_json)
        .transpose()
        .map_err(|err| format!("encode Iceberg schema {label} JSON: {err}"))
}

fn encode_iceberg_data_file_info(
    src: &table_model::IcebergDataFileInfo,
) -> Result<plan::IcebergDataFileInfo, String> {
    Ok(plan::IcebergDataFileInfo {
        path: src.path.clone(),
        size: src.size,
        row_count: src.row_count,
        column_stats: src
            .column_stats
            .as_ref()
            .map(|stats| plan::IcebergColumnStatsMap {
                entries: stats
                    .iter()
                    .map(|(name, stats)| (name.clone(), encode_iceberg_column_stats(stats)))
                    .collect::<HashMap<_, _>>(),
            }),
        partition_spec_id: src.partition_spec_id,
        partition_key: src.partition_key.clone(),
        first_row_id: src.first_row_id,
        data_sequence_number: src.data_sequence_number,
        ivm_change_op: src.ivm_change_op.map(i32::from),
        included_positions: src
            .included_positions
            .as_ref()
            .map(|values| plan::Int64List {
                values: values.clone(),
            }),
        delete_files: src
            .delete_files
            .iter()
            .map(encode_iceberg_delete_file_info)
            .collect(),
        manifest_path: src.manifest_path.clone(),
        partition_values: src
            .partition_values
            .iter()
            .map(encode_iceberg_partition_field_value)
            .collect(),
    })
}

fn encode_iceberg_column_stats(src: &table_model::IcebergColumnStats) -> plan::IcebergColumnStats {
    plan::IcebergColumnStats {
        null_count: src.null_count,
        value_count: src.value_count,
        column_size: src.column_size,
        lower_bound: src.lower_bound.clone(),
        upper_bound: src.upper_bound.clone(),
    }
}

fn encode_iceberg_delete_file_info(
    src: &table_model::IcebergDeleteFileInfo,
) -> plan::IcebergDeleteFileInfo {
    plan::IcebergDeleteFileInfo {
        path: src.path.clone(),
        file_format: match src.file_format {
            table_model::IcebergDeleteFileFormat::Parquet => {
                plan::IcebergDeleteFileFormat::Parquet as i32
            }
            table_model::IcebergDeleteFileFormat::Puffin => {
                plan::IcebergDeleteFileFormat::Puffin as i32
            }
        },
        file_content: match src.file_content {
            table_model::IcebergDeleteFileContent::Position => {
                plan::IcebergDeleteFileContent::Position as i32
            }
            table_model::IcebergDeleteFileContent::Equality => {
                plan::IcebergDeleteFileContent::Equality as i32
            }
        },
        length: src.length,
        content_offset: src.content_offset,
        content_size_in_bytes: src.content_size_in_bytes,
        sequence_number: src.sequence_number,
        partition_spec_id: src.partition_spec_id,
        partition_key: src.partition_key.clone(),
        equality_column_names: src.equality_column_names.clone(),
        equality_field_ids: src.equality_field_ids.clone(),
    }
}

fn encode_iceberg_partition_field_value(
    src: &table_model::IcebergPartitionFieldValue,
) -> plan::IcebergPartitionFieldValue {
    plan::IcebergPartitionFieldValue {
        source_column: src.source_column.clone(),
        field_name: src.field_name.clone(),
        transform: src.transform.clone(),
        value: src.value.as_ref().map(encode_iceberg_partition_value),
    }
}

fn encode_iceberg_partition_value(
    src: &table_model::IcebergPartitionValue,
) -> plan::IcebergPartitionValue {
    use plan::iceberg_partition_value::Value;

    plan::IcebergPartitionValue {
        value: Some(match src {
            table_model::IcebergPartitionValue::Boolean(value) => Value::BoolValue(*value),
            table_model::IcebergPartitionValue::Int32(value) => Value::Int32Value(*value),
            table_model::IcebergPartitionValue::Int64(value) => Value::Int64Value(*value),
            table_model::IcebergPartitionValue::Float(value) => Value::FloatValue(*value),
            table_model::IcebergPartitionValue::Double(value) => Value::DoubleValue(*value),
            table_model::IcebergPartitionValue::String(value) => Value::StringValue(value.clone()),
            table_model::IcebergPartitionValue::Binary(value) => Value::BinaryValue(value.clone()),
        }),
    }
}

fn encode_iceberg_write_sink_spec(
    src: &IcebergWriteSinkSpec,
) -> Result<plan::IcebergWriteSinkSpec, String> {
    Ok(plan::IcebergWriteSinkSpec {
        mode: encode_iceberg_write_sink_mode(src.mode),
        target_table_id: src.target_table_id,
        target_table: Some(encode_table_def(&src.target_table)?),
        iceberg: Some(encode_iceberg_table_info(&src.iceberg)?),
        target_columns: src
            .target_columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        table_location: src.table_location.clone(),
        data_location: src.data_location.clone(),
        target_partition_spec_id: src.target_partition_spec_id,
        cloud_properties: src.cloud_properties.clone().into_iter().collect(),
        file_format: src.file_format.clone(),
        compression: match src.compression {
            IcebergWriteFileCompression::Snappy => plan::IcebergWriteFileCompression::Snappy as i32,
        },
        position_delete_output_descriptor: src
            .position_delete_output_descriptor
            .as_ref()
            .map(encode_position_delete_descriptor)
            .transpose()?,
    })
}

fn encode_iceberg_write_input_binding(
    src: &IcebergWriteInputBinding,
) -> plan::IcebergWriteInputBinding {
    use plan::iceberg_write_input_binding::Kind;

    plan::IcebergWriteInputBinding {
        kind: Some(match src {
            IcebergWriteInputBinding::RootOutputByOrdinal => Kind::RootOutputByOrdinal(true),
            IcebergWriteInputBinding::OutputOrdinals(values) => {
                Kind::OutputOrdinals(plan::UInt64List {
                    values: values.iter().map(|value| usize_to_u64(*value)).collect(),
                })
            }
        }),
    }
}

fn encode_position_delete_descriptor(
    src: &crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorInput,
) -> Result<plan::PositionDeleteDescriptorInput, String> {
    Ok(plan::PositionDeleteDescriptorInput {
        file_path: Some(encode_position_delete_output_field(&src.file_path)?),
        pos: Some(encode_position_delete_output_field(&src.pos)?),
        partition_source_fields: src
            .partition_source_fields
            .iter()
            .map(encode_position_delete_partition_source_field)
            .collect::<Result<Vec<_>, _>>()?,
        target_partition_spec_id: src.target_partition_spec_id,
    })
}

fn encode_position_delete_output_field(
    src: &crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField,
) -> Result<plan::PositionDeleteOutputField, String> {
    Ok(plan::PositionDeleteOutputField {
        output_expr_index: usize_to_u64(src.output_expr_index),
        name: src.name.clone(),
        data_type: Some(encode_type(&src.data_type)?),
        field_id: src.field_id,
    })
}

fn encode_position_delete_partition_source_field(
    src: &crate::connector::iceberg::position_delete_descriptor::PositionDeletePartitionSourceField,
) -> Result<plan::PositionDeletePartitionSourceField, String> {
    Ok(plan::PositionDeletePartitionSourceField {
        output_expr_index: usize_to_u64(src.output_expr_index),
        source_column_name: src.source_column_name.clone(),
        partition_field_name: src.partition_field_name.clone(),
        transform_expr: src.transform_expr.clone(),
        source_field_id: src.source_field_id,
        data_type: Some(encode_type(&src.data_type)?),
    })
}

fn encode_output_columns(
    src: &[crate::sql::analysis::OutputColumn],
) -> Result<Vec<common::OutputColumn>, String> {
    src.iter().map(encode_output_column).collect()
}

fn encode_output_column(
    src: &crate::sql::analysis::OutputColumn,
) -> Result<common::OutputColumn, String> {
    Ok(common::OutputColumn {
        column_id: src.column_id.0,
        name: src.name.clone(),
        r#type: Some(encode_type(&src.data_type)?),
        nullable: src.nullable,
        is_internal: src.is_internal,
    })
}

fn encode_exprs(
    src: &[crate::sql::analysis::TypedExpr],
) -> Result<Vec<crate::proto::expr::Expr>, String> {
    src.iter().map(encode_expr).collect()
}

fn encode_sql_type(src: &SqlType) -> Result<common::TypeDesc, String> {
    use common::type_desc::Kind;

    Ok(common::TypeDesc {
        kind: Some(match src {
            SqlType::Array(element) => Kind::List(Box::new(common::ListType {
                element: Some(Box::new(encode_sql_type(element)?)),
            })),
            SqlType::Map(key, value) => Kind::Map(Box::new(common::MapType {
                key: Some(Box::new(encode_sql_type(key)?)),
                value: Some(Box::new(encode_sql_type(value)?)),
            })),
            SqlType::Struct(fields) => Kind::Strct(common::StructType {
                fields: fields
                    .iter()
                    .map(|(name, ty)| {
                        Ok(common::StructField {
                            name: name.clone(),
                            r#type: Some(encode_sql_type(ty)?),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            }),
            other => Kind::Scalar(sql_scalar_type(other)?),
        }),
    })
}

fn sql_scalar_type(src: &SqlType) -> Result<common::ScalarType, String> {
    use common::PrimitiveType;

    let (primitive, precision, scale, time_unit) = match src {
        SqlType::TinyInt => (PrimitiveType::Tinyint, None, None, None),
        SqlType::SmallInt => (PrimitiveType::Smallint, None, None, None),
        SqlType::Int => (PrimitiveType::Int, None, None, None),
        SqlType::BigInt => (PrimitiveType::Bigint, None, None, None),
        SqlType::LargeInt => (PrimitiveType::Largeint, None, None, None),
        SqlType::Float => (PrimitiveType::Float, None, None, None),
        SqlType::Double => (PrimitiveType::Double, None, None, None),
        SqlType::Decimal { precision, scale } => (
            PrimitiveType::Decimal128,
            Some(i32::from(*precision)),
            Some(i32::from(*scale)),
            None,
        ),
        SqlType::String => (PrimitiveType::Varchar, None, None, None),
        SqlType::Json => (PrimitiveType::Json, None, None, None),
        SqlType::Binary => (PrimitiveType::Varbinary, None, None, None),
        SqlType::Bitmap => (PrimitiveType::Bitmap, None, None, None),
        SqlType::Hll => (PrimitiveType::Hll, None, None, None),
        SqlType::Boolean => (PrimitiveType::Boolean, None, None, None),
        SqlType::Date => (PrimitiveType::Date, None, None, None),
        SqlType::DateTime => (PrimitiveType::Datetime, None, None, None),
        SqlType::DateTimeNs => (PrimitiveType::Datetime, None, None, Some(3)),
        SqlType::Time => (PrimitiveType::Time, None, None, None),
        SqlType::Variant => (PrimitiveType::Variant, None, None, None),
        SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_) => {
            return Err("nested SqlType cannot be encoded as scalar TypeDesc".to_string());
        }
    };
    Ok(common::ScalarType {
        r#type: primitive as i32,
        len: None,
        precision,
        scale,
        time_unit,
    })
}

fn encode_edge_partition_type(src: &DataPartition) -> i32 {
    match src.kind {
        PartitionKind::Unpartitioned => plan::PartitionType::Unpartitioned as i32,
        PartitionKind::Random => plan::PartitionType::Random as i32,
        PartitionKind::Hash => plan::PartitionType::Hash as i32,
    }
}

fn encode_join_kind(src: JoinKind) -> i32 {
    match src {
        JoinKind::Inner => plan::JoinKind::Inner as i32,
        JoinKind::LeftOuter => plan::JoinKind::LeftOuter as i32,
        JoinKind::RightOuter => plan::JoinKind::RightOuter as i32,
        JoinKind::FullOuter => plan::JoinKind::FullOuter as i32,
        JoinKind::Cross => plan::JoinKind::Cross as i32,
        JoinKind::LeftSemi => plan::JoinKind::LeftSemi as i32,
        JoinKind::RightSemi => plan::JoinKind::RightSemi as i32,
        JoinKind::LeftAnti => plan::JoinKind::LeftAnti as i32,
        JoinKind::RightAnti => plan::JoinKind::RightAnti as i32,
        JoinKind::NullAwareLeftAnti => plan::JoinKind::NullAwareLeftAnti as i32,
    }
}

fn encode_join_distribution(src: &JoinDistribution) -> i32 {
    match src {
        JoinDistribution::Unknown => plan::JoinDistribution::Unknown as i32,
        JoinDistribution::Shuffle => plan::JoinDistribution::Shuffle as i32,
        JoinDistribution::Broadcast => plan::JoinDistribution::Broadcast as i32,
        JoinDistribution::Colocate => plan::JoinDistribution::Colocate as i32,
    }
}

fn encode_join_execution_mode(src: JoinExecutionMode) -> i32 {
    match src {
        JoinExecutionMode::Broadcast => plan::JoinExecutionMode::Broadcast as i32,
        JoinExecutionMode::Partitioned => plan::JoinExecutionMode::Partitioned as i32,
        JoinExecutionMode::Colocate => plan::JoinExecutionMode::Colocate as i32,
    }
}

fn encode_agg_mode(src: AggMode) -> i32 {
    match src {
        AggMode::Single => plan::AggMode::Single as i32,
        AggMode::Local => plan::AggMode::Local as i32,
        AggMode::Global => plan::AggMode::Global as i32,
        AggMode::DistinctGlobal => plan::AggMode::DistinctGlobal as i32,
        AggMode::DistinctLocal => plan::AggMode::DistinctLocal as i32,
    }
}

fn encode_topn_phase(src: TopNPhase) -> i32 {
    match src {
        TopNPhase::Partial => plan::TopNPhase::TopnPhasePartial as i32,
        TopNPhase::Final => plan::TopNPhase::TopnPhaseFinal as i32,
    }
}

fn encode_set_op_kind(src: PlanSetOpKind) -> i32 {
    match src {
        PlanSetOpKind::UnionAll => plan::PlanSetOpKind::UnionAll as i32,
        PlanSetOpKind::UnionDistinct => plan::PlanSetOpKind::UnionDistinct as i32,
        PlanSetOpKind::Intersect => plan::PlanSetOpKind::Intersect as i32,
        PlanSetOpKind::Except => plan::PlanSetOpKind::Except as i32,
    }
}

fn encode_change_stream_branch_kind(src: ChangeStreamBranchKind) -> i32 {
    match src {
        ChangeStreamBranchKind::DeleteDv => plan::ChangeStreamBranchKind::DeleteDv as i32,
        ChangeStreamBranchKind::ReuseData => plan::ChangeStreamBranchKind::ReuseData as i32,
        ChangeStreamBranchKind::FreshData => plan::ChangeStreamBranchKind::FreshData as i32,
    }
}

fn encode_sort_topn_type(src: crate::exec::node::sort::SortTopNType) -> i32 {
    match src {
        crate::exec::node::sort::SortTopNType::RowNumber => {
            plan::SortTopNType::SortTopnTypeRowNumber as i32
        }
        crate::exec::node::sort::SortTopNType::Rank => plan::SortTopNType::SortTopnTypeRank as i32,
        crate::exec::node::sort::SortTopNType::DenseRank => {
            plan::SortTopNType::SortTopnTypeDenseRank as i32
        }
    }
}

fn encode_hash_source(src: HashSource) -> i32 {
    match src {
        HashSource::ShuffleAgg => plan::HashSource::ShuffleAgg as i32,
        HashSource::ShuffleJoin => plan::HashSource::ShuffleJoin as i32,
    }
}

fn encode_redistribute_mode(src: &RedistributeMode) -> plan::RedistributeMode {
    use plan::redistribute_mode::Mode;

    plan::RedistributeMode {
        mode: Some(match src {
            RedistributeMode::Gather => Mode::Gather(true),
            RedistributeMode::Hash { cols, source } => Mode::Hash(plan::RedistributeHash {
                cols: cols.iter().map(|id| id.0).collect(),
                source: encode_hash_source(*source),
            }),
            RedistributeMode::Broadcast => Mode::Broadcast(true),
        }),
    }
}

fn encode_iceberg_metadata_table_type(
    src: &crate::connector::iceberg::IcebergMetadataTableType,
) -> i32 {
    match src {
        crate::connector::iceberg::IcebergMetadataTableType::Files => {
            plan::IcebergMetadataTableType::Files as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Manifests => {
            plan::IcebergMetadataTableType::Manifests as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::LogicalIcebergMetadata => {
            plan::IcebergMetadataTableType::LogicalIcebergMetadata as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Snapshots => {
            plan::IcebergMetadataTableType::Snapshots as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::History => {
            plan::IcebergMetadataTableType::History as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Refs => {
            plan::IcebergMetadataTableType::Refs as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Partitions => {
            plan::IcebergMetadataTableType::Partitions as i32
        }
    }
}

fn encode_iceberg_write_sink_mode(src: IcebergWriteSinkMode) -> i32 {
    match src {
        IcebergWriteSinkMode::Data => plan::IcebergWriteSinkMode::Data as i32,
        IcebergWriteSinkMode::RowLineageData => plan::IcebergWriteSinkMode::RowLineageData as i32,
        IcebergWriteSinkMode::PositionDeletes => plan::IcebergWriteSinkMode::PositionDeletes as i32,
        IcebergWriteSinkMode::DeletionVectors => plan::IcebergWriteSinkMode::DeletionVectors as i32,
        IcebergWriteSinkMode::EqualityDeletes => plan::IcebergWriteSinkMode::EqualityDeletes as i32,
    }
}

fn usize_to_u64(value: usize) -> u64 {
    value as u64
}

fn usize_to_u32(value: usize) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("value {value} does not fit in u32"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::coordinator::prepare::scan::{
        ResolvedIcebergDeltaScan, ResolvedIcebergFileScan, ResolvedReadColumn, ResolvedReadReason,
        ResolvedScanBinding, ResolvedScanColumn, ResolvedScanColumnKind, ResolvedScanExecution,
        ScanExecutionBindings,
    };
    use crate::proto::expr::expr;
    use crate::runtime_filter::model::contract::ChannelId;
    use crate::runtime_filter::model::graph::RuntimeFilterGraph;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::codegen::scan::iceberg_delta::IcebergDeltaScanRuntimePlan;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::DataPartition;
    use crate::sql::planner::distributed::write::change_stream::{
        IcebergChangeStreamBranchRoute, IcebergChangeStreamRouterSink,
    };
    use crate::sql::planner::physical::runtime_filter::{
        RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
    };
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
    use arrow::datatypes::{DataType, TimeUnit};

    fn encode_write_default_json_for_test(
        data_type: DataType,
        value: ColumnDefault,
    ) -> Result<Option<String>, String> {
        encode_column_def(&crate::catalog::schema::ColumnDef {
            name: "defaulted".to_string(),
            data_type,
            nullable: true,
            write_default: Some(value),
            logical_type: None,
        })
        .map(|column| column.write_default_json)
    }

    fn field_with_iceberg_id(
        id: i32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Arc<Field> {
        Arc::new(
            Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])),
        )
    }

    #[test]
    fn column_write_default_json_preserves_primitive_and_temporal_lexical_bytes() {
        let cases = [
            (
                "boolean",
                DataType::Boolean,
                ColumnDefault::Boolean(true),
                "true",
            ),
            ("integer", DataType::Int32, ColumnDefault::Int32(-7), "-7"),
            (
                "decimal",
                DataType::Decimal128(10, 2),
                ColumnDefault::Decimal {
                    unscaled: 999,
                    precision: 10,
                    scale: 2,
                },
                "\"9.99\"",
            ),
            (
                "date",
                DataType::Date32,
                ColumnDefault::Date {
                    days_since_epoch: 0,
                },
                "\"1970-01-01\"",
            ),
            (
                "time",
                DataType::Time64(TimeUnit::Microsecond),
                ColumnDefault::TimeMicros {
                    micros_since_midnight: 0,
                },
                "\"00:00:00\"",
            ),
            (
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                ColumnDefault::TimestampMicros {
                    micros_since_epoch: 1_234_567,
                },
                "\"1970-01-01T00:00:01.234567\"",
            ),
            (
                "timestamptz-normalized",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ColumnDefault::TimestamptzMicros {
                    micros_since_epoch: 1_234_567,
                },
                "\"1970-01-01T00:00:01.234567\"",
            ),
            (
                "timestamp-ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                ColumnDefault::TimestampNanos {
                    nanos_since_epoch: 1_234_567_890,
                },
                "\"1970-01-01T00:00:01.234567890\"",
            ),
            (
                "binary",
                DataType::Binary,
                ColumnDefault::Binary(vec![0x00, 0x0f, 0x10, 0xff]),
                "\"0f10ff\"",
            ),
        ];

        for (name, data_type, literal, expected) in cases {
            assert_eq!(
                encode_write_default_json_for_test(data_type, literal)
                    .unwrap_or_else(|error| panic!("encode {name} write default: {error}"))
                    .as_deref(),
                Some(expected),
                "case={name}"
            );
        }
    }

    #[test]
    fn column_write_default_json_preserves_empty_and_nested_collection_lexical_bytes() {
        let empty_list_type =
            DataType::List(field_with_iceberg_id(1, "element", DataType::Int32, true));
        assert_eq!(
            encode_write_default_json_for_test(empty_list_type, ColumnDefault::Array(Vec::new()),)
                .unwrap()
                .as_deref(),
            Some("[]")
        );

        let empty_map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        field_with_iceberg_id(2, "key", DataType::Utf8, false),
                        field_with_iceberg_id(3, "value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert_eq!(
            encode_write_default_json_for_test(empty_map_type, ColumnDefault::Map(Vec::new()),)
                .unwrap()
                .as_deref(),
            Some(r#"{"keys":[],"values":[]}"#)
        );

        let list_type = DataType::List(field_with_iceberg_id(11, "element", DataType::Int32, true));
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        field_with_iceberg_id(13, "key", DataType::Utf8, false),
                        field_with_iceberg_id(14, "value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let nested_type = DataType::Struct(
            vec![
                field_with_iceberg_id(10, "items", list_type, true),
                field_with_iceberg_id(12, "attributes", map_type, true),
            ]
            .into(),
        );
        let nested_literal = ColumnDefault::Struct(vec![
            (
                "items".to_string(),
                ColumnDefault::Array(vec![ColumnDefault::Int32(1), ColumnDefault::Null]),
            ),
            (
                "attributes".to_string(),
                ColumnDefault::Map(vec![
                    (
                        ColumnDefault::String("first".to_string()),
                        ColumnDefault::Int32(2),
                    ),
                    (
                        ColumnDefault::String("second".to_string()),
                        ColumnDefault::Null,
                    ),
                ]),
            ),
        ]);
        assert_eq!(
            encode_write_default_json_for_test(nested_type, nested_literal)
                .unwrap()
                .as_deref(),
            Some(r#"{"10":[1,null],"12":{"keys":["first","second"],"values":[2,null]}}"#)
        );
    }

    #[test]
    fn column_write_default_json_preserves_non_finite_as_legacy_null() {
        let cases = [
            (
                "float-nan",
                DataType::Float32,
                ColumnDefault::Float32 { bits: 0x7fc0_1234 },
            ),
            (
                "float-positive-infinity",
                DataType::Float32,
                ColumnDefault::Float32 {
                    bits: f32::INFINITY.to_bits(),
                },
            ),
            (
                "float-negative-infinity",
                DataType::Float32,
                ColumnDefault::Float32 {
                    bits: f32::NEG_INFINITY.to_bits(),
                },
            ),
            (
                "double-nan",
                DataType::Float64,
                ColumnDefault::Float64 {
                    bits: 0x7ff8_0000_0000_1234,
                },
            ),
            (
                "double-positive-infinity",
                DataType::Float64,
                ColumnDefault::Float64 {
                    bits: f64::INFINITY.to_bits(),
                },
            ),
            (
                "double-negative-infinity",
                DataType::Float64,
                ColumnDefault::Float64 {
                    bits: f64::NEG_INFINITY.to_bits(),
                },
            ),
        ];

        for (name, data_type, literal) in cases {
            assert_eq!(
                encode_write_default_json_for_test(data_type, literal)
                    .unwrap_or_else(|error| panic!("encode {name} write default: {error}"))
                    .as_deref(),
                Some("null"),
                "case={name}"
            );
        }
    }

    #[test]
    fn column_write_default_json_preserves_uuid_and_fixed_unsupported_errors() {
        let cases = [
            (
                DataType::FixedSizeBinary(16),
                ColumnDefault::Uuid(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff_u128.to_be_bytes()),
                "native plan cannot encode write_default_json for Arrow type FixedSizeBinary(16) without a logical Iceberg type",
            ),
            (
                DataType::FixedSizeBinary(4),
                ColumnDefault::Fixed {
                    size: 4,
                    bytes: vec![0x00, 0x7f, 0x80, 0xff],
                },
                "Arrow-to-native TypeDesc conversion does not support data type FixedSizeBinary(4)",
            ),
        ];

        for (data_type, literal, expected) in cases {
            assert_eq!(
                encode_write_default_json_for_test(data_type, literal).unwrap_err(),
                expected
            );
        }
    }

    #[test]
    fn rfd_5a_graph_projection_encodes_native_runtime_filter_wire_fields() {
        let build_expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(1),
                qualifier: Some("build".to_string()),
                column: "k".to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        };
        let probe_expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(2),
                qualifier: Some("probe".to_string()),
                column: "k".to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        };
        let build = GraphRuntimeFilterBuild {
            filter_id: 7,
            channel_id: ChannelId::new(7),
            build_expr: build_expr.clone(),
            probe_expr: probe_expr.clone(),
            expr_order: 3,
            execution_mode: JoinExecutionMode::Partitioned,
            source_fragment_id: 4,
            target_fragment_ids: vec![1, 2],
        };
        let probe = GraphRuntimeFilterProbe {
            filter_id: 7,
            channel_id: ChannelId::new(7),
            probe_expr,
            source_fragment_id: 4,
        };

        let encoded_build = encode_graph_runtime_filter_build(&build).expect("encode build");
        let encoded_probe = encode_graph_runtime_filter_probe(&probe).expect("encode probe");

        assert_eq!(encoded_build.filter_id, 7);
        assert_eq!(encoded_build.expr_order, 3);
        assert_eq!(encoded_build.source_fragment_id, 4);
        assert_eq!(encoded_build.target_fragment_ids, vec![1, 2]);
        assert!(encoded_build.build_expr.is_some());
        assert!(encoded_build.probe_expr.is_some());
        assert_eq!(encoded_probe.filter_id, 7);
        assert_eq!(encoded_probe.source_fragment_id, 4);
        assert!(encoded_probe.probe_expr.is_some());
    }

    #[test]
    fn populated_runtime_filter_graph_is_the_only_source_of_native_wire_filters() {
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
                    eq_conditions: vec![
                        crate::sql::planner::physical::PhysicalHashJoinEqCondition {
                            left: probe_expr.clone(),
                            right: build_expr.clone(),
                            null_safe: false,
                        },
                    ],
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

        let distributed =
            crate::sql::planner::distributed::build::build_distributed_plan(&physical)
                .expect("build Graph-owned RF plan");
        assert_eq!(distributed.runtime_filter_graph().channel_count(), 1);
        let encoded = encode_distributed_plan(&distributed).expect("encode Graph-owned RF plan");
        let root = encoded.fragments[0].root.as_ref().expect("encoded root");
        assert_eq!(root.build_runtime_filters.len(), 1);
        assert_eq!(root.build_runtime_filters[0].filter_id, 0);
        assert_eq!(root.build_runtime_filters[0].expr_order, 0);
        assert_eq!(root.children[0].probe_runtime_filters.len(), 1);
        assert_eq!(root.children[0].probe_runtime_filters[0].filter_id, 0);
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref()
        else {
            panic!("expected physical HashJoin root");
        };
        let Some(plan::plan_node::Kind::HashJoin(join)) = physical.kind.as_ref() else {
            panic!("expected HashJoin payload");
        };
        assert_eq!(join.build_runtime_filters.len(), 1);
        assert_eq!(join.build_runtime_filters[0].filter_id, 0);
    }

    #[test]
    fn change_stream_router_encoder_materializes_partition_exprs() {
        let plan = single_fragment_router_plan_for_test();

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");
        let root = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == encoded.root_fragment_id)
            .expect("root fragment");
        let Some(plan::data_sink::Kind::IcebergChangeStreamRouter(router)) =
            root.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected Iceberg change-stream router sink");
        };
        let branch = router.branches.first().expect("router branch");
        assert_eq!(branch.output_partition_ordinals, vec![2]);
        let partition = branch
            .output_partition
            .as_ref()
            .expect("branch output partition");
        assert_eq!(partition.kind, plan::PartitionKind::Hash as i32);
        let [expr] = partition.exprs.as_slice() else {
            panic!("expected one materialized partition expr");
        };
        let Some(expr::Kind::ColumnRef(column_ref)) = expr.kind.as_ref() else {
            panic!("expected partition expr to be a column ref");
        };
        assert_eq!(column_ref.column_id, 3);
    }

    #[test]
    fn stream_sink_projection_and_receiver_schema_follow_edge_output_slots() {
        let plan = two_fragment_stream_plan_for_test();

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");

        let source = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("source fragment");
        let Some(plan::data_sink::Kind::DataStream(sink)) =
            source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected DataStream sink");
        };
        assert_eq!(sink.output_columns, vec![2, 1]);

        let target = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("target fragment");
        let receiver = target.root.as_ref().expect("target root");
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
        else {
            panic!("expected Exchange receiver");
        };
        assert_eq!(
            exchange
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(2, "delta"), (1, "old")]
        );
    }

    #[test]
    fn stream_sink_uses_source_slots_while_receiver_schema_uses_exchange_columns() {
        let plan = two_fragment_stream_plan_with_lowered_slots_for_test();

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");

        let source = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("source fragment");
        let Some(plan::data_sink::Kind::DataStream(sink)) =
            source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected DataStream sink");
        };
        assert_eq!(sink.output_columns, vec![10, 20]);

        let target = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("target fragment");
        let receiver = target.root.as_ref().expect("target root");
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
        else {
            panic!("expected Exchange receiver");
        };
        assert_eq!(
            exchange
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(10, "employee_id"), (20, "name")]
        );
    }

    #[test]
    fn stream_sink_allows_zero_column_values_source() {
        let plan = two_fragment_zero_column_stream_plan_for_test();

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");

        let source = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("source fragment");
        let Some(plan::data_sink::Kind::DataStream(sink)) =
            source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected DataStream sink");
        };
        assert!(sink.output_columns.is_empty());

        let target = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("target fragment");
        let receiver = target.root.as_ref().expect("target root");
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
        else {
            panic!("expected Exchange receiver");
        };
        assert!(exchange.output_columns.is_empty());
    }

    #[test]
    fn encoded_join_output_maps_reconciled_children_not_stale_payload() {
        // The join payload lists a stale id (999) that neither child produces --
        // the divergence a marker/anti join or a pruned probe scan creates. The
        // sealed node-output contract reconciles the join against its children,
        // and the encoder maps that contract 1:1, so the encoded join emits
        // [1, 2], not the stale [1, 2, 999]. Were the reconciliation missing, the
        // encoder (now a pure map of the contract) would emit 999 and the BE sink
        // would fail with "output_columns slot id 999 not found in chunk schema".
        let left = output_column(1, "l_k", DataType::Int64);
        let right = output_column(2, "r_k", DataType::Int64);
        let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 1,
                    fragment_id: 0,
                    tuple_ids: vec![1],
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                    children: vec![
                        DistributedNode {
                            node_id: 2,
                            fragment_id: 0,
                            tuple_ids: vec![2],
                            nullable_tuple_ids: Vec::new(),
                            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                            children: Vec::new(),
                            stats: stats(),
                            payload: DistributedNodeKind::Values(
                                crate::sql::planner::payload::PlanValuesNode {
                                    rows: Vec::new(),
                                    columns: vec![left.clone()],
                                },
                            ),
                        },
                        DistributedNode {
                            node_id: 3,
                            fragment_id: 0,
                            tuple_ids: vec![3],
                            nullable_tuple_ids: Vec::new(),
                            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                            children: Vec::new(),
                            stats: stats(),
                            payload: DistributedNodeKind::Values(
                                crate::sql::planner::payload::PlanValuesNode {
                                    rows: Vec::new(),
                                    columns: vec![right.clone()],
                                },
                            ),
                        },
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
                            output_columns: vec![
                                left.clone(),
                                right.clone(),
                                output_column(999, "stale", DataType::Int64),
                            ],
                        },
                    )),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: vec![left.clone(), right.clone()],
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        };

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");
        let root = encoded.fragments[0].root.as_ref().expect("root");
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref()
        else {
            panic!("expected physical join root");
        };
        assert_eq!(
            physical
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "l_k"), (2, "r_k")],
            "the encoder maps the reconciled contract, dropping the stale id 999"
        );
    }

    #[test]
    fn iceberg_delta_table_encoder_consumes_prepared_binding_payload() {
        use crate::sql::codegen::proto_encode::plan;

        let plan = iceberg_delta_distributed_plan_for_test();
        let source_column = crate::catalog::schema::ColumnDef {
            name: "physical_order_id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        let mut plan =
            crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
        root_scan_for_test(&mut plan)
            .table
            .columns
            .push(column_def_for_test(
                "stale_unprojected",
                DataType::Utf8,
                true,
            ));
        let plan = plan.seal().expect("seal prepared delta fixture");
        let hidden_equality_column = column_def_for_test("tenant_id", DataType::Int64, false);
        let mut bindings = ScanExecutionBindings::default();
        bindings
            .insert_binding(ResolvedScanBinding {
                node_id: 10,
                execution: ResolvedScanExecution::IcebergDelta(ResolvedIcebergDeltaScan {
                    runtime_plan: IcebergDeltaScanRuntimePlan {
                        table_location: "s3://prepared/orders".to_string(),
                        data_columns: Vec::new(),
                        cloud_properties: BTreeMap::from([(
                            "endpoint".to_string(),
                            "http://prepared-minio".to_string(),
                        )]),
                        change_files: Vec::new(),
                        delete_side: None,
                    },
                }),
                physical_columns: vec![ResolvedScanColumn {
                    planner: output_column(1, "bound_order_id", DataType::Int64),
                    source: source_column.clone(),
                    kind: ResolvedScanColumnKind::PhysicalTableColumn,
                }],
                required_reads: vec![
                    ResolvedReadColumn {
                        planner_column_id: Some(ColumnId::new_for_test(1)),
                        source: source_column,
                        reason: ResolvedReadReason::PlannerRequiredOrOutput,
                    },
                    ResolvedReadColumn {
                        planner_column_id: None,
                        source: hidden_equality_column,
                        reason: ResolvedReadReason::EqualityDeleteKey,
                    },
                ],
            })
            .expect("insert prepared delta binding");

        let encoded = plan::encode_distributed_plan_with_context(
            &plan,
            plan::NativePlanEncodeContext {
                scan_bindings: Some(&bindings),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_projection: None,
            },
        )
        .expect("encode prepared delta binding");

        let root = encoded.fragments[0].root.as_ref().expect("encoded root");
        let Some(crate::proto::plan::distributed_node::Payload::Physical(physical)) =
            root.payload.as_ref()
        else {
            panic!("expected physical root");
        };
        let Some(crate::proto::plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
            panic!("expected scan root");
        };
        assert_eq!(scan.columns[0].name, "physical_order_id");
        assert_eq!(
            scan.required_columns,
            vec!["physical_order_id", "tenant_id"]
        );
        let table = scan.table.as_ref().expect("bound table");
        assert_eq!(
            table
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["physical_order_id", "tenant_id"]
        );
        assert!(table.iceberg_row_lineage_metadata_columns.is_empty());
        let Some(crate::proto::plan::scan_source::Kind::IcebergDeltaTable(delta)) = table
            .source
            .as_ref()
            .and_then(|source| source.kind.as_ref())
        else {
            panic!("expected encoded delta source");
        };
        let runtime = delta.delta_plan.as_ref().expect("prepared runtime payload");
        assert_eq!(runtime.table_location, "s3://prepared/orders");
        assert_eq!(
            runtime.cloud_properties.get("endpoint").map(String::as_str),
            Some("http://prepared-minio")
        );
    }

    #[test]
    fn ordinary_iceberg_binding_preserves_existing_encoding() {
        let plan = iceberg_delta_distributed_plan_for_test();
        let mut plan =
            crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
        let scan = root_scan_for_test(&mut plan);
        scan.table.columns.push(column_def_for_test(
            "unprojected_payload",
            DataType::Utf8,
            true,
        ));
        let table = iceberg_table_info_for_test();
        scan.table.source = table_model::ScanSource::IcebergDataFiles {
            table: table.clone(),
            files: Vec::new(),
            cloud_properties: BTreeMap::from([("region".to_string(), "test".to_string())]),
            binding: table_model::IcebergDataFileBinding::CurrentSnapshot,
        };
        scan.required_columns = Some(vec!["order_id".to_string()]);
        let plan = plan.seal().expect("seal ordinary Iceberg fixture");

        let without_binding = encode_distributed_plan(&plan).expect("encode ordinary Iceberg scan");
        let mut bindings = ScanExecutionBindings::default();
        bindings
            .insert_binding(file_binding_for_test(
                10,
                table,
                table_model::IcebergDataFileBinding::CurrentSnapshot,
                vec![bound_column_for_test(
                    1,
                    "order_id",
                    "order_id",
                    ResolvedScanColumnKind::PhysicalTableColumn,
                )],
                vec![bound_read_for_test(Some(1), "order_id")],
            ))
            .expect("insert ordinary Iceberg binding");
        let with_binding = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&bindings),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_projection: None,
            },
        )
        .expect("encode ordinary Iceberg binding");

        assert_eq!(with_binding, without_binding);
    }

    #[test]
    fn refresh_file_bindings_drive_source_projection_metadata_and_hidden_reads() {
        let refresh_sources = [
            table_model::ScanSource::IcebergVersionTable {
                table: iceberg_table_info_for_test(),
                snapshot_id: 1,
            },
            table_model::ScanSource::IcebergMvTargetLocator(
                table_model::IcebergMvTargetLocatorScan {
                    catalog: "ice".to_string(),
                    database: "db".to_string(),
                    table: "orders".to_string(),
                    target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                    target_snapshot_id: Some(1),
                    apply_key_column: "bound_order_id".to_string(),
                    branch_id_column: None,
                },
            ),
            table_model::ScanSource::IcebergMvTargetState(table_model::IcebergMvTargetStateScan {
                catalog: "ice".to_string(),
                database: "db".to_string(),
                table: "orders".to_string(),
                target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                target_snapshot_id: Some(1),
                aggregate_state_layout_version: 1,
                columns: Vec::new(),
                group_key_names: vec!["bound_order_id".to_string()],
                aggregate_state_names: Vec::new(),
                physical_column_names: vec!["bound_order_id".to_string()],
                row_id_column_name: "bound_order_id".to_string(),
                row_filter: table_model::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                    row_id_column_name: "bound_order_id".to_string(),
                    branch_scope: None,
                },
                partition_constraint:
                    table_model::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
            }),
        ];

        for source in refresh_sources {
            let plan = iceberg_delta_distributed_plan_for_test();
            let mut plan =
                crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
            let scan = root_scan_for_test(&mut plan);
            scan.table.source = source;
            scan.table.columns = vec![
                column_def_for_test("stale", DataType::Utf8, true),
                column_def_for_test("stale_unprojected", DataType::Utf8, true),
            ];
            scan.columns = vec![
                output_column(1, "stale", DataType::Utf8),
                output_column(2, "stale_meta", DataType::Int64),
            ];
            let plan = plan.seal().expect("seal refresh-source fixture");

            let mut resolved_table = iceberg_table_info_for_test();
            resolved_table.current_snapshot_id = Some(1);
            resolved_table.location = "s3://resolved/orders".to_string();
            resolved_table.schema.fields[0].name = "physical_order_id".to_string();
            resolved_table
                .schema
                .fields
                .push(table_model::IcebergSchemaFieldDef {
                    field_id: 2,
                    name: "tenant_id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                });
            let mut bindings = ScanExecutionBindings::default();
            bindings
                .insert_binding(file_binding_for_test(
                    10,
                    resolved_table,
                    table_model::IcebergDataFileBinding::ExplicitFiles,
                    vec![
                        ResolvedScanColumn {
                            planner: output_column(1, "bound_order_id", DataType::Int64),
                            source: column_def_for_test(
                                "physical_order_id",
                                DataType::Int64,
                                false,
                            ),
                            kind: ResolvedScanColumnKind::PhysicalTableColumn,
                        },
                        ResolvedScanColumn {
                            planner: output_column(2, "bound_file", DataType::Utf8),
                            source: column_def_for_test("_file", DataType::Utf8, false),
                            kind: ResolvedScanColumnKind::IcebergMetadataColumn,
                        },
                    ],
                    vec![
                        bound_read_for_test(Some(1), "physical_order_id"),
                        ResolvedReadColumn {
                            planner_column_id: None,
                            source: column_def_for_test("tenant_id", DataType::Int64, false),
                            reason: ResolvedReadReason::EqualityDeleteKey,
                        },
                    ],
                ))
                .expect("insert refresh file binding");

            let encoded = encode_distributed_plan_with_context(
                &plan,
                NativePlanEncodeContext {
                    scan_bindings: Some(&bindings),
                    node_outputs: None,
                    fragment_edge_outputs: None,
                    write_contracts: None,
                    runtime_filter_projection: None,
                },
            )
            .expect("encode refresh binding");
            let scan = encoded_root_scan_for_test(&encoded);
            assert_eq!(
                scan.columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["physical_order_id", "_file"]
            );
            assert_eq!(
                scan.required_columns,
                vec!["physical_order_id", "tenant_id"]
            );
            let table = scan.table.as_ref().expect("bound table");
            assert_eq!(
                table
                    .columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["physical_order_id", "tenant_id"],
                "resolver-required sources must encode only binding-owned physical columns and hidden reads"
            );
            assert_eq!(
                table
                    .iceberg_row_lineage_metadata_columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["_file"]
            );
            let Some(crate::proto::plan::scan_source::Kind::IcebergDataFiles(files)) = table
                .source
                .as_ref()
                .and_then(|source| source.kind.as_ref())
            else {
                panic!("refresh source must encode as resolved IcebergDataFiles");
            };
            assert_eq!(
                files.table.as_ref().expect("resolved table").location,
                "s3://resolved/orders"
            );
            assert_eq!(
                files.binding,
                crate::proto::plan::IcebergDataFileBinding::ExplicitFiles as i32
            );
            let (read_columns, variants) = crate::lower::novarocks::scan_read_binding_for_test(
                scan,
                files.table.as_ref().expect("resolved table"),
                &scan.columns,
            )
            .expect("lower bound refresh read plan");
            assert!(
                read_columns.iter().any(|column| column == "tenant_id"),
                "native lowering must resolve hidden equality key from TableDef"
            );
            assert!(variants.is_empty());
        }
    }

    #[test]
    fn required_bindings_reject_missing_node_and_execution_variant_mismatch() {
        let plan = iceberg_delta_distributed_plan_for_test();
        let missing = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&ScanExecutionBindings::default()),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_projection: None,
            },
        )
        .expect_err("delta source without prepared binding must fail");
        assert!(missing.contains("node_id=10"), "{missing}");
        assert!(missing.contains("IcebergDeltaTable"), "{missing}");
        assert!(missing.contains("from_snapshot_id=1"), "{missing}");
        assert!(missing.contains("to_snapshot_id=2"), "{missing}");

        let mut wrong_node = ScanExecutionBindings::default();
        wrong_node
            .insert_binding(delta_binding_for_test(11))
            .expect("insert binding for wrong node");
        let err = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&wrong_node),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_projection: None,
            },
        )
        .expect_err("binding at another node id must not be reused");
        assert!(err.contains("node_id=10"), "{err}");

        let mut wrong_execution = ScanExecutionBindings::default();
        wrong_execution
            .insert_binding(file_binding_for_test(
                10,
                iceberg_table_info_for_test(),
                table_model::IcebergDataFileBinding::ExplicitFiles,
                vec![bound_column_for_test(
                    1,
                    "order_id",
                    "order_id",
                    ResolvedScanColumnKind::PhysicalTableColumn,
                )],
                vec![bound_read_for_test(Some(1), "order_id")],
            ))
            .expect("insert wrong execution variant");
        let err = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&wrong_execution),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_projection: None,
            },
        )
        .expect_err("delta source with file binding must fail");
        assert!(err.contains("execution variant mismatch"), "{err}");
        assert!(err.contains("IcebergFiles"), "{err}");
    }

    #[test]
    fn binding_encoder_preserves_variant_synthetic_output_and_required_name() {
        let plan = iceberg_delta_distributed_plan_for_test();
        let mut plan =
            crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
        let scan = root_scan_for_test(&mut plan);
        let mut table = iceberg_table_info_for_test();
        table.schema.fields[0].name = "v".to_string();
        scan.table.columns = vec![column_def_for_test("v", DataType::LargeBinary, false)];
        scan.table.source = table_model::ScanSource::IcebergDataFiles {
            table: table.clone(),
            files: Vec::new(),
            cloud_properties: BTreeMap::new(),
            binding: table_model::IcebergDataFileBinding::ExplicitFiles,
        };
        scan.columns = vec![
            output_column(1, "v", DataType::LargeBinary),
            OutputColumn {
                column_id: ColumnId::new_for_test(2),
                name: "__nr_var_v_0".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: true,
            },
        ];
        scan.required_columns = Some(vec!["__nr_var_v_0".to_string()]);
        scan.variant_columns = vec![crate::sql::common::ScanVariantColumn {
            source_column_id: ColumnId::new_for_test(1),
            source_column: "v".to_string(),
            synthetic_column_id: ColumnId::new_for_test(2),
            synthetic_column: "__nr_var_v_0".to_string(),
            canonical_path: "$.a.b".to_string(),
            requested_type: DataType::Int64,
            strict: true,
        }];
        let plan = plan.seal().expect("seal variant fixture");
        let mut bindings = ScanExecutionBindings::default();
        bindings
            .insert_binding(file_binding_for_test(
                10,
                table,
                table_model::IcebergDataFileBinding::ExplicitFiles,
                vec![ResolvedScanColumn {
                    planner: output_column(1, "v", DataType::LargeBinary),
                    source: column_def_for_test("v", DataType::LargeBinary, false),
                    kind: ResolvedScanColumnKind::PhysicalTableColumn,
                }],
                Vec::new(),
            ))
            .expect("insert variant binding");

        let encoded = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&bindings),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_projection: None,
            },
        )
        .expect("encode bound VARIANT scan");
        let scan = encoded_root_scan_for_test(&encoded);
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "v"), (2, "__nr_var_v_0")]
        );
        assert_eq!(scan.required_columns, vec!["__nr_var_v_0"]);
        assert_eq!(scan.variant_columns[0].synthetic_column_id, 2);
        let table = scan.table.as_ref().expect("bound table");
        let Some(crate::proto::plan::scan_source::Kind::IcebergDataFiles(files)) = table
            .source
            .as_ref()
            .and_then(|source| source.kind.as_ref())
        else {
            panic!("variant binding must encode as IcebergDataFiles");
        };
        let (read_columns, variants) = crate::lower::novarocks::scan_read_binding_for_test(
            scan,
            files.table.as_ref().expect("resolved table"),
            &scan.columns[1..],
        )
        .expect("lower encoded bound VARIANT scan");
        assert_eq!(read_columns, vec!["v"]);
        assert_eq!(variants, vec![(1, 2)]);
    }

    fn root_scan_for_test(
        plan: &mut crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder,
    ) -> &mut crate::sql::planner::payload::PlanScanNode {
        let DistributedNodeKind::Scan(scan) = &mut plan.fragments_mut()[0].root.payload else {
            panic!("expected root scan");
        };
        scan
    }

    fn encoded_root_scan_for_test(plan: &plan::DistributedPlan) -> &plan::ScanNode {
        let root = plan.fragments[0].root.as_ref().expect("encoded root");
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref()
        else {
            panic!("expected physical root");
        };
        let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
            panic!("expected scan root");
        };
        scan
    }

    fn file_binding_for_test(
        node_id: i32,
        table: table_model::IcebergTableInfo,
        file_binding: table_model::IcebergDataFileBinding,
        physical_columns: Vec<ResolvedScanColumn>,
        required_reads: Vec<ResolvedReadColumn>,
    ) -> ResolvedScanBinding {
        ResolvedScanBinding {
            node_id,
            execution: ResolvedScanExecution::IcebergFiles(ResolvedIcebergFileScan {
                table,
                files: Vec::new(),
                cloud_properties: BTreeMap::from([("region".to_string(), "test".to_string())]),
                binding: file_binding,
            }),
            physical_columns,
            required_reads,
        }
    }

    fn delta_binding_for_test(node_id: i32) -> ResolvedScanBinding {
        ResolvedScanBinding {
            node_id,
            execution: ResolvedScanExecution::IcebergDelta(ResolvedIcebergDeltaScan {
                runtime_plan: IcebergDeltaScanRuntimePlan {
                    table_location: "s3://prepared/orders".to_string(),
                    data_columns: Vec::new(),
                    cloud_properties: BTreeMap::new(),
                    change_files: Vec::new(),
                    delete_side: None,
                },
            }),
            physical_columns: vec![bound_column_for_test(
                1,
                "order_id",
                "order_id",
                ResolvedScanColumnKind::PhysicalTableColumn,
            )],
            required_reads: vec![bound_read_for_test(Some(1), "order_id")],
        }
    }

    fn bound_column_for_test(
        id: u32,
        planner_name: &str,
        source_name: &str,
        kind: ResolvedScanColumnKind,
    ) -> ResolvedScanColumn {
        ResolvedScanColumn {
            planner: output_column(id, planner_name, DataType::Int64),
            source: column_def_for_test(source_name, DataType::Int64, false),
            kind,
        }
    }

    fn bound_read_for_test(planner_id: Option<u32>, source_name: &str) -> ResolvedReadColumn {
        ResolvedReadColumn {
            planner_column_id: planner_id.map(ColumnId::new_for_test),
            source: column_def_for_test(source_name, DataType::Int64, false),
            reason: if planner_id.is_some() {
                ResolvedReadReason::PlannerRequiredOrOutput
            } else {
                ResolvedReadReason::EqualityDeleteKey
            },
        }
    }

    fn column_def_for_test(
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> crate::catalog::schema::ColumnDef {
        crate::catalog::schema::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_delta_distributed_plan_for_test() -> DistributedPlan {
        let output_columns = vec![output_column(1, "order_id", DataType::Int64)];
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 10,
                    fragment_id: 0,
                    tuple_ids: vec![10],
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                    children: Vec::new(),
                    stats: stats(),
                    payload: DistributedNodeKind::Scan(
                        crate::sql::planner::payload::PlanScanNode {
                            database: "db".to_string(),
                            table: iceberg_delta_table_for_test(),
                            alias: None,
                            columns: output_columns.clone(),
                            predicates: Vec::new(),
                            required_columns: None,
                            variant_columns: Vec::new(),
                            mv_rewritten_from: None,
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
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        }
    }

    fn iceberg_delta_table_for_test() -> table_model::TableDef {
        table_model::TableDef {
            name: "orders".to_string(),
            columns: vec![crate::catalog::schema::ColumnDef {
                name: "order_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: table_model::ScanSource::IcebergDeltaTable {
                table: iceberg_table_info_for_test(),
                from_snapshot_id: 1,
                to_snapshot_id: 2,
            },
        }
    }

    fn iceberg_table_info_for_test() -> table_model::IcebergTableInfo {
        table_model::IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(2),
            schema_id: 1,
            location: "file:///warehouse/orders".to_string(),
            schema: table_model::IcebergSchemaDef {
                fields: vec![table_model::IcebergSchemaFieldDef {
                    field_id: 1,
                    name: "order_id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                }],
            },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    #[test]
    fn stream_sink_derives_generate_series_source_schema() {
        let plan = two_fragment_generate_series_stream_plan_for_test();

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");

        let source = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("source fragment");
        let Some(plan::data_sink::Kind::DataStream(sink)) =
            source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected DataStream sink");
        };
        assert_eq!(sink.output_columns, vec![7]);

        let target = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("target fragment");
        let receiver = target.root.as_ref().expect("target root");
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
        else {
            panic!("expected Exchange receiver");
        };
        assert_eq!(
            exchange
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str(), column.nullable))
                .collect::<Vec<_>>(),
            vec![(7, "generate_series", false)]
        );
    }

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
                payload: DistributedNodeKind::Values(
                    crate::sql::planner::payload::PlanValuesNode {
                        rows: Vec::new(),
                        columns: child_columns,
                    },
                ),
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
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        };

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");
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
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        };

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");
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
            runtime_filter_graph: RuntimeFilterGraph::default(),
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
        let encoded = encode_distributed_plan(&plan).expect("encode native plan");
        let root = encoded.fragments[0].root.as_ref().expect("encoded root");
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref()
        else {
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
            runtime_filter_graph: RuntimeFilterGraph::default(),
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
        let encoded = encode_distributed_plan(&plan).expect("encode native plan");
        let root = encoded.fragments[0].root.as_ref().expect("encoded root");
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref()
        else {
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
                payload: DistributedNodeKind::Values(
                    crate::sql::planner::payload::PlanValuesNode {
                        rows: Vec::new(),
                        columns: vec![child_column.clone()],
                    },
                ),
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
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        };

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");
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
                payload: DistributedNodeKind::Values(
                    crate::sql::planner::payload::PlanValuesNode {
                        rows: Vec::new(),
                        columns: child_columns.clone(),
                    },
                ),
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
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        };

        let encoded = encode_distributed_plan(&plan).expect("encode native plan");
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

    fn two_fragment_stream_plan_for_test() -> DistributedPlan {
        let source_columns = vec![
            output_column(1, "old", DataType::Int64),
            output_column(2, "delta", DataType::Int64),
        ];
        let receiver_columns = vec![source_columns[1].clone(), source_columns[0].clone()];
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![
                PlanFragment {
                    fragment_id: 1,
                    root: DistributedNode {
                        node_id: 10,
                        fragment_id: 1,
                        tuple_ids: vec![10],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::Values(
                            crate::sql::planner::payload::PlanValuesNode {
                                rows: Vec::new(),
                                columns: source_columns.clone(),
                            },
                        ),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: source_columns,
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 0,
                    root: DistributedNode {
                        node_id: 20,
                        fragment_id: 0,
                        tuple_ids: vec![20],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                            partition: DataPartition::unpartitioned(),
                            source_fragment_id: 1,
                            output_columns: receiver_columns,
                            output_qualifier: None,
                            flavor: ExchangeFlavor::Distribution,
                        }),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Result,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            ],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 20,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![2, 1],
            }],
        }
    }

    fn two_fragment_stream_plan_with_lowered_slots_for_test() -> DistributedPlan {
        let source_columns = vec![
            output_column(10, "employee_id", DataType::Int64),
            output_column(20, "name", DataType::Utf8),
            output_column(30, "title", DataType::Utf8),
        ];
        let receiver_columns = source_columns[..2].to_vec();
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![
                PlanFragment {
                    fragment_id: 1,
                    root: DistributedNode {
                        node_id: 10,
                        fragment_id: 1,
                        tuple_ids: vec![10],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::Values(
                            crate::sql::planner::payload::PlanValuesNode {
                                rows: Vec::new(),
                                columns: source_columns.clone(),
                            },
                        ),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: source_columns,
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 0,
                    root: DistributedNode {
                        node_id: 20,
                        fragment_id: 0,
                        tuple_ids: vec![20],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                            partition: DataPartition::unpartitioned(),
                            source_fragment_id: 1,
                            output_columns: receiver_columns,
                            output_qualifier: None,
                            flavor: ExchangeFlavor::Distribution,
                        }),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Result,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            ],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 20,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![43, 44],
            }],
        }
    }

    fn two_fragment_zero_column_stream_plan_for_test() -> DistributedPlan {
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![
                PlanFragment {
                    fragment_id: 1,
                    root: DistributedNode {
                        node_id: 10,
                        fragment_id: 1,
                        tuple_ids: vec![10],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::Values(
                            crate::sql::planner::payload::PlanValuesNode {
                                rows: vec![Vec::new()],
                                columns: Vec::new(),
                            },
                        ),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 0,
                    root: DistributedNode {
                        node_id: 20,
                        fragment_id: 0,
                        tuple_ids: vec![20],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                            partition: DataPartition::unpartitioned(),
                            source_fragment_id: 1,
                            output_columns: Vec::new(),
                            output_qualifier: None,
                            flavor: ExchangeFlavor::Distribution,
                        }),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Result,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            ],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 20,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: Vec::new(),
            }],
        }
    }

    fn two_fragment_generate_series_stream_plan_for_test() -> DistributedPlan {
        let output_columns = vec![output_column(7, "generate_series", DataType::Int64)];
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![
                PlanFragment {
                    fragment_id: 1,
                    root: DistributedNode {
                        node_id: 10,
                        fragment_id: 1,
                        tuple_ids: vec![10],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::GenerateSeries(
                            crate::sql::planner::payload::PlanGenerateSeriesNode {
                                start: 1,
                                end: 3,
                                step: 1,
                                column_name: "generate_series".to_string(),
                                alias: None,
                                output_column_id: ColumnId::new_for_test(7),
                            },
                        ),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 0,
                    root: DistributedNode {
                        node_id: 20,
                        fragment_id: 0,
                        tuple_ids: vec![20],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                            partition: DataPartition::unpartitioned(),
                            source_fragment_id: 1,
                            output_columns,
                            output_qualifier: None,
                            flavor: ExchangeFlavor::Distribution,
                        }),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Result,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            ],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 20,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![7],
            }],
        }
    }

    fn single_fragment_router_plan_for_test() -> DistributedPlan {
        let output_columns = vec![
            output_column(1, "op", DataType::Int32),
            output_column(2, "route", DataType::Int32),
            output_column(3, "bucket", DataType::Int32),
        ];
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 10,
                    fragment_id: 0,
                    tuple_ids: vec![10],
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                    children: Vec::new(),
                    stats: stats(),
                    payload: DistributedNodeKind::Values(
                        crate::sql::planner::payload::PlanValuesNode {
                            rows: Vec::new(),
                            columns: output_columns.clone(),
                        },
                    ),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::IcebergChangeStreamRouter(IcebergChangeStreamRouterSink {
                    group_id: 0,
                    change_op_output_ordinal: 0,
                    data_route_output_ordinal: Some(1),
                    branches: vec![IcebergChangeStreamBranchRoute {
                        branch_id: 0,
                        branch_kind: ChangeStreamBranchKind::DeleteDv,
                        target_fragment_id: 1,
                        target_exchange_node_id: 20,
                        output_ordinals: vec![2],
                        output_partition_ordinals: vec![2],
                    }],
                }),
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        }
    }

    fn output_column(id: u32, name: &str, data_type: DataType) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable: false,
            is_internal: false,
        }
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 1.0,
            row_count_confidence: PlannerConfidence::Exact,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }
}
