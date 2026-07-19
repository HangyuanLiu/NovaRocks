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
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use thrift::OrderedFloat;

use crate::common::thrift::thrift_compact_serialize;
use crate::exec::expr::ExprArena;
use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
use crate::exec::row_position::RowPositionDescriptor;
use crate::novarocks_connectors::ConnectorRegistry;

use crate::common::config::debug_exec_node_output;
use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
#[cfg(feature = "compat")]
use crate::exec::operators::OlapTableSinkFactory;
use crate::exec::operators::{
    DataStreamSinkFactory, DataStreamSinkFactoryInput, IcebergChangeStreamRouterBranchFactoryInput,
    IcebergChangeStreamRouterSinkFactory, IcebergChangeStreamRouterSinkFactoryInput,
    IcebergTableSinkFactory, MultiCastDataStreamSinkFactory, NoopSinkFactory,
    ResultBufferSinkFactory, SplitDataStreamSinkFactory,
};
use crate::exec::pipeline::executor::{
    execute_compat_plan_with_pipeline, execute_compat_plan_with_pipeline_with_root_sink_dop,
};
use crate::protocol::common::error::FieldPath;
use crate::protocol::starrocks::decode::layout::{
    build_tuple_slot_order, infer_tuple_slot_order, reorder_tuple_slots,
};
use crate::protocol::starrocks::decode::node::{Lowered, lower_plan};
use crate::protocol::starrocks::decode::type_lowering::{
    native_primitive_type_from_desc, render_schema_from_type_desc,
};
use crate::protocol::starrocks::decode::{
    StarRocksExternalDependency, StarRocksExternalDependencyDraft, decode_fragment_destination,
    decode_query_options, decode_runtime_endpoint, decode_runtime_filter_params,
};
use crate::runtime::endpoint::FragmentDestination;
use crate::runtime::fragment::runtime_state::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::runtime::fragment_output::FragmentOutput;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_filter_params::RuntimeFilterParams;
use crate::service::result_batch_wire::{ResultProjection, ResultSinkConfig};
use crate::thrift::{data, data_sinks, descriptors, internal_service, planner, types};
use crate::types::PrimitiveType;

enum FragmentDecodeAttempt<T> {
    Ready(T),
    Pending(Vec<StarRocksExternalDependency>),
    DecodeError(String),
}

fn classify_fragment_decode_attempt<T>(
    result: Result<T, String>,
    draft: &StarRocksExternalDependencyDraft,
) -> FragmentDecodeAttempt<T> {
    match result {
        Err(error) => FragmentDecodeAttempt::DecodeError(error),
        Ok(value) => {
            let requirements = draft.external_dependencies();
            if requirements.is_empty() {
                FragmentDecodeAttempt::Ready(value)
            } else {
                // Discard the draft value: it may contain dependency placeholders.
                FragmentDecodeAttempt::Pending(requirements)
            }
        }
    }
}

fn process_fragment_decode_attempt<T>(
    attempt: FragmentDecodeAttempt<T>,
    mut resolve: impl FnMut(&StarRocksExternalDependency) -> Result<bool, String>,
) -> Result<Option<T>, String> {
    match attempt {
        FragmentDecodeAttempt::Ready(value) => Ok(Some(value)),
        FragmentDecodeAttempt::DecodeError(error) => Err(error),
        FragmentDecodeAttempt::Pending(requirements) => {
            let mut resolved = 0usize;
            for requirement in &requirements {
                resolved += usize::from(resolve(requirement)?);
            }
            if resolved != requirements.len() {
                return Err(format!(
                    "StarRocks plan decode dependency resolution completed {resolved}/{} requirements",
                    requirements.len(),
                ));
            }
            Ok(None)
        }
    }
}

fn merge_row_pos_descs(
    target: &mut HashMap<i32, RowPositionDescriptor>,
    incoming: &HashMap<i32, RowPositionDescriptor>,
) -> Result<(), String> {
    for (tuple_id, desc) in incoming {
        match target.get(tuple_id) {
            None => {
                target.insert(*tuple_id, desc.clone());
            }
            Some(existing) => {
                if existing.row_position_type != desc.row_position_type
                    || existing.row_source_slot != desc.row_source_slot
                    || existing.fetch_ref_slots != desc.fetch_ref_slots
                    || existing.lookup_ref_slots != desc.lookup_ref_slots
                {
                    return Err(format!(
                        "conflicting row position descriptor for tuple_id={}",
                        tuple_id
                    ));
                }
            }
        }
    }
    Ok(())
}

fn collect_glm_metadata(
    node: &ExecNode,
    row_pos_descs: &mut HashMap<i32, RowPositionDescriptor>,
) -> Result<(), String> {
    match &node.kind {
        ExecNodeKind::LookUp(lookup) => {
            merge_row_pos_descs(row_pos_descs, &lookup.row_pos_descs)?;
        }
        ExecNodeKind::Fetch(fetch) => {
            merge_row_pos_descs(row_pos_descs, &fetch.row_pos_descs)?;
            collect_glm_metadata(&fetch.input, row_pos_descs)?;
        }
        ExecNodeKind::AssertNumRows(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Project(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Filter(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Repeat(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::ChangeEventExpand(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::UnionAll(node) => {
            for input in &node.inputs {
                collect_glm_metadata(input, row_pos_descs)?;
            }
        }
        ExecNodeKind::Limit(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::ExchangeSource(_) => {}
        ExecNodeKind::Scan(_) => {}
        ExecNodeKind::Aggregate(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Join(node) => {
            collect_glm_metadata(&node.left, row_pos_descs)?;
            collect_glm_metadata(&node.right, row_pos_descs)?;
        }
        ExecNodeKind::NestedLoopJoin(node) => {
            collect_glm_metadata(&node.left, row_pos_descs)?;
            collect_glm_metadata(&node.right, row_pos_descs)?;
        }
        ExecNodeKind::Sort(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::TableFunction(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Analytic(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::SetOp(node) => {
            for input in &node.inputs {
                collect_glm_metadata(input, row_pos_descs)?;
            }
        }
        ExecNodeKind::NativeRuntimeFilterConsumer(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Values(_) => {}
        ExecNodeKind::IcebergDeltaScan(_) => {}
    }
    Ok(())
}

fn unique_id_from_exec_params(exec_params: &internal_service::TPlanFragmentExecParams) -> UniqueId {
    UniqueId {
        hi: exec_params.fragment_instance_id.hi,
        lo: exec_params.fragment_instance_id.lo,
    }
}

fn runtime_destination_from_thrift(
    dest: &data_sinks::TPlanFragmentDestination,
    path: FieldPath,
) -> Result<FragmentDestination, String> {
    decode_fragment_destination(dest, path).map_err(|error| error.to_string())
}

fn runtime_destinations_from_thrift(
    destinations: Vec<data_sinks::TPlanFragmentDestination>,
    path: FieldPath,
) -> Result<Vec<FragmentDestination>, String> {
    destinations
        .iter()
        .enumerate()
        .map(|(index, destination)| {
            runtime_destination_from_thrift(destination, path.clone().index(index))
        })
        .collect()
}

fn lower_stream_partition_exprs(
    stream: &data_sinks::TDataStreamSink,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<Vec<crate::exec::expr::ExprId>, String> {
    let partition_type =
        crate::protocol::starrocks::decode::sink::decode_data_stream_partition_type(
            stream.output_partition.type_,
        )?;
    if !partition_type.requires_exprs() {
        return Ok(Vec::new());
    }
    stream
        .output_partition
        .partition_exprs
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            crate::protocol::starrocks::decode::expr::lower_t_expr(
                expr,
                arena,
                layout,
                last_query_id,
                external_dependencies,
            )
            .map_err(|err| format!("DATA_STREAM_SINK partition expr[{idx}]: {err}"))
        })
        .collect()
}

fn data_stream_input_from_compat(
    stream: &data_sinks::TDataStreamSink,
    destinations: Vec<data_sinks::TPlanFragmentDestination>,
    destinations_path: FieldPath,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<DataStreamSinkFactoryInput, String> {
    let partition_exprs =
        lower_stream_partition_exprs(stream, arena, layout, last_query_id, external_dependencies)?;
    let partition_type =
        crate::protocol::starrocks::decode::sink::decode_data_stream_partition_type(
            stream.output_partition.type_,
        )?;
    DataStreamSinkFactoryInput::try_new(
        stream.dest_node_id,
        partition_type,
        Vec::new(),
        partition_exprs,
        stream.output_columns.clone().unwrap_or_default(),
        runtime_destinations_from_thrift(destinations, destinations_path)?,
    )
}

fn multi_cast_inputs_from_compat(
    multi_cast: &data_sinks::TMultiCastDataStreamSink,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<Vec<(DataStreamSinkFactoryInput, Option<i64>)>, String> {
    if multi_cast.sinks.len() != multi_cast.destinations.len() {
        return Err(format!(
            "MULTI_CAST_DATA_STREAM_SINK: sinks size {} != destinations size {}",
            multi_cast.sinks.len(),
            multi_cast.destinations.len()
        ));
    }
    multi_cast
        .sinks
        .iter()
        .zip(multi_cast.destinations.iter())
        .enumerate()
        .map(|(branch_index, (stream, destinations))| {
            Ok((
                data_stream_input_from_compat(
                    stream,
                    destinations.clone(),
                    FieldPath::root("exec_plan_fragment")
                        .field("fragment")
                        .field("output_sink")
                        .field("multi_cast_stream_sink")
                        .field("destinations")
                        .index(branch_index),
                    arena,
                    layout,
                    last_query_id,
                    external_dependencies,
                )?,
                stream.limit,
            ))
        })
        .collect()
}

fn split_inputs_from_compat(
    split: &data_sinks::TSplitDataStreamSink,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<Vec<DataStreamSinkFactoryInput>, String> {
    let sinks = split.sinks.as_ref().cloned().unwrap_or_default();
    let destinations = split.destinations.as_ref().cloned().unwrap_or_default();
    if sinks.len() != destinations.len() {
        return Err(format!(
            "SPLIT_DATA_STREAM_SINK: sinks size {} != destinations size {}",
            sinks.len(),
            destinations.len()
        ));
    }
    sinks
        .iter()
        .zip(destinations)
        .enumerate()
        .map(|(branch_index, (stream, destinations))| {
            data_stream_input_from_compat(
                stream,
                destinations,
                FieldPath::root("exec_plan_fragment")
                    .field("fragment")
                    .field("output_sink")
                    .field("split_stream_sink")
                    .field("destinations")
                    .index(branch_index),
                arena,
                layout,
                last_query_id,
                external_dependencies,
            )
        })
        .collect()
}

fn iceberg_router_input_from_compat(
    router: &data_sinks::TIcebergChangeStreamRouterSink,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<IcebergChangeStreamRouterSinkFactoryInput, String> {
    let branches = router
        .branches
        .iter()
        .enumerate()
        .map(|(branch_index, branch)| {
            let branch_kind = branch_kind_from_thrift(branch.branch_kind)?;
            Ok(IcebergChangeStreamRouterBranchFactoryInput {
                branch_id: branch.branch_id,
                branch_kind,
                stream_sink: data_stream_input_from_compat(
                    &branch.stream_sink,
                    branch.destinations.clone(),
                    FieldPath::root("exec_plan_fragment")
                        .field("fragment")
                        .field("output_sink")
                        .field("iceberg_change_stream_router_sink")
                        .field("branches")
                        .index(branch_index)
                        .field("destinations"),
                    arena,
                    layout,
                    last_query_id,
                    external_dependencies,
                )?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(IcebergChangeStreamRouterSinkFactoryInput {
        change_op_slot_id: router.change_op_slot_id,
        data_route_slot_id: router.data_route_slot_id,
        branches,
    })
}

enum PreparedFragmentSink {
    DataStream {
        input: DataStreamSinkFactoryInput,
        fragment_instance_id: UniqueId,
        sender_id: Option<i32>,
    },
    MultiCast {
        inputs: Vec<(DataStreamSinkFactoryInput, Option<i64>)>,
        fragment_instance_id: UniqueId,
        sender_id: Option<i32>,
    },
    Split {
        inputs: Vec<DataStreamSinkFactoryInput>,
        split_expr_ids: Vec<crate::exec::expr::ExprId>,
        fragment_instance_id: UniqueId,
        sender_id: Option<i32>,
    },
    IcebergChangeStreamRouter {
        input: IcebergChangeStreamRouterSinkFactoryInput,
        fragment_instance_id: UniqueId,
        sender_id: Option<i32>,
    },
    Result {
        config: ResultSinkConfig,
        projections: Option<Vec<ResultProjection>>,
        exchange_finst_id: Option<(i64, i64)>,
    },
    Noop {
        exchange_finst_id: Option<(i64, i64)>,
    },
    Iceberg {
        input: crate::connector::iceberg::sink_plan::IcebergSinkFactoryInput,
        root_sink_dop: Option<i32>,
    },
    #[cfg(feature = "compat")]
    Olap {
        input: crate::connector::starrocks::sink::plan::StarRocksSinkFactoryInput,
    },
}

fn exchange_finst_id(
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
) -> Option<(i64, i64)> {
    exec_params.map(|params| {
        (
            params.fragment_instance_id.hi,
            params.fragment_instance_id.lo,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn decode_fragment_sink(
    sink: &data_sinks::TDataSink,
    fragment: &planner::TPlanFragment,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    arena: &mut ExprArena,
    lowered: &Lowered,
    last_query_id: Option<&str>,
    session_time_zone: Option<&str>,
    external_dependencies: &StarRocksExternalDependencyDraft,
) -> Result<PreparedFragmentSink, String> {
    match sink.type_ {
        data_sinks::TDataSinkType::DATA_STREAM_SINK => {
            let stream_sink = sink
                .stream_sink
                .as_ref()
                .ok_or_else(|| "DATA_STREAM_SINK missing stream_sink payload".to_string())?;
            let exec_params =
                exec_params.ok_or_else(|| "DATA_STREAM_SINK requires exec_params".to_string())?;
            let input = data_stream_input_from_compat(
                stream_sink,
                exec_params.destinations.clone().unwrap_or_default(),
                FieldPath::root("exec_plan_fragment")
                    .field("params")
                    .field("destinations"),
                arena,
                &lowered.layout,
                last_query_id,
                Some(external_dependencies),
            )?;
            Ok(PreparedFragmentSink::DataStream {
                input,
                fragment_instance_id: unique_id_from_exec_params(exec_params),
                sender_id: exec_params.sender_id,
            })
        }
        data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
            let multi_cast = sink.multi_cast_stream_sink.as_ref().ok_or_else(|| {
                "MULTI_CAST_DATA_STREAM_SINK missing multi_cast_stream_sink payload".to_string()
            })?;
            let exec_params = exec_params
                .ok_or_else(|| "MULTI_CAST_DATA_STREAM_SINK requires exec_params".to_string())?;
            let inputs = multi_cast_inputs_from_compat(
                multi_cast,
                arena,
                &lowered.layout,
                last_query_id,
                Some(external_dependencies),
            )?;
            Ok(PreparedFragmentSink::MultiCast {
                inputs,
                fragment_instance_id: unique_id_from_exec_params(exec_params),
                sender_id: exec_params.sender_id,
            })
        }
        data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
            let split = sink.split_stream_sink.as_ref().ok_or_else(|| {
                "SPLIT_DATA_STREAM_SINK missing split_stream_sink payload".to_string()
            })?;
            let exec_params = exec_params
                .ok_or_else(|| "SPLIT_DATA_STREAM_SINK requires exec_params".to_string())?;
            let split_exprs = split
                .split_exprs
                .as_ref()
                .ok_or_else(|| "SPLIT_DATA_STREAM_SINK missing split_exprs payload".to_string())?;
            let split_expr_ids = split_exprs
                .iter()
                .map(|expr| {
                    crate::protocol::starrocks::decode::expr::lower_t_expr(
                        expr,
                        arena,
                        &lowered.layout,
                        last_query_id,
                        Some(external_dependencies),
                    )
                })
                .collect::<Result<Vec<_>, String>>()?;
            let inputs = split_inputs_from_compat(
                split,
                arena,
                &lowered.layout,
                last_query_id,
                Some(external_dependencies),
            )?;
            Ok(PreparedFragmentSink::Split {
                inputs,
                split_expr_ids,
                fragment_instance_id: unique_id_from_exec_params(exec_params),
                sender_id: exec_params.sender_id,
            })
        }
        data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK => {
            let router = sink
                .iceberg_change_stream_router_sink
                .as_ref()
                .ok_or_else(|| {
                    "ICEBERG_CHANGE_STREAM_ROUTER_SINK missing iceberg_change_stream_router_sink"
                        .to_string()
                })?;
            let exec_params = exec_params.ok_or_else(|| {
                "ICEBERG_CHANGE_STREAM_ROUTER_SINK requires exec_params".to_string()
            })?;
            let input = iceberg_router_input_from_compat(
                router,
                arena,
                &lowered.layout,
                last_query_id,
                Some(external_dependencies),
            )?;
            Ok(PreparedFragmentSink::IcebergChangeStreamRouter {
                input,
                fragment_instance_id: unique_id_from_exec_params(exec_params),
                sender_id: exec_params.sender_id,
            })
        }
        data_sinks::TDataSinkType::RESULT_SINK => {
            let result_sink = sink
                .result_sink
                .as_ref()
                .ok_or_else(|| "RESULT_SINK missing result_sink payload".to_string())?;
            Ok(PreparedFragmentSink::Result {
                config: result_sink_config_from_thrift(result_sink)?,
                projections: result_projections_from_thrift_exprs(fragment.output_exprs.as_ref())?,
                exchange_finst_id: exchange_finst_id(exec_params),
            })
        }
        data_sinks::TDataSinkType::NOOP_SINK | data_sinks::TDataSinkType::SCHEMA_TABLE_SINK => {
            Ok(PreparedFragmentSink::Noop {
                exchange_finst_id: exchange_finst_id(exec_params),
            })
        }
        data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
        | data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
        | data_sinks::TDataSinkType::ICEBERG_DV_SINK
        | data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK => {
            let sink_type_name = iceberg_sink_type_name(sink.type_);
            let iceberg_sink = sink
                .iceberg_table_sink
                .as_ref()
                .ok_or_else(|| format!("{sink_type_name} missing iceberg_table_sink payload"))?;
            let output_exprs = fragment
                .output_exprs
                .as_ref()
                .ok_or_else(|| format!("{sink_type_name} missing output_exprs"))?;
            let desc_tbl =
                desc_tbl.ok_or_else(|| format!("{sink_type_name} requires descriptor table"))?;
            let sink_mode =
                crate::protocol::starrocks::decode::sink::iceberg::iceberg_sink_mode_for_type(
                    sink.type_,
                );
            let input = crate::protocol::starrocks::decode::sink::iceberg::lower_iceberg_sink_factory_input(
                iceberg_sink,
                sink_mode,
                output_exprs,
                &lowered.layout,
                desc_tbl,
                last_query_id,
                Some(external_dependencies),
            )?;
            Ok(PreparedFragmentSink::Iceberg {
                input,
                root_sink_dop: (sink_mode
                    == crate::connector::iceberg::IcebergSinkMode::DeletionVectors)
                    .then_some(1),
            })
        }
        data_sinks::TDataSinkType::OLAP_TABLE_SINK => {
            #[cfg(feature = "compat")]
            {
                let olap_sink = sink
                    .olap_table_sink
                    .as_ref()
                    .ok_or_else(|| "OLAP_TABLE_SINK missing olap_table_sink payload".to_string())?;
                let draft_plan = ExecPlan {
                    arena: arena.clone(),
                    root: lowered.node.clone(),
                };
                let input = crate::protocol::starrocks::decode::sink::starrocks::lower_starrocks_sink_factory_input(
                    olap_sink,
                    fragment.output_exprs.as_deref(),
                    Some(&draft_plan),
                    Some(&lowered.layout),
                    last_query_id,
                    session_time_zone,
                    Some(external_dependencies),
                )?;
                Ok(PreparedFragmentSink::Olap { input })
            }
            #[cfg(not(feature = "compat"))]
            Err("OLAP_TABLE_SINK requires the compat feature".to_string())
        }
        other => Err(format!(
            "unsupported sink type: {:?}. Only DATA_STREAM_SINK, MULTI_CAST_DATA_STREAM_SINK, SPLIT_DATA_STREAM_SINK, ICEBERG_CHANGE_STREAM_ROUTER_SINK, RESULT_SINK, NOOP_SINK, SCHEMA_TABLE_SINK, ICEBERG_TABLE_SINK, ICEBERG_DELETE_SINK, ICEBERG_DV_SINK, ICEBERG_EQUALITY_DELETE_SINK, and OLAP_TABLE_SINK are supported",
            other
        )),
    }
}

fn branch_kind_from_thrift(
    value: data_sinks::TIcebergChangeStreamRouterBranchKind,
) -> Result<crate::sql::common::ChangeStreamBranchKind, String> {
    use crate::sql::common::ChangeStreamBranchKind;

    match value {
        data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV => {
            Ok(ChangeStreamBranchKind::DeleteDv)
        }
        data_sinks::TIcebergChangeStreamRouterBranchKind::REUSE_DATA => {
            Ok(ChangeStreamBranchKind::ReuseData)
        }
        data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA => {
            Ok(ChangeStreamBranchKind::FreshData)
        }
        _ => Err(format!(
            "unsupported Iceberg change-stream router branch kind {}",
            value.0
        )),
    }
}

fn iceberg_sink_type_name(t: data_sinks::TDataSinkType) -> &'static str {
    match t {
        data_sinks::TDataSinkType::ICEBERG_DELETE_SINK => "ICEBERG_DELETE_SINK",
        data_sinks::TDataSinkType::ICEBERG_DV_SINK => "ICEBERG_DV_SINK",
        data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK => "ICEBERG_EQUALITY_DELETE_SINK",
        _ => "ICEBERG_TABLE_SINK",
    }
}

fn result_sink_config_from_thrift(
    result_sink: &data_sinks::TResultSink,
) -> Result<ResultSinkConfig, String> {
    let sink_type = result_sink
        .type_
        .unwrap_or(data_sinks::TResultSinkType::MYSQL_PROTOCAL);
    match sink_type {
        t if t == data_sinks::TResultSinkType::MYSQL_PROTOCAL => Ok(ResultSinkConfig::mysql()),
        t if t == data_sinks::TResultSinkType::HTTP_PROTOCAL => {
            let format = result_sink
                .format
                .unwrap_or(data_sinks::TResultSinkFormatType::JSON);
            if format != data_sinks::TResultSinkFormatType::JSON {
                return Err(format!(
                    "HTTP_PROTOCAL result sink only supports JSON format, got {:?}",
                    format
                ));
            }
            Ok(ResultSinkConfig::http_json())
        }
        t if t == data_sinks::TResultSinkType::STATISTIC => {
            Ok(ResultSinkConfig::statistic(thrift_statistic_row_encoder))
        }
        other => Err(format!("unsupported RESULT_SINK type {:?}", other)),
    }
}

const STATISTIC_DATA_VERSION_V1: i32 = 1;
const STATISTIC_HISTOGRAM_VERSION: i32 = 2;
const STATISTIC_TABLE_VERSION: i32 = 3;
const STATISTIC_BATCH_VERSION: i32 = 4;
const STATISTIC_EXTERNAL_VERSION: i32 = 5;
const STATISTIC_EXTERNAL_QUERY_VERSION: i32 = 6;
const STATISTIC_EXTERNAL_HISTOGRAM_VERSION: i32 = 7;
const STATISTIC_EXTERNAL_QUERY_VERSION_V2: i32 = 8;
const STATISTIC_BATCH_VERSION_V5: i32 = 9;
const STATISTIC_DATA_VERSION_V2: i32 = 10;
const STATISTIC_PARTITION_VERSION: i32 = 11;
const STATISTIC_MULTI_COLUMN_VERSION: i32 = 12;
const STATISTIC_QUERY_MULTI_COLUMN_VERSION: i32 = 13;
const STATISTIC_PARTITION_VERSION_V2: i32 = 20;
const STATISTIC_DICT_VERSION: i32 = 101;

fn field_bytes<'a>(
    fields: &'a [Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<Option<&'a [u8]>, String> {
    let value = fields
        .get(idx)
        .ok_or_else(|| format!("missing field {field_name} at column {idx}"))?;
    Ok(value.as_deref())
}

fn field_optional_i64(
    fields: &[Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<Option<i64>, String> {
    let Some(raw) = field_bytes(fields, idx, field_name)? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(raw)
        .map_err(|e| format!("field {field_name} is not valid UTF-8: {e}"))?;
    text.parse::<i64>()
        .map(Some)
        .map_err(|e| format!("field {field_name} parse i64 failed: {e}"))
}

fn field_optional_f64(
    fields: &[Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<Option<f64>, String> {
    let Some(raw) = field_bytes(fields, idx, field_name)? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(raw)
        .map_err(|e| format!("field {field_name} is not valid UTF-8: {e}"))?;
    text.parse::<f64>()
        .map(Some)
        .map_err(|e| format!("field {field_name} parse f64 failed: {e}"))
}

fn field_optional_string(
    fields: &[Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<Option<String>, String> {
    let Some(raw) = field_bytes(fields, idx, field_name)? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(raw)
        .map_err(|e| format!("field {field_name} is not valid UTF-8: {e}"))?;
    Ok(Some(text.to_string()))
}

fn normalize_hll_hex_payload(raw: &[u8]) -> Vec<u8> {
    if raw.len().is_multiple_of(2) && raw.iter().all(|b| b.is_ascii_hexdigit()) {
        return raw.to_vec();
    }
    hex::encode_upper(raw).into_bytes()
}

fn field_optional_hll_hex_bytes(
    fields: &[Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<Option<Vec<u8>>, String> {
    let Some(raw) = field_bytes(fields, idx, field_name)? else {
        return Ok(None);
    };
    Ok(Some(normalize_hll_hex_payload(raw)))
}

fn decode_dict_base64(input: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(input)
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(input))
        .map_err(|e| format!("decode dict base64 failed: {e}"))
}

fn parse_global_dict_json(raw: &str) -> Result<data::TGlobalDict, String> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| format!("parse dict json failed: {e}"))?;
    let strings_list = value
        .get("2")
        .and_then(|v| v.get("lst"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| "dict json missing 2.lst".to_string())?;
    let ids_list = value
        .get("3")
        .and_then(|v| v.get("lst"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| "dict json missing 3.lst".to_string())?;

    if strings_list.len() < 2 || ids_list.len() < 2 {
        return Err("dict json list is too short".to_string());
    }
    let string_type = strings_list[0]
        .as_str()
        .ok_or_else(|| "dict strings type is not string".to_string())?;
    if !string_type.eq_ignore_ascii_case("str") {
        return Err(format!("dict strings type mismatch: {string_type}"));
    }
    let ids_type = ids_list[0]
        .as_str()
        .ok_or_else(|| "dict ids type is not string".to_string())?;
    if !ids_type.eq_ignore_ascii_case("i32") {
        return Err(format!("dict ids type mismatch: {ids_type}"));
    }

    let mut strings = Vec::with_capacity(strings_list.len().saturating_sub(2));
    for item in strings_list.iter().skip(2) {
        let encoded = item
            .as_str()
            .ok_or_else(|| "dict encoded string item is not string".to_string())?;
        strings.push(decode_dict_base64(encoded)?);
    }

    let mut ids = Vec::with_capacity(ids_list.len().saturating_sub(2));
    for item in ids_list.iter().skip(2) {
        let id = item
            .as_i64()
            .ok_or_else(|| "dict id item is not integer".to_string())?;
        let id = i32::try_from(id).map_err(|_| "dict id overflows i32".to_string())?;
        ids.push(id);
    }

    Ok(data::TGlobalDict::new(None, Some(strings), Some(ids), None))
}

fn rows_to_statistic_data(
    version: i32,
    fields: &[Option<Vec<u8>>],
) -> Result<data::TStatisticData, String> {
    let cols = fields.len();
    let mut out = data::TStatisticData::default();
    match version {
        STATISTIC_DICT_VERSION => {
            if cols != 3 {
                return Err(format!(
                    "statistic version {version} expects 3 columns, got {cols}"
                ));
            }
            out.meta_version = field_optional_i64(fields, 1, "meta_version")?;
            if let Some(dict_json) = field_optional_string(fields, 2, "dict_json")? {
                out.dict = Some(parse_global_dict_json(&dict_json)?);
            }
        }
        STATISTIC_DATA_VERSION_V1 => {
            if cols != 11 {
                return Err(format!(
                    "statistic version {version} expects 11 columns, got {cols}"
                ));
            }
            out.update_time = field_optional_string(fields, 1, "update_time")?;
            out.db_id = field_optional_i64(fields, 2, "db_id")?;
            out.table_id = field_optional_i64(fields, 3, "table_id")?;
            out.column_name = field_optional_string(fields, 4, "column_name")?;
            out.row_count = field_optional_i64(fields, 5, "row_count")?;
            out.data_size = field_optional_f64(fields, 6, "data_size")?.map(OrderedFloat);
            out.count_distinct = field_optional_i64(fields, 7, "count_distinct")?;
            out.null_count = field_optional_i64(fields, 8, "null_count")?;
            out.max = field_optional_string(fields, 9, "max")?;
            out.min = field_optional_string(fields, 10, "min")?;
        }
        STATISTIC_DATA_VERSION_V2 => {
            if cols != 12 {
                return Err(format!(
                    "statistic version {version} expects 12 columns, got {cols}"
                ));
            }
            out.update_time = field_optional_string(fields, 1, "update_time")?;
            out.db_id = field_optional_i64(fields, 2, "db_id")?;
            out.table_id = field_optional_i64(fields, 3, "table_id")?;
            out.column_name = field_optional_string(fields, 4, "column_name")?;
            out.row_count = field_optional_i64(fields, 5, "row_count")?;
            out.data_size = field_optional_f64(fields, 6, "data_size")?.map(OrderedFloat);
            out.count_distinct = field_optional_i64(fields, 7, "count_distinct")?;
            out.null_count = field_optional_i64(fields, 8, "null_count")?;
            out.max = field_optional_string(fields, 9, "max")?;
            out.min = field_optional_string(fields, 10, "min")?;
            out.collection_size = field_optional_i64(fields, 11, "collection_size")?;
        }
        STATISTIC_HISTOGRAM_VERSION => {
            if cols != 5 {
                return Err(format!(
                    "statistic version {version} expects 5 columns, got {cols}"
                ));
            }
            out.db_id = field_optional_i64(fields, 1, "db_id")?;
            out.table_id = field_optional_i64(fields, 2, "table_id")?;
            out.column_name = field_optional_string(fields, 3, "column_name")?;
            out.histogram = field_optional_string(fields, 4, "histogram")?;
        }
        STATISTIC_EXTERNAL_HISTOGRAM_VERSION => {
            if cols != 3 {
                return Err(format!(
                    "statistic version {version} expects 3 columns, got {cols}"
                ));
            }
            out.column_name = field_optional_string(fields, 1, "column_name")?;
            out.histogram = field_optional_string(fields, 2, "histogram")?;
        }
        STATISTIC_TABLE_VERSION => {
            if cols != 3 {
                return Err(format!(
                    "statistic version {version} expects 3 columns, got {cols}"
                ));
            }
            out.partition_id = field_optional_i64(fields, 1, "partition_id")?;
            out.row_count = field_optional_i64(fields, 2, "row_count")?;
        }
        STATISTIC_BATCH_VERSION => {
            if cols != 9 {
                return Err(format!(
                    "statistic version {version} expects 9 columns, got {cols}"
                ));
            }
            out.partition_id = field_optional_i64(fields, 1, "partition_id")?;
            out.column_name = field_optional_string(fields, 2, "column_name")?;
            out.row_count = field_optional_i64(fields, 3, "row_count")?;
            out.data_size = field_optional_f64(fields, 4, "data_size")?.map(OrderedFloat);
            out.hll = field_optional_hll_hex_bytes(fields, 5, "hll")?;
            out.null_count = field_optional_i64(fields, 6, "null_count")?;
            out.max = field_optional_string(fields, 7, "max")?;
            out.min = field_optional_string(fields, 8, "min")?;
        }
        STATISTIC_BATCH_VERSION_V5 => {
            if cols != 10 {
                return Err(format!(
                    "statistic version {version} expects 10 columns, got {cols}"
                ));
            }
            out.partition_id = field_optional_i64(fields, 1, "partition_id")?;
            out.column_name = field_optional_string(fields, 2, "column_name")?;
            out.row_count = field_optional_i64(fields, 3, "row_count")?;
            out.data_size = field_optional_f64(fields, 4, "data_size")?.map(OrderedFloat);
            out.hll = field_optional_hll_hex_bytes(fields, 5, "hll")?;
            out.null_count = field_optional_i64(fields, 6, "null_count")?;
            out.max = field_optional_string(fields, 7, "max")?;
            out.min = field_optional_string(fields, 8, "min")?;
            out.collection_size = field_optional_i64(fields, 9, "collection_size")?;
        }
        STATISTIC_PARTITION_VERSION => {
            if cols != 4 {
                return Err(format!(
                    "statistic version {version} expects 4 columns, got {cols}"
                ));
            }
            out.partition_id = field_optional_i64(fields, 1, "partition_id")?;
            out.column_name = field_optional_string(fields, 2, "column_name")?;
            out.count_distinct = field_optional_i64(fields, 3, "count_distinct")?;
        }
        STATISTIC_PARTITION_VERSION_V2 => {
            if cols != 6 {
                return Err(format!(
                    "statistic version {version} expects 6 columns, got {cols}"
                ));
            }
            out.partition_id = field_optional_i64(fields, 1, "partition_id")?;
            out.column_name = field_optional_string(fields, 2, "column_name")?;
            out.count_distinct = field_optional_i64(fields, 3, "count_distinct")?;
            out.null_count = field_optional_i64(fields, 4, "null_count")?;
            out.row_count = field_optional_i64(fields, 5, "row_count")?;
        }
        STATISTIC_EXTERNAL_VERSION => {
            if cols != 9 {
                return Err(format!(
                    "statistic version {version} expects 9 columns, got {cols}"
                ));
            }
            out.partition_name = field_optional_string(fields, 1, "partition_name")?;
            out.column_name = field_optional_string(fields, 2, "column_name")?;
            out.row_count = field_optional_i64(fields, 3, "row_count")?;
            out.data_size = field_optional_f64(fields, 4, "data_size")?.map(OrderedFloat);
            out.hll = field_optional_hll_hex_bytes(fields, 5, "hll")?;
            out.null_count = field_optional_i64(fields, 6, "null_count")?;
            out.max = field_optional_string(fields, 7, "max")?;
            out.min = field_optional_string(fields, 8, "min")?;
        }
        STATISTIC_EXTERNAL_QUERY_VERSION => {
            if cols != 8 {
                return Err(format!(
                    "statistic version {version} expects 8 columns, got {cols}"
                ));
            }
            out.column_name = field_optional_string(fields, 1, "column_name")?;
            out.row_count = field_optional_i64(fields, 2, "row_count")?;
            out.data_size = field_optional_f64(fields, 3, "data_size")?.map(OrderedFloat);
            out.count_distinct = field_optional_i64(fields, 4, "count_distinct")?;
            out.null_count = field_optional_i64(fields, 5, "null_count")?;
            out.max = field_optional_string(fields, 6, "max")?;
            out.min = field_optional_string(fields, 7, "min")?;
        }
        STATISTIC_EXTERNAL_QUERY_VERSION_V2 => {
            if cols != 9 {
                return Err(format!(
                    "statistic version {version} expects 9 columns, got {cols}"
                ));
            }
            out.column_name = field_optional_string(fields, 1, "column_name")?;
            out.row_count = field_optional_i64(fields, 2, "row_count")?;
            out.data_size = field_optional_f64(fields, 3, "data_size")?.map(OrderedFloat);
            out.count_distinct = field_optional_i64(fields, 4, "count_distinct")?;
            out.null_count = field_optional_i64(fields, 5, "null_count")?;
            out.max = field_optional_string(fields, 6, "max")?;
            out.min = field_optional_string(fields, 7, "min")?;
            out.update_time = field_optional_string(fields, 8, "update_time")?;
        }
        STATISTIC_MULTI_COLUMN_VERSION => {
            if cols != 3 {
                return Err(format!(
                    "statistic version {version} expects 3 columns, got {cols}"
                ));
            }
            out.column_name = field_optional_string(fields, 1, "column_name")?;
            out.count_distinct = field_optional_i64(fields, 2, "count_distinct")?;
        }
        STATISTIC_QUERY_MULTI_COLUMN_VERSION => {
            if cols != 5 {
                return Err(format!(
                    "statistic version {version} expects 5 columns, got {cols}"
                ));
            }
            out.db_id = field_optional_i64(fields, 1, "db_id")?;
            out.table_id = field_optional_i64(fields, 2, "table_id")?;
            out.column_name = field_optional_string(fields, 3, "column_name")?;
            out.count_distinct = field_optional_i64(fields, 4, "count_distinct")?;
        }
        _ => {
            return Err(format!("unsupported statistic version: {version}"));
        }
    }
    Ok(out)
}

fn thrift_statistic_row_encoder(
    version: i32,
    fields: &[Option<Vec<u8>>],
) -> Result<Vec<u8>, String> {
    let row_sd = rows_to_statistic_data(version, fields)?;
    thrift_compact_serialize(&row_sd)
}

fn result_projection_from_thrift_expr(
    expr: &crate::thrift::exprs::TExpr,
    idx: usize,
) -> Result<ResultProjection, String> {
    let root = expr
        .nodes
        .first()
        .ok_or_else(|| format!("RESULT_SINK output_exprs[{idx}] is empty"))?;
    if root.node_type != crate::thrift::exprs::TExprNodeType::SLOT_REF {
        return Err(format!(
            "RESULT_SINK output_exprs[{idx}] unsupported node_type {:?} (expected SLOT_REF)",
            root.node_type
        ));
    }
    let slot = root
        .slot_ref
        .as_ref()
        .ok_or_else(|| format!("RESULT_SINK output_exprs[{idx}] missing slot_ref payload"))?;
    Ok(ResultProjection {
        slot_id: SlotId::try_from(slot.slot_id)?,
        primitive: native_primitive_type_from_desc(&root.type_).unwrap_or(PrimitiveType::Invalid),
        field_schema: render_schema_from_type_desc(&root.type_)?,
    })
}

fn result_projections_from_thrift_exprs(
    output_exprs: Option<&Vec<crate::thrift::exprs::TExpr>>,
) -> Result<Option<Vec<ResultProjection>>, String> {
    let Some(output_exprs) = output_exprs.filter(|exprs| !exprs.is_empty()) else {
        return Ok(None);
    };
    output_exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| result_projection_from_thrift_expr(expr, idx))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

#[cfg(feature = "compat")]
fn runtime_query_options_from_thrift(
    query_opts: Option<&internal_service::TQueryOptions>,
) -> Result<Option<QueryOptions>, String> {
    query_opts
        .map(|opts| decode_query_options(Some(opts)).map_err(|error| error.to_string()))
        .transpose()
}

#[cfg(not(feature = "compat"))]
fn runtime_query_options_from_thrift(
    query_opts: Option<&internal_service::TQueryOptions>,
) -> Result<Option<QueryOptions>, String> {
    if query_opts.is_some() {
        return Err("thrift query options require the compat feature".to_string());
    }
    Ok(None)
}

#[cfg(feature = "compat")]
fn runtime_filter_params_from_thrift(
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
) -> Result<Option<RuntimeFilterParams>, String> {
    exec_params
        .and_then(|params| params.runtime_filter_params.as_ref())
        .map(|params| {
            decode_runtime_filter_params(
                params,
                FieldPath::root("exec_plan_fragment")
                    .field("params")
                    .field("runtime_filter_params"),
            )
            .map_err(|error| error.to_string())
        })
        .transpose()
}

#[cfg(not(feature = "compat"))]
fn runtime_filter_params_from_thrift(
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
) -> Result<Option<RuntimeFilterParams>, String> {
    if exec_params
        .and_then(|params| params.runtime_filter_params.as_ref())
        .is_some()
    {
        return Err("thrift runtime filter params require the compat feature".to_string());
    }
    Ok(None)
}

pub(crate) fn execute_fragment(
    fragment: &planner::TPlanFragment,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    batch_exchange_sender_counts: &HashMap<i32, usize>,
    query_opts: Option<&internal_service::TQueryOptions>,
    session_time_zone: Option<&str>,
    pipeline_dop: i32,
    _group_execution_scan_dop: Option<i32>,
    db_name: Option<&str>,
    profiler: Option<Profiler>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
    backend_num: Option<i32>,
    mem_tracker: Option<std::sync::Arc<crate::runtime::mem_tracker::MemTracker>>,
    typed_result_sink: bool,
) -> Result<FragmentOutput, String> {
    let runtime_fe_addr = fe_addr
        .map(|address| {
            decode_runtime_endpoint(
                address,
                FieldPath::root("exec_plan_fragment").field("coord"),
            )
            .map_err(|error| error.to_string())
        })
        .transpose()?;
    let runtime_query_opts = runtime_query_options_from_thrift(query_opts)?;
    let runtime_query_opts = apply_query_option_overrides(runtime_query_opts);

    let profile_name = fragment
        .plan
        .as_ref()
        .and_then(|plan| plan.nodes.first().map(|n| n.node_id))
        .filter(|id| *id >= 0)
        .map(|id| format!("execute_fragment (plan_node_id={id})"));
    let profiler = if profiler.is_some() {
        profiler
    } else if runtime_query_opts
        .as_ref()
        .map(|opts| opts.enable_profile)
        .unwrap_or(false)
    {
        Some(Profiler::new(
            profile_name.as_deref().unwrap_or("execute_fragment"),
        ))
    } else {
        None
    };

    let query_id = exec_params.map(|params| QueryId {
        hi: params.query_id.hi,
        lo: params.query_id.lo,
    });
    let runtime_filter_params = runtime_filter_params_from_thrift(exec_params)?;
    let fragment_instance_id = exec_params.map(|params| UniqueId {
        hi: params.fragment_instance_id.hi,
        lo: params.fragment_instance_id.lo,
    });
    let runtime_state = build_runtime_state(
        RuntimeStateInputs {
            query_options: runtime_query_opts.clone(),
            query_id,
            runtime_filter_params,
            fragment_instance_id,
            backend_num,
            mem_tracker,
        },
        profiler.as_ref(),
    )?;

    if let Some(plan) = fragment.plan.as_ref() {
        let mut tuple_slots = build_tuple_slot_order(desc_tbl);
        let inferred = infer_tuple_slot_order(fragment);
        if tuple_slots.is_empty() {
            tuple_slots = inferred.clone();
        } else {
            for (tuple_id, slots) in &inferred {
                if tuple_slots.contains_key(tuple_id) {
                    continue;
                }
                tuple_slots.insert(*tuple_id, slots.clone());
            }
        }
        reorder_tuple_slots(&mut tuple_slots, desc_tbl);
        let allow_throw_exception = runtime_query_opts
            .as_ref()
            .map(|opts| opts.allow_throw_exception)
            .unwrap_or(false);
        let allow_throw_exception = allow_throw_exception
            || query_opts.is_some_and(|opts| {
                matches!(
                    opts.overflow_mode,
                    Some(mode) if mode == internal_service::TOverflowMode::REPORT_ERROR
                )
            });
        // Layout hints are used by scan nodes to decide which columns to materialize.
        //
        // For exchange fragments, pruning only by "local usage" is not correct because downstream
        // fragments may require additional columns that do not appear in this fragment's exprs.
        // The descriptor table already encodes the materialized slots for each tuple, so we use it
        // as the source of truth to avoid producing mismatched layouts at runtime.
        let layout_hints = tuple_slots.clone();
        let connectors = ConnectorRegistry::default();
        let sink = fragment
            .output_sink
            .as_ref()
            .ok_or_else(|| "PlanFragment must have output_sink field".to_string())?;
        let mut resolved_query_profiles = BTreeMap::new();
        let mut resolved_lake_meta_storage = BTreeMap::new();
        let (arena, lowered, prepared_sink) = loop {
            let external_dependencies =
                StarRocksExternalDependencyDraft::new_with_lake_meta_storage(
                    runtime_fe_addr.clone(),
                    resolved_query_profiles.clone(),
                    resolved_lake_meta_storage.clone(),
                );
            let mut arena = ExprArena::default();
            arena.set_allow_throw_exception(allow_throw_exception);
            arena.set_session_time_zone(session_time_zone.map(|s| s.to_string()));
            let decode_result = {
                let _lower_timer = profiler.as_ref().map(|p| p.scoped_timer("LowerPlanTime"));
                (|| {
                    let lowered = lower_plan(
                        plan,
                        &mut arena,
                        &tuple_slots,
                        desc_tbl,
                        fragment.query_global_dicts.as_deref(),
                        fragment.query_global_dict_exprs.as_ref(),
                        exec_params,
                        batch_exchange_sender_counts,
                        query_opts,
                        db_name,
                        &connectors,
                        &layout_hints,
                        last_query_id,
                        Some(&external_dependencies),
                    )?;
                    let prepared_sink = decode_fragment_sink(
                        sink,
                        fragment,
                        exec_params,
                        desc_tbl,
                        &mut arena,
                        &lowered,
                        last_query_id,
                        session_time_zone,
                        &external_dependencies,
                    )?;
                    Ok((lowered, prepared_sink))
                })()
            };
            let attempt = classify_fragment_decode_attempt(decode_result, &external_dependencies);
            let decoded = process_fragment_decode_attempt(
                attempt,
                |dependency| match dependency {
                    StarRocksExternalDependency::QueryProfile { query_id, .. } => {
                        if resolved_query_profiles.contains_key(query_id) {
                            return Ok(false);
                        }
                        let coord = fe_addr.ok_or_else(|| {
                        "StarRocks plan decode requires a frontend address to resolve query-profile dependencies"
                            .to_string()
                    })?;
                        let profile =
                            crate::service::fe_report::fetch_query_profile(coord, query_id)?;
                        resolved_query_profiles.insert(query_id.clone(), profile);
                        Ok(true)
                    }
                    StarRocksExternalDependency::LakeMetaStorage { id, request } => {
                        if resolved_lake_meta_storage.contains_key(id) {
                            return Ok(false);
                        }
                        let facts = crate::connector::starrocks::lake_meta_storage::resolve_lake_meta_storage(
                        request,
                    )?;
                        resolved_lake_meta_storage.insert(*id, facts);
                        Ok(true)
                    }
                },
            )?;
            if let Some((lowered, prepared_sink)) = decoded {
                break (arena, lowered, prepared_sink);
            }
        };

        let mut exec_plan = ExecPlan {
            arena,
            root: lowered.node,
        };
        if let Some(query_id) = query_id {
            let mut row_pos_descs = HashMap::new();
            collect_glm_metadata(&exec_plan.root, &mut row_pos_descs)?;
            if !row_pos_descs.is_empty() {
                query_context_manager().register_row_pos_descs(query_id, row_pos_descs)?;
            }
        }
        crate::protocol::starrocks::decode::runtime_filter_pushdown::push_down_local_runtime_filters(
            &mut exec_plan.root,
            &exec_plan.arena,
        );
        let root_plan_node_id = plan.nodes.first().map(|n| n.node_id).unwrap_or(-1);

        match prepared_sink {
            PreparedFragmentSink::DataStream {
                input,
                fragment_instance_id,
                sender_id,
            } => {
                let exchange_finst_id = Some((fragment_instance_id.hi, fragment_instance_id.lo));
                let sink_factory = DataStreamSinkFactory::new(
                    input,
                    fragment_instance_id,
                    sender_id,
                    root_plan_node_id,
                    exec_plan.arena.clone(),
                );
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_compat_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    runtime_fe_addr.clone(),
                    backend_num,
                )?;
            }
            PreparedFragmentSink::MultiCast {
                inputs,
                fragment_instance_id,
                sender_id,
            } => {
                let exchange_finst_id = Some((fragment_instance_id.hi, fragment_instance_id.lo));
                let sink_factory = MultiCastDataStreamSinkFactory::new(
                    inputs,
                    fragment_instance_id,
                    sender_id,
                    exec_plan.arena.clone(),
                    root_plan_node_id,
                );
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_compat_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    runtime_fe_addr.clone(),
                    backend_num,
                )?;
            }
            PreparedFragmentSink::Split {
                inputs,
                split_expr_ids,
                fragment_instance_id,
                sender_id,
            } => {
                let exchange_finst_id = Some((fragment_instance_id.hi, fragment_instance_id.lo));
                let sink_factory = SplitDataStreamSinkFactory::new(
                    inputs,
                    fragment_instance_id,
                    sender_id,
                    exec_plan.arena.clone(),
                    root_plan_node_id,
                    Arc::new(exec_plan.arena.clone()),
                    split_expr_ids,
                );
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_compat_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    runtime_fe_addr.clone(),
                    backend_num,
                )?;
            }
            PreparedFragmentSink::IcebergChangeStreamRouter {
                input,
                fragment_instance_id,
                sender_id,
            } => {
                let exchange_finst_id = Some((fragment_instance_id.hi, fragment_instance_id.lo));
                let sink_factory = IcebergChangeStreamRouterSinkFactory::try_new(
                    input,
                    fragment_instance_id,
                    sender_id,
                    exec_plan.arena.clone(),
                    root_plan_node_id,
                )?;
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_compat_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    runtime_fe_addr.clone(),
                    backend_num,
                )?;
            }
            PreparedFragmentSink::Result {
                config,
                projections,
                exchange_finst_id,
            } => {
                let sink_factory =
                    ResultBufferSinkFactory::new(projections, config, None, typed_result_sink);
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_compat_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    runtime_fe_addr.clone(),
                    backend_num,
                )?;
            }
            PreparedFragmentSink::Noop { exchange_finst_id } => {
                let sink_factory = NoopSinkFactory::new();
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_compat_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    runtime_fe_addr.clone(),
                    backend_num,
                )?;
            }
            PreparedFragmentSink::Iceberg {
                input,
                root_sink_dop,
            } => {
                let sink_factory = IcebergTableSinkFactory::try_new(input)?;
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_compat_plan_with_pipeline_with_root_sink_dop(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    None,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    runtime_fe_addr.clone(),
                    backend_num,
                    root_sink_dop,
                )?;
            }
            #[cfg(feature = "compat")]
            PreparedFragmentSink::Olap { input } => {
                let sink_factory = OlapTableSinkFactory::try_new(input)?;
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_compat_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    None,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    runtime_fe_addr.clone(),
                    backend_num,
                )?;
            }
        }
        return Ok(FragmentOutput { profile_json: None });
    }

    Err("unsupported fragment: missing plan".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::starrocks::decode::layout::Layout;
    use crate::thrift::exprs::{TExpr, TExprNode, TExprNodeType, TStringLiteral};
    use crate::thrift::partitions::{TDataPartition, TPartitionType};

    fn test_expr_node(
        node_type: TExprNodeType,
        type_: types::TTypeDesc,
        num_children: i32,
    ) -> TExprNode {
        TExprNode {
            node_type,
            type_,
            opcode: None,
            num_children,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: None,
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: -1,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }
    }

    fn get_query_profile_expr(query_id: &str) -> TExpr {
        let string_type = crate::types::arrow_thrift::thrift_type_desc_from_primitive(
            types::TPrimitiveType::VARCHAR,
        );
        let mut call = test_expr_node(TExprNodeType::FUNCTION_CALL, string_type.clone(), 1);
        call.fn_ = Some(types::TFunction::new(
            types::TFunctionName::new(None, "get_query_profile".to_string()),
            types::TFunctionBinaryType::BUILTIN,
            vec![string_type.clone()],
            string_type.clone(),
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        let mut literal = test_expr_node(TExprNodeType::STRING_LITERAL, string_type, 0);
        literal.string_literal = Some(TStringLiteral::new(query_id.to_string()));
        TExpr::new(vec![call, literal])
    }

    fn profile_partitioned_stream_sink(query_id: &str) -> data_sinks::TDataStreamSink {
        data_sinks::TDataStreamSink::new(
            7,
            TDataPartition::new(
                TPartitionType::HASH_PARTITIONED,
                vec![get_query_profile_expr(query_id)],
                None,
                None,
            ),
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn profile_partitioned_data_sink(query_id: &str) -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::DATA_STREAM_SINK,
            profile_partitioned_stream_sink(query_id),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn test_fragment(sink: data_sinks::TDataSink) -> planner::TPlanFragment {
        planner::TPlanFragment::new(
            None,
            None,
            sink,
            TDataPartition::new(TPartitionType::UNPARTITIONED, None, None, None),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn test_exec_params() -> internal_service::TPlanFragmentExecParams {
        internal_service::TPlanFragmentExecParams::new(
            types::TUniqueId::new(1, 2),
            types::TUniqueId::new(3, 4),
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            Some(0),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn empty_lowered() -> Lowered {
        Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Values(crate::exec::node::values::ValuesNode {
                    chunk: crate::exec::chunk::Chunk::default(),
                    node_id: 0,
                }),
            },
            layout: empty_layout(),
        }
    }

    fn unpartitioned_stream_sink() -> data_sinks::TDataStreamSink {
        data_sinks::TDataStreamSink::new(
            7,
            TDataPartition::new(TPartitionType::UNPARTITIONED, None, None, None),
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn destination_without_endpoint() -> data_sinks::TPlanFragmentDestination {
        data_sinks::TPlanFragmentDestination::new(types::TUniqueId::new(11, 12), None, None, None)
    }

    fn empty_layout() -> Layout {
        Layout {
            order: Vec::new(),
            index: HashMap::new(),
        }
    }

    #[test]
    fn decode_error_is_not_reclassified_as_pending() {
        let draft = StarRocksExternalDependencyDraft::new(None, BTreeMap::new());
        let _ = draft.query_profile("query-7");
        let attempt =
            classify_fragment_decode_attempt::<()>(Err("malformed plan".to_string()), &draft);
        let mut resolver_calls = 0;

        let error = process_fragment_decode_attempt(attempt, |_| {
            resolver_calls += 1;
            Ok(true)
        })
        .expect_err("a real decode failure must be preserved");

        assert_eq!(error, "malformed plan");
        assert_eq!(resolver_calls, 0);
    }

    #[test]
    fn successful_decode_with_requirements_is_pending_not_ready() {
        let draft = StarRocksExternalDependencyDraft::new(None, BTreeMap::new());
        let _ = draft.query_profile("query-7");

        let attempt = classify_fragment_decode_attempt(Ok(7), &draft);

        assert!(matches!(attempt, FragmentDecodeAttempt::Pending(_)));
    }

    #[test]
    fn stream_sink_dependency_is_resolved_once_before_decode_is_ready() {
        let sink = profile_partitioned_data_sink("query-7");
        let fragment = test_fragment(sink.clone());
        let exec_params = test_exec_params();
        let lowered = empty_lowered();
        let mut profiles = BTreeMap::new();
        let mut resolver_calls = 0;

        let draft = StarRocksExternalDependencyDraft::new(None, profiles.clone());
        let first = decode_fragment_sink(
            &sink,
            &fragment,
            Some(&exec_params),
            None,
            &mut ExprArena::default(),
            &lowered,
            None,
            None,
            &draft,
        );
        let first = classify_fragment_decode_attempt(first, &draft);
        let retry = process_fragment_decode_attempt(first, |dependency| {
            resolver_calls += 1;
            let StarRocksExternalDependency::QueryProfile { query_id, .. } = dependency else {
                return Err("unexpected dependency".to_string());
            };
            profiles.insert(query_id.clone(), "resolved-profile".to_string());
            Ok(true)
        })
        .expect("dependency resolution must succeed");
        assert!(retry.is_none());

        let draft = StarRocksExternalDependencyDraft::new(None, profiles);
        let second = decode_fragment_sink(
            &sink,
            &fragment,
            Some(&exec_params),
            None,
            &mut ExprArena::default(),
            &lowered,
            None,
            None,
            &draft,
        );
        let second = classify_fragment_decode_attempt(second, &draft);
        let ready = process_fragment_decode_attempt(second, |_| {
            resolver_calls += 1;
            Ok(true)
        })
        .expect("resolved sink decode must succeed");

        assert!(matches!(
            ready,
            Some(PreparedFragmentSink::DataStream { .. })
        ));
        assert_eq!(resolver_calls, 1);
    }

    #[test]
    fn multicast_nested_destination_reports_fragment_branch_path() {
        let stream = unpartitioned_stream_sink();
        let multi_cast = data_sinks::TMultiCastDataStreamSink::new(
            vec![stream.clone(), stream],
            vec![vec![], vec![destination_without_endpoint()]],
        );
        let error = multi_cast_inputs_from_compat(
            &multi_cast,
            &mut ExprArena::default(),
            &empty_layout(),
            None,
            None,
        )
        .err()
        .expect("nested destination without an endpoint must be rejected");

        assert_eq!(
            error,
            "starrocks protocol error at exec_plan_fragment.fragment.output_sink.multi_cast_stream_sink.destinations[1][0].brpc_server (missing field): destination requires brpc_server or deprecated_server"
        );
    }

    #[test]
    fn router_branch_destination_reports_fragment_branch_path() {
        let stream = unpartitioned_stream_sink();
        let router = data_sinks::TIcebergChangeStreamRouterSink::new(
            23,
            None,
            vec![
                data_sinks::TIcebergChangeStreamRouterBranch::new(
                    0,
                    data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV,
                    stream.clone(),
                    vec![],
                ),
                data_sinks::TIcebergChangeStreamRouterBranch::new(
                    1,
                    data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA,
                    stream,
                    vec![destination_without_endpoint()],
                ),
            ],
        );
        let error = iceberg_router_input_from_compat(
            &router,
            &mut ExprArena::default(),
            &empty_layout(),
            None,
            None,
        )
        .err()
        .expect("router destination without an endpoint must be rejected");

        assert_eq!(
            error,
            "starrocks protocol error at exec_plan_fragment.fragment.output_sink.iceberg_change_stream_router_sink.branches[1].destinations[0].brpc_server (missing field): destination requires brpc_server or deprecated_server"
        );
    }
}
