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

use base64::Engine;
use thrift::OrderedFloat;

use crate::common::ids::SlotId;
use crate::common::thrift::thrift_compact_serialize;
use crate::exec::expr::ExprArena;
use crate::exec::fragment::program::FragmentSinkSpec;
use crate::exec::fragment::sink::{
    DataStreamSinkBranchProgram, DataStreamSinkProgram, FragmentSinkProgram,
    IcebergChangeStreamRouterBranchProgram, IcebergChangeStreamRouterProgram,
    IcebergTableSinkProgram, MultiCastDataStreamSinkProgram, SplitDataStreamSinkProgram,
};
use crate::exec::node::ExecPlan;
use crate::exec::operators::{
    DataStreamSinkFactoryInput, IcebergChangeStreamRouterBranchFactoryInput,
    IcebergChangeStreamRouterSinkFactoryInput,
};
use crate::protocol::common::error::FieldPath;
use crate::protocol::starrocks::decode::node::Lowered;
use crate::protocol::starrocks::decode::type_lowering::{
    native_primitive_type_from_desc, render_schema_from_type_desc,
};
use crate::protocol::starrocks::decode::{
    FragmentExprArenaOwner, StarRocksExternalDependencyDraft, StarRocksFragmentDecodeError,
    decode_fragment_destination,
};
use crate::runtime::endpoint::FragmentDestination;
use crate::runtime::fragment::instance::FragmentSinkAssignment;
use crate::service::result_batch_wire::{ResultProjection, ResultSinkConfig};
use crate::thrift::{data, data_sinks, descriptors, planner};
use novarocks_types::PrimitiveType;

fn runtime_destination_from_thrift(
    dest: &data_sinks::TPlanFragmentDestination,
    path: FieldPath,
) -> Result<FragmentDestination, StarRocksFragmentDecodeError> {
    decode_fragment_destination(dest, path).map_err(StarRocksFragmentDecodeError::from)
}

fn runtime_destinations_from_thrift(
    destinations: Vec<data_sinks::TPlanFragmentDestination>,
    path: FieldPath,
) -> Result<Vec<FragmentDestination>, StarRocksFragmentDecodeError> {
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
    stream_path: FieldPath,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<Vec<crate::exec::expr::ExprId>, StarRocksFragmentDecodeError> {
    let output_partition_path = stream_path.field("output_partition");
    let partition_type =
        crate::protocol::starrocks::decode::sink::decode_data_stream_partition_type(
            stream.output_partition.type_,
        )
        .map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_enum(
                output_partition_path.clone().field("type"),
                detail,
            )
        })?;
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
            crate::protocol::starrocks::decode::expr::lower_t_expr_at(
                expr,
                arena,
                layout,
                last_query_id,
                external_dependencies,
                output_partition_path
                    .clone()
                    .field("partition_exprs")
                    .index(idx),
            )
        })
        .collect()
}

fn data_stream_input_from_compat(
    stream: &data_sinks::TDataStreamSink,
    stream_path: FieldPath,
    destinations: Vec<data_sinks::TPlanFragmentDestination>,
    destinations_path: FieldPath,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<DataStreamSinkFactoryInput, StarRocksFragmentDecodeError> {
    let partition_exprs = lower_stream_partition_exprs(
        stream,
        stream_path.clone(),
        arena,
        layout,
        last_query_id,
        external_dependencies,
    )?;
    let partition_type =
        crate::protocol::starrocks::decode::sink::decode_data_stream_partition_type(
            stream.output_partition.type_,
        )
        .map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_enum(
                stream_path.clone().field("output_partition").field("type"),
                detail,
            )
        })?;
    DataStreamSinkFactoryInput::try_new(
        stream.dest_node_id,
        partition_type,
        Vec::new(),
        partition_exprs,
        stream.output_columns.clone().unwrap_or_default(),
        runtime_destinations_from_thrift(destinations, destinations_path)?,
    )
    .map_err(|detail| StarRocksFragmentDecodeError::invalid_value(stream_path, detail))
}

pub(crate) fn multi_cast_inputs_from_compat(
    multi_cast: &data_sinks::TMultiCastDataStreamSink,
    multi_cast_path: FieldPath,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<Vec<(DataStreamSinkFactoryInput, Option<i64>)>, StarRocksFragmentDecodeError> {
    if multi_cast.sinks.len() != multi_cast.destinations.len() {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            multi_cast_path.clone().field("destinations"),
            format!(
                "MULTI_CAST_DATA_STREAM_SINK: sinks size {} != destinations size {}",
                multi_cast.sinks.len(),
                multi_cast.destinations.len()
            ),
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
                    multi_cast_path.clone().field("sinks").index(branch_index),
                    destinations.clone(),
                    multi_cast_path
                        .clone()
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
    split_path: FieldPath,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<Vec<DataStreamSinkFactoryInput>, StarRocksFragmentDecodeError> {
    let sinks = split.sinks.as_ref().cloned().unwrap_or_default();
    let destinations = split.destinations.as_ref().cloned().unwrap_or_default();
    if sinks.len() != destinations.len() {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            split_path.clone().field("destinations"),
            format!(
                "SPLIT_DATA_STREAM_SINK: sinks size {} != destinations size {}",
                sinks.len(),
                destinations.len()
            ),
        ));
    }
    sinks
        .iter()
        .zip(destinations)
        .enumerate()
        .map(|(branch_index, (stream, destinations))| {
            data_stream_input_from_compat(
                stream,
                split_path.clone().field("sinks").index(branch_index),
                destinations,
                split_path.clone().field("destinations").index(branch_index),
                arena,
                layout,
                last_query_id,
                external_dependencies,
            )
        })
        .collect()
}

pub(crate) fn iceberg_router_input_from_compat(
    router: &data_sinks::TIcebergChangeStreamRouterSink,
    router_path: FieldPath,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<IcebergChangeStreamRouterSinkFactoryInput, StarRocksFragmentDecodeError> {
    let branches = router
        .branches
        .iter()
        .enumerate()
        .map(|(branch_index, branch)| {
            let branch_path = router_path.clone().field("branches").index(branch_index);
            let branch_kind = branch_kind_from_thrift(branch.branch_kind).map_err(|detail| {
                StarRocksFragmentDecodeError::invalid_enum(
                    branch_path.clone().field("branch_kind"),
                    detail,
                )
            })?;
            Ok(IcebergChangeStreamRouterBranchFactoryInput {
                branch_id: branch.branch_id,
                branch_kind,
                stream_sink: data_stream_input_from_compat(
                    &branch.stream_sink,
                    branch_path.clone().field("stream_sink"),
                    branch.destinations.clone(),
                    branch_path.field("destinations"),
                    arena,
                    layout,
                    last_query_id,
                    external_dependencies,
                )?,
            })
        })
        .collect::<Result<Vec<_>, StarRocksFragmentDecodeError>>()?;
    Ok(IcebergChangeStreamRouterSinkFactoryInput {
        change_op_slot_id: router.change_op_slot_id,
        data_route_slot_id: router.data_route_slot_id,
        branches,
    })
}

pub(crate) struct DecodedStarRocksFragmentSink {
    pub(crate) spec: FragmentSinkSpec,
    pub(crate) assignment: FragmentSinkAssignment,
    pub(crate) result_override: Option<(ResultSinkConfig, Option<Vec<ResultProjection>>)>,
    pub(crate) root_sink_dop: Option<i32>,
}

fn static_branch_from_factory_input(
    input: DataStreamSinkFactoryInput,
    limit: Option<i64>,
    path: FieldPath,
) -> Result<(DataStreamSinkBranchProgram, Vec<FragmentDestination>), StarRocksFragmentDecodeError> {
    let program = DataStreamSinkBranchProgram::try_new(
        input.dest_node_id,
        input.output_exprs,
        input.output_partition_type,
        input.output_partition_exprs,
        input.output_columns,
        limit,
    )
    .map_err(|detail| StarRocksFragmentDecodeError::invalid_value(path, detail))?;
    Ok((program, input.destinations))
}

fn static_stream_from_factory_input(
    input: DataStreamSinkFactoryInput,
    limit: Option<i64>,
    arena: ExprArena,
    path: FieldPath,
) -> Result<(DataStreamSinkProgram, Vec<FragmentDestination>), StarRocksFragmentDecodeError> {
    let program = DataStreamSinkProgram::try_new(
        input.dest_node_id,
        input.output_exprs,
        input.output_partition_type,
        input.output_partition_exprs,
        input.output_columns,
        limit,
        arena,
    )
    .map_err(|detail| StarRocksFragmentDecodeError::invalid_value(path, detail))?;
    Ok((program, input.destinations))
}

fn decoded_compat_sink(
    program: FragmentSinkProgram,
    assignment: FragmentSinkAssignment,
    path: FieldPath,
) -> Result<DecodedStarRocksFragmentSink, StarRocksFragmentDecodeError> {
    Ok(DecodedStarRocksFragmentSink {
        spec: FragmentSinkSpec::try_new(program)
            .map_err(|detail| StarRocksFragmentDecodeError::invalid_value(path, detail))?,
        assignment,
        result_override: None,
        root_sink_dop: None,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_fragment_sink(
    sink: &data_sinks::TDataSink,
    fragment: &planner::TPlanFragment,
    destinations: &[FragmentDestination],
    sender_id: Option<i32>,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    arena: &mut ExprArena,
    lowered: &Lowered,
    last_query_id: Option<&str>,
    session_time_zone: Option<&str>,
    external_dependencies: &StarRocksExternalDependencyDraft,
    sink_path: FieldPath,
    fragment_path: FieldPath,
) -> Result<DecodedStarRocksFragmentSink, StarRocksFragmentDecodeError> {
    match sink.type_ {
        data_sinks::TDataSinkType::DATA_STREAM_SINK => {
            let stream_path = sink_path.clone().field("stream_sink");
            let stream_sink = sink.stream_sink.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    stream_path.clone(),
                    "DATA_STREAM_SINK missing stream_sink payload",
                )
            })?;
            let mut sink_arena = arena.clone();
            let partition_exprs = external_dependencies.with_expr_arena_owner(
                FragmentExprArenaOwner::DataStream,
                || {
                    lower_stream_partition_exprs(
                        stream_sink,
                        stream_path.clone(),
                        &mut sink_arena,
                        &lowered.layout,
                        last_query_id,
                        Some(external_dependencies),
                    )
                },
            )?;
            let partition_type =
                crate::protocol::starrocks::decode::sink::decode_data_stream_partition_type(
                    stream_sink.output_partition.type_,
                )
                .map_err(|detail| {
                    StarRocksFragmentDecodeError::invalid_enum(
                        stream_path.clone().field("output_partition").field("type"),
                        detail,
                    )
                })?;
            let input = DataStreamSinkFactoryInput::try_new(
                stream_sink.dest_node_id,
                partition_type,
                Vec::new(),
                partition_exprs,
                stream_sink.output_columns.clone().unwrap_or_default(),
                destinations.to_vec(),
            )
            .map_err(|detail| {
                StarRocksFragmentDecodeError::invalid_value(stream_path.clone(), detail)
            })?;
            let (program, destinations) = static_stream_from_factory_input(
                input,
                stream_sink.limit,
                sink_arena,
                stream_path,
            )?;
            decoded_compat_sink(
                FragmentSinkProgram::DataStream(program),
                FragmentSinkAssignment::StreamDestinations {
                    destinations,
                    sender_id,
                },
                sink_path,
            )
        }
        data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
            let multi_cast_path = sink_path.clone().field("multi_cast_stream_sink");
            let multi_cast = sink.multi_cast_stream_sink.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    multi_cast_path.clone(),
                    "MULTI_CAST_DATA_STREAM_SINK missing multi_cast_stream_sink payload",
                )
            })?;
            let mut sink_arena = arena.clone();
            let inputs = external_dependencies.with_expr_arena_owner(
                FragmentExprArenaOwner::MultiCastDataStream,
                || {
                    multi_cast_inputs_from_compat(
                        multi_cast,
                        multi_cast_path.clone(),
                        &mut sink_arena,
                        &lowered.layout,
                        last_query_id,
                        Some(external_dependencies),
                    )
                },
            )?;
            let (programs, groups): (Vec<_>, Vec<_>) = inputs
                .into_iter()
                .enumerate()
                .map(|(index, (input, limit))| {
                    static_branch_from_factory_input(
                        input,
                        limit,
                        multi_cast_path.clone().field("sinks").index(index),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .unzip();
            let program = MultiCastDataStreamSinkProgram::try_new(programs, sink_arena).map_err(
                |detail| StarRocksFragmentDecodeError::invalid_value(multi_cast_path, detail),
            )?;
            decoded_compat_sink(
                FragmentSinkProgram::MultiCastDataStream(program),
                FragmentSinkAssignment::DestinationGroups { groups, sender_id },
                sink_path,
            )
        }
        data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
            let split_path = sink_path.clone().field("split_stream_sink");
            let split = sink.split_stream_sink.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    split_path.clone(),
                    "SPLIT_DATA_STREAM_SINK missing split_stream_sink payload",
                )
            })?;
            let split_exprs = split.split_exprs.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    split_path.clone().field("split_exprs"),
                    "SPLIT_DATA_STREAM_SINK missing split_exprs payload",
                )
            })?;
            let mut sink_arena = arena.clone();
            let (split_expr_ids, inputs) = external_dependencies.with_expr_arena_owner(
                FragmentExprArenaOwner::SplitDataStream,
                || {
                    let split_expr_ids = split_exprs
                        .iter()
                        .enumerate()
                        .map(|(index, expr)| {
                            crate::protocol::starrocks::decode::expr::lower_t_expr_at(
                                expr,
                                &mut sink_arena,
                                &lowered.layout,
                                last_query_id,
                                Some(external_dependencies),
                                split_path.clone().field("split_exprs").index(index),
                            )
                        })
                        .collect::<Result<Vec<_>, StarRocksFragmentDecodeError>>()?;
                    let inputs = split_inputs_from_compat(
                        split,
                        split_path.clone(),
                        &mut sink_arena,
                        &lowered.layout,
                        last_query_id,
                        Some(external_dependencies),
                    )?;
                    Ok::<_, StarRocksFragmentDecodeError>((split_expr_ids, inputs))
                },
            )?;
            let (programs, groups): (Vec<_>, Vec<_>) = inputs
                .into_iter()
                .enumerate()
                .map(|(index, input)| {
                    static_branch_from_factory_input(
                        input,
                        None,
                        split_path.clone().field("sinks").index(index),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .unzip();
            let program = SplitDataStreamSinkProgram::try_new(programs, split_expr_ids, sink_arena)
                .map_err(|detail| {
                    StarRocksFragmentDecodeError::invalid_value(split_path, detail)
                })?;
            decoded_compat_sink(
                FragmentSinkProgram::SplitDataStream(program),
                FragmentSinkAssignment::DestinationGroups { groups, sender_id },
                sink_path,
            )
        }
        data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK => {
            let router_path = sink_path.clone().field("iceberg_change_stream_router_sink");
            let router = sink
                .iceberg_change_stream_router_sink
                .as_ref()
                .ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        router_path.clone(),
                        "ICEBERG_CHANGE_STREAM_ROUTER_SINK missing iceberg_change_stream_router_sink",
                    )
                })?;
            let mut sink_arena = arena.clone();
            let input = external_dependencies.with_expr_arena_owner(
                FragmentExprArenaOwner::IcebergChangeStreamRouter,
                || {
                    iceberg_router_input_from_compat(
                        router,
                        router_path.clone(),
                        &mut sink_arena,
                        &lowered.layout,
                        last_query_id,
                        Some(external_dependencies),
                    )
                },
            )?;
            let change_op_slot_id =
                SlotId::try_from(input.change_op_slot_id).map_err(|detail| {
                    StarRocksFragmentDecodeError::invalid_value(
                        router_path.clone().field("change_op_slot_id"),
                        detail,
                    )
                })?;
            let data_route_slot_id = input
                .data_route_slot_id
                .map(SlotId::try_from)
                .transpose()
                .map_err(|detail| {
                    StarRocksFragmentDecodeError::invalid_value(
                        router_path.clone().field("data_route_slot_id"),
                        detail,
                    )
                })?;
            let (branches, groups): (Vec<_>, Vec<_>) = input
                .branches
                .into_iter()
                .enumerate()
                .map(|(index, branch)| {
                    let (stream, destinations) = static_branch_from_factory_input(
                        branch.stream_sink,
                        None,
                        router_path
                            .clone()
                            .field("branches")
                            .index(index)
                            .field("stream_sink"),
                    )?;
                    Ok((
                        IcebergChangeStreamRouterBranchProgram::new(
                            branch.branch_id,
                            branch.branch_kind,
                            stream,
                        ),
                        destinations,
                    ))
                })
                .collect::<Result<Vec<_>, StarRocksFragmentDecodeError>>()?
                .into_iter()
                .unzip();
            let program = IcebergChangeStreamRouterProgram::try_new(
                change_op_slot_id,
                data_route_slot_id,
                branches,
                sink_arena,
            )
            .map_err(|detail| StarRocksFragmentDecodeError::invalid_value(router_path, detail))?;
            decoded_compat_sink(
                FragmentSinkProgram::IcebergChangeStreamRouter(program),
                FragmentSinkAssignment::DestinationGroups { groups, sender_id },
                sink_path,
            )
        }
        data_sinks::TDataSinkType::RESULT_SINK => {
            let result_sink_path = sink_path.clone().field("result_sink");
            let result_sink = sink.result_sink.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    result_sink_path.clone(),
                    "RESULT_SINK missing result_sink payload",
                )
            })?;
            let mut decoded = decoded_compat_sink(
                FragmentSinkProgram::Result,
                FragmentSinkAssignment::None,
                sink_path,
            )?;
            decoded.result_override = Some((
                result_sink_config_from_thrift(result_sink, result_sink_path)?,
                result_projections_from_thrift_exprs(
                    fragment.output_exprs.as_ref(),
                    fragment_path.field("output_exprs"),
                )?,
            ));
            Ok(decoded)
        }
        data_sinks::TDataSinkType::NOOP_SINK | data_sinks::TDataSinkType::SCHEMA_TABLE_SINK => {
            decoded_compat_sink(
                FragmentSinkProgram::Noop,
                FragmentSinkAssignment::None,
                sink_path,
            )
        }
        data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
        | data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
        | data_sinks::TDataSinkType::ICEBERG_DV_SINK
        | data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK => {
            let sink_type_name = iceberg_sink_type_name(sink.type_);
            let iceberg_sink_path = sink_path.clone().field("iceberg_table_sink");
            let iceberg_sink = sink.iceberg_table_sink.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    iceberg_sink_path.clone(),
                    format!("{sink_type_name} missing iceberg_table_sink payload"),
                )
            })?;
            let output_exprs = fragment.output_exprs.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    fragment_path.clone().field("output_exprs"),
                    format!("{sink_type_name} missing output_exprs"),
                )
            })?;
            let desc_tbl = desc_tbl.ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    FieldPath::root("exec_plan_fragment").field("desc_tbl"),
                    format!("{sink_type_name} requires descriptor table"),
                )
            })?;
            let sink_mode =
                crate::protocol::starrocks::decode::sink::iceberg::iceberg_sink_mode_for_type(
                    sink.type_,
                );
            let input = external_dependencies.with_expr_arena_owner(
                FragmentExprArenaOwner::IcebergTable,
                || crate::protocol::starrocks::decode::sink::iceberg::lower_iceberg_sink_factory_input(
                    iceberg_sink,
                    sink_mode,
                    output_exprs,
                    &lowered.layout,
                    desc_tbl,
                    last_query_id,
                    Some(external_dependencies),
                    iceberg_sink_path.clone(),
                    fragment_path.clone().field("output_exprs"),
                ),
            )
            .map_err(|error| error.into_fragment(iceberg_sink_path.clone()))?;
            let program =
                IcebergTableSinkProgram::try_from_factory_input(input).map_err(|detail| {
                    StarRocksFragmentDecodeError::invalid_value(iceberg_sink_path, detail)
                })?;
            let mut decoded = decoded_compat_sink(
                FragmentSinkProgram::IcebergTable(program),
                FragmentSinkAssignment::None,
                sink_path,
            )?;
            decoded.root_sink_dop = (sink_mode
                == crate::connector::iceberg::IcebergSinkMode::DeletionVectors)
                .then_some(1);
            Ok(decoded)
        }
        data_sinks::TDataSinkType::OLAP_TABLE_SINK => {
            #[cfg(feature = "compat")]
            {
                let olap_sink_path = sink_path.clone().field("olap_table_sink");
                let olap_sink = sink.olap_table_sink.as_ref().ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        olap_sink_path.clone(),
                        "OLAP_TABLE_SINK missing olap_table_sink payload",
                    )
                })?;
                let draft_plan = ExecPlan {
                    arena: arena.clone(),
                    root: lowered.node.clone(),
                };
                let (program, assignment) = crate::protocol::starrocks::decode::sink::starrocks::lower_starrocks_table_sink(
                    olap_sink,
                    fragment.output_exprs.as_deref(),
                    Some(&draft_plan),
                    Some(&lowered.layout),
                    last_query_id,
                    session_time_zone,
                    Some(external_dependencies),
                    olap_sink_path.clone(),
                    fragment_path.clone().field("output_exprs"),
                )
                .map_err(|error| error.into_fragment(olap_sink_path.clone()))?;
                decoded_compat_sink(
                    FragmentSinkProgram::StarRocksTable(program),
                    FragmentSinkAssignment::StarRocksTable(assignment),
                    sink_path,
                )
            }
            #[cfg(not(feature = "compat"))]
            Err(StarRocksFragmentDecodeError::unsupported(
                sink_path,
                "OLAP_TABLE_SINK requires the compat feature",
            ))
        }
        other => Err(StarRocksFragmentDecodeError::unsupported(
            sink_path.field("type"),
            format!(
                "unsupported sink type: {:?}. Only DATA_STREAM_SINK, MULTI_CAST_DATA_STREAM_SINK, SPLIT_DATA_STREAM_SINK, ICEBERG_CHANGE_STREAM_ROUTER_SINK, RESULT_SINK, NOOP_SINK, SCHEMA_TABLE_SINK, ICEBERG_TABLE_SINK, ICEBERG_DELETE_SINK, ICEBERG_DV_SINK, ICEBERG_EQUALITY_DELETE_SINK, and OLAP_TABLE_SINK are supported",
                other
            ),
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
    result_sink_path: FieldPath,
) -> Result<ResultSinkConfig, StarRocksFragmentDecodeError> {
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
                return Err(StarRocksFragmentDecodeError::invalid_enum(
                    result_sink_path.field("format"),
                    format!(
                        "HTTP_PROTOCAL result sink only supports JSON format, got {:?}",
                        format
                    ),
                ));
            }
            Ok(ResultSinkConfig::http_json())
        }
        t if t == data_sinks::TResultSinkType::STATISTIC => {
            Ok(ResultSinkConfig::statistic(thrift_statistic_row_encoder))
        }
        other => Err(StarRocksFragmentDecodeError::invalid_enum(
            result_sink_path.field("type"),
            format!("unsupported RESULT_SINK type {:?}", other),
        )),
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
    expr_path: FieldPath,
) -> Result<ResultProjection, StarRocksFragmentDecodeError> {
    let root_path = expr_path.field("nodes").index(0);
    let root = expr.nodes.first().ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            root_path.clone(),
            "RESULT_SINK output expression is empty",
        )
    })?;
    if root.node_type != crate::thrift::exprs::TExprNodeType::SLOT_REF {
        return Err(StarRocksFragmentDecodeError::invalid_enum(
            root_path.clone().field("node_type"),
            format!(
                "RESULT_SINK output expression has unsupported node_type {:?} (expected SLOT_REF)",
                root.node_type
            ),
        ));
    }
    let slot = root.slot_ref.as_ref().ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            root_path.clone().field("slot_ref"),
            "RESULT_SINK output expression missing slot_ref payload",
        )
    })?;
    Ok(ResultProjection {
        slot_id: SlotId::try_from(slot.slot_id).map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_value(
                root_path.clone().field("slot_ref").field("slot_id"),
                detail,
            )
        })?,
        primitive: native_primitive_type_from_desc(&root.type_).unwrap_or(PrimitiveType::Invalid),
        field_schema: render_schema_from_type_desc(&root.type_).map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_value(root_path.field("type"), detail)
        })?,
    })
}

fn result_projections_from_thrift_exprs(
    output_exprs: Option<&Vec<crate::thrift::exprs::TExpr>>,
    output_exprs_path: FieldPath,
) -> Result<Option<Vec<ResultProjection>>, StarRocksFragmentDecodeError> {
    let Some(output_exprs) = output_exprs.filter(|exprs| !exprs.is_empty()) else {
        return Ok(None);
    };
    output_exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            result_projection_from_thrift_expr(expr, output_exprs_path.clone().index(idx))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}
