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

use std::sync::Arc;

use super::{
    data_stream_input_from_native, fragment_instance_id_from_native_params,
    lower_stream_partition_exprs_from_native, stream_destinations_from_native,
};
use crate::common::types::UniqueId;
use crate::exec::expr::ExprArena;
use crate::exec::operators::{
    DataStreamSinkFactory, DataStreamSinkFactoryInput, IcebergChangeStreamRouterBranchFactoryInput,
    IcebergChangeStreamRouterSinkFactory, IcebergChangeStreamRouterSinkFactoryInput,
    IcebergTableSinkFactory, MultiCastDataStreamSinkFactory, NoopSinkFactory,
    ResultBufferSinkFactory,
};
use crate::proto;
use crate::protocol::native::decode;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::result_buffer;
use crate::service::result_batch_wire::ResultSinkConfig;

pub(super) fn prepare_result_buffer_for_native_sink(
    sink: &proto::plan::DataSink,
    finst_id: UniqueId,
    typed_result_sink: bool,
    mem_tracker: Option<&Arc<MemTracker>>,
) -> Result<(), String> {
    let uses_fetch_result_buffer = matches!(
        sink.kind.as_ref(),
        Some(proto::plan::data_sink::Kind::Result(true))
    );
    if !uses_fetch_result_buffer {
        return Ok(());
    }
    if typed_result_sink {
        result_buffer::create_typed_sender(finst_id);
    } else {
        result_buffer::create_sender(finst_id);
    }
    if let Some(root) = mem_tracker {
        let label = format!("ResultBuffer: finst={}", finst_id);
        let tracker = MemTracker::new_child(label, root);
        result_buffer::set_mem_tracker(finst_id, tracker);
    }
    Ok(())
}

pub(super) fn sink_factory_from_native(
    fragment: &proto::plan::PlanFragment,
    sink: &proto::plan::DataSink,
    instance_params: &proto::novarocks::InstanceParams,
    typed_result_sink: bool,
    layout: &super::super::layout::Layout,
) -> Result<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>, String> {
    let kind = sink
        .kind
        .as_ref()
        .ok_or_else(|| "native PlanFragment sink kind missing".to_string())?;
    match kind {
        proto::plan::data_sink::Kind::Result(true) => {
            if !fragment.output_exprs.is_empty() {
                return Err(
                    "native RESULT sink does not support fragment output_exprs yet".to_string(),
                );
            }
            Ok(Box::new(ResultBufferSinkFactory::new(
                None,
                ResultSinkConfig::mysql(),
                None,
                typed_result_sink,
            )))
        }
        proto::plan::data_sink::Kind::Noop(true) => Ok(Box::new(NoopSinkFactory::new())),
        proto::plan::data_sink::Kind::Result(false) => {
            Err("native RESULT sink marker must be true".to_string())
        }
        proto::plan::data_sink::Kind::Noop(false) => {
            Err("native NOOP sink marker must be true".to_string())
        }
        proto::plan::data_sink::Kind::DataStream(stream) => {
            let mut partition_arena = ExprArena::default();
            let partition = stream
                .output_partition
                .as_ref()
                .ok_or_else(|| "native DATA_STREAM_SINK missing output_partition".to_string())?;
            let partition_exprs = lower_stream_partition_exprs_from_native(
                partition,
                &mut partition_arena,
                layout,
                |idx| format!("native DATA_STREAM_SINK partition expr[{idx}]"),
            )?;
            let destinations = decode::decode_destinations(&instance_params.destinations)
                .map_err(|error| error.to_string())?;
            let sink_input = data_stream_input_from_native(stream, destinations, partition_exprs)?;
            let fragment_instance_id = fragment_instance_id_from_native_params(instance_params)?;
            let root_plan_node_id = fragment
                .root
                .as_ref()
                .map(|node| node.node_id)
                .unwrap_or(-1);
            Ok(Box::new(DataStreamSinkFactory::new(
                sink_input,
                fragment_instance_id,
                None,
                root_plan_node_id,
                partition_arena,
            )))
        }
        proto::plan::data_sink::Kind::MultiCastDataStream(multi_cast) => {
            if multi_cast.sinks.len() != multi_cast.destinations.len() {
                return Err(format!(
                    "native MULTI_CAST_DATA_STREAM_SINK sinks size {} != destinations size {}",
                    multi_cast.sinks.len(),
                    multi_cast.destinations.len()
                ));
            }
            let mut partition_arena = ExprArena::default();
            let sink_inputs = multi_cast
                .sinks
                .iter()
                .zip(multi_cast.destinations.iter())
                .enumerate()
                .map(|(sink_idx, (stream, destinations))| {
                    let partition = stream
                        .output_partition
                        .as_ref()
                        .ok_or_else(|| {
                            format!(
                                "native MULTI_CAST_DATA_STREAM_SINK sink[{sink_idx}] missing output_partition"
                            )
                        })?;
                    let partition_exprs = lower_stream_partition_exprs_from_native(
                        partition,
                        &mut partition_arena,
                        layout,
                        |expr_idx| {
                            format!(
                                "native MULTI_CAST_DATA_STREAM_SINK sink[{sink_idx}] partition expr[{expr_idx}]"
                            )
                        },
                    )?;
                    let destinations = stream_destinations_from_native(destinations)?;
                    Ok((
                        data_stream_input_from_native(stream, destinations, partition_exprs)?,
                        stream.limit,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            let fragment_instance_id = fragment_instance_id_from_native_params(instance_params)?;
            let root_plan_node_id = fragment
                .root
                .as_ref()
                .map(|node| node.node_id)
                .unwrap_or(-1);
            Ok(Box::new(MultiCastDataStreamSinkFactory::new(
                sink_inputs,
                fragment_instance_id,
                None,
                partition_arena,
                root_plan_node_id,
            )))
        }
        proto::plan::data_sink::Kind::IcebergWrite(iceberg) => {
            let (sink_input, _sink_mode) =
                super::super::sink::lower_iceberg_write_sink_factory_input(
                    iceberg,
                    &fragment.output_exprs,
                    &fragment.output_columns,
                    layout,
                )?;
            Ok(Box::new(IcebergTableSinkFactory::try_new(sink_input)?))
        }
        proto::plan::data_sink::Kind::IcebergChangeStreamRouter(router) => {
            let (router_input, partition_arena) =
                lower_iceberg_change_stream_router_sink_from_native(
                    router,
                    &fragment.output_exprs,
                    &fragment.output_columns,
                    layout,
                )?;
            let fragment_instance_id = fragment_instance_id_from_native_params(instance_params)?;
            let root_plan_node_id = fragment
                .root
                .as_ref()
                .map(|node| node.node_id)
                .unwrap_or(-1);
            Ok(Box::new(IcebergChangeStreamRouterSinkFactory::try_new(
                router_input,
                fragment_instance_id,
                None,
                partition_arena,
                root_plan_node_id,
            )?))
        }
    }
}

fn lower_iceberg_change_stream_router_sink_from_native(
    router: &proto::plan::IcebergChangeStreamRouterSink,
    output_exprs: &[proto::expr::Expr],
    output_columns: &[proto::common::OutputColumn],
    layout: &super::super::layout::Layout,
) -> Result<(IcebergChangeStreamRouterSinkFactoryInput, ExprArena), String> {
    let change_op_slot_id =
        output_slot_id_for_ordinal(output_columns, router.change_op_output_ordinal, "change_op")?;
    let data_route_slot_id = router
        .data_route_output_ordinal
        .map(|ordinal| output_slot_id_for_ordinal(output_columns, ordinal, "data_route"))
        .transpose()?;
    let mut branches = Vec::with_capacity(router.branches.len());
    let mut partition_arena = ExprArena::default();
    for (branch_idx, branch) in router.branches.iter().enumerate() {
        let partition = branch_partition_from_native(branch, output_exprs)?;
        let partition_exprs = lower_stream_partition_exprs_from_native(
            &partition,
            &mut partition_arena,
            layout,
            |expr_idx| {
                format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK branch[{branch_idx}] partition expr[{expr_idx}]"
                )
            },
        )?;
        let output_columns = branch
            .output_ordinals
            .iter()
            .map(|ordinal| {
                output_slot_id_for_ordinal(
                    output_columns,
                    *ordinal,
                    &format!("branch[{branch_idx}] output"),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let destinations = branch
            .destinations
            .as_ref()
            .ok_or_else(|| {
                format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK branch[{branch_idx}] missing destinations"
                )
            })
            .and_then(stream_destinations_from_native)?;
        let branch_kind = match proto::plan::ChangeStreamBranchKind::try_from(branch.branch_kind)
            .map_err(|_| {
                format!(
                    "unknown native ChangeStreamBranchKind value {}",
                    branch.branch_kind
                )
            })? {
            proto::plan::ChangeStreamBranchKind::DeleteDv => {
                crate::sql::common::ChangeStreamBranchKind::DeleteDv
            }
            proto::plan::ChangeStreamBranchKind::ReuseData => {
                crate::sql::common::ChangeStreamBranchKind::ReuseData
            }
            proto::plan::ChangeStreamBranchKind::FreshData => {
                crate::sql::common::ChangeStreamBranchKind::FreshData
            }
            proto::plan::ChangeStreamBranchKind::Unspecified => {
                return Err("native ChangeStreamBranchKind is unspecified".to_string());
            }
        };
        branches.push(IcebergChangeStreamRouterBranchFactoryInput {
            branch_id: branch.branch_id,
            branch_kind,
            stream_sink: DataStreamSinkFactoryInput::try_new(
                branch.target_exchange_node_id,
                DataStreamSinkFactoryInput::partition_type_from_native_kind(partition.kind)?,
                Vec::new(),
                partition_exprs,
                output_columns,
                destinations,
            )?,
        });
    }
    Ok((
        IcebergChangeStreamRouterSinkFactoryInput {
            change_op_slot_id,
            data_route_slot_id,
            branches,
        },
        partition_arena,
    ))
}

fn branch_partition_from_native(
    branch: &proto::plan::IcebergChangeStreamBranchRoute,
    output_exprs: &[proto::expr::Expr],
) -> Result<proto::plan::DataPartition, String> {
    if let Some(partition) = branch.output_partition.as_ref() {
        return Ok(partition.clone());
    }
    let exprs = branch
        .output_partition_ordinals
        .iter()
        .map(|ordinal| {
            let idx = usize::try_from(*ordinal).map_err(|_| {
                format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK partition ordinal {ordinal} overflows usize"
                )
            })?;
            output_exprs.get(idx).cloned().ok_or_else(|| {
                format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK partition ordinal {ordinal} is out of range"
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let kind = if exprs.is_empty() {
        proto::plan::PartitionKind::Unpartitioned
    } else {
        proto::plan::PartitionKind::Hash
    };
    Ok(proto::plan::DataPartition {
        kind: kind as i32,
        exprs,
    })
}

fn output_slot_id_for_ordinal(
    output_columns: &[proto::common::OutputColumn],
    ordinal: u64,
    label: &str,
) -> Result<i32, String> {
    let idx = usize::try_from(ordinal)
        .map_err(|_| format!("native router {label} output ordinal {ordinal} overflows usize"))?;
    let column = output_columns
        .get(idx)
        .ok_or_else(|| format!("native router {label} output ordinal {ordinal} is out of range"))?;
    i32::try_from(column.column_id).map_err(|_| {
        format!(
            "native router {label} output ordinal {ordinal} column id {} exceeds i32",
            column.column_id
        )
    })
}
