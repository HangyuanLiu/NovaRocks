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

//! Native proto sink lowering.

mod equality_delete;
mod metadata;
mod partition;
mod position_delete;

use std::collections::HashMap;
use std::sync::Arc;

use super::decode_type;
use super::error::NativeFragmentLeafDecodeError;
use super::expr::decode_expr;
use crate::common::ids::SlotId;
use crate::connector::iceberg::position_delete_descriptor::PositionDeleteExpectedBinding;
use crate::connector::iceberg::schema::build_full_output_schema;
use crate::connector::iceberg::sink_plan::{
    IcebergSinkFactoryInput, IcebergSinkMode, IcebergSinkPlan,
};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::exec::fragment::sink::{
    DataStreamSinkBranchProgram, FragmentSinkProgram, IcebergChangeStreamRouterBranchProgram,
    IcebergChangeStreamRouterProgram, IcebergTableSinkProgram, MultiCastDataStreamSinkProgram,
};
use crate::exec::operators::DataStreamPartitionType;
use crate::proto::{common, expr, novarocks, plan};
use crate::protocol::common::error::FieldPath;
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::fragment::instance::FragmentSinkAssignment;

use self::equality_delete::{
    build_equality_delete_output_schema, validate_equality_delete_unpartitioned_target_metadata,
};
use self::metadata::{
    iceberg_table_descriptor_from_native, iceberg_table_location, map_native_compression,
    parse_target_table_metadata, resolve_native_sink_s3_config,
    schema_has_reserved_row_lineage_columns, validate_iceberg_sink_file_format,
};
use self::partition::{
    build_partition_exprs_from_output_exprs, partition_info_from_metadata,
    partition_source_field_ids_from_metadata,
};
use self::position_delete::{
    bind_position_delete_descriptor_from_native, build_position_delete_data_file_partition_index,
};

pub(crate) fn decode_fragment_sink_program(
    fragment: &plan::PlanFragment,
    layout: &super::layout::Layout,
) -> Result<FragmentSinkProgram, super::NativeFragmentDecodeError> {
    let decoded = (|| -> Result<FragmentSinkProgram, String> {
        let sink = fragment
            .sink
            .as_ref()
            .ok_or_else(|| "native PlanFragment missing sink".to_string())?;
        let kind = sink
            .kind
            .as_ref()
            .ok_or_else(|| "native PlanFragment sink kind missing".to_string())?;
        match kind {
            plan::data_sink::Kind::Result(true) => {
                if !fragment.output_exprs.is_empty() {
                    return Err(
                        "native RESULT sink does not support fragment output_exprs yet".to_string(),
                    );
                }
                Ok(FragmentSinkProgram::Result)
            }
            plan::data_sink::Kind::Noop(true) => Ok(FragmentSinkProgram::Noop),
            plan::data_sink::Kind::Result(false) => {
                Err("native RESULT sink marker must be true".to_string())
            }
            plan::data_sink::Kind::Noop(false) => {
                Err("native NOOP sink marker must be true".to_string())
            }
            plan::data_sink::Kind::DataStream(stream) => {
                let mut partition_arena = ExprArena::default();
                let branch = decode_data_stream_branch(
                    stream,
                    &mut partition_arena,
                    layout,
                    "native DATA_STREAM_SINK",
                )?;
                branch
                    .into_program(partition_arena)
                    .map(FragmentSinkProgram::DataStream)
                    .map_err(|error| error.to_string())
            }
            plan::data_sink::Kind::MultiCastDataStream(grouped) => {
                let mut partition_arena = ExprArena::default();
                let sinks = grouped
                    .sinks
                    .iter()
                    .enumerate()
                    .map(|(index, stream)| {
                        decode_data_stream_branch(
                            stream,
                            &mut partition_arena,
                            layout,
                            &format!("native MULTI_CAST_DATA_STREAM_SINK sink[{index}]"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(FragmentSinkProgram::MultiCastDataStream(
                    MultiCastDataStreamSinkProgram::try_new(sinks, partition_arena)
                        .map_err(|error| error.to_string())?,
                ))
            }
            plan::data_sink::Kind::IcebergWrite(iceberg) => {
                let (input, _mode) = decode_iceberg_write_sink_factory_input(
                    iceberg,
                    &fragment.output_exprs,
                    &fragment.output_columns,
                    layout,
                )?;
                Ok(FragmentSinkProgram::IcebergTable(
                    IcebergTableSinkProgram::try_from_factory_input(input)
                        .map_err(|error| error.to_string())?,
                ))
            }
            plan::data_sink::Kind::IcebergChangeStreamRouter(router) => {
                Ok(decode_change_stream_router_program(
                    router,
                    &fragment.output_exprs,
                    &fragment.output_columns,
                    layout,
                )
                .map(FragmentSinkProgram::IcebergChangeStreamRouter)?)
            }
        }
    })();
    decoded.map_err(|error| {
        super::NativeFragmentDecodeError::invalid_value(
            FieldPath::root("plan_fragment").field("sink"),
            error,
        )
    })
}

pub(crate) fn decode_fragment_sink_assignment(
    sink: &plan::DataSink,
    instance: &novarocks::InstanceParams,
) -> Result<FragmentSinkAssignment, super::NativeFragmentDecodeError> {
    let path = FieldPath::root("plan_fragment").field("sink");
    let kind = sink.kind.as_ref().ok_or_else(|| {
        super::NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            "native PlanFragment sink requires kind",
        )
    })?;
    match kind {
        plan::data_sink::Kind::DataStream(_) => Ok(FragmentSinkAssignment::StreamDestinations {
            destinations: super::instance::decode_destinations(&instance.destinations)?,
            sender_id: None,
        }),
        plan::data_sink::Kind::MultiCastDataStream(grouped) => {
            let groups = grouped
                .destinations
                .iter()
                .enumerate()
                .map(|(index, group)| {
                    decode_stream_destination_list(
                        group,
                        path.clone()
                            .field("multi_cast_data_stream")
                            .field("destinations")
                            .index(index),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            })
        }
        plan::data_sink::Kind::IcebergChangeStreamRouter(router) => {
            let groups = router
                .branches
                .iter()
                .enumerate()
                .map(|(index, branch)| {
                    let group_path = path
                        .clone()
                        .field("iceberg_change_stream_router")
                        .field("branches")
                        .index(index)
                        .field("destinations");
                    let group = branch.destinations.as_ref().ok_or_else(|| {
                        super::NativeFragmentDecodeError::missing(
                            group_path.clone(),
                            "native change-stream branch requires destinations",
                        )
                    })?;
                    decode_stream_destination_list(group, group_path)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            })
        }
        plan::data_sink::Kind::Result(_)
        | plan::data_sink::Kind::Noop(_)
        | plan::data_sink::Kind::IcebergWrite(_) => {
            if instance.destinations.is_empty() {
                Ok(FragmentSinkAssignment::None)
            } else {
                Ok(FragmentSinkAssignment::StreamDestinations {
                    destinations: super::instance::decode_destinations(&instance.destinations)?,
                    sender_id: None,
                })
            }
        }
    }
}

fn decode_data_stream_branch(
    stream: &plan::DataStreamSink,
    partition_arena: &mut ExprArena,
    layout: &super::layout::Layout,
    context: &str,
) -> Result<DataStreamSinkBranchProgram, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<DataStreamSinkBranchProgram, String> {
        let partition = stream
            .output_partition
            .as_ref()
            .ok_or_else(|| format!("{context} missing output_partition"))?;
        let partition_type = decode_stream_partition_type(partition.kind)?;
        let output_partition_exprs = if partition_type.requires_exprs() {
            partition
                .exprs
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    decode_expr(expression, partition_arena, layout)
                        .map_err(|error| format!("{context} partition expr[{index}]: {error}"))
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        let output_columns = decode_output_slot_ids(&stream.output_columns, context)?;
        DataStreamSinkBranchProgram::try_new(
            stream.dest_node_id,
            Vec::new(),
            partition_type,
            output_partition_exprs,
            output_columns,
            stream.limit,
        )
        .map_err(|error| error.to_string())
    })();
    decoded.map_err(Into::into)
}

fn decode_output_slot_ids(raw_ids: &[i32], context: &str) -> Result<Vec<SlotId>, String> {
    let mut seen = std::collections::HashSet::new();
    raw_ids
        .iter()
        .map(|raw| {
            let slot_id = SlotId::try_from(*raw)
                .map_err(|error| format!("{context}: invalid output_columns slot id: {error}"))?;
            if !seen.insert(slot_id) {
                return Err(format!(
                    "{context}: duplicate output_columns slot id: {slot_id}"
                ));
            }
            Ok(slot_id)
        })
        .collect()
}

fn decode_change_stream_router_program(
    router: &plan::IcebergChangeStreamRouterSink,
    output_exprs: &[expr::Expr],
    output_columns: &[common::OutputColumn],
    layout: &super::layout::Layout,
) -> Result<IcebergChangeStreamRouterProgram, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<IcebergChangeStreamRouterProgram, String> {
        let change_op_slot_id = SlotId::try_from(output_slot_id_for_ordinal(
            output_columns,
            router.change_op_output_ordinal,
            "change_op",
        )?)?;
        let data_route_slot_id = router
            .data_route_output_ordinal
            .map(|ordinal| output_slot_id_for_ordinal(output_columns, ordinal, "data_route"))
            .transpose()?
            .map(SlotId::try_from)
            .transpose()?;
        let mut partition_arena = ExprArena::default();
        let branches = router
        .branches
        .iter()
        .enumerate()
        .map(|(index, branch)| {
            let partition = branch_partition_from_native(branch, output_exprs)?;
            let partition_type = decode_stream_partition_type(partition.kind)?;
            let output_partition_exprs = if partition_type.requires_exprs() {
                partition
                    .exprs
                    .iter()
                    .enumerate()
                    .map(|(expr_index, expression)| {
                        decode_expr(expression, &mut partition_arena, layout).map_err(|error| {
                            format!(
                                "native ICEBERG_CHANGE_STREAM_ROUTER_SINK branch[{index}] partition expr[{expr_index}]: {error}"
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                Vec::new()
            };
            let branch_output_columns = decode_router_output_slots(
                &branch.output_ordinals,
                output_columns,
                &format!("branch[{index}] output"),
            )?;
            Ok(IcebergChangeStreamRouterBranchProgram::new(
                branch.branch_id,
                decode_change_stream_branch_kind(branch.branch_kind)?,
                DataStreamSinkBranchProgram::try_new(
                    branch.target_exchange_node_id,
                    Vec::new(),
                    partition_type,
                    output_partition_exprs,
                    branch_output_columns,
                    None,
                )
                .map_err(|error| error.to_string())?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
        IcebergChangeStreamRouterProgram::try_new(
            change_op_slot_id,
            data_route_slot_id,
            branches,
            partition_arena,
        )
        .map_err(|error| error.to_string())
    })();
    decoded.map_err(Into::into)
}

fn branch_partition_from_native(
    branch: &plan::IcebergChangeStreamBranchRoute,
    output_exprs: &[expr::Expr],
) -> Result<plan::DataPartition, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<plan::DataPartition, String> {
        if let Some(partition) = branch.output_partition.as_ref() {
            return Ok(partition.clone());
        }
        let exprs = branch
        .output_partition_ordinals
        .iter()
        .map(|ordinal| {
            let index = usize::try_from(*ordinal).map_err(|_| {
                format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK partition ordinal {ordinal} overflows usize"
                )
            })?;
            output_exprs.get(index).cloned().ok_or_else(|| {
                format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK partition ordinal {ordinal} is out of range"
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
        let kind = if exprs.is_empty() {
            plan::PartitionKind::Unpartitioned
        } else {
            plan::PartitionKind::Hash
        };
        Ok(plan::DataPartition {
            kind: kind as i32,
            exprs,
        })
    })();
    decoded.map_err(Into::into)
}

fn output_slot_id_for_ordinal(
    output_columns: &[common::OutputColumn],
    ordinal: u64,
    label: &str,
) -> Result<i32, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<i32, String> {
        let index = usize::try_from(ordinal).map_err(|_| {
            format!("native router {label} output ordinal {ordinal} overflows usize")
        })?;
        let column = output_columns.get(index).ok_or_else(|| {
            format!("native router {label} output ordinal {ordinal} is out of range")
        })?;
        i32::try_from(column.column_id).map_err(|_| {
            format!(
                "native router {label} output ordinal {ordinal} column id {} exceeds i32",
                column.column_id
            )
        })
    })();
    decoded.map_err(Into::into)
}

fn decode_router_output_slots(
    ordinals: &[u64],
    output_columns: &[common::OutputColumn],
    label: &str,
) -> Result<Vec<SlotId>, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<Vec<SlotId>, String> {
        let mut seen = std::collections::HashSet::new();
        ordinals
        .iter()
        .map(|ordinal| {
            let raw_slot_id = output_slot_id_for_ordinal(output_columns, *ordinal, label)?;
            let slot_id = SlotId::try_from(raw_slot_id)?;
            if !seen.insert(slot_id) {
                return Err(format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK {label}: duplicate output slot id: {slot_id}"
                ));
            }
            Ok(slot_id)
        })
        .collect()
    })();
    decoded.map_err(Into::into)
}

fn decode_change_stream_branch_kind(
    value: i32,
) -> Result<crate::sql::common::ChangeStreamBranchKind, String> {
    match plan::ChangeStreamBranchKind::try_from(value)
        .map_err(|_| format!("unknown native ChangeStreamBranchKind value {value}"))?
    {
        plan::ChangeStreamBranchKind::DeleteDv => {
            Ok(crate::sql::common::ChangeStreamBranchKind::DeleteDv)
        }
        plan::ChangeStreamBranchKind::ReuseData => {
            Ok(crate::sql::common::ChangeStreamBranchKind::ReuseData)
        }
        plan::ChangeStreamBranchKind::FreshData => {
            Ok(crate::sql::common::ChangeStreamBranchKind::FreshData)
        }
        plan::ChangeStreamBranchKind::Unspecified => {
            Err("native ChangeStreamBranchKind is unspecified".to_string())
        }
    }
}

fn decode_stream_destination_list(
    group: &plan::StreamDestinationList,
    path: FieldPath,
) -> Result<Vec<FragmentDestination>, super::NativeFragmentDecodeError> {
    group
        .destinations
        .iter()
        .enumerate()
        .map(|(index, destination)| {
            let destination_path = path.clone().field("destinations").index(index);
            let finst_id = destination.finst_id.as_ref().ok_or_else(|| {
                super::NativeFragmentDecodeError::missing(
                    destination_path.clone().field("finst_id"),
                    "native stream destination requires finst_id",
                )
            })?;
            Ok(FragmentDestination::new(
                crate::common::types::UniqueId {
                    hi: finst_id.hi,
                    lo: finst_id.lo,
                },
                RuntimeEndpoint::parse(&destination.endpoint).map_err(|error| {
                    super::NativeFragmentDecodeError::invalid_value(
                        destination_path.field("endpoint"),
                        error,
                    )
                })?,
            ))
        })
        .collect()
}

pub(crate) fn decode_iceberg_write_sink_factory_input(
    sink: &plan::IcebergWriteFragmentSink,
    fragment_output_exprs: &[expr::Expr],
    fragment_output_columns: &[common::OutputColumn],
    layout: &super::layout::Layout,
) -> Result<(IcebergSinkFactoryInput, IcebergSinkMode), NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<(IcebergSinkFactoryInput, IcebergSinkMode), String> {
        let spec = sink
            .spec
            .as_ref()
            .ok_or_else(|| "native Iceberg write sink missing spec".to_string())?;
        let mode = iceberg_sink_mode_from_native(spec.mode)?;

        let mut arena = ExprArena::default();
        let lowered_output_exprs = if fragment_output_exprs.is_empty() {
            lower_output_columns_as_slot_refs(
                fragment_output_columns,
                sink.input.as_ref(),
                &mut arena,
            )?
        } else {
            lower_output_exprs(fragment_output_exprs, &mut arena, layout)?
        };

        let target_table = spec
            .target_table
            .as_ref()
            .ok_or_else(|| "native Iceberg write sink missing target_table".to_string())?;
        let writer_columns = if spec.target_columns.is_empty() {
            target_table.columns.as_slice()
        } else {
            spec.target_columns.as_slice()
        };
        if lowered_output_exprs.len() != writer_columns.len() {
            return Err(format!(
                "native Iceberg write sink input column count {} does not match target column count {}",
                lowered_output_exprs.len(),
                writer_columns.len()
            ));
        }

        let iceberg_table = spec
            .iceberg
            .as_ref()
            .ok_or_else(|| "native Iceberg write sink missing iceberg table info".to_string())?;
        let iceberg_table =
            iceberg_table_descriptor_from_native(iceberg_table, &target_table.columns, mode)?;
        let target_partition_spec_id = spec.target_partition_spec_id;

        let target_table_metadata = parse_target_table_metadata(&iceberg_table, mode)?;
        let (partition_source_column_names, partition_column_names, transform_exprs) =
            partition_info_from_metadata(target_table_metadata.as_ref(), target_partition_spec_id)?;
        let position_delete_binding = if matches!(
            mode,
            IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors
        ) {
            let target_schema = build_full_output_schema(&iceberg_table)?;
            let metadata = target_table_metadata.as_ref().ok_or_else(|| {
                format!(
                    "native Iceberg {:?} sink requires serialized target table metadata",
                    mode
                )
            })?;
            let partition_source_field_ids =
                partition_source_field_ids_from_metadata(metadata, &partition_source_column_names)?;
            let expected = PositionDeleteExpectedBinding {
                target_partition_spec_id,
                partition_source_column_names: partition_source_column_names.clone(),
                partition_column_names: partition_column_names.clone(),
                partition_transform_exprs: transform_exprs.clone(),
                partition_source_field_ids,
                output_expr_count: lowered_output_exprs.len(),
            };
            let binding = bind_position_delete_descriptor_from_native(
                spec.position_delete_output_descriptor.as_ref(),
                expected,
            )?;
            Some((target_schema, binding))
        } else {
            None
        };

        let (output_schema, target_schema, equality_delete_columns) = match mode {
            IcebergSinkMode::Data => {
                let target_schema = build_full_output_schema(&iceberg_table)?;
                if lowered_output_exprs.len() != target_schema.fields().len() {
                    return Err(format!(
                        "native Iceberg sink output expr count mismatch: exprs={} columns={}",
                        lowered_output_exprs.len(),
                        target_schema.fields().len()
                    ));
                }
                (Arc::clone(&target_schema), target_schema, Vec::new())
            }
            IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors => {
                let (target_schema, binding) = position_delete_binding
                    .as_ref()
                    .expect("position delete binding must exist for delete-like sink");
                (
                    Arc::clone(&binding.output_schema),
                    Arc::clone(target_schema),
                    Vec::new(),
                )
            }
            IcebergSinkMode::EqualityDeletes => {
                validate_equality_delete_unpartitioned_target_metadata(
                    &iceberg_table,
                    target_partition_spec_id,
                )?;
                let (schema, columns) = build_equality_delete_output_schema(&iceberg_table)?;
                if lowered_output_exprs.len() != schema.fields().len() {
                    return Err(format!(
                        "native Iceberg equality-delete sink expects {} output exprs; got {}",
                        schema.fields().len(),
                        lowered_output_exprs.len()
                    ));
                }
                (Arc::clone(&schema), schema, columns)
            }
        };
        let partition_exprs = if mode == IcebergSinkMode::Data {
            build_partition_exprs_from_output_exprs(
                &partition_source_column_names,
                &transform_exprs,
                writer_columns,
                &lowered_output_exprs,
                &mut arena,
            )?
        } else {
            Vec::new()
        };

        let row_lineage_data = mode == IcebergSinkMode::Data
            && schema_has_reserved_row_lineage_columns(&target_schema)?;
        let table_location = if spec.table_location.is_empty() {
            iceberg_table_location(iceberg_table.serialized_metadata.as_deref()).unwrap_or_else(
                || {
                    spec.iceberg
                        .as_ref()
                        .map(|t| t.location.clone())
                        .unwrap_or_default()
                },
            )
        } else {
            spec.table_location.clone()
        };
        if table_location.is_empty() {
            return Err("native Iceberg write sink missing table location".to_string());
        }
        let data_location = if spec.data_location.is_empty() {
            format!("{}/data", table_location.trim_end_matches('/'))
        } else {
            spec.data_location.clone()
        };
        let object_store_s3 =
            resolve_native_sink_s3_config(&data_location, &spec.cloud_properties)?;
        let target_snapshot_id = iceberg_table.current_snapshot_id;
        let position_delete_data_file_partitions = if matches!(
            mode,
            IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors
        ) {
            let metadata = target_table_metadata.as_ref().ok_or_else(|| {
                "native Iceberg delete sink missing target table metadata".to_string()
            })?;
            build_position_delete_data_file_partition_index(
                metadata,
                target_snapshot_id,
                &table_location,
                object_store_s3.as_ref(),
            )?
        } else {
            HashMap::new()
        };
        let (file_format, report_file_format) =
            validate_iceberg_sink_file_format(&spec.file_format)?;
        let compression = map_native_compression(spec.compression)?;

        let plan = IcebergSinkPlan {
            mode,
            table_location,
            data_location,
            target_partition_spec_id,
            target_table_metadata,
            target_snapshot_id,
            position_delete_data_file_partitions,
            object_store_s3,
            file_format,
            report_file_format,
            compression,
            output_schema,
            target_schema,
            equality_delete_columns,
            row_lineage_data,
            output_exprs: lowered_output_exprs,
            partition_exprs,
            partition_source_column_names,
            partition_column_names,
            transform_exprs,
            position_delete_binding: position_delete_binding.map(|(_, binding)| binding),
        };

        Ok((
            IcebergSinkFactoryInput {
                name: "ICEBERG_TABLE_SINK".to_string(),
                arena,
                plan,
            },
            mode,
        ))
    })();
    decoded.map_err(Into::into)
}

fn decode_stream_partition_type(kind: i32) -> Result<DataStreamPartitionType, String> {
    match plan::PartitionKind::try_from(kind)
        .map_err(|_| format!("unknown native PartitionKind value {kind}"))?
    {
        plan::PartitionKind::Unpartitioned => Ok(DataStreamPartitionType::Unpartitioned),
        plan::PartitionKind::Random => Ok(DataStreamPartitionType::Random),
        plan::PartitionKind::Hash => Ok(DataStreamPartitionType::HashPartitioned),
        plan::PartitionKind::Unspecified => {
            Err("native DataPartition kind is unspecified".to_string())
        }
    }
}

fn lower_output_exprs(
    output_exprs: &[expr::Expr],
    arena: &mut ExprArena,
    layout: &super::layout::Layout,
) -> Result<Vec<ExprId>, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<Vec<ExprId>, String> {
        if output_exprs.is_empty() {
            return Err("native Iceberg sink missing output exprs".to_string());
        }
        output_exprs
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                decode_expr(expr, arena, layout)
                    .map_err(|err| format!("native Iceberg sink output_exprs[{idx}]: {err}"))
            })
            .collect()
    })();
    decoded.map_err(Into::into)
}

fn lower_output_columns_as_slot_refs(
    output_columns: &[common::OutputColumn],
    input: Option<&plan::IcebergWriteInputBinding>,
    arena: &mut ExprArena,
) -> Result<Vec<ExprId>, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<Vec<ExprId>, String> {
        let selected =
            match input.and_then(|input| input.kind.as_ref()) {
                Some(plan::iceberg_write_input_binding::Kind::RootOutputByOrdinal(true)) | None => {
                    output_columns.iter().collect::<Vec<_>>()
                }
                Some(plan::iceberg_write_input_binding::Kind::RootOutputByOrdinal(false)) => {
                    return Err(
                        "native Iceberg write sink root_output_by_ordinal marker must be true"
                            .to_string(),
                    );
                }
                Some(plan::iceberg_write_input_binding::Kind::OutputOrdinals(ordinals)) => {
                    ordinals
                        .values
                        .iter()
                        .map(|ordinal| {
                            let idx = usize::try_from(*ordinal).map_err(|_| {
                    format!("native Iceberg write sink output ordinal {ordinal} overflows usize")
                })?;
                            output_columns.get(idx).ok_or_else(|| {
                    format!("native Iceberg write sink output ordinal {ordinal} is out of range")
                })
                        })
                        .collect::<Result<Vec<_>, _>>()?
                }
            };
        if selected.is_empty() {
            return Err(
                "native Iceberg write sink requires at least one output column".to_string(),
            );
        }
        selected
            .into_iter()
            .map(|column| {
                let data_type = column
                    .r#type
                    .as_ref()
                    .ok_or_else(|| {
                        format!(
                            "native Iceberg write sink output column {} missing type",
                            column.name
                        )
                    })
                    .and_then(decode_type)?;
                Ok(arena.push_typed(ExprNode::SlotId(SlotId::new(column.column_id)), data_type))
            })
            .collect()
    })();
    decoded.map_err(Into::into)
}

fn iceberg_sink_mode_from_native(value: i32) -> Result<IcebergSinkMode, String> {
    let mode = plan::IcebergWriteSinkMode::try_from(value)
        .map_err(|_| format!("unknown native IcebergWriteSinkMode value {value}"))?;
    match mode {
        plan::IcebergWriteSinkMode::Data | plan::IcebergWriteSinkMode::RowLineageData => {
            Ok(IcebergSinkMode::Data)
        }
        plan::IcebergWriteSinkMode::PositionDeletes => Ok(IcebergSinkMode::PositionDeletes),
        plan::IcebergWriteSinkMode::DeletionVectors => Ok(IcebergSinkMode::DeletionVectors),
        plan::IcebergWriteSinkMode::EqualityDeletes => Ok(IcebergSinkMode::EqualityDeletes),
        plan::IcebergWriteSinkMode::Unspecified => {
            Err("native Iceberg write sink mode is unspecified".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_destination(id: i64) -> plan::StreamDestination {
        plan::StreamDestination {
            finst_id: Some(common::UniqueId { hi: 1, lo: id }),
            endpoint: "127.0.0.1:8060".to_string(),
        }
    }

    fn instance_destination(id: i64) -> novarocks::Destination {
        novarocks::Destination {
            finst_id: Some(common::UniqueId { hi: 2, lo: id }),
            endpoint: "127.0.0.1:8061".to_string(),
        }
    }

    fn assert_single_destination_group(assignment: FragmentSinkAssignment, expected_lo: i64) {
        let FragmentSinkAssignment::DestinationGroups { groups, sender_id } = assignment else {
            panic!("expected destination groups");
        };
        assert_eq!(sender_id, None);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 1);
        assert_eq!(groups[0][0].finst_id().lo, expected_lo);
    }

    #[test]
    fn multicast_assignment_ignores_redundant_flat_instance_destinations() {
        let sink = plan::DataSink {
            kind: Some(plan::data_sink::Kind::MultiCastDataStream(
                plan::MultiCastDataStreamSink {
                    sinks: Vec::new(),
                    destinations: vec![plan::StreamDestinationList {
                        destinations: vec![plan_destination(11)],
                    }],
                },
            )),
        };
        let instance = novarocks::InstanceParams {
            destinations: vec![instance_destination(99)],
            ..Default::default()
        };

        let assignment = decode_fragment_sink_assignment(&sink, &instance)
            .expect("redundant flat destinations must remain wire compatible");

        assert_single_destination_group(assignment, 11);
    }

    #[test]
    fn stream_assignment_preserves_instance_destination_field_path() {
        let sink = plan::DataSink {
            kind: Some(plan::data_sink::Kind::DataStream(
                plan::DataStreamSink::default(),
            )),
        };
        let instance = novarocks::InstanceParams {
            destinations: vec![novarocks::Destination::default()],
            ..Default::default()
        };

        let error = decode_fragment_sink_assignment(&sink, &instance)
            .expect_err("missing destination finst id must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "instance_params.destinations[0].finst_id"
        );
        assert_eq!(
            protocol.kind(),
            crate::protocol::common::error::ProtocolErrorKind::MissingField
        );
    }

    #[test]
    fn router_assignment_ignores_redundant_flat_instance_destinations() {
        let sink = plan::DataSink {
            kind: Some(plan::data_sink::Kind::IcebergChangeStreamRouter(
                plan::IcebergChangeStreamRouterSink {
                    branches: vec![plan::IcebergChangeStreamBranchRoute {
                        destinations: Some(plan::StreamDestinationList {
                            destinations: vec![plan_destination(12)],
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )),
        };
        let instance = novarocks::InstanceParams {
            destinations: vec![instance_destination(98)],
            ..Default::default()
        };

        let assignment = decode_fragment_sink_assignment(&sink, &instance)
            .expect("redundant flat destinations must remain wire compatible");

        assert_single_destination_group(assignment, 12);
    }

    #[test]
    fn router_branch_rejects_duplicate_output_slots() {
        let output_columns = vec![common::OutputColumn {
            column_id: 7,
            name: "value".to_string(),
            ..Default::default()
        }];

        let error = decode_router_output_slots(&[0, 0], &output_columns, "branch[0] output")
            .expect_err("duplicate router output slots must be rejected during decode");

        assert_eq!(
            error,
            "native ICEBERG_CHANGE_STREAM_ROUTER_SINK branch[0] output: duplicate output slot id: 7"
        );
    }
}
