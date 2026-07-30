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

use super::scan::{encode_column_def, encode_iceberg_table_info, encode_table_def_with_context};
use super::type_mapping::{
    encode_change_stream_branch_kind, encode_data_partition, encode_iceberg_write_sink_mode,
    usize_to_u64,
};
use super::{NativePlanEncodeContext, required_context_ref};
use crate::proto::plan;
use crate::protocol::native::type_mapping::encode_type;
use crate::sql::planner::distributed::FragmentId;
use crate::sql::planner::distributed::write::change_stream::IcebergChangeStreamRouterSink;
use crate::sql::planner::distributed::write::sink::{
    IcebergWriteFileCompression, IcebergWriteInputBinding, IcebergWriteSinkSpec,
};

pub(super) fn encode_iceberg_change_stream_router_sink(
    src: &IcebergChangeStreamRouterSink,
    fragment_id: FragmentId,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::IcebergChangeStreamRouterSink, String> {
    Ok(plan::IcebergChangeStreamRouterSink {
        group_id: src.group_id,
        change_op_output_ordinal: usize_to_u64(src.change_op_output_ordinal),
        data_route_output_ordinal: src.data_route_output_ordinal.map(usize_to_u64),
        branches: src
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

/// Map a change-stream router branch's finalized partition from the sealed write
/// contract (CGO-9C Task 3). The planner already reconstructed the partition
/// expression from the branch's ordinals against the router fragment's output
/// columns at seal; the encoder maps the typed result 1:1.
fn encode_finalized_router_branch_partition(
    ctx: &NativePlanEncodeContext<'_>,
    fragment_id: FragmentId,
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

pub(super) fn encode_iceberg_write_sink_spec(
    src: &IcebergWriteSinkSpec,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::IcebergWriteSinkSpec, String> {
    Ok(plan::IcebergWriteSinkSpec {
        mode: encode_iceberg_write_sink_mode(src.mode),
        target_table_id: src.target_table_id,
        target_table: Some(encode_table_def_with_context(
            &src.target_table,
            None,
            None,
            None,
            None,
            None,
            ctx,
        )?),
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

pub(super) fn encode_iceberg_write_input_binding(
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
