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

//! Compat-only adapters from native plan sink payloads to StarRocks thrift.

use crate::common::types::UniqueId;
use crate::proto;
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::thrift::{data_sinks, partitions, types};

pub(crate) fn data_partition_without_exprs(
    src: &proto::plan::DataPartition,
) -> Result<partitions::TDataPartition, String> {
    let partition_type = match proto::plan::PartitionKind::try_from(src.kind)
        .map_err(|_| format!("unknown native PartitionKind value {}", src.kind))?
    {
        proto::plan::PartitionKind::Unpartitioned => partitions::TPartitionType::UNPARTITIONED,
        proto::plan::PartitionKind::Random => partitions::TPartitionType::RANDOM,
        proto::plan::PartitionKind::Hash => partitions::TPartitionType::HASH_PARTITIONED,
        proto::plan::PartitionKind::Unspecified => {
            return Err("native DataPartition kind is unspecified".to_string());
        }
    };
    Ok(partitions::TDataPartition::new(
        partition_type,
        None::<Vec<crate::thrift::exprs::TExpr>>,
        None::<Vec<partitions::TRangePartition>>,
        None::<Vec<partitions::TBucketProperty>>,
    ))
}

pub(crate) fn data_stream_sink_from_native(
    src: &proto::plan::DataStreamSink,
) -> Result<data_sinks::TDataStreamSink, String> {
    let output_partition = src
        .output_partition
        .as_ref()
        .ok_or_else(|| "native DATA_STREAM_SINK missing output_partition".to_string())
        .and_then(data_partition_without_exprs)?;
    let output_columns = (!src.output_columns.is_empty()).then_some(src.output_columns.clone());
    Ok(data_sinks::TDataStreamSink::new(
        src.dest_node_id,
        output_partition,
        None::<bool>,
        None::<bool>,
        None::<i32>,
        output_columns,
        src.limit,
    ))
}

pub(crate) fn stream_destination_from_native(
    src: &proto::plan::StreamDestination,
) -> Result<FragmentDestination, String> {
    let finst_id = src
        .finst_id
        .as_ref()
        .ok_or_else(|| "native StreamDestination missing finst_id".to_string())?;
    Ok(FragmentDestination::new(
        UniqueId {
            hi: finst_id.hi,
            lo: finst_id.lo,
        },
        RuntimeEndpoint::parse(&src.endpoint)?,
    ))
}

pub(crate) fn stream_destinations_from_native(
    src: &proto::plan::StreamDestinationList,
) -> Result<Vec<FragmentDestination>, String> {
    src.destinations
        .iter()
        .map(stream_destination_from_native)
        .collect()
}

pub(crate) fn multi_cast_data_stream_sink_from_native(
    src: &proto::plan::MultiCastDataStreamSink,
) -> Result<data_sinks::TMultiCastDataStreamSink, String> {
    if src.sinks.len() != src.destinations.len() {
        return Err(format!(
            "native MULTI_CAST_DATA_STREAM_SINK sinks size {} != destinations size {}",
            src.sinks.len(),
            src.destinations.len()
        ));
    }
    let sinks = src
        .sinks
        .iter()
        .map(data_stream_sink_from_native)
        .collect::<Result<Vec<_>, _>>()?;
    let destinations = src
        .destinations
        .iter()
        .map(stream_destinations_from_native)
        .collect::<Result<Vec<_>, _>>()?;
    let destinations = destinations
        .into_iter()
        .map(|group| {
            group
                .into_iter()
                .map(fragment_destination_to_thrift)
                .collect()
        })
        .collect();
    Ok(data_sinks::TMultiCastDataStreamSink::new(
        sinks,
        destinations,
    ))
}

pub(crate) fn iceberg_change_stream_router_sink_from_native(
    src: &proto::plan::IcebergChangeStreamRouterSink,
) -> Result<data_sinks::TIcebergChangeStreamRouterSink, String> {
    let mut branches = Vec::with_capacity(src.branches.len());
    for branch in &src.branches {
        let stream_sink = data_sinks::TDataStreamSink::new(
            branch.target_exchange_node_id,
            branch
                .output_partition
                .as_ref()
                .ok_or_else(|| {
                    format!(
                        "native ICEBERG_CHANGE_STREAM_ROUTER_SINK branch {} missing output_partition",
                        branch.branch_id
                    )
                })
                .and_then(data_partition_without_exprs)?,
            None::<bool>,
            None::<bool>,
            None::<i32>,
            (!branch.output_ordinals.is_empty()).then_some(
                branch
                    .output_ordinals
                    .iter()
                    .map(|ordinal| {
                        i32::try_from(*ordinal).map_err(|_| {
                            format!(
                                "native ICEBERG_CHANGE_STREAM_ROUTER_SINK branch {} output ordinal {} exceeds i32",
                                branch.branch_id, ordinal
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            None::<i64>,
        );
        let destinations = branch
            .destinations
            .as_ref()
            .ok_or_else(|| {
                format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK branch {} missing destinations",
                    branch.branch_id
                )
            })
            .and_then(stream_destinations_from_native)?
            .into_iter()
            .map(fragment_destination_to_thrift)
            .collect();
        branches.push(data_sinks::TIcebergChangeStreamRouterBranch::new(
            branch.branch_id,
            iceberg_change_stream_branch_kind_from_native(branch.branch_kind)?,
            stream_sink,
            destinations,
        ));
    }
    Ok(data_sinks::TIcebergChangeStreamRouterSink::new(
        i32::try_from(src.change_op_output_ordinal).map_err(|_| {
            format!(
                "native ICEBERG_CHANGE_STREAM_ROUTER_SINK change_op ordinal {} exceeds i32",
                src.change_op_output_ordinal
            )
        })?,
        src.data_route_output_ordinal
            .map(|ordinal| {
                i32::try_from(ordinal).map_err(|_| {
                    format!(
                        "native ICEBERG_CHANGE_STREAM_ROUTER_SINK data_route ordinal {ordinal} exceeds i32"
                    )
                })
            })
            .transpose()?,
        branches,
    ))
}

pub(crate) fn iceberg_change_stream_branch_kind_from_native(
    value: i32,
) -> Result<data_sinks::TIcebergChangeStreamRouterBranchKind, String> {
    match proto::plan::ChangeStreamBranchKind::try_from(value)
        .map_err(|_| format!("unknown native ChangeStreamBranchKind value {value}"))?
    {
        proto::plan::ChangeStreamBranchKind::DeleteDv => {
            Ok(data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV)
        }
        proto::plan::ChangeStreamBranchKind::ReuseData => {
            Ok(data_sinks::TIcebergChangeStreamRouterBranchKind::REUSE_DATA)
        }
        proto::plan::ChangeStreamBranchKind::FreshData => {
            Ok(data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA)
        }
        proto::plan::ChangeStreamBranchKind::Unspecified => {
            Err("native ChangeStreamBranchKind is unspecified".to_string())
        }
    }
}

fn fragment_destination_to_thrift(
    destination: FragmentDestination,
) -> data_sinks::TPlanFragmentDestination {
    data_sinks::TPlanFragmentDestination::new(
        types::TUniqueId::new(destination.finst_id().hi, destination.finst_id().lo),
        None::<types::TNetworkAddress>,
        Some(destination.endpoint().to_network_address()),
        None::<i32>,
    )
}
