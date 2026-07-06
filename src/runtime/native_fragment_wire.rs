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

//! Runtime wire adapters for native fragment submission.

use std::collections::BTreeMap;

use thrift::OrderedFloat;

use crate::proto;
use crate::runtime::runtime_state::{QueryOptions, RuntimeFilterParams};
use crate::thrift::{data_sinks, internal_service, partitions, runtime_filter, types};

pub(crate) type NetworkAddress = types::TNetworkAddress;
pub(crate) type DataStreamSink = data_sinks::TDataStreamSink;
pub(crate) type IcebergChangeStreamRouterBranch = data_sinks::TIcebergChangeStreamRouterBranch;
pub(crate) type IcebergChangeStreamRouterBranchKind =
    data_sinks::TIcebergChangeStreamRouterBranchKind;
pub(crate) type IcebergChangeStreamRouterSink = data_sinks::TIcebergChangeStreamRouterSink;
pub(crate) type MultiCastDataStreamSink = data_sinks::TMultiCastDataStreamSink;
pub(crate) type PlanFragmentDestination = data_sinks::TPlanFragmentDestination;
pub(crate) type PlanFragmentExecParams = internal_service::TPlanFragmentExecParams;
pub(crate) type DataPartition = partitions::TDataPartition;
pub(crate) type ResultSinkType = data_sinks::TResultSinkType;

#[cfg(test)]
pub(crate) type SpillMode = internal_service::TSpillMode;
#[cfg(test)]
pub(crate) type UniqueId = types::TUniqueId;

pub(crate) fn query_options_from_native(
    src: &proto::novarocks::QueryOptions,
) -> Result<QueryOptions, String> {
    let mut opts = internal_service::TQueryOptions::default();
    opts.batch_size = (src.batch_size > 0).then_some(src.batch_size);
    opts.query_timeout = (src.query_timeout > 0).then_some(src.query_timeout);
    opts.enable_profile = Some(src.enable_profile);
    opts.pipeline_dop = (src.pipeline_dop > 0).then_some(src.pipeline_dop);
    opts.query_mem_limit = (src.query_mem_limit > 0).then_some(src.query_mem_limit);
    opts.connector_io_tasks_per_scan_operator = (src.connector_io_tasks_per_scan_operator > 0)
        .then_some(src.connector_io_tasks_per_scan_operator);
    opts.io_tasks_per_scan_operator = opts.connector_io_tasks_per_scan_operator;
    opts.runtime_filter_scan_wait_time_ms =
        (src.runtime_filter_scan_wait_time_ms > 0).then_some(src.runtime_filter_scan_wait_time_ms);
    opts.runtime_filter_wait_timeout_ms =
        (src.runtime_filter_wait_timeout_ms > 0).then_some(src.runtime_filter_wait_timeout_ms);
    opts.allow_throw_exception = Some(src.allow_throw_exception);
    opts.group_concat_max_len = (src.group_concat_max_len > 0).then_some(src.group_concat_max_len);
    opts.enable_spill = Some(src.enable_spill);
    opts.spill_options = src.spill_options.as_ref().map(spill_options_from_native);
    if let Some(spill_options) = opts.spill_options.as_ref() {
        opts.spill_mode = spill_options.spill_mode;
        opts.spill_mem_table_size = spill_options.spill_mem_table_size;
        opts.spill_mem_table_num = spill_options.spill_mem_table_num;
        opts.spill_mem_limit_threshold = spill_options.spill_mem_limit_threshold;
        opts.spill_operator_min_bytes = spill_options.spill_operator_min_bytes;
        opts.spill_operator_max_bytes = spill_options.spill_operator_max_bytes;
        opts.spill_encode_level = spill_options.spill_encode_level;
    } else if src.enable_spill {
        return Err("native QueryOptions enable_spill=true requires spill_options".to_string());
    }
    Ok(opts)
}

fn spill_options_from_native(
    src: &proto::novarocks::SpillOptions,
) -> internal_service::TSpillOptions {
    let mut opts = internal_service::TSpillOptions::default();
    opts.spill_mode = (src.spill_mode != 0).then_some(src.spill_mode.into());
    opts.spill_mem_limit_threshold = (src.spill_mem_limit_threshold > 0.0)
        .then_some(OrderedFloat(src.spill_mem_limit_threshold));
    opts.spill_operator_min_bytes =
        (src.spill_operator_min_bytes > 0).then_some(src.spill_operator_min_bytes);
    opts.spill_operator_max_bytes =
        (src.spill_operator_max_bytes > 0).then_some(src.spill_operator_max_bytes);
    opts.spill_encode_level = (src.spill_encode_level > 0).then_some(src.spill_encode_level);
    opts.enable_spill_buffer_read = Some(src.enable_spill_buffer_read);
    opts.max_spill_read_buffer_bytes_per_driver = (src.max_spill_read_buffer_bytes_per_driver > 0)
        .then_some(src.max_spill_read_buffer_bytes_per_driver);
    opts.spill_mem_table_size = (src.spill_mem_table_size > 0).then_some(src.spill_mem_table_size);
    opts.spill_mem_table_num = (src.spill_mem_table_num > 0).then_some(src.spill_mem_table_num);
    opts
}

pub(crate) fn runtime_filter_params_from_native(
    src: &proto::novarocks::RuntimeFilterParams,
) -> Result<RuntimeFilterParams, String> {
    let id_to_prober_params = src
        .id_to_prober_params
        .iter()
        .map(|(filter_id, list)| {
            let params = list
                .params
                .iter()
                .map(prober_params_from_native)
                .collect::<Result<Vec<_>, _>>()?;
            Ok((*filter_id, params))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    let runtime_filter_builder_number = src
        .runtime_filter_builder_number
        .iter()
        .map(|(filter_id, count)| (*filter_id, *count))
        .collect::<BTreeMap<_, _>>();

    Ok(runtime_filter::TRuntimeFilterParams::new(
        (!id_to_prober_params.is_empty()).then_some(id_to_prober_params),
        (!runtime_filter_builder_number.is_empty()).then_some(runtime_filter_builder_number),
        (src.runtime_filter_max_size > 0).then_some(src.runtime_filter_max_size),
        None,
    ))
}

fn prober_params_from_native(
    src: &proto::novarocks::ProberParams,
) -> Result<runtime_filter::TRuntimeFilterProberParams, String> {
    let fragment_instance_id = src
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "native ProberParams missing fragment_instance_id".to_string())?;
    let fragment_instance_address = network_address_from_native(&src.fragment_instance_address)?;
    Ok(runtime_filter::TRuntimeFilterProberParams::new(
        types::TUniqueId::new(fragment_instance_id.hi, fragment_instance_id.lo),
        fragment_instance_address,
    ))
}

pub(crate) fn network_address_from_native(src: &str) -> Result<NetworkAddress, String> {
    let (host, port) = src
        .rsplit_once(':')
        .ok_or_else(|| format!("native network address must be host:port, got '{src}'"))?;
    if host.is_empty() {
        return Err(format!("native network address has empty host: '{src}'"));
    }
    let port = port
        .parse::<i32>()
        .map_err(|e| format!("native network address has invalid port '{src}': {e}"))?;
    Ok(types::TNetworkAddress::new(host.to_string(), port))
}

pub(crate) fn destination_from_native(
    src: &proto::novarocks::Destination,
) -> Result<PlanFragmentDestination, String> {
    let finst_id = src
        .finst_id
        .as_ref()
        .ok_or_else(|| "native Destination missing finst_id".to_string())?;
    Ok(data_sinks::TPlanFragmentDestination::new(
        types::TUniqueId::new(finst_id.hi, finst_id.lo),
        None::<types::TNetworkAddress>,
        Some(network_address_from_native(&src.brpc_addr)?),
        None::<i32>,
    ))
}

pub(crate) fn destinations_from_native(
    src: &[proto::novarocks::Destination],
) -> Result<Vec<PlanFragmentDestination>, String> {
    src.iter().map(destination_from_native).collect()
}

pub(crate) fn exec_params_from_native(
    src: &proto::novarocks::InstanceParams,
    destinations: Vec<PlanFragmentDestination>,
) -> Result<PlanFragmentExecParams, String> {
    let query_id = src
        .query_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing query_id".to_string())?;
    let fragment_instance_id = src
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing fragment_instance_id".to_string())?;
    Ok(internal_service::TPlanFragmentExecParams {
        query_id: types::TUniqueId::new(query_id.hi, query_id.lo),
        fragment_instance_id: types::TUniqueId::new(
            fragment_instance_id.hi,
            fragment_instance_id.lo,
        ),
        per_node_scan_ranges: Default::default(),
        per_exch_num_senders: src
            .per_exch_num_senders
            .iter()
            .map(|(node_id, count)| (*node_id, *count))
            .collect(),
        destinations: Some(destinations),
        sender_id: None,
        num_senders: None,
        send_query_statistics_with_every_batch: None,
        use_vectorized: None,
        runtime_filter_params: None,
        instances_number: None,
        enable_exchange_pass_through: None,
        node_to_per_driver_seq_scan_ranges: None,
        enable_exchange_perf: None,
        pipeline_sink_dop: None,
        report_when_finish: None,
        exec_debug_options: None,
    })
}

pub(crate) fn data_partition_without_exprs(
    src: &proto::plan::DataPartition,
) -> Result<DataPartition, String> {
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
) -> Result<DataStreamSink, String> {
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
) -> Result<PlanFragmentDestination, String> {
    let finst_id = src
        .finst_id
        .as_ref()
        .ok_or_else(|| "native StreamDestination missing finst_id".to_string())?;
    Ok(data_sinks::TPlanFragmentDestination::new(
        types::TUniqueId::new(finst_id.hi, finst_id.lo),
        None::<types::TNetworkAddress>,
        Some(network_address_from_native(&src.brpc_addr)?),
        None::<i32>,
    ))
}

pub(crate) fn stream_destinations_from_native(
    src: &proto::plan::StreamDestinationList,
) -> Result<Vec<PlanFragmentDestination>, String> {
    src.destinations
        .iter()
        .map(stream_destination_from_native)
        .collect()
}

pub(crate) fn multi_cast_data_stream_sink_from_native(
    src: &proto::plan::MultiCastDataStreamSink,
) -> Result<MultiCastDataStreamSink, String> {
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
    Ok(data_sinks::TMultiCastDataStreamSink::new(
        sinks,
        destinations,
    ))
}

pub(crate) fn iceberg_change_stream_branch_kind_from_native(
    value: i32,
) -> Result<IcebergChangeStreamRouterBranchKind, String> {
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
