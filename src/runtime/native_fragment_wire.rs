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
use crate::thrift::{data_sinks, internal_service, runtime_filter, types};

pub(crate) type NetworkAddress = types::TNetworkAddress;
pub(crate) type QueryOptions = internal_service::TQueryOptions;
pub(crate) type ResultSinkType = data_sinks::TResultSinkType;
pub(crate) type RuntimeFilterParams = runtime_filter::TRuntimeFilterParams;

#[cfg(test)]
pub(crate) type SpillMode = internal_service::TSpillMode;
#[cfg(test)]
pub(crate) type UniqueId = types::TUniqueId;

pub(crate) fn query_options_from_native(
    src: &proto::novarocks::QueryOptions,
) -> Result<QueryOptions, String> {
    let mut opts = internal_service::TQueryOptions::default();
    opts.batch_size = (src.batch_size > 0).then_some(src.batch_size);
    opts.mem_limit = (src.mem_limit > 0).then_some(src.mem_limit);
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
