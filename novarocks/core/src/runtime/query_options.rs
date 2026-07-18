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

use std::time::Duration;

use crate::exec::spill::{SpillConfig, SpillMode};

#[cfg(feature = "compat")]
use crate::thrift::internal_service::{TQueryOptions, TSpillMode};

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct QueryOptions {
    pub(crate) batch_size: Option<i32>,
    pub(crate) query_timeout: Option<i32>,
    pub(crate) query_delivery_timeout: Option<i32>,
    pub(crate) enable_profile: bool,
    pub(crate) runtime_profile_report_interval: Option<i64>,
    pub(crate) pipeline_dop: Option<i32>,
    pub(crate) exec_mem_limit: Option<i64>,
    pub(crate) connector_io_tasks_per_scan_operator: Option<i32>,
    pub(crate) runtime_filter_scan_wait_time_ms: Option<i64>,
    pub(crate) runtime_filter_wait_timeout_ms: Option<i32>,
    pub(crate) allow_throw_exception: bool,
    pub(crate) group_concat_max_len: Option<i64>,
    pub(crate) enable_join_runtime_bitset_filter: Option<bool>,
    pub(crate) global_runtime_filter_build_max_size: Option<i64>,
    pub(crate) cache: QueryCacheOptions,
    pub(crate) spill: Option<SpillConfig>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct QueryCacheOptions {
    pub(crate) enable_scan_datacache: bool,
    pub(crate) enable_populate_datacache: bool,
    pub(crate) enable_datacache_async_populate_mode: bool,
    pub(crate) enable_datacache_io_adaptor: bool,
    pub(crate) enable_cache_select: bool,
    pub(crate) datacache_evict_probability: Option<i32>,
    pub(crate) datacache_priority: Option<i32>,
    pub(crate) datacache_ttl_seconds: Option<i64>,
    pub(crate) datacache_sharing_work_period: Option<i64>,
}

impl QueryOptions {
    #[cfg(feature = "compat")]
    pub(crate) fn from_thrift(opts: Option<&TQueryOptions>) -> Result<Self, String> {
        let Some(opts) = opts else {
            return Ok(Self::default());
        };
        Ok(Self {
            batch_size: opts.batch_size,
            query_timeout: opts.query_timeout,
            query_delivery_timeout: opts.query_delivery_timeout,
            enable_profile: opts.enable_profile.unwrap_or(false),
            runtime_profile_report_interval: opts.runtime_profile_report_interval,
            pipeline_dop: opts.pipeline_dop,
            exec_mem_limit: opts.query_mem_limit.or(opts.mem_limit),
            connector_io_tasks_per_scan_operator: opts
                .connector_io_tasks_per_scan_operator
                .or(opts.io_tasks_per_scan_operator),
            runtime_filter_scan_wait_time_ms: opts.runtime_filter_scan_wait_time_ms,
            runtime_filter_wait_timeout_ms: opts.runtime_filter_wait_timeout_ms,
            allow_throw_exception: opts.allow_throw_exception.unwrap_or(false),
            group_concat_max_len: opts.group_concat_max_len,
            enable_join_runtime_bitset_filter: opts.enable_join_runtime_bitset_filter,
            global_runtime_filter_build_max_size: opts.global_runtime_filter_build_max_size,
            cache: QueryCacheOptions {
                enable_scan_datacache: opts.enable_scan_datacache.unwrap_or(false),
                enable_populate_datacache: opts.enable_populate_datacache.unwrap_or(false),
                enable_datacache_async_populate_mode: opts
                    .enable_datacache_async_populate_mode
                    .unwrap_or(false),
                enable_datacache_io_adaptor: opts.enable_datacache_io_adaptor.unwrap_or(false),
                enable_cache_select: opts.enable_cache_select.unwrap_or(false),
                datacache_evict_probability: opts.datacache_evict_probability,
                datacache_priority: opts.datacache_priority,
                datacache_ttl_seconds: opts.datacache_ttl_seconds,
                datacache_sharing_work_period: opts.datacache_sharing_work_period,
            },
            spill: spill_config_from_thrift(opts)?,
        })
    }
}

pub(crate) fn query_expire_durations(query_opts: Option<&QueryOptions>) -> (Duration, Duration) {
    let default_timeout = 300i32;
    let query_timeout = query_opts
        .and_then(|o| o.query_timeout)
        .unwrap_or(default_timeout)
        .max(1);
    let delivery_timeout = query_opts
        .and_then(|o| o.query_delivery_timeout)
        .map(|v| v.max(1).min(query_timeout))
        .unwrap_or(query_timeout);
    (
        Duration::from_secs(delivery_timeout as u64),
        Duration::from_secs(query_timeout as u64),
    )
}

#[cfg(feature = "compat")]
fn spill_config_from_thrift(opts: &TQueryOptions) -> Result<Option<SpillConfig>, String> {
    let enable_spill = opts.enable_spill.unwrap_or(false);
    if !enable_spill {
        return Ok(None);
    }

    let spill_opts = opts.spill_options.as_ref();
    let spill_mode = spill_opts
        .and_then(|v| v.spill_mode)
        .or(opts.spill_mode)
        .ok_or_else(|| "spill_mode is required when enable_spill=true".to_string())
        .and_then(spill_mode_from_thrift)?;
    validate_spill_mode(spill_mode)?;

    let spill_enable_direct_io = spill_opts
        .and_then(|v| v.spill_enable_direct_io)
        .or(opts.spill_enable_direct_io)
        .unwrap_or(false);
    if spill_enable_direct_io {
        return Err("spill_enable_direct_io=true is not supported".to_string());
    }

    let enable_spill_to_remote_storage = spill_opts
        .and_then(|v| v.enable_spill_to_remote_storage)
        .unwrap_or(false);
    if enable_spill_to_remote_storage {
        return Err("spill to remote storage is not supported".to_string());
    }

    if let Some(opts) = spill_opts.and_then(|v| v.spill_to_remote_storage_options.as_ref())
        && opts.disable_spill_to_local_disk.unwrap_or(false)
    {
        return Err(
            "spill_to_remote_storage_options.disable_spill_to_local_disk=true is not supported"
                .to_string(),
        );
    }

    Ok(Some(SpillConfig {
        enable_spill,
        spill_mode,
        spill_mem_limit_threshold: spill_opts
            .and_then(|v| v.spill_mem_limit_threshold.map(|v| v.into_inner()))
            .or_else(|| opts.spill_mem_limit_threshold.map(|v| v.into_inner())),
        spill_operator_min_bytes: spill_opts
            .and_then(|v| v.spill_operator_min_bytes)
            .or(opts.spill_operator_min_bytes),
        spill_operator_max_bytes: spill_opts
            .and_then(|v| v.spill_operator_max_bytes)
            .or(opts.spill_operator_max_bytes),
        spill_encode_level: spill_opts
            .and_then(|v| v.spill_encode_level)
            .or(opts.spill_encode_level),
        enable_spill_buffer_read: spill_opts.and_then(|v| v.enable_spill_buffer_read),
        max_spill_read_buffer_bytes_per_driver: spill_opts
            .and_then(|v| v.max_spill_read_buffer_bytes_per_driver),
        spill_mem_table_size: spill_opts
            .and_then(|v| v.spill_mem_table_size)
            .or(opts.spill_mem_table_size),
        spill_mem_table_num: spill_opts
            .and_then(|v| v.spill_mem_table_num)
            .or(opts.spill_mem_table_num),
    }))
}

#[cfg(feature = "compat")]
fn spill_mode_from_thrift(mode: TSpillMode) -> Result<SpillMode, String> {
    match mode {
        TSpillMode::NONE => Ok(SpillMode::None),
        TSpillMode::FORCE => Ok(SpillMode::Force),
        TSpillMode::AUTO => Ok(SpillMode::Auto),
        TSpillMode::RANDOM => Ok(SpillMode::Random),
        TSpillMode(value) => Err(format!("unknown spill_mode value: {value}")),
    }
}

fn validate_spill_mode(mode: SpillMode) -> Result<(), String> {
    if mode == SpillMode::Random {
        return Err("spill_mode RANDOM is not supported yet".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_expire_durations_use_delivery_timeout_cap() {
        let native = QueryOptions {
            query_timeout: Some(60),
            query_delivery_timeout: Some(120),
            ..Default::default()
        };
        let (delivery, query) = query_expire_durations(Some(&native));
        assert_eq!(query.as_secs(), 60);
        assert_eq!(delivery.as_secs(), 60);
    }
}
