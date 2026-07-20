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

use crate::exec::spill::{SpillConfig, SpillMode};
use crate::protocol::common::error::FieldPath;
use crate::protocol::starrocks::compat::options::normalize_query_option_aliases;
use crate::runtime::query_options::{QueryCacheOptions, QueryOptions};
use crate::thrift::internal_service::{TQueryOptions, TSpillMode};

use super::StarRocksFragmentDecodeError;

pub(crate) fn decode_query_options(
    options: Option<&TQueryOptions>,
) -> Result<QueryOptions, StarRocksFragmentDecodeError> {
    let Some(options) = options else {
        return Ok(QueryOptions::default());
    };
    let aliases = normalize_query_option_aliases(options);
    Ok(QueryOptions {
        batch_size: options.batch_size,
        query_timeout: options.query_timeout,
        query_delivery_timeout: options.query_delivery_timeout,
        enable_profile: options.enable_profile.unwrap_or(false),
        runtime_profile_report_interval: options.runtime_profile_report_interval,
        pipeline_dop: options.pipeline_dop,
        exec_mem_limit: aliases.exec_mem_limit,
        connector_io_tasks_per_scan_operator: aliases.connector_io_tasks_per_scan_operator,
        orc_use_column_names: options.orc_use_column_names.unwrap_or(false),
        enable_file_metacache: options.enable_file_metacache.unwrap_or(false),
        enable_file_pagecache: options.enable_file_pagecache.unwrap_or(false),
        enable_parquet_reader_page_index: options.enable_parquet_reader_page_index.unwrap_or(false),
        runtime_filter_scan_wait_time_ms: options.runtime_filter_scan_wait_time_ms,
        runtime_filter_wait_timeout_ms: options.runtime_filter_wait_timeout_ms,
        allow_throw_exception: options.allow_throw_exception.unwrap_or(false),
        group_concat_max_len: options.group_concat_max_len,
        enable_join_runtime_bitset_filter: options.enable_join_runtime_bitset_filter,
        global_runtime_filter_build_max_size: options.global_runtime_filter_build_max_size,
        cache: QueryCacheOptions {
            enable_scan_datacache: options.enable_scan_datacache.unwrap_or(false),
            enable_populate_datacache: options.enable_populate_datacache.unwrap_or(false),
            enable_datacache_async_populate_mode: options
                .enable_datacache_async_populate_mode
                .unwrap_or(false),
            enable_datacache_io_adaptor: options.enable_datacache_io_adaptor.unwrap_or(false),
            enable_cache_select: options.enable_cache_select.unwrap_or(false),
            datacache_evict_probability: options.datacache_evict_probability,
            datacache_priority: options.datacache_priority,
            datacache_ttl_seconds: options.datacache_ttl_seconds,
            datacache_sharing_work_period: options.datacache_sharing_work_period,
        },
        spill: decode_spill_config(options, aliases)?,
    })
}

fn decode_spill_config(
    options: &TQueryOptions,
    aliases: crate::protocol::starrocks::compat::options::NormalizedQueryOptionAliases,
) -> Result<Option<SpillConfig>, StarRocksFragmentDecodeError> {
    if !options.enable_spill.unwrap_or(false) {
        return Ok(None);
    }
    let root = FieldPath::root("exec_plan_fragment").field("query_options");
    let spill_options = options.spill_options.as_ref();
    let spill_mode_path = if spill_options.and_then(|spill| spill.spill_mode).is_some() {
        root.clone().field("spill_options").field("spill_mode")
    } else {
        root.clone().field("spill_mode")
    };
    let spill_mode = aliases.spill_mode.ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            spill_mode_path.clone(),
            "spill_mode is required when enable_spill=true",
        )
    })?;
    let spill_mode = decode_spill_mode(spill_mode, spill_mode_path)?;

    if aliases.spill_enable_direct_io.unwrap_or(false) {
        let path = if spill_options
            .and_then(|spill| spill.spill_enable_direct_io)
            .is_some()
        {
            root.clone()
                .field("spill_options")
                .field("spill_enable_direct_io")
        } else {
            root.clone().field("spill_enable_direct_io")
        };
        return Err(StarRocksFragmentDecodeError::unsupported(
            path,
            "spill_enable_direct_io=true is not supported",
        ));
    }
    if spill_options
        .and_then(|spill| spill.enable_spill_to_remote_storage)
        .unwrap_or(false)
    {
        return Err(StarRocksFragmentDecodeError::unsupported(
            root.clone()
                .field("spill_options")
                .field("enable_spill_to_remote_storage"),
            "spill to remote storage is not supported",
        ));
    }
    if spill_options
        .and_then(|spill| spill.spill_to_remote_storage_options.as_ref())
        .and_then(|remote| remote.disable_spill_to_local_disk)
        .unwrap_or(false)
    {
        return Err(StarRocksFragmentDecodeError::unsupported(
            root.field("spill_options")
                .field("spill_to_remote_storage_options")
                .field("disable_spill_to_local_disk"),
            "disable_spill_to_local_disk=true is not supported",
        ));
    }

    Ok(Some(SpillConfig {
        enable_spill: true,
        spill_mode,
        spill_mem_limit_threshold: aliases.spill_mem_limit_threshold,
        spill_operator_min_bytes: aliases.spill_operator_min_bytes,
        spill_operator_max_bytes: aliases.spill_operator_max_bytes,
        spill_encode_level: aliases.spill_encode_level,
        enable_spill_buffer_read: spill_options.and_then(|spill| spill.enable_spill_buffer_read),
        max_spill_read_buffer_bytes_per_driver: spill_options
            .and_then(|spill| spill.max_spill_read_buffer_bytes_per_driver),
        spill_mem_table_size: aliases.spill_mem_table_size,
        spill_mem_table_num: aliases.spill_mem_table_num,
    }))
}

fn decode_spill_mode(
    mode: TSpillMode,
    path: FieldPath,
) -> Result<SpillMode, StarRocksFragmentDecodeError> {
    match mode {
        TSpillMode::NONE => Ok(SpillMode::None),
        TSpillMode::FORCE => Ok(SpillMode::Force),
        TSpillMode::AUTO => Ok(SpillMode::Auto),
        TSpillMode::RANDOM => Err(StarRocksFragmentDecodeError::unsupported(
            path,
            "spill_mode RANDOM is not supported yet",
        )),
        TSpillMode(value) => Err(StarRocksFragmentDecodeError::invalid_enum(
            path,
            format!("unknown spill_mode value: {value}"),
        )),
    }
}
