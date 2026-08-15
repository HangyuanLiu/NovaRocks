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

//! Canonical execution query-options mapping for native lifecycle DTOs.
//!
//! Query options are part of the query-attempt contract.  Both participant
//! manifests and fragment instance parameters must therefore use this one
//! mapping rather than depending on each other's encoding leaves.

use novarocks_execution::exec::spill::{SpillConfig, SpillMode};
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_protocol::novarocks;

/// Encode execution-owned query options for the native query lifecycle.
///
/// Zero values intentionally preserve the existing native wire defaults for
/// unset optional execution options.
pub fn encode_query_options(src: &QueryOptions) -> novarocks::QueryOptions {
    novarocks::QueryOptions {
        batch_size: src.batch_size.unwrap_or_default(),
        query_timeout: src.query_timeout.unwrap_or_default(),
        enable_profile: src.enable_profile,
        pipeline_dop: src.pipeline_dop.unwrap_or_default(),
        query_mem_limit: src.exec_mem_limit.unwrap_or_default(),
        connector_io_tasks_per_scan_operator: src
            .connector_io_tasks_per_scan_operator
            .unwrap_or_default(),
        runtime_filter_scan_wait_time_ms: src.runtime_filter_scan_wait_time_ms,
        runtime_filter_wait_timeout_ms: src.runtime_filter_wait_timeout_ms,
        allow_throw_exception: src.allow_throw_exception,
        group_concat_max_len: src.group_concat_max_len,
        enable_spill: src.spill.is_some(),
        spill_options: src.spill.as_ref().map(encode_spill_config),
        enable_scan_datacache: src.cache.enable_scan_datacache,
        enable_populate_datacache: src.cache.enable_populate_datacache,
        enable_datacache_async_populate_mode: src.cache.enable_datacache_async_populate_mode,
        enable_datacache_io_adaptor: src.cache.enable_datacache_io_adaptor,
        enable_cache_select: src.cache.enable_cache_select,
        datacache_evict_probability: src.cache.datacache_evict_probability,
        datacache_priority: src.cache.datacache_priority.unwrap_or_default(),
        datacache_ttl_seconds: src.cache.datacache_ttl_seconds.unwrap_or_default(),
        datacache_sharing_work_period: src.cache.datacache_sharing_work_period.unwrap_or_default(),
        query_delivery_timeout: src.query_delivery_timeout.unwrap_or_default(),
        runtime_profile_report_interval: src.runtime_profile_report_interval.unwrap_or_default(),
        enable_join_runtime_bitset_filter: src.enable_join_runtime_bitset_filter,
        global_runtime_filter_build_max_size: src
            .global_runtime_filter_build_max_size
            .unwrap_or_default(),
        orc_use_column_names: src.orc_use_column_names,
        enable_file_metacache: src.enable_file_metacache,
        enable_file_pagecache: src.enable_file_pagecache,
        enable_parquet_reader_page_index: src.enable_parquet_reader_page_index,
    }
}

fn encode_spill_config(src: &SpillConfig) -> novarocks::SpillOptions {
    novarocks::SpillOptions {
        spill_mode: match src.spill_mode {
            SpillMode::Auto => 0,
            SpillMode::Force => 1,
            SpillMode::None => 2,
            SpillMode::Random => 3,
        },
        spill_mem_limit_threshold: src.spill_mem_limit_threshold.unwrap_or_default(),
        spill_operator_min_bytes: src.spill_operator_min_bytes.unwrap_or_default(),
        spill_operator_max_bytes: src.spill_operator_max_bytes.unwrap_or_default(),
        spill_encode_level: src.spill_encode_level.unwrap_or_default(),
        enable_spill_buffer_read: src.enable_spill_buffer_read.unwrap_or(false),
        max_spill_read_buffer_bytes_per_driver: src
            .max_spill_read_buffer_bytes_per_driver
            .unwrap_or_default(),
        spill_mem_table_size: src.spill_mem_table_size.unwrap_or_default(),
        spill_mem_table_num: src.spill_mem_table_num.unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::encode_query_options;
    use novarocks_execution::exec::spill::{SpillConfig, SpillMode};
    use novarocks_execution::runtime::query_options::{QueryCacheOptions, QueryOptions};

    #[test]
    fn encodes_all_query_options_with_existing_defaults() {
        let options = QueryOptions {
            batch_size: Some(4096),
            query_timeout: Some(120),
            query_delivery_timeout: Some(60),
            enable_profile: true,
            runtime_profile_report_interval: Some(10),
            pipeline_dop: Some(4),
            exec_mem_limit: Some(1 << 30),
            connector_io_tasks_per_scan_operator: Some(8),
            orc_use_column_names: true,
            enable_file_metacache: true,
            enable_file_pagecache: true,
            enable_parquet_reader_page_index: true,
            runtime_filter_scan_wait_time_ms: Some(250),
            runtime_filter_wait_timeout_ms: Some(500),
            allow_throw_exception: true,
            group_concat_max_len: Some(1024),
            enable_join_runtime_bitset_filter: Some(true),
            global_runtime_filter_build_max_size: Some(1 << 20),
            cache: QueryCacheOptions {
                enable_scan_datacache: true,
                enable_populate_datacache: true,
                enable_datacache_async_populate_mode: true,
                enable_datacache_io_adaptor: true,
                enable_cache_select: true,
                datacache_evict_probability: Some(10),
                datacache_priority: Some(2),
                datacache_ttl_seconds: Some(300),
                datacache_sharing_work_period: Some(30),
            },
            spill: Some(SpillConfig {
                enable_spill: true,
                spill_mode: SpillMode::Force,
                spill_mem_limit_threshold: Some(0.75),
                spill_operator_min_bytes: Some(1024),
                spill_operator_max_bytes: Some(8192),
                spill_encode_level: Some(3),
                enable_spill_buffer_read: Some(true),
                max_spill_read_buffer_bytes_per_driver: Some(16384),
                spill_mem_table_size: Some(512),
                spill_mem_table_num: Some(2),
            }),
            ..Default::default()
        };

        let encoded = encode_query_options(&options);

        assert_eq!(encoded.batch_size, 4096);
        assert_eq!(encoded.query_timeout, 120);
        assert_eq!(encoded.query_delivery_timeout, 60);
        assert!(encoded.enable_profile);
        assert_eq!(encoded.runtime_profile_report_interval, 10);
        assert_eq!(encoded.pipeline_dop, 4);
        assert_eq!(encoded.query_mem_limit, 1 << 30);
        assert_eq!(encoded.connector_io_tasks_per_scan_operator, 8);
        assert!(encoded.orc_use_column_names);
        assert!(encoded.enable_file_metacache);
        assert!(encoded.enable_file_pagecache);
        assert!(encoded.enable_parquet_reader_page_index);
        assert_eq!(encoded.runtime_filter_scan_wait_time_ms, Some(250));
        assert_eq!(encoded.runtime_filter_wait_timeout_ms, Some(500));
        assert!(encoded.allow_throw_exception);
        assert_eq!(encoded.group_concat_max_len, Some(1024));
        assert_eq!(encoded.enable_join_runtime_bitset_filter, Some(true));
        assert_eq!(encoded.global_runtime_filter_build_max_size, 1 << 20);
        assert!(encoded.enable_spill);
        let spill = encoded.spill_options.expect("spill options");
        assert_eq!(spill.spill_mode, 1);
        assert_eq!(spill.spill_mem_limit_threshold, 0.75);
        assert_eq!(spill.spill_operator_min_bytes, 1024);
        assert_eq!(spill.spill_operator_max_bytes, 8192);
        assert_eq!(spill.spill_encode_level, 3);
        assert!(spill.enable_spill_buffer_read);
        assert_eq!(spill.max_spill_read_buffer_bytes_per_driver, 16384);
        assert_eq!(spill.spill_mem_table_size, 512);
        assert_eq!(spill.spill_mem_table_num, 2);
        assert!(encoded.enable_scan_datacache);
        assert!(encoded.enable_populate_datacache);
        assert!(encoded.enable_datacache_async_populate_mode);
        assert!(encoded.enable_datacache_io_adaptor);
        assert!(encoded.enable_cache_select);
        assert_eq!(encoded.datacache_evict_probability, Some(10));
        assert_eq!(encoded.datacache_priority, 2);
        assert_eq!(encoded.datacache_ttl_seconds, 300);
        assert_eq!(encoded.datacache_sharing_work_period, 30);
        assert_eq!(
            crate::protocol::decode_native_query_options(&encoded)
                .expect("encoded query options decode"),
            options
        );
    }

    #[test]
    fn keeps_unset_options_at_native_defaults() {
        let encoded = encode_query_options(&QueryOptions::default());

        assert_eq!(encoded.batch_size, 0);
        assert_eq!(encoded.query_timeout, 0);
        assert_eq!(encoded.query_delivery_timeout, 0);
        assert_eq!(encoded.pipeline_dop, 0);
        assert_eq!(encoded.query_mem_limit, 0);
        assert_eq!(encoded.connector_io_tasks_per_scan_operator, 0);
        assert!(!encoded.enable_spill);
        assert!(encoded.spill_options.is_none());
        assert_eq!(encoded.datacache_priority, 0);
        assert_eq!(encoded.datacache_ttl_seconds, 0);
        assert_eq!(encoded.datacache_sharing_work_period, 0);
        assert_eq!(encoded.global_runtime_filter_build_max_size, 0);
        assert_eq!(
            crate::protocol::decode_native_query_options(&encoded)
                .expect("default query options decode"),
            QueryOptions::default()
        );
    }
}
