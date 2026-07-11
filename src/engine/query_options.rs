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

use crate::exec::spill::SpillConfig;
use crate::runtime::query_options::QueryOptions;

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StandaloneQueryOptions {
    pub pipeline_dop: Option<i32>,
    pub query_timeout: Option<i32>,
    pub batch_size: Option<i32>,
    pub enable_profile: bool,
    pub exec_mem_limit: Option<i64>,
    pub connector_io_tasks_per_scan_operator: Option<i32>,
    pub runtime_filter_scan_wait_time_ms: Option<i64>,
    pub runtime_filter_wait_timeout_ms: Option<i32>,
    pub allow_throw_exception: bool,
    pub group_concat_max_len: Option<i64>,
    pub spill: Option<SpillConfig>,
}

impl StandaloneQueryOptions {
    pub(crate) fn to_runtime(&self) -> QueryOptions {
        QueryOptions {
            pipeline_dop: self.pipeline_dop,
            query_timeout: self.query_timeout,
            batch_size: self.batch_size,
            enable_profile: self.enable_profile,
            exec_mem_limit: self.exec_mem_limit,
            connector_io_tasks_per_scan_operator: self.connector_io_tasks_per_scan_operator,
            runtime_filter_scan_wait_time_ms: self.runtime_filter_scan_wait_time_ms,
            runtime_filter_wait_timeout_ms: self.runtime_filter_wait_timeout_ms,
            allow_throw_exception: self.allow_throw_exception,
            group_concat_max_len: self.group_concat_max_len,
            spill: self.spill.clone(),
            ..Default::default()
        }
    }

    pub(crate) fn optional_to_runtime(opts: Option<&Self>) -> Option<QueryOptions> {
        opts.map(Self::to_runtime)
    }
}

#[cfg(test)]
mod tests {
    use crate::exec::spill::{SpillConfig, SpillMode};

    use super::StandaloneQueryOptions;

    fn spill_config() -> SpillConfig {
        SpillConfig {
            enable_spill: true,
            spill_mode: SpillMode::Auto,
            spill_mem_limit_threshold: Some(0.7),
            spill_operator_min_bytes: Some(64),
            spill_operator_max_bytes: Some(1024),
            spill_encode_level: Some(3),
            enable_spill_buffer_read: Some(true),
            max_spill_read_buffer_bytes_per_driver: Some(4096),
            spill_mem_table_size: Some(256),
            spill_mem_table_num: Some(4),
        }
    }

    #[test]
    fn standalone_options_map_all_runtime_fields() {
        let opts = StandaloneQueryOptions {
            pipeline_dop: Some(8),
            query_timeout: Some(60),
            batch_size: Some(4096),
            enable_profile: true,
            exec_mem_limit: Some(1 << 30),
            connector_io_tasks_per_scan_operator: Some(12),
            runtime_filter_scan_wait_time_ms: Some(250),
            runtime_filter_wait_timeout_ms: Some(5_000),
            allow_throw_exception: true,
            group_concat_max_len: Some(65_535),
            spill: None,
        };

        let runtime = opts.to_runtime();

        assert_eq!(runtime.pipeline_dop, Some(8));
        assert_eq!(runtime.query_timeout, Some(60));
        assert_eq!(runtime.batch_size, Some(4096));
        assert!(runtime.enable_profile);
        assert_eq!(runtime.exec_mem_limit, Some(1 << 30));
        assert_eq!(runtime.connector_io_tasks_per_scan_operator, Some(12));
        assert_eq!(runtime.runtime_filter_scan_wait_time_ms, Some(250));
        assert_eq!(runtime.runtime_filter_wait_timeout_ms, Some(5_000));
        assert!(runtime.allow_throw_exception);
        assert_eq!(runtime.group_concat_max_len, Some(65_535));
    }

    #[test]
    fn standalone_options_preserve_native_spill_config() {
        let spill = spill_config();
        let opts = StandaloneQueryOptions {
            spill: Some(spill.clone()),
            ..Default::default()
        };

        assert_eq!(opts.to_runtime().spill, Some(spill));
    }

    #[test]
    fn optional_standalone_options_preserve_presence() {
        assert!(StandaloneQueryOptions::optional_to_runtime(None).is_none());

        let opts = StandaloneQueryOptions {
            pipeline_dop: Some(3),
            ..Default::default()
        };
        assert_eq!(
            StandaloneQueryOptions::optional_to_runtime(Some(&opts))
                .and_then(|runtime| runtime.pipeline_dop),
            Some(3)
        );
    }
}
