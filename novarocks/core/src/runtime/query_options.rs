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

use crate::exec::spill::SpillConfig;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryOptions {
    pub(crate) batch_size: Option<i32>,
    pub(crate) query_timeout: Option<i32>,
    pub(crate) query_delivery_timeout: Option<i32>,
    pub(crate) enable_profile: bool,
    pub(crate) runtime_profile_report_interval: Option<i64>,
    pub(crate) pipeline_dop: Option<i32>,
    pub(crate) exec_mem_limit: Option<i64>,
    pub(crate) connector_io_tasks_per_scan_operator: Option<i32>,
    pub(crate) orc_use_column_names: bool,
    pub(crate) enable_file_metacache: bool,
    pub(crate) enable_file_pagecache: bool,
    pub(crate) enable_parquet_reader_page_index: bool,
    pub(crate) runtime_filter_scan_wait_time_ms: Option<i64>,
    pub(crate) runtime_filter_wait_timeout_ms: Option<i32>,
    pub(crate) allow_throw_exception: bool,
    pub(crate) group_concat_max_len: Option<i64>,
    pub(crate) enable_join_runtime_bitset_filter: Option<bool>,
    pub(crate) global_runtime_filter_build_max_size: Option<i64>,
    pub(crate) cache: QueryCacheOptions,
    pub(crate) spill: Option<SpillConfig>,
}

impl QueryOptions {
    pub const fn enable_profile(&self) -> bool {
        self.enable_profile
    }

    pub const fn runtime_profile_report_interval(&self) -> Option<i64> {
        self.runtime_profile_report_interval
    }
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

pub fn query_expire_durations(query_opts: Option<&QueryOptions>) -> (Duration, Duration) {
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
