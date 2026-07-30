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

use novarocks::thrift::internal_service::{TQueryOptions, TSpillMode};

pub(crate) struct NormalizedQueryOptionAliases {
    pub(crate) exec_mem_limit: Option<i64>,
    pub(crate) connector_io_tasks_per_scan_operator: Option<i32>,
    pub(crate) spill_mode: Option<TSpillMode>,
    pub(crate) spill_enable_direct_io: Option<bool>,
    pub(crate) spill_mem_limit_threshold: Option<f64>,
    pub(crate) spill_operator_min_bytes: Option<i64>,
    pub(crate) spill_operator_max_bytes: Option<i64>,
    pub(crate) spill_encode_level: Option<i32>,
    pub(crate) spill_mem_table_size: Option<i32>,
    pub(crate) spill_mem_table_num: Option<i32>,
}

/// Normalizes query-option aliases from current and historical FE wire shapes.
///
/// Current FEs use `query_mem_limit`, `connector_io_tasks_per_scan_operator`, and
/// nested `spill_options`; older FEs used the corresponding flat legacy fields.
/// The current field wins whenever both shapes are present. This rule can be
/// removed after the minimum supported FE version sends only the current shape.
pub(crate) fn normalize_query_option_aliases(
    options: &TQueryOptions,
) -> NormalizedQueryOptionAliases {
    let spill = options.spill_options.as_ref();
    NormalizedQueryOptionAliases {
        exec_mem_limit: options.query_mem_limit.or(options.mem_limit),
        connector_io_tasks_per_scan_operator: options
            .connector_io_tasks_per_scan_operator
            .or(options.io_tasks_per_scan_operator),
        spill_mode: spill
            .and_then(|spill| spill.spill_mode)
            .or(options.spill_mode),
        spill_enable_direct_io: spill
            .and_then(|spill| spill.spill_enable_direct_io)
            .or(options.spill_enable_direct_io),
        spill_mem_limit_threshold: spill
            .and_then(|spill| spill.spill_mem_limit_threshold)
            .or(options.spill_mem_limit_threshold)
            .map(|value| value.into_inner()),
        spill_operator_min_bytes: spill
            .and_then(|spill| spill.spill_operator_min_bytes)
            .or(options.spill_operator_min_bytes),
        spill_operator_max_bytes: spill
            .and_then(|spill| spill.spill_operator_max_bytes)
            .or(options.spill_operator_max_bytes),
        spill_encode_level: spill
            .and_then(|spill| spill.spill_encode_level)
            .or(options.spill_encode_level),
        spill_mem_table_size: spill
            .and_then(|spill| spill.spill_mem_table_size)
            .or(options.spill_mem_table_size),
        spill_mem_table_num: spill
            .and_then(|spill| spill.spill_mem_table_num)
            .or(options.spill_mem_table_num),
    }
}
