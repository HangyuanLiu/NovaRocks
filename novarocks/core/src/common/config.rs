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
use crate::novarocks_config::config as novarocks_app_config;
use std::path::PathBuf;

pub(crate) fn debug_exec_node_output() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.debug.exec_node_output)
        .unwrap_or(false)
}

pub(crate) fn debug_fault_inject_fetch_not_ready_count() -> Option<usize> {
    novarocks_app_config()
        .ok()
        .and_then(|c| c.debug.fault_inject_fetch_not_ready_count())
}

pub fn debug_emit_cancel_marker() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.debug.emit_cancel_marker())
        .unwrap_or(false)
        || sql_test_fragment_failure_harness_enabled()
}

pub(crate) fn debug_emit_grpc_fragment_marker() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.debug.emit_grpc_fragment_marker())
        .unwrap_or(false)
        || sql_test_fragment_failure_harness_enabled()
}

/// Returns whether execution should emit connector-reader evidence markers.
/// Backend native plan decoding uses this without accessing runtime state.
pub fn debug_emit_connector_reader_marker() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.debug.emit_connector_reader_marker())
        .unwrap_or(false)
}

pub(crate) fn sql_test_fragment_failure_harness_enabled() -> bool {
    std::env::var_os("NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE").is_some()
}

pub fn exchange_wait_ms() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.exchange_wait_ms)
        .unwrap_or(120_000)
}

pub(crate) fn optimizer_query_mem_limit_bytes() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.optimizer_query_mem_limit_bytes)
        .unwrap_or(2 * 1024 * 1024 * 1024)
}

pub(crate) fn enable_tablet_write_log() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.enable_tablet_write_log)
        .unwrap_or(false)
}

pub(crate) fn tablet_write_log_buffer_size() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.tablet_write_log_buffer_size.max(1))
        .unwrap_or(100_000)
}

pub(crate) fn be_txn_info_history_size() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.be_txn_info_history_size.max(1))
        .unwrap_or(20_000)
}

pub(crate) fn data_runtime_worker_threads() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.actual_data_runtime_threads())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
}

pub(crate) fn data_runtime_max_blocking_threads() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.data_runtime_max_blocking_threads.max(1))
        .unwrap_or(64)
}
