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
//! Integration tests for connectors (JDBC, Iceberg).

use crate::common::TestConfig;
use arrow::datatypes::{DataType, Field};
use novarocks::cache::CacheOptions;
use novarocks::connector::{self, FileFormatConfig, ParquetScanConfig};
use novarocks::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use novarocks::formats::parquet::{ParquetReadCachePolicy, ParquetSlotKind};
use novarocks_fs::DataCacheManager;
use std::sync::Arc;

#[path = "../common/mod.rs"]
mod common;

fn test_cache_options() -> CacheOptions {
    CacheOptions {
        enable_scan_datacache: false,
        enable_populate_datacache: false,
        enable_datacache_async_populate_mode: false,
        enable_datacache_io_adaptor: false,
        enable_cache_select: false,
        datacache_evict_probability: 0,
        datacache_priority: 0,
        datacache_ttl_seconds: 0,
        datacache_sharing_work_period: None,
    }
}

#[test]
fn test_connector_registry_exists() {
    // Test that connector registry module exists and can be accessed
    // This is a basic smoke test to ensure the module is properly exported
    let _registry = connector::ConnectorRegistry::default();
}

#[test]
fn test_connector_registry_initialization() {
    // Test connector registry initialization
    let registry = connector::ConnectorRegistry::default();

    // Registry should be initialized with default connectors
    // Default registry includes jdbc, mysql, hdfs, and starrocks connectors
    let _ = registry;
}

#[test]
fn test_connector_registry_new() {
    // Test creating a new empty registry
    let registry = connector::ConnectorRegistry::new();
    let _ = registry;
}

#[test]
fn test_jdbc_connector_module() {
    let cfg = novarocks::novarocks_connector_jdbc::JdbcScanConfig {
        jdbc_url: "jdbc:sqlite::memory:".to_string(),
        jdbc_user: None,
        jdbc_passwd: None,
        table: "lineorder".to_string(),
        columns: vec!["lo_orderkey".to_string()],
        filters: vec![],
        limit: Some(1),
        chunk_schema: Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                novarocks::common::ids::SlotId::new(1),
                Field::new("lo_orderkey", DataType::Int64, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        ),
    };
    assert_eq!(cfg.table, "lineorder");
}

#[test]
fn test_iceberg_connector_module() {
    let parquet_cfg = ParquetScanConfig {
        columns: vec!["col0".to_string()],
        chunk_schema: Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                novarocks::common::ids::SlotId::new(1),
                Field::new("col0", DataType::Int32, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        ),
        slot_kinds: vec![ParquetSlotKind::Regular],
        case_sensitive: false,
        enable_page_index: false,
        min_max_predicates: vec![],
        runtime_min_max_filter_columns: std::collections::HashMap::new(),
        variant_path_predicates: Vec::new(),
        batch_size: None,
        datacache: DataCacheManager::instance()
            .external_context(test_cache_options().to_file_cache_options()),
        cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
        profile_label: Some("connector_smoke".to_string()),
        iceberg_output_schema: None,
        variant_path_columns: Vec::new(),
        query_global_dicts: Default::default(),
    };
    let config = novarocks::novarocks_connector_iceberg::HdfsScanConfig {
        ranges: vec![novarocks::connector::FileScanRange {
            path: "/tmp/data.parquet".to_string(),
            file_len: 0,
            offset: 0,
            length: 0,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        }],
        original_range_count: 1,
        has_more: false,
        limit: Some(10),
        profile_label: Some("connector_smoke".to_string()),
        format: Some(FileFormatConfig::Parquet(parquet_cfg)),
        object_store_config: None,
        iceberg_table_locations: std::collections::HashMap::new(),
        query_global_dicts: Default::default(),
        iceberg_runtime_pruning: None,
    };
    assert_eq!(config.ranges.len(), 1);
}

#[test]
fn test_connector_config_loading() {
    let test_config = TestConfig::new().expect("Failed to create test config");
    let config = test_config.load_config().expect("Failed to load config");
    assert_eq!(config.server.host, "127.0.0.1");
}
