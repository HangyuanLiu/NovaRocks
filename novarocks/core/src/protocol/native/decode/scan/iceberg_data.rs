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

use std::collections::HashMap;

use super::super::node::{DecodedNode, NativePlanDecodeContext};
use super::common::{
    lower_scan_predicate, parse_scan_limit, resolve_cloud_object_store_config, scan_batch_size,
    table_location_map,
};
use super::file_range::decode_file_scan_ranges;
use super::read_plan::{ScanReadPlan, maybe_project_data_scan_output};
use crate::cache::{CacheOptions, DataCacheContext};
use crate::connector::{HdfsIcebergRuntimePruningConfig, HdfsScanConfig, ScanConfig};
use crate::exec::expr::ExprArena;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::formats::FileFormatConfig;
use crate::formats::parquet::{ParquetReadCachePolicy, ParquetScanConfig};
use crate::proto::plan;
use crate::protocol::common::error::ProtocolErrorKind;
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;

pub(super) fn lower_iceberg_data_files_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &plan::IcebergDataFiles,
    read_plan: ScanReadPlan,
    ctx: &NativePlanDecodeContext,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentLeafDecodeError> {
    let table = source.table.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "table",
            "IcebergDataFiles table missing",
        )
    })?;
    let ranges = decode_file_scan_ranges(node.node_id, table, ctx.scan_ranges(node.node_id)?)?;
    let cache_options = CacheOptions::from_query_options(ctx.query_options()).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "query_options",
            error,
        )
    })?;
    let batch_size = scan_batch_size(ctx.query_options())?;
    let parquet_cfg = ParquetScanConfig {
        columns: read_plan.read_columns.clone(),
        chunk_schema: read_plan.parquet_schema.clone(),
        slot_kinds: read_plan.slot_kinds.clone(),
        case_sensitive: true,
        enable_page_index: false,
        min_max_predicates: Vec::new(),
        runtime_min_max_filter_columns: HashMap::new(),
        variant_path_predicates: Vec::new(),
        batch_size: Some(batch_size),
        datacache: DataCacheContext::external(cache_options),
        cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
        profile_label: Some(format!("native_scan_node_id={}", node.node_id)),
        iceberg_output_schema: Some(read_plan.parquet_schema.arrow_schema_ref()),
        variant_path_columns: read_plan.variant_path_columns.clone(),
        query_global_dicts: Default::default(),
    };
    let object_store_config = resolve_cloud_object_store_config(&source.cloud_properties)?;
    let iceberg_runtime_pruning = Some(HdfsIcebergRuntimePruningConfig {
        slot_to_column: read_plan
            .read_slot_ids
            .iter()
            .copied()
            .zip(read_plan.read_columns.iter().cloned())
            .collect(),
        min_max_filter_columns: HashMap::new(),
        discrete_set_max_values: 256,
    });
    let cfg = HdfsScanConfig {
        original_range_count: ranges.len(),
        ranges,
        has_more: false,
        limit: parse_scan_limit(node.limit)?,
        profile_label: Some(format!("native_scan_node_id={}", node.node_id)),
        format: Some(FileFormatConfig::Parquet(parquet_cfg)),
        object_store_config,
        iceberg_table_locations: table_location_map(table),
        query_global_dicts: Default::default(),
        iceberg_runtime_pruning,
    };
    let predicate = lower_scan_predicate(scan, arena, &read_plan.read_layout)?;
    let scan_node = ctx
        .connectors()?
        .create_scan_node("hdfs", ScanConfig::Hdfs(Box::new(cfg)))
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "files", error)
        })?
        .with_node_id(node.node_id)
        .with_output_chunk_schema(read_plan.read_schema.clone())
        .with_limit(parse_scan_limit(node.limit)?)
        .with_conjunct_predicate(predicate)
        .with_iceberg_virtual(Some(read_plan.iceberg_virtual.clone()))
        .with_accept_empty_scan_ranges(true);
    let scan_lowered = DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan_node),
        },
        layout: read_plan.read_layout.clone(),
        output_schema: read_plan.read_schema.clone(),
    };
    Ok(maybe_project_data_scan_output(
        node.node_id,
        scan_lowered,
        read_plan,
        arena,
    ))
}

#[cfg(test)]
mod purity_tests {
    #[test]
    fn native_iceberg_data_decoder_does_not_access_cache_singleton() {
        let source = include_str!("iceberg_data.rs");
        let singleton_call = concat!("DataCache", "Manager::instance()");
        assert!(
            !source.contains(singleton_call),
            "native Iceberg data decoding must construct DataCacheContext as a pure value"
        );
    }
}
