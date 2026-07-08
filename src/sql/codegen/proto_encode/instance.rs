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

use std::collections::{BTreeMap, HashMap};

use crate::proto::{common, novarocks};
use crate::runtime::endpoint::{
    FragmentDestination, RuntimeEndpoint, RuntimeFilterProberDestination,
};
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_filter_params::RuntimeFilterParams;
use crate::runtime::scan_range;
use crate::runtime::scheduler::FragmentInstancePlacement;
use crate::thrift::types;

pub(crate) fn encode_instance_params(
    query_id: &types::TUniqueId,
    placement: &FragmentInstancePlacement,
    query_options: Option<&QueryOptions>,
    runtime_filter_prober_params: &BTreeMap<i32, Vec<RuntimeFilterProberDestination>>,
    runtime_filter_builder_number: &BTreeMap<i32, i32>,
    runtime_filter_max_size: i64,
    backend_num: i32,
    report_endpoint: Option<&RuntimeEndpoint>,
    typed_result_sink: bool,
) -> Result<novarocks::InstanceParams, String> {
    Ok(novarocks::InstanceParams {
        query_id: Some(encode_unique_id(query_id)),
        fragment_instance_id: Some(encode_unique_id(&placement.finst_id)),
        backend_num,
        per_node_scan_ranges: placement
            .scan_ranges
            .iter()
            .map(|(node_id, ranges)| {
                Ok((
                    *node_id,
                    novarocks::ScanRangeList {
                        ranges: ranges
                            .iter()
                            .map(encode_scan_range_params)
                            .collect::<Result<Vec<_>, _>>()?,
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>, String>>()?,
        per_exch_num_senders: placement
            .per_exch_num_senders
            .iter()
            .map(|(node_id, senders)| (*node_id, *senders))
            .collect(),
        destinations: placement
            .destinations
            .iter()
            .map(encode_destination)
            .collect::<Result<Vec<_>, _>>()?,
        runtime_filter_params: encode_runtime_filter_params(
            runtime_filter_prober_params,
            runtime_filter_builder_number,
            runtime_filter_max_size,
        )?,
        query_options: query_options.map(encode_query_options),
        report_endpoint: report_endpoint.map(RuntimeEndpoint::as_host_port),
        typed_result_sink,
    })
}

fn encode_unique_id(src: &types::TUniqueId) -> common::UniqueId {
    common::UniqueId {
        hi: src.hi,
        lo: src.lo,
    }
}

fn encode_runtime_unique_id(src: &crate::common::types::UniqueId) -> common::UniqueId {
    common::UniqueId {
        hi: src.hi,
        lo: src.lo,
    }
}

fn encode_scan_range_params(
    src: &scan_range::ScanRangeParams,
) -> Result<novarocks::ScanRangeParams, String> {
    Ok(novarocks::ScanRangeParams {
        range: Some(encode_scan_range(&src.range)?),
        volume_id: src.volume_id,
        empty: src.empty,
        has_more: src.has_more,
    })
}

fn encode_scan_range(src: &scan_range::ScanRange) -> Result<novarocks::ScanRange, String> {
    match src {
        scan_range::ScanRange::File(file) => Ok(novarocks::ScanRange {
            kind: Some(novarocks::scan_range::Kind::File(encode_file_scan_range(
                file,
            )?)),
        }),
    }
}

fn encode_file_scan_range(
    src: &scan_range::FileScanRange,
) -> Result<novarocks::FileScanRange, String> {
    Ok(novarocks::FileScanRange {
        file_format: src.file_format.as_native_name().to_string(),
        full_path: src.full_path.clone(),
        relative_path: src.relative_path.clone(),
        table_id: src.table_id,
        offset: src.offset,
        length: src.length,
        file_length: src.file_length,
        delete_files: src
            .delete_files
            .iter()
            .map(encode_iceberg_delete_file)
            .collect::<Result<Vec<_>, _>>()?,
        deletion_vector_descriptor: src
            .deletion_vector_descriptor
            .as_ref()
            .map(encode_deletion_vector_descriptor),
        first_row_id: src.first_row_id,
        data_sequence_number: src.data_sequence_number,
        modification_time: src.modification_time,
        datacache_options: src.datacache_options.as_ref().map(encode_datacache_options),
        included_positions: src.included_positions.clone(),
        serialized_split: src.serialized_split.clone(),
        use_iceberg_jni_metadata_reader: src.use_iceberg_jni_metadata_reader,
        change_op: src.ivm_change_op.map(i32::from),
        file_pruning_min_max_values: src
            .file_pruning_min_max_values
            .as_ref()
            .map(|values| {
                values
                    .iter()
                    .map(|(ordinal, value)| (*ordinal, encode_file_pruning_min_max_value(value)))
                    .collect()
            })
            .unwrap_or_default(),
    })
}

fn encode_file_pruning_min_max_value(
    src: &scan_range::FilePruningMinMaxValue,
) -> novarocks::FilePruningMinMaxValue {
    novarocks::FilePruningMinMaxValue {
        value_kind: encode_file_pruning_value_kind(src.value_kind),
        has_null: src.has_null,
        all_null: src.all_null,
        min_int_value: src.min_int_value,
        max_int_value: src.max_int_value,
        min_float_value: src.min_float_value,
        max_float_value: src.max_float_value,
    }
}

fn encode_file_pruning_value_kind(src: scan_range::FilePruningValueKind) -> i32 {
    match src {
        scan_range::FilePruningValueKind::Bool => 1,
        scan_range::FilePruningValueKind::Int => 2,
        scan_range::FilePruningValueKind::Float => 3,
    }
}

fn encode_iceberg_delete_file(
    src: &scan_range::IcebergDeleteFile,
) -> Result<novarocks::IcebergDeleteFile, String> {
    Ok(novarocks::IcebergDeleteFile {
        full_path: src.full_path.clone(),
        file_format: src.file_format.as_native_name().to_string(),
        file_content: src.file_content.as_native_name().to_string(),
        length: src.length,
    })
}

fn encode_deletion_vector_descriptor(
    src: &scan_range::DeletionVectorDescriptor,
) -> novarocks::DeletionVectorDescriptor {
    novarocks::DeletionVectorDescriptor {
        storage_type: src.storage_type.clone(),
        path_or_inline_dv: src.path_or_inline_dv.clone(),
        offset: src.offset,
        size_in_bytes: src.size_in_bytes,
        cardinality: src.cardinality,
    }
}

fn encode_datacache_options(src: &scan_range::DatacacheOptions) -> novarocks::DatacacheOptions {
    novarocks::DatacacheOptions {
        enable_populate_datacache: src.enable_populate_datacache,
        priority: src.priority,
    }
}

fn encode_destination(src: &FragmentDestination) -> Result<novarocks::Destination, String> {
    Ok(novarocks::Destination {
        finst_id: Some(encode_runtime_unique_id(src.finst_id())),
        endpoint: src.endpoint().as_host_port(),
    })
}

fn encode_runtime_filter_params(
    prober_params: &BTreeMap<i32, Vec<RuntimeFilterProberDestination>>,
    runtime_filter_builder_number: &BTreeMap<i32, i32>,
    runtime_filter_max_size: i64,
) -> Result<Option<novarocks::RuntimeFilterParams>, String> {
    let params = RuntimeFilterParams::new(
        prober_params.clone(),
        runtime_filter_builder_number.clone(),
        (runtime_filter_max_size > 0).then_some(runtime_filter_max_size),
    );
    if params.is_empty() {
        return Ok(None);
    }
    Ok(Some(params.to_native()))
}

fn encode_query_options(src: &QueryOptions) -> novarocks::QueryOptions {
    src.to_native()
}
