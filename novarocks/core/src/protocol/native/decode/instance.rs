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

use std::collections::BTreeMap;

use crate::common::types::UniqueId;
use crate::exec::spill::{SpillConfig, SpillMode};
use crate::proto::novarocks;
use crate::protocol::common::error::FieldPath;
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::query_options::{QueryCacheOptions, QueryOptions};
use crate::runtime::scan_range::{
    DatacacheOptions, DeletionVectorDescriptor, FileFormat, FilePruningMinMaxValue,
    FilePruningValueKind, FileScanRange, IcebergDeleteFile, IcebergFileContent, IcebergFileFormat,
    ScanRange, ScanRangeParams,
};

use super::NativeFragmentDecodeError;

#[derive(Clone, Debug)]
pub(crate) struct NativeSubmissionMetadata {
    backend_num: i32,
    report_endpoint: Option<RuntimeEndpoint>,
    typed_result_sink: bool,
}

impl NativeSubmissionMetadata {
    pub(crate) fn new(
        backend_num: i32,
        report_endpoint: Option<RuntimeEndpoint>,
        typed_result_sink: bool,
    ) -> Self {
        Self {
            backend_num,
            report_endpoint,
            typed_result_sink,
        }
    }

    pub(crate) fn backend_num(&self) -> i32 {
        self.backend_num
    }

    pub(crate) fn report_endpoint(&self) -> Option<&RuntimeEndpoint> {
        self.report_endpoint.as_ref()
    }

    pub(crate) fn typed_result_sink(&self) -> bool {
        self.typed_result_sink
    }
}

pub(crate) fn decode_query_options(
    src: &novarocks::QueryOptions,
) -> Result<QueryOptions, NativeFragmentDecodeError> {
    let path = FieldPath::root("instance_params").field("query_options");
    Ok(QueryOptions {
        batch_size: (src.batch_size > 0).then_some(src.batch_size),
        query_timeout: (src.query_timeout > 0).then_some(src.query_timeout),
        query_delivery_timeout: (src.query_delivery_timeout > 0)
            .then_some(src.query_delivery_timeout),
        enable_profile: src.enable_profile,
        runtime_profile_report_interval: (src.runtime_profile_report_interval > 0)
            .then_some(src.runtime_profile_report_interval),
        pipeline_dop: (src.pipeline_dop > 0).then_some(src.pipeline_dop),
        exec_mem_limit: (src.query_mem_limit > 0).then_some(src.query_mem_limit),
        connector_io_tasks_per_scan_operator: (src.connector_io_tasks_per_scan_operator > 0)
            .then_some(src.connector_io_tasks_per_scan_operator),
        orc_use_column_names: false,
        enable_file_metacache: false,
        enable_file_pagecache: false,
        enable_parquet_reader_page_index: false,
        runtime_filter_scan_wait_time_ms: src.runtime_filter_scan_wait_time_ms,
        runtime_filter_wait_timeout_ms: src.runtime_filter_wait_timeout_ms,
        allow_throw_exception: src.allow_throw_exception,
        group_concat_max_len: src.group_concat_max_len,
        enable_join_runtime_bitset_filter: src.enable_join_runtime_bitset_filter,
        global_runtime_filter_build_max_size: (src.global_runtime_filter_build_max_size > 0)
            .then_some(src.global_runtime_filter_build_max_size),
        cache: QueryCacheOptions {
            enable_scan_datacache: src.enable_scan_datacache,
            enable_populate_datacache: src.enable_populate_datacache,
            enable_datacache_async_populate_mode: src.enable_datacache_async_populate_mode,
            enable_datacache_io_adaptor: src.enable_datacache_io_adaptor,
            enable_cache_select: src.enable_cache_select,
            datacache_evict_probability: src.datacache_evict_probability,
            datacache_priority: (src.datacache_priority != 0).then_some(src.datacache_priority),
            datacache_ttl_seconds: (src.datacache_ttl_seconds > 0)
                .then_some(src.datacache_ttl_seconds),
            datacache_sharing_work_period: (src.datacache_sharing_work_period > 0)
                .then_some(src.datacache_sharing_work_period),
        },
        spill: decode_spill_config(src, path.field("spill_options"))?,
    })
}

pub(crate) fn decode_endpoint(src: &str) -> Result<RuntimeEndpoint, NativeFragmentDecodeError> {
    decode_endpoint_at(
        src,
        FieldPath::root("instance_params").field("report_endpoint"),
    )
}

pub(crate) fn decode_destinations(
    src: &[novarocks::Destination],
) -> Result<Vec<FragmentDestination>, NativeFragmentDecodeError> {
    src.iter()
        .enumerate()
        .map(|(index, destination)| {
            let path = FieldPath::root("instance_params")
                .field("destinations")
                .index(index);
            let finst_id = destination.finst_id.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone().field("finst_id"),
                    "native Destination requires finst_id",
                )
            })?;
            Ok(FragmentDestination::new(
                unique_id(finst_id),
                decode_endpoint_at(&destination.endpoint, path.field("endpoint"))?,
            ))
        })
        .collect()
}

pub(crate) fn decode_scan_range_params(
    src: &novarocks::ScanRangeParams,
) -> Result<ScanRangeParams, NativeFragmentDecodeError> {
    decode_scan_range_params_at(
        src,
        FieldPath::root("instance_params").field("per_node_scan_ranges"),
    )
}

fn decode_spill_config(
    src: &novarocks::QueryOptions,
    path: FieldPath,
) -> Result<Option<SpillConfig>, NativeFragmentDecodeError> {
    if !src.enable_spill {
        return Ok(None);
    }
    let spill = src.spill_options.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(path.clone(), "enable_spill=true requires spill_options")
    })?;
    let spill_mode = match spill.spill_mode {
        0 => SpillMode::Auto,
        1 => SpillMode::Force,
        2 => SpillMode::None,
        3 => SpillMode::Random,
        value => {
            return Err(NativeFragmentDecodeError::invalid_enum(
                path.clone().field("spill_mode"),
                format!("unknown spill_mode value {value}"),
            ));
        }
    };
    if spill_mode == SpillMode::Random {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.field("spill_mode"),
            "spill_mode RANDOM is not supported yet",
        ));
    }
    Ok(Some(SpillConfig {
        enable_spill: true,
        spill_mode,
        spill_mem_limit_threshold: (spill.spill_mem_limit_threshold > 0.0)
            .then_some(spill.spill_mem_limit_threshold),
        spill_operator_min_bytes: (spill.spill_operator_min_bytes > 0)
            .then_some(spill.spill_operator_min_bytes),
        spill_operator_max_bytes: (spill.spill_operator_max_bytes > 0)
            .then_some(spill.spill_operator_max_bytes),
        spill_encode_level: (spill.spill_encode_level > 0).then_some(spill.spill_encode_level),
        enable_spill_buffer_read: Some(spill.enable_spill_buffer_read),
        max_spill_read_buffer_bytes_per_driver: (spill.max_spill_read_buffer_bytes_per_driver > 0)
            .then_some(spill.max_spill_read_buffer_bytes_per_driver),
        spill_mem_table_size: (spill.spill_mem_table_size > 0)
            .then_some(spill.spill_mem_table_size),
        spill_mem_table_num: (spill.spill_mem_table_num > 0).then_some(spill.spill_mem_table_num),
    }))
}

fn decode_endpoint_at(
    src: &str,
    path: FieldPath,
) -> Result<RuntimeEndpoint, NativeFragmentDecodeError> {
    RuntimeEndpoint::parse(src)
        .map_err(|detail| NativeFragmentDecodeError::invalid_value(path, detail))
}

pub(super) fn decode_scan_range_params_at(
    src: &novarocks::ScanRangeParams,
    path: FieldPath,
) -> Result<ScanRangeParams, NativeFragmentDecodeError> {
    let range = src.range.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("range"),
            "native ScanRangeParams requires range",
        )
    })?;
    let kind = range.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("range").field("kind"),
            "native ScanRange requires kind",
        )
    })?;
    let range = match kind {
        novarocks::scan_range::Kind::File(file) => ScanRange::File(decode_file_scan_range(
            file,
            path.clone().field("range").field("file"),
        )?),
        novarocks::scan_range::Kind::StarrocksTablet(tablet) => ScanRange::StarRocksTablet(
            crate::runtime::scan_range::StarRocksTabletScanRange::try_new(
                tablet.tablet_id,
                tablet.partition_id,
                tablet.version,
            )
            .map_err(|detail| {
                NativeFragmentDecodeError::invalid_value(
                    path.clone().field("range").field("starrocks_tablet"),
                    detail,
                )
            })?,
        ),
    };
    Ok(ScanRangeParams {
        range,
        volume_id: src.volume_id,
        empty: src.empty,
        has_more: src.has_more,
    })
}

fn decode_file_scan_range(
    src: &novarocks::FileScanRange,
    path: FieldPath,
) -> Result<FileScanRange, NativeFragmentDecodeError> {
    let file_format = match src.file_format.to_ascii_uppercase().as_str() {
        "PARQUET" => FileFormat::Parquet,
        "ORC" => FileFormat::Orc,
        value => {
            return Err(NativeFragmentDecodeError::invalid_enum(
                path.clone().field("file_format"),
                format!("unsupported file_format {value}"),
            ));
        }
    };
    let delete_files = src
        .delete_files
        .iter()
        .enumerate()
        .map(|(index, delete_file)| {
            decode_iceberg_delete_file(delete_file, path.clone().field("delete_files").index(index))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let file_pruning_min_max_values = if src.file_pruning_min_max_values.is_empty() {
        None
    } else {
        let mut ordinals = src
            .file_pruning_min_max_values
            .keys()
            .copied()
            .collect::<Vec<_>>();
        ordinals.sort_unstable();
        let mut values = BTreeMap::new();
        for ordinal in ordinals {
            let value = decode_file_pruning_value(
                &src.file_pruning_min_max_values[&ordinal],
                path.clone()
                    .field("file_pruning_min_max_values")
                    .map_key(ordinal.to_string()),
            )?;
            values.insert(ordinal, value);
        }
        Some(values)
    };
    let ivm_change_op = src
        .change_op
        .map(|value| {
            i8::try_from(value).map_err(|_| {
                NativeFragmentDecodeError::out_of_range(
                    path.clone().field("change_op"),
                    format!("change_op {value} exceeds i8 range"),
                )
            })
        })
        .transpose()?;
    Ok(FileScanRange {
        file_format,
        full_path: src.full_path.clone(),
        relative_path: src.relative_path.clone(),
        table_id: src.table_id,
        offset: src.offset,
        length: src.length,
        file_length: src.file_length,
        delete_files,
        deletion_vector_descriptor: src.deletion_vector_descriptor.as_ref().map(|descriptor| {
            DeletionVectorDescriptor {
                storage_type: descriptor.storage_type.clone(),
                path_or_inline_dv: descriptor.path_or_inline_dv.clone(),
                offset: descriptor.offset,
                size_in_bytes: descriptor.size_in_bytes,
                cardinality: descriptor.cardinality,
            }
        }),
        first_row_id: src.first_row_id,
        data_sequence_number: src.data_sequence_number,
        modification_time: src.modification_time,
        datacache_options: src
            .datacache_options
            .as_ref()
            .map(|options| DatacacheOptions {
                enable_populate_datacache: options.enable_populate_datacache,
                priority: options.priority,
            }),
        candidate_node: None,
        included_positions: src.included_positions.clone(),
        serialized_split: src.serialized_split.clone(),
        use_iceberg_jni_metadata_reader: src.use_iceberg_jni_metadata_reader,
        ivm_change_op,
        file_pruning_min_max_values,
    })
}

fn decode_iceberg_delete_file(
    src: &novarocks::IcebergDeleteFile,
    path: FieldPath,
) -> Result<IcebergDeleteFile, NativeFragmentDecodeError> {
    let file_format = match src.file_format.to_ascii_uppercase().as_str() {
        "PARQUET" => IcebergFileFormat::Parquet,
        value => {
            return Err(NativeFragmentDecodeError::invalid_enum(
                path.clone().field("file_format"),
                format!("unsupported Iceberg file_format {value}"),
            ));
        }
    };
    let file_content = match src.file_content.to_ascii_uppercase().as_str() {
        "POSITION_DELETES" => IcebergFileContent::PositionDeletes,
        "EQUALITY_DELETES" => IcebergFileContent::EqualityDeletes,
        value => {
            return Err(NativeFragmentDecodeError::invalid_enum(
                path.field("file_content"),
                format!("unsupported Iceberg file_content {value}"),
            ));
        }
    };
    Ok(IcebergDeleteFile {
        full_path: src.full_path.clone(),
        file_format,
        file_content,
        length: src.length,
    })
}

fn decode_file_pruning_value(
    src: &novarocks::FilePruningMinMaxValue,
    path: FieldPath,
) -> Result<FilePruningMinMaxValue, NativeFragmentDecodeError> {
    let value_kind = match src.value_kind {
        1 => FilePruningValueKind::Bool,
        2 => FilePruningValueKind::Int,
        3 => FilePruningValueKind::Float,
        value => {
            return Err(NativeFragmentDecodeError::invalid_enum(
                path.field("value_kind"),
                format!("unsupported file pruning value_kind {value}"),
            ));
        }
    };
    Ok(FilePruningMinMaxValue {
        value_kind,
        has_null: src.has_null,
        all_null: src.all_null,
        min_int_value: src.min_int_value,
        max_int_value: src.max_int_value,
        min_float_value: src.min_float_value,
        max_float_value: src.max_float_value,
    })
}

fn unique_id(src: &crate::proto::common::UniqueId) -> UniqueId {
    UniqueId {
        hi: src.hi,
        lo: src.lo,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::common::error::{ProtocolErrorKind, ProtocolFamily};
    use crate::protocol::native::encode::instance::encode_query_options;

    #[test]
    fn query_options_decode_is_owned_by_native_protocol() {
        let decoded = decode_query_options(&crate::proto::novarocks::QueryOptions {
            batch_size: 1024,
            pipeline_dop: 4,
            ..Default::default()
        })
        .expect("native query options");
        assert_eq!(decoded.batch_size, Some(1024));
        assert_eq!(decoded.pipeline_dop, Some(4));
    }

    #[test]
    fn destination_missing_id_has_typed_path() {
        let error = decode_destinations(&[crate::proto::novarocks::Destination {
            finst_id: None,
            endpoint: "127.0.0.1:9070".to_string(),
        }])
        .expect_err("missing finst id");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.family(), ProtocolFamily::Native);
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            protocol.path().to_string(),
            "instance_params.destinations[0].finst_id"
        );
    }

    #[test]
    fn query_options_preserve_explicit_zero_and_absent_bitset() {
        let options = QueryOptions {
            runtime_filter_scan_wait_time_ms: Some(0),
            runtime_filter_wait_timeout_ms: Some(0),
            group_concat_max_len: Some(0),
            cache: QueryCacheOptions {
                datacache_evict_probability: Some(0),
                ..Default::default()
            },
            ..Default::default()
        };

        let decoded = decode_query_options(&encode_query_options(&options))
            .expect("round trip native query options");

        assert_eq!(decoded.runtime_filter_scan_wait_time_ms, Some(0));
        assert_eq!(decoded.runtime_filter_wait_timeout_ms, Some(0));
        assert_eq!(decoded.group_concat_max_len, Some(0));
        assert_eq!(decoded.cache.datacache_evict_probability, Some(0));
        assert_eq!(decoded.enable_join_runtime_bitset_filter, None);
    }

    #[test]
    fn query_options_reject_spill_without_options() {
        let error = decode_query_options(&crate::proto::novarocks::QueryOptions {
            enable_spill: true,
            ..Default::default()
        })
        .expect_err("spill options are required");

        assert_eq!(
            error.protocol().expect("protocol error").kind(),
            ProtocolErrorKind::MissingField
        );
        assert!(error.to_string().contains("spill_options"), "{error}");
    }
}
