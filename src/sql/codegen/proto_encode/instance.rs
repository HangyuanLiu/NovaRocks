use std::collections::{BTreeMap, HashMap};

use crate::proto::{common, novarocks};
use crate::runtime::endpoint::{
    FragmentDestination, RuntimeEndpoint, RuntimeFilterProberDestination,
};
use crate::runtime::scan_range;
use crate::runtime::scheduler::FragmentInstancePlacement;
use crate::thrift::{internal_service, types};

pub(crate) fn encode_instance_params(
    query_id: &types::TUniqueId,
    placement: &FragmentInstancePlacement,
    query_options: Option<&internal_service::TQueryOptions>,
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
        finst_id: Some(encode_unique_id(src.finst_id())),
        grpc_endpoint: src.endpoint().as_host_port(),
    })
}

fn encode_runtime_filter_params(
    prober_params: &BTreeMap<i32, Vec<RuntimeFilterProberDestination>>,
    runtime_filter_builder_number: &BTreeMap<i32, i32>,
    runtime_filter_max_size: i64,
) -> Result<Option<novarocks::RuntimeFilterParams>, String> {
    if prober_params.is_empty()
        && runtime_filter_builder_number.is_empty()
        && runtime_filter_max_size == 0
    {
        return Ok(None);
    }

    Ok(Some(novarocks::RuntimeFilterParams {
        id_to_prober_params: prober_params
            .iter()
            .map(|(filter_id, params)| {
                Ok((
                    *filter_id,
                    novarocks::ProberParamsList {
                        params: params
                            .iter()
                            .map(encode_prober_params)
                            .collect::<Result<Vec<_>, _>>()?,
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>, String>>()?,
        runtime_filter_builder_number: runtime_filter_builder_number
            .iter()
            .map(|(filter_id, count)| (*filter_id, *count))
            .collect(),
        runtime_filter_max_size,
    }))
}

fn encode_prober_params(
    src: &RuntimeFilterProberDestination,
) -> Result<novarocks::ProberParams, String> {
    Ok(novarocks::ProberParams {
        fragment_instance_id: Some(encode_unique_id(src.fragment_instance_id())),
        grpc_endpoint: src.endpoint().as_host_port(),
    })
}

fn encode_query_options(src: &internal_service::TQueryOptions) -> novarocks::QueryOptions {
    novarocks::QueryOptions {
        batch_size: src.batch_size.unwrap_or_default(),
        query_timeout: src.query_timeout.unwrap_or_default(),
        enable_profile: src.enable_profile.unwrap_or(false),
        pipeline_dop: src.pipeline_dop.unwrap_or_default(),
        query_mem_limit: src.query_mem_limit.unwrap_or_default(),
        connector_io_tasks_per_scan_operator: src
            .connector_io_tasks_per_scan_operator
            .or(src.io_tasks_per_scan_operator)
            .unwrap_or_default(),
        runtime_filter_scan_wait_time_ms: src.runtime_filter_scan_wait_time_ms.unwrap_or_default(),
        runtime_filter_wait_timeout_ms: src.runtime_filter_wait_timeout_ms.unwrap_or_default(),
        allow_throw_exception: src.allow_throw_exception.unwrap_or(false),
        group_concat_max_len: src.group_concat_max_len.unwrap_or_default(),
        enable_spill: src.enable_spill.unwrap_or(false),
        spill_options: src.spill_options.as_ref().map(encode_spill_options),
    }
}

fn encode_spill_options(src: &internal_service::TSpillOptions) -> novarocks::SpillOptions {
    novarocks::SpillOptions {
        spill_mode: src.spill_mode.map(i32::from).unwrap_or_default(),
        spill_mem_limit_threshold: src
            .spill_mem_limit_threshold
            .map(|value| value.into_inner())
            .unwrap_or_default(),
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
