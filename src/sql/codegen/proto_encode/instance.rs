use std::collections::HashMap;

use crate::proto::{common, novarocks};
use crate::runtime::scheduler::FragmentInstancePlacement;
use crate::thrift::{data_sinks, descriptors, internal_service, plan_nodes, runtime_filter, types};

pub(crate) fn encode_instance_params(
    query_id: &types::TUniqueId,
    placement: &FragmentInstancePlacement,
    query_options: Option<&internal_service::TQueryOptions>,
    runtime_filter_params: Option<&runtime_filter::TRuntimeFilterParams>,
    backend_num: i32,
    report_addr: Option<&types::TNetworkAddress>,
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
        runtime_filter_params: runtime_filter_params
            .map(encode_runtime_filter_params)
            .transpose()?,
        query_options: query_options.map(encode_query_options),
        report_addr: report_addr.map(format_network_address),
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
    src: &internal_service::TScanRangeParams,
) -> Result<novarocks::ScanRange, String> {
    let mut range = encode_scan_range(&src.scan_range)?;
    range.volume_id = src.volume_id;
    range.empty = src.empty;
    range.has_more = src.has_more;
    Ok(range)
}

fn encode_scan_range(src: &plan_nodes::TScanRange) -> Result<novarocks::ScanRange, String> {
    let mut populated_arms = 0;
    populated_arms += src.hdfs_scan_range.is_some() as usize;
    populated_arms += src.internal_scan_range.is_some() as usize;
    populated_arms += src.kudu_scan_token.is_some() as usize;
    populated_arms += src.broker_scan_range.is_some() as usize;
    populated_arms += src.es_scan_range.is_some() as usize;
    populated_arms += src.binlog_scan_range.is_some() as usize;
    populated_arms += src.benchmark_scan_range.is_some() as usize;
    if populated_arms != 1 {
        return Err(format!(
            "TScanRange must have exactly one populated arm for native encoding, found {populated_arms}"
        ));
    }

    if let Some(hdfs) = src.hdfs_scan_range.as_ref() {
        return Ok(novarocks::ScanRange {
            kind: Some(novarocks::scan_range::Kind::Hdfs(encode_hdfs_scan_range(
                hdfs,
            )?)),
            volume_id: None,
            empty: None,
            has_more: None,
        });
    }
    if let Some(internal) = src.internal_scan_range.as_ref() {
        return Ok(novarocks::ScanRange {
            kind: Some(novarocks::scan_range::Kind::Internal(
                encode_internal_scan_range(internal)?,
            )),
            volume_id: None,
            empty: None,
            has_more: None,
        });
    }

    Err("native InstanceParams only supports HDFS and internal scan ranges".to_string())
}

fn encode_hdfs_scan_range(
    src: &plan_nodes::THdfsScanRange,
) -> Result<novarocks::HdfsScanRange, String> {
    Ok(novarocks::HdfsScanRange {
        file_format: encode_hdfs_file_format(src.file_format.as_ref().ok_or_else(|| {
            "THdfsScanRange.file_format is required for native InstanceParams".to_string()
        })?)?
        .to_string(),
        full_path: src.full_path.clone(),
        relative_path: src.relative_path.clone(),
        table_id: src.table_id,
        offset: src.offset.unwrap_or_default(),
        length: src.length.unwrap_or_default(),
        file_length: src.file_length.unwrap_or_default(),
        delete_files: src
            .delete_files
            .as_deref()
            .unwrap_or_default()
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
        included_positions: src.included_positions.clone().unwrap_or_default(),
        serialized_split: src.serialized_split.clone(),
        use_iceberg_jni_metadata_reader: src.use_iceberg_jni_metadata_reader.unwrap_or(false),
    })
}

fn encode_internal_scan_range(
    src: &plan_nodes::TInternalScanRange,
) -> Result<novarocks::InternalScanRange, String> {
    let version = src.version.parse::<i64>().map_err(|e| {
        format!(
            "TInternalScanRange.version must be an i64-compatible string for native encoding: {e}"
        )
    })?;
    Ok(novarocks::InternalScanRange {
        version,
        tablet_id: src.tablet_id,
        partition_id: src.partition_id.unwrap_or_default(),
        db_name: Some(src.db_name.clone()),
        table_name: src.table_name.clone(),
        catalog_name: src.catalog_name.clone(),
        fill_data_cache: src.fill_data_cache.unwrap_or(false),
        skip_page_cache: src.skip_page_cache.unwrap_or(false),
        skip_disk_cache: src.skip_disk_cache.unwrap_or(false),
    })
}

fn encode_iceberg_delete_file(
    src: &plan_nodes::TIcebergDeleteFile,
) -> Result<novarocks::IcebergDeleteFile, String> {
    Ok(novarocks::IcebergDeleteFile {
        full_path: src.full_path.clone(),
        file_format: encode_hdfs_file_format(src.file_format.as_ref().ok_or_else(|| {
            "TIcebergDeleteFile.file_format is required for native InstanceParams".to_string()
        })?)?
        .to_string(),
        file_content: encode_iceberg_file_content(src.file_content.as_ref().ok_or_else(|| {
            "TIcebergDeleteFile.file_content is required for native InstanceParams".to_string()
        })?)?
        .to_string(),
        length: src.length,
    })
}

fn encode_deletion_vector_descriptor(
    src: &plan_nodes::TDeletionVectorDescriptor,
) -> novarocks::DeletionVectorDescriptor {
    novarocks::DeletionVectorDescriptor {
        storage_type: src.storage_type.clone(),
        path_or_inline_dv: src.path_or_inline_dv.clone(),
        offset: src.offset,
        size_in_bytes: src.size_in_bytes,
        cardinality: src.cardinality,
    }
}

fn encode_datacache_options(
    src: &crate::thrift::data_cache::TDataCacheOptions,
) -> novarocks::DatacacheOptions {
    novarocks::DatacacheOptions {
        enable_populate_datacache: src.enable_populate_datacache,
        priority: src.priority,
    }
}

fn encode_destination(
    src: &data_sinks::TPlanFragmentDestination,
) -> Result<novarocks::Destination, String> {
    let brpc_addr = src.brpc_server.as_ref().ok_or_else(|| {
        "TPlanFragmentDestination.brpc_server is required for native InstanceParams".to_string()
    })?;
    Ok(novarocks::Destination {
        finst_id: Some(encode_unique_id(&src.fragment_instance_id)),
        brpc_addr: format_network_address(brpc_addr),
    })
}

fn encode_runtime_filter_params(
    src: &runtime_filter::TRuntimeFilterParams,
) -> Result<novarocks::RuntimeFilterParams, String> {
    Ok(novarocks::RuntimeFilterParams {
        id_to_prober_params: src
            .id_to_prober_params
            .as_ref()
            .map(|entries| {
                entries
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
                    .collect::<Result<HashMap<_, _>, String>>()
            })
            .transpose()?
            .unwrap_or_default(),
        runtime_filter_builder_number: src
            .runtime_filter_builder_number
            .as_ref()
            .map(|values| values.iter().map(|(k, v)| (*k, *v)).collect())
            .unwrap_or_default(),
        runtime_filter_max_size: src.runtime_filter_max_size.unwrap_or_default(),
    })
}

fn encode_prober_params(
    src: &runtime_filter::TRuntimeFilterProberParams,
) -> Result<novarocks::ProberParams, String> {
    let fragment_instance_id = src.fragment_instance_id.as_ref().ok_or_else(|| {
        "TRuntimeFilterProberParams.fragment_instance_id is required for native encoding"
            .to_string()
    })?;
    let address = src.fragment_instance_address.as_ref().ok_or_else(|| {
        "TRuntimeFilterProberParams.fragment_instance_address is required for native encoding"
            .to_string()
    })?;
    Ok(novarocks::ProberParams {
        fragment_instance_id: Some(encode_unique_id(fragment_instance_id)),
        fragment_instance_address: format_network_address(address),
    })
}

fn encode_query_options(src: &internal_service::TQueryOptions) -> novarocks::QueryOptions {
    novarocks::QueryOptions {
        batch_size: src.batch_size.unwrap_or_default(),
        mem_limit: src.mem_limit.unwrap_or_default(),
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

fn encode_hdfs_file_format(src: &descriptors::THdfsFileFormat) -> Result<&'static str, String> {
    match *src {
        descriptors::THdfsFileFormat::PARQUET => Ok("PARQUET"),
        descriptors::THdfsFileFormat::ORC => Ok("ORC"),
        _ => Err(format!(
            "unsupported HDFS file format for native InstanceParams: {}",
            i32::from(*src)
        )),
    }
}

fn encode_iceberg_file_content(src: &types::TIcebergFileContent) -> Result<&'static str, String> {
    match *src {
        types::TIcebergFileContent::POSITION_DELETES => Ok("POSITION_DELETES"),
        types::TIcebergFileContent::EQUALITY_DELETES => Ok("EQUALITY_DELETES"),
        types::TIcebergFileContent::DATA => Ok("DATA"),
        _ => Err(format!(
            "unsupported Iceberg file content for native InstanceParams: {}",
            i32::from(*src)
        )),
    }
}

fn format_network_address(src: &types::TNetworkAddress) -> String {
    format!("{}:{}", src.hostname, src.port)
}
