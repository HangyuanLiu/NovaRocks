use std::collections::HashMap;

use prost::Message;

use crate::proto::{common, novarocks, plan};

fn roundtrip_message<M>(value: &M) -> M
where
    M: Message + Default,
{
    M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
}

fn encoded_field_numbers<M: Message>(message: &M) -> Vec<u32> {
    let bytes = message.encode_to_vec();
    let mut fields = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let key = read_varint(&bytes, &mut offset);
        let field_number = (key >> 3) as u32;
        let wire_type = (key & 0x7) as u8;
        fields.push(field_number);
        match wire_type {
            0 => {
                let _ = read_varint(&bytes, &mut offset);
            }
            1 => offset += 8,
            2 => {
                let len = read_varint(&bytes, &mut offset) as usize;
                offset += len;
            }
            5 => offset += 4,
            other => panic!("unsupported wire type {other} in encoded proto"),
        }
    }
    fields
}

fn read_varint(bytes: &[u8], offset: &mut usize) -> u64 {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes
            .get(*offset)
            .unwrap_or_else(|| panic!("truncated varint at offset {}", *offset));
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
        shift += 7;
        assert!(shift < 64, "varint overflow");
    }
}

fn id(hi: i64, lo: i64) -> common::UniqueId {
    common::UniqueId { hi, lo }
}

fn hdfs_scan_range() -> novarocks::ScanRange {
    novarocks::ScanRange {
        kind: Some(novarocks::scan_range::Kind::Hdfs(
            novarocks::HdfsScanRange {
                file_format: "PARQUET".to_string(),
                full_path: Some("s3://warehouse/t/data-000.parquet".to_string()),
                relative_path: Some("data-000.parquet".to_string()),
                table_id: Some(42),
                offset: 128,
                length: 4096,
                file_length: 8192,
                delete_files: vec![novarocks::IcebergDeleteFile {
                    full_path: Some("s3://warehouse/t/delete-000.parquet".to_string()),
                    file_format: "PARQUET".to_string(),
                    file_content: "POSITION_DELETES".to_string(),
                    length: Some(256),
                }],
                deletion_vector_descriptor: Some(novarocks::DeletionVectorDescriptor {
                    storage_type: Some("PUFFIN".to_string()),
                    path_or_inline_dv: Some("s3://warehouse/t/dv.puffin".to_string()),
                    offset: Some(12),
                    size_in_bytes: Some(34),
                    cardinality: Some(5),
                }),
                first_row_id: Some(1000),
                data_sequence_number: Some(7),
                modification_time: Some(1_717_171_717),
                datacache_options: Some(novarocks::DatacacheOptions {
                    enable_populate_datacache: Some(true),
                    priority: Some(3),
                }),
                included_positions: vec![1000, 1003, 1008],
                serialized_split: Some("manifest-entry".to_string()),
                use_iceberg_jni_metadata_reader: true,
            },
        )),
        volume_id: None,
        empty: None,
        has_more: None,
    }
}

fn internal_scan_range() -> novarocks::ScanRange {
    novarocks::ScanRange {
        kind: Some(novarocks::scan_range::Kind::Internal(
            novarocks::InternalScanRange {
                version: 11,
                tablet_id: 22,
                partition_id: 33,
                db_name: Some("db1".to_string()),
                table_name: Some("tbl1".to_string()),
                catalog_name: Some("internal".to_string()),
                fill_data_cache: true,
                skip_page_cache: false,
                skip_disk_cache: true,
            },
        )),
        volume_id: None,
        empty: None,
        has_more: None,
    }
}

fn destination() -> novarocks::Destination {
    novarocks::Destination {
        finst_id: Some(id(3, 4)),
        brpc_addr: "10.0.0.8:8060".to_string(),
    }
}

fn runtime_filter_params() -> novarocks::RuntimeFilterParams {
    novarocks::RuntimeFilterParams {
        id_to_prober_params: HashMap::from([(
            77,
            novarocks::ProberParamsList {
                params: vec![novarocks::ProberParams {
                    fragment_instance_id: Some(id(5, 6)),
                    fragment_instance_address: "10.0.0.9:9060".to_string(),
                }],
            },
        )]),
        runtime_filter_builder_number: HashMap::from([(77, 2)]),
        runtime_filter_max_size: 1 << 20,
    }
}

fn query_options() -> novarocks::QueryOptions {
    novarocks::QueryOptions {
        batch_size: 4096,
        query_timeout: 300,
        enable_profile: true,
        pipeline_dop: 8,
        query_mem_limit: 512 << 20,
        connector_io_tasks_per_scan_operator: 4,
        runtime_filter_scan_wait_time_ms: 1500,
        runtime_filter_wait_timeout_ms: 3000,
        allow_throw_exception: true,
        group_concat_max_len: 65_536,
        enable_spill: true,
        spill_options: Some(novarocks::SpillOptions {
            spill_mode: 2,
            spill_mem_limit_threshold: 0.8,
            spill_operator_min_bytes: 1 << 20,
            spill_operator_max_bytes: 64 << 20,
            spill_encode_level: 1,
            enable_spill_buffer_read: true,
            max_spill_read_buffer_bytes_per_driver: 8 << 20,
            spill_mem_table_size: 16 << 20,
            spill_mem_table_num: 3,
        }),
        ..Default::default()
    }
}

#[test]
fn query_options_use_pre_release_reset_tags() {
    let query_mem_limit_only = novarocks::QueryOptions {
        query_mem_limit: 512 << 20,
        ..Default::default()
    };
    let fields = encoded_field_numbers(&query_mem_limit_only);

    assert_eq!(fields, vec![5], "query_mem_limit must use reset tag 5");
}

#[test]
fn scan_range_arms_survive_proto_roundtrip() {
    let hdfs = hdfs_scan_range();
    let decoded_hdfs: novarocks::ScanRange = roundtrip_message(&hdfs);
    assert_eq!(hdfs, decoded_hdfs);

    let internal = internal_scan_range();
    let decoded_internal: novarocks::ScanRange = roundtrip_message(&internal);
    assert_eq!(internal, decoded_internal);
}

#[test]
fn instance_params_survives_proto_roundtrip() {
    let params = novarocks::InstanceParams {
        query_id: Some(id(1, 2)),
        fragment_instance_id: Some(id(3, 4)),
        backend_num: 9,
        per_node_scan_ranges: HashMap::from([(
            10,
            novarocks::ScanRangeList {
                ranges: vec![hdfs_scan_range(), internal_scan_range()],
            },
        )]),
        per_exch_num_senders: HashMap::from([(20, 3)]),
        destinations: vec![destination()],
        runtime_filter_params: Some(runtime_filter_params()),
        query_options: Some(query_options()),
        report_addr: Some("10.0.0.10:9070".to_string()),
        typed_result_sink: true,
    };

    let decoded: novarocks::InstanceParams = roundtrip_message(&params);
    assert_eq!(params, decoded);
}

#[test]
fn submit_fragment_request_carries_native_fields_only() {
    let request = novarocks::SubmitFragmentRequest {
        plan: Some(plan::PlanFragment::default()),
        instance_params: Some(novarocks::InstanceParams {
            query_id: Some(id(1, 2)),
            fragment_instance_id: Some(id(3, 4)),
            backend_num: 1,
            per_node_scan_ranges: HashMap::new(),
            per_exch_num_senders: HashMap::new(),
            destinations: vec![destination()],
            runtime_filter_params: Some(runtime_filter_params()),
            query_options: Some(query_options()),
            report_addr: Some("10.0.0.10:9070".to_string()),
            typed_result_sink: true,
        }),
        ..Default::default()
    };
    let fields = encoded_field_numbers(&request);

    assert!(fields.contains(&1), "plan must use reset tag 1");
    assert!(fields.contains(&2), "instance_params must use reset tag 2");
    assert!(
        !fields.contains(&3),
        "pre-release reset must not keep old instance_params tag 3"
    );

    let decoded: novarocks::SubmitFragmentRequest = roundtrip_message(&request);
    assert_eq!(request, decoded);
}
