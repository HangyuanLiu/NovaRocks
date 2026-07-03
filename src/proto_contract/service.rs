use prost::Message;

use crate::proto::{common, novarocks};

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

fn sample_exec_status_report() -> novarocks::ExecStatusReport {
    novarocks::ExecStatusReport {
        query_id: Some(common::UniqueId { hi: 1, lo: 2 }),
        fragment_instance_id: Some(common::UniqueId { hi: 3, lo: 4 }),
        backend_num: 5,
        status: Some(common::Status {
            code: 0,
            message: String::new(),
        }),
        done: true,
        iceberg_commits: Vec::new(),
        loaded_rows: 0,
        sink_load_bytes: 0,
        filtered_rows: 0,
        profile: None,
    }
}

#[test]
fn fetch_result_response_keeps_retired_thrift_tags_reserved() {
    use novarocks::fetch_result_response::Status;

    assert_eq!(Status::ResultStatusUnspecified as i32, 0);
    assert_eq!(Status::Ready as i32, 4);
    assert_eq!(Status::NotReady as i32, 5);
    assert_eq!(Status::Eof as i32, 6);
    assert_eq!(Status::Error as i32, 7);

    let response = novarocks::FetchResultResponse {
        status: Status::Ready as i32,
        message: "ready".to_string(),
        packet_seq: 9,
        eos: true,
        result_arrow_ipc: b"NRX1".to_vec(),
    };
    let fields = encoded_field_numbers(&response);

    assert!(fields.contains(&1), "status must stay on tag 1");
    assert!(
        !fields.contains(&2),
        "old result_batch_thrift tag 2 must stay retired"
    );
    assert!(fields.contains(&3), "message must stay on tag 3");
    assert!(
        fields.contains(&4),
        "result_arrow_ipc must stay on its NIDL-0 tag 4"
    );
    assert!(fields.contains(&5), "packet_seq must use a new tag");
    assert!(fields.contains(&6), "eos must use a new tag");
}

#[test]
fn exec_status_report_requests_keep_old_thrift_tag_reserved() {
    let single = novarocks::ReportExecStatusRequest {
        report: Some(sample_exec_status_report()),
    };
    let single_fields = encoded_field_numbers(&single);
    assert!(
        !single_fields.contains(&1),
        "old report_exec_status_params_thrift tag 1 must stay retired"
    );
    assert!(
        single_fields.contains(&2),
        "native report must use a new request tag"
    );

    let batch = novarocks::BatchReportExecStatusRequest {
        reports: vec![sample_exec_status_report()],
    };
    let batch_fields = encoded_field_numbers(&batch);
    assert!(
        !batch_fields.contains(&1),
        "old report_exec_status_params_thrift repeated tag 1 must stay retired"
    );
    assert!(
        batch_fields.contains(&2),
        "native reports must use a new request tag"
    );
}
