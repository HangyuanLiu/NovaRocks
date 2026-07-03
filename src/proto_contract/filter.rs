use prost::Message;

use crate::proto::{common, filter};

fn roundtrip_message<M>(value: &M) -> M
where
    M: Message + Default,
{
    M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
}

fn sample_unique_id() -> common::UniqueId {
    common::UniqueId {
        hi: 0x1122_3344_5566_7788,
        lo: -0x1020_3040_5060_708,
    }
}

fn sample_status() -> common::Status {
    common::Status {
        code: 0,
        message: "OK".to_string(),
    }
}

fn sample_scalar_type_desc() -> common::TypeDesc {
    common::TypeDesc {
        kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
            r#type: common::PrimitiveType::Decimal128 as i32,
            len: None,
            precision: Some(18),
            scale: Some(2),
            time_unit: None,
        })),
    }
}

fn sample_column(slot_id: i32, bytes: Vec<u8>) -> filter::Column {
    let data_size = bytes.len() as i64;
    filter::Column {
        slot_id,
        data_size,
        data: bytes,
    }
}

#[test]
fn transmit_runtime_filter_request_survives_proto_roundtrip() {
    let original = filter::TransmitRuntimeFilterRequest {
        is_partial: true,
        query_id: Some(sample_unique_id()),
        filter_id: 42,
        data: vec![0x03, 0x00, 0xff, 0x7f, 0x2a],
        build_be_number: 3,
        column_type: Some(sample_scalar_type_desc()),
    };

    assert_eq!(original, roundtrip_message(&original));
}

#[test]
fn transmit_runtime_filter_response_survives_proto_roundtrip() {
    let original = filter::TransmitRuntimeFilterResponse {
        status: Some(sample_status()),
        filter_id: 42,
    };

    assert_eq!(original, roundtrip_message(&original));
}

#[test]
fn lookup_request_with_multiple_columns_survives_proto_roundtrip() {
    let original = filter::LookupRequest {
        query_id: Some(sample_unique_id()),
        lookup_node_id: 7,
        request_tuple_id: 9,
        request_columns: vec![
            sample_column(11, vec![0x00, 0x01, 0x02, 0x03]),
            sample_column(12, vec![0xff, 0x80, 0x40, 0x00]),
        ],
    };

    assert_eq!(original, roundtrip_message(&original));
}

#[test]
fn lookup_response_with_status_and_column_survives_proto_roundtrip() {
    let original = filter::LookupResponse {
        status: Some(common::Status {
            code: 13,
            message: "lookup failed".to_string(),
        }),
        columns: vec![sample_column(21, vec![0x08, 0x96, 0x01, 0x00])],
    };

    assert_eq!(original, roundtrip_message(&original));
}

#[test]
fn column_preserves_opaque_bytes_across_proto_roundtrip() {
    let original = sample_column(31, vec![0x00, 0xff, 0x2a, 0x80, 0x7f, 0x00]);

    let decoded: filter::Column = roundtrip_message(&original);
    assert_eq!(original, decoded);
    assert_eq!(decoded.data, vec![0x00, 0xff, 0x2a, 0x80, 0x7f, 0x00]);
}
