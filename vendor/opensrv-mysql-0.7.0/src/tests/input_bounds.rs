// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::packet_reader::PacketReader;
use crate::{ProtocolLimits, U24_MAX};

#[tokio::test]
async fn declared_limit_is_checked_before_payload_allocation() {
    let wire = [5u8, 0, 0, 0];
    let mut reader = PacketReader::with_limit(wire.as_slice(), 4);
    assert!(reader
        .next_async()
        .await
        .unwrap_err()
        .to_string()
        .contains("input limit"));
}
#[tokio::test]
async fn pipeline_and_partial_packets_have_independent_owned_lifetimes() {
    let wire = [2, 0, 0, 0, b'a', b'b', 1, 0, 0, 0, b'c'];
    let mut reader = PacketReader::with_limit(wire.as_slice(), 3);
    reader.set_expected_first(Some(0));
    assert_eq!(
        reader.next_async().await.unwrap().unwrap().1.as_ref(),
        b"ab"
    );
    assert_eq!(reader.next_async().await.unwrap().unwrap().1.as_ref(), b"c");
    assert!(reader.next_async().await.unwrap().is_none());
    for wire in [&[1, 0][..], &[2, 0, 0, 0, b'a'][..]] {
        let mut reader = PacketReader::with_limit(wire, 3);
        assert!(reader.next_async().await.is_err());
    }
}
#[tokio::test]
async fn continuation_sequence_wrap_and_aggregate_limit() {
    let mut wire = vec![255, 255, 255, 255];
    wire.resize(4 + U24_MAX, b'a');
    wire.extend_from_slice(&[1, 0, 0, 0, b'b']);
    let mut reader = PacketReader::with_limit(wire.as_slice(), U24_MAX + 1);
    let (seq, packet) = reader.next_async().await.unwrap().unwrap();
    assert_eq!(seq, 0);
    assert_eq!(packet.len(), U24_MAX + 1);
    let mut reader = PacketReader::with_limit(wire.as_slice(), U24_MAX);
    assert!(reader
        .next_async()
        .await
        .unwrap_err()
        .to_string()
        .contains("input limit"));
    wire[4 + U24_MAX + 3] = 1;
    let mut reader = PacketReader::with_limit(wire.as_slice(), U24_MAX + 1);
    assert!(reader
        .next_async()
        .await
        .unwrap_err()
        .to_string()
        .contains("sequence"));
}
#[tokio::test]
async fn authentication_and_command_sequence_are_bounded() {
    let limits = ProtocolLimits::default();
    let length = limits.auth_bytes + 1;
    let wire = [
        (length & 255) as u8,
        ((length >> 8) & 255) as u8,
        ((length >> 16) & 255) as u8,
        1,
    ];
    let mut reader = PacketReader::with_limit(wire.as_slice(), limits.auth_bytes);
    assert!(reader.next_async().await.is_err());
    let mut reader = PacketReader::with_limit([1, 0, 0, 1, b'a'].as_slice(), limits.command_bytes);
    reader.set_expected_first(Some(0));
    assert!(reader
        .next_async()
        .await
        .unwrap_err()
        .to_string()
        .contains("sequence"));
}
#[test]
fn long_data_checks_parameter_and_aggregate_before_growth() {
    let mut statement = crate::StatementData {
        params: 2,
        ..Default::default()
    };
    assert!(statement.append_long_data(2, b"a", 4).is_err());
    statement.append_long_data(0, b"abc", 4).unwrap();
    statement.append_long_data(1, b"d", 4).unwrap();
    assert!(statement.append_long_data(1, b"e", 4).is_err());
    assert_eq!(statement.long_data[&1], b"d");
}

#[test]
fn malformed_execute_parameters_return_errors_before_shim() {
    for payload in [
        &[][..],
        &[0][..],
        &[0, 0][..],
        &[0, 2][..],
        &[0, 1, 255, 0][..],
        &[0, 1, 3, 0, 1][..],
    ] {
        let mut statement = crate::StatementData {
            params: 1,
            ..Default::default()
        };
        assert!(crate::ParamParser::new(payload, &mut statement).is_err());
    }
    let mut statement = crate::StatementData {
        params: 1,
        ..Default::default()
    };
    let parser = crate::ParamParser::new(&[0, 1, 3, 0, 42, 0, 0, 0], &mut statement).unwrap();
    assert_eq!(
        parser.into_iter().next().unwrap().value.into_inner(),
        crate::ValueInner::Int(42)
    );
    let parser = crate::ParamParser::new(&[0, 0, 43, 0, 0, 0], &mut statement).unwrap();
    assert_eq!(
        parser.into_iter().next().unwrap().value.into_inner(),
        crate::ValueInner::Int(43)
    );
    assert!(crate::ParamParser::new(&[0, 0, 43, 0, 0, 0, 1], &mut statement).is_err());
}

fn temporal_parameter(kind: crate::ColumnType, bytes: &[u8]) -> Vec<u8> {
    let mut payload = vec![0, 1, kind as u8, 0, bytes.len() as u8];
    payload.extend_from_slice(bytes);
    payload
}
#[test]
fn temporal_lengths_and_unrepresentable_content_fail_before_shim() {
    use crate::ColumnType::*;
    let mut statement = crate::StatementData {
        params: 1,
        ..Default::default()
    };
    // The review reproducer is one DATE with an illegal one-byte value.
    assert!(crate::ParamParser::new(&[0, 1, 10, 0, 1, 0], &mut statement).is_err());
    for kind in [
        MYSQL_TYPE_DATE,
        MYSQL_TYPE_DATETIME,
        MYSQL_TYPE_TIMESTAMP,
        MYSQL_TYPE_TIME,
    ] {
        for length in 0..=14 {
            let valid = match kind {
                MYSQL_TYPE_DATE => matches!(length, 0 | 4),
                MYSQL_TYPE_TIME => matches!(length, 0 | 8 | 12),
                _ => matches!(length, 0 | 4 | 7 | 11),
            };
            if !valid {
                assert!(crate::ParamParser::new(
                    &temporal_parameter(kind, &vec![0; length]),
                    &mut statement
                )
                .is_err());
            }
        }
    }
    let cases: &[(crate::ColumnType, &[u8])] = &[
        (MYSQL_TYPE_DATE, &[]),
        (MYSQL_TYPE_DATETIME, &[]),
        (MYSQL_TYPE_DATE, &[0xe8, 7, 2, 30]),
        (MYSQL_TYPE_DATE, &[0xe8, 7, 13, 1]),
        (MYSQL_TYPE_DATETIME, &[0xe8, 7, 2, 29, 24, 0, 0]),
        (MYSQL_TYPE_DATETIME, &[0xe8, 7, 2, 29, 1, 60, 0]),
        (
            MYSQL_TYPE_DATETIME,
            &[0xe8, 7, 2, 29, 1, 2, 3, 0x40, 0x42, 0x0f, 0],
        ),
        (MYSQL_TYPE_TIME, &[1, 0, 0, 0, 0, 0, 0, 0]),
        (MYSQL_TYPE_TIME, &[0, 0, 0, 0, 0, 24, 0, 0]),
        (MYSQL_TYPE_TIME, &[0, 0, 0, 0, 0, 0, 60, 0]),
        (MYSQL_TYPE_TIME, &[0, 0, 0, 0, 0, 0, 0, 60]),
        (
            MYSQL_TYPE_TIME,
            &[0, 0, 0, 0, 0, 0, 0, 0, 0x40, 0x42, 0x0f, 0],
        ),
    ];
    for &(kind, bytes) in cases {
        assert!(
            crate::ParamParser::new(&temporal_parameter(kind, bytes), &mut statement).is_err(),
            "{kind:?}: {bytes:?}"
        );
    }
    for kind in [MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME] {
        assert_eq!(
            crate::ParamParser::new(&temporal_parameter(kind, &[]), &mut statement)
                .err()
                .unwrap()
                .kind(),
            std::io::ErrorKind::Unsupported
        );
    }
    let parser =
        crate::ParamParser::new(&temporal_parameter(MYSQL_TYPE_TIME, &[]), &mut statement).err();
    assert!(parser.is_none());
}

#[test]
fn valid_temporal_parameters_keep_existing_conversions() {
    use crate::ColumnType::*;
    for (kind, bytes) in [
        (MYSQL_TYPE_DATE, vec![0xe8, 7, 2, 29]),
        (MYSQL_TYPE_DATETIME, vec![0xe8, 7, 2, 29]),
        (MYSQL_TYPE_DATETIME, vec![0xe8, 7, 2, 29, 23, 59, 59]),
        (
            MYSQL_TYPE_TIMESTAMP,
            vec![0xe8, 7, 2, 29, 23, 59, 59, 0x3f, 0x42, 0x0f, 0],
        ),
        (MYSQL_TYPE_TIME, vec![]),
        (MYSQL_TYPE_TIME, vec![0, 0, 0, 0, 0, 23, 59, 59]),
        (
            MYSQL_TYPE_TIME,
            vec![0, 0, 0, 0, 0, 23, 59, 59, 0x3f, 0x42, 0x0f, 0],
        ),
    ] {
        let payload = temporal_parameter(kind, &bytes);
        let mut statement = crate::StatementData {
            params: 1,
            ..Default::default()
        };
        let value = crate::ParamParser::new(&payload, &mut statement)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .value;
        match kind {
            MYSQL_TYPE_DATE => {
                let _: chrono::NaiveDate = value.into();
            }
            MYSQL_TYPE_TIME => {
                let value: std::time::Duration = value.into();
                if bytes.is_empty() {
                    assert!(value.is_zero());
                }
            }
            _ => {
                let _: chrono::NaiveDateTime = value.into();
            }
        }
    }
}

#[test]
fn aggregate_positions_and_empty_bindings_use_only_reserved_slots() {
    let limits = ProtocolLimits::default();
    let mut states = crate::input::PreparedStatements::new(limits).unwrap();
    states.prepare(0, 4096).unwrap();
    let before = states.usage().unwrap();
    assert!(states.prepare(1, 4096).is_err());
    assert_eq!(states.usage().unwrap(), before);
    for param in 0..4096 {
        for _ in 0..4 {
            states.append_long_data(0, param, &[]).unwrap();
        }
    }
    let after = states.usage().unwrap();
    assert_eq!(after.total_capacity_bytes, before.total_capacity_bytes);
    assert_eq!(after.long_data_capacity_bytes, 0);
    assert_eq!(after.long_data_entries, 4096);
    assert_eq!(before.long_data_entries, 0);
    assert!(before.total_capacity_bytes <= limits.connection_input_bytes);

    let mut states = crate::input::PreparedStatements::new(limits).unwrap();
    for id in 0..64 {
        states.prepare(id, 64).unwrap();
    }
    let before = states.usage().unwrap();
    for id in 0..64 {
        for param in 0..64 {
            states.append_long_data(id, param, &[]).unwrap();
        }
    }
    let after = states.usage().unwrap();
    assert_eq!(after.total_capacity_bytes, before.total_capacity_bytes);
    assert_eq!(after.long_data_capacity_bytes, 0);
    assert_eq!(after.long_data_entries, 4096);
    assert_eq!(before.parameter_count, 4096);
    assert!(states.prepare(64, 1).is_err());
}
#[test]
fn aggregate_input_limits_count_vec_capacity_and_reallocation_peaks() {
    let limits = ProtocolLimits::default();
    let mut states = crate::input::PreparedStatements::new(limits).unwrap();
    for id in 0..64 {
        states.prepare(id, 64).unwrap();
    }
    for id in 0..64 {
        for param in 0..64 {
            states.append_long_data(id, param, &[0; 32]).unwrap();
        }
    }
    let before = states.usage().unwrap();
    assert_eq!(before.long_data_entries, 4096);
    assert_eq!(before.long_data_payload_bytes, 4096 * 32);
    assert!(before.long_data_capacity_bytes >= before.long_data_payload_bytes);
    assert!(before.total_capacity_bytes <= limits.connection_input_bytes);
    assert!(states.append_long_data(0, 0, &vec![0; 900_000]).is_err());
    assert_eq!(states.usage().unwrap(), before);
    states.clear_long_data(0);
    let mut payload = vec![0; 8];
    payload.push(1);
    for _ in 0..64 {
        payload.extend_from_slice(&[crate::ColumnType::MYSQL_TYPE_LONG as u8, 0]);
    }
    payload.extend_from_slice(&[0; 64 * 4]);
    assert_eq!(states.parser(0, &payload).unwrap().into_iter().count(), 64);
    assert_eq!(
        states.usage().unwrap().bound_type_capacity_bytes,
        before.bound_type_capacity_bytes
    );
    states.remove(0);
    assert!(states.usage().unwrap().total_capacity_bytes < before.total_capacity_bytes);
}

#[test]
fn empty_long_data_binding_executes_as_empty_blob_with_entry_limit() {
    let mut single = crate::input::PreparedStatements::new(ProtocolLimits::default()).unwrap();
    single.prepare(1, 1).unwrap();
    single.append_long_data(1, 0, b"").unwrap();
    let value = single
        .parser(1, &[0, 1, crate::ColumnType::MYSQL_TYPE_BLOB as u8, 0])
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .value;
    assert_eq!(value.into_inner(), crate::ValueInner::Bytes(b""));
    let limits = ProtocolLimits {
        long_data_entries: 1,
        ..ProtocolLimits::default()
    };
    let mut states = crate::input::PreparedStatements::new(limits).unwrap();
    states.prepare(1, 2).unwrap();
    states.append_long_data(1, 0, b"").unwrap();
    let before = states.usage().unwrap();
    assert_eq!(before.long_data_entries, 1);
    assert_eq!(before.long_data_capacity_bytes, 0);
    assert!(states.append_long_data(1, 1, b"").is_err());
    assert_eq!(states.usage().unwrap(), before);
    let payload = [
        2,
        1,
        crate::ColumnType::MYSQL_TYPE_BLOB as u8,
        0,
        crate::ColumnType::MYSQL_TYPE_BLOB as u8,
        0,
    ];
    let value = states
        .parser(1, &payload)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .value;
    assert_eq!(value.into_inner(), crate::ValueInner::Bytes(b""));
    // A streamed byte binding cannot masquerade as a temporal/numeric Value.
    assert!(states
        .parser(
            1,
            &[
                2,
                1,
                crate::ColumnType::MYSQL_TYPE_DATE as u8,
                0,
                crate::ColumnType::MYSQL_TYPE_BLOB as u8,
                0
            ]
        )
        .is_err());
}
#[test]
fn long_data_reallocation_covers_old_and_new_even_when_final_state_fits() {
    let limits = ProtocolLimits::default();
    let mut states = crate::input::PreparedStatements::new(limits).unwrap();
    states.prepare(1, 1).unwrap();
    states.append_long_data(1, 0, &vec![0; 500_000]).unwrap();
    let before = states.usage().unwrap();
    assert!(
        before.total_capacity_bytes - before.long_data_capacity_bytes + 950_000
            <= limits.connection_input_bytes
    );
    assert!(states.append_long_data(1, 0, &vec![0; 450_000]).is_err());
    assert_eq!(states.usage().unwrap(), before);
}
