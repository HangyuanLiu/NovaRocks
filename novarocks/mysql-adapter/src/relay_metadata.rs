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

//! Frozen MySQL result metadata for relayed Backend-encoded rows.
//!
//! A relayed result writes its metadata through the streaming writer as one
//! frozen packet set, so a cancellation cut can finish or abandon it at an
//! exact packet boundary. The packets are byte-for-byte what opensrv's own
//! result writer emits for the same columns: a column-count packet, one
//! ColumnDefinition41 packet per column, and an EOF packet unless the client
//! negotiated `CLIENT_DEPRECATE_EOF`. Every packet is checked against the
//! writer's metadata limits before its backing is allocated.

use std::io;

use opensrv_mysql::{CapabilityFlags, Column, FrozenMetadata, ProtocolLimits};

/// `utf8_general_ci`, the character set opensrv declares for every column.
const UTF8_GENERAL_CI: u16 = 33;
/// The column length opensrv declares for every column.
const COLUMN_LENGTH: u32 = 1024;
/// Length of the fixed-length fields that follow the names.
const FIXED_FIELDS_LENGTH: u64 = 0x0c;

/// Builds the frozen metadata of one result.
pub(crate) fn frozen_result_metadata(
    columns: &[Column],
    capabilities: CapabilityFlags,
    limits: ProtocolLimits,
) -> io::Result<FrozenMetadata> {
    if columns.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a MySQL result set needs at least one column",
        ));
    }
    let mut metadata = FrozenMetadata::builder(limits)?;
    let mut packet = Vec::new();
    write_lenenc_int(&mut packet, columns.len() as u64);
    metadata.try_push_bytes(&packet)?;
    for column in columns {
        packet.clear();
        write_lenenc_str(&mut packet, b"def");
        write_lenenc_str(&mut packet, b"");
        write_lenenc_str(&mut packet, column.table.as_bytes());
        write_lenenc_str(&mut packet, b"");
        write_lenenc_str(&mut packet, column.column.as_bytes());
        write_lenenc_str(&mut packet, b"");
        write_lenenc_int(&mut packet, FIXED_FIELDS_LENGTH);
        packet.extend_from_slice(&UTF8_GENERAL_CI.to_le_bytes());
        packet.extend_from_slice(&COLUMN_LENGTH.to_le_bytes());
        packet.push(column.coltype as u8);
        packet.extend_from_slice(&column.colflags.bits().to_le_bytes());
        // Decimals, then two unused bytes.
        packet.extend_from_slice(&[0x00, 0x00, 0x00]);
        metadata.try_push_bytes(&packet)?;
    }
    if !capabilities.contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
        // EOF with no warnings and empty status flags.
        metadata.try_push_bytes(&[0xFE, 0x00, 0x00, 0x00, 0x00])?;
    }
    Ok(metadata)
}

fn write_lenenc_int(output: &mut Vec<u8>, value: u64) {
    if value < 251 {
        output.push(value as u8);
    } else if value < 1 << 16 {
        output.push(0xFC);
        output.extend_from_slice(&(value as u16).to_le_bytes());
    } else if value < 1 << 24 {
        output.push(0xFD);
        output.extend_from_slice(&value.to_le_bytes()[..3]);
    } else {
        output.push(0xFE);
        output.extend_from_slice(&value.to_le_bytes());
    }
}

fn write_lenenc_str(output: &mut Vec<u8>, bytes: &[u8]) {
    write_lenenc_int(output, bytes.len() as u64);
    output.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use opensrv_mysql::{ColumnFlags, ColumnType};

    use super::*;

    fn column(name: &str, coltype: ColumnType, flags: ColumnFlags) -> Column {
        Column {
            table: String::new(),
            column: name.to_string(),
            coltype,
            colflags: flags,
        }
    }

    #[test]
    fn lenenc_integers_use_the_mysql_prefixes() {
        for (value, expected) in [
            (0_u64, vec![0x00]),
            (250, vec![0xFA]),
            (251, vec![0xFC, 0xFB, 0x00]),
            (65_535, vec![0xFC, 0xFF, 0xFF]),
            (65_536, vec![0xFD, 0x00, 0x00, 0x01]),
            (1 << 24, vec![0xFE, 0, 0, 0, 1, 0, 0, 0, 0]),
        ] {
            let mut output = Vec::new();
            write_lenenc_int(&mut output, value);
            assert_eq!(output, expected, "{value}");
        }
    }

    /// Split framed wire bytes into (sequence, payload) packets.
    fn packets(mut wire: &[u8]) -> Vec<(u8, Vec<u8>)> {
        let mut packets = Vec::new();
        while !wire.is_empty() {
            let length =
                usize::from(wire[0]) | usize::from(wire[1]) << 8 | usize::from(wire[2]) << 16;
            packets.push((wire[3], wire[4..4 + length].to_vec()));
            wire = &wire[4 + length..];
        }
        packets
    }

    #[tokio::test]
    async fn wire_packets_match_the_opensrv_column_definitions() {
        let columns = [
            column(
                "a",
                ColumnType::MYSQL_TYPE_LONGLONG,
                ColumnFlags::NOT_NULL_FLAG,
            ),
            column(
                "bb",
                ColumnType::MYSQL_TYPE_VAR_STRING,
                ColumnFlags::empty(),
            ),
        ];
        let limits = ProtocolLimits::default();
        let metadata = frozen_result_metadata(&columns, CapabilityFlags::empty(), limits).unwrap();
        let mut writer =
            opensrv_mysql::OwnedStreamingMysqlWriter::new(Vec::<u8>::new(), limits, 1).unwrap();
        writer.start_metadata(metadata).unwrap();
        writer.finish_metadata().await.unwrap();
        let wire = writer.into_inner();

        // What opensrv's column_definitions writes, spelled out literally.
        let definition = |name: &[u8], coltype: u8, flags: u16| {
            let mut packet = vec![3, b'd', b'e', b'f', 0, 0, 0, name.len() as u8];
            packet.extend_from_slice(name);
            packet.extend_from_slice(&[0, 0x0C, 33, 0, 0x00, 0x04, 0, 0, coltype]);
            packet.extend_from_slice(&flags.to_le_bytes());
            packet.extend_from_slice(&[0, 0, 0]);
            packet
        };
        assert_eq!(
            packets(&wire),
            vec![
                (1, vec![2]),
                (
                    2,
                    definition(
                        b"a",
                        ColumnType::MYSQL_TYPE_LONGLONG as u8,
                        ColumnFlags::NOT_NULL_FLAG.bits()
                    )
                ),
                (
                    3,
                    definition(b"bb", ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0)
                ),
                (4, vec![0xFE, 0, 0, 0, 0]),
            ]
        );
        // A client that deprecated EOF gets no EOF packet.
        let deprecated =
            frozen_result_metadata(&columns, CapabilityFlags::CLIENT_DEPRECATE_EOF, limits)
                .unwrap();
        assert_eq!(deprecated.packet_count(), 3);
    }

    #[test]
    fn empty_and_oversized_metadata_is_refused() {
        let limits = ProtocolLimits::default();
        assert!(frozen_result_metadata(&[], CapabilityFlags::empty(), limits).is_err());
        let wide = "w".repeat(limits.metadata_bytes);
        let columns = [column(
            &wide,
            ColumnType::MYSQL_TYPE_VAR_STRING,
            ColumnFlags::empty(),
        )];
        assert!(frozen_result_metadata(&columns, CapabilityFlags::empty(), limits).is_err());
    }
}
