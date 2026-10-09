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

//! Exact matrix reader over one original AsyncMysqlStream, with one caller clock.
//! Child module of actors::mysql_stream; ordinary client methods are untouched.

use super::{AsyncMysqlStream, handshake_connection_id};
use crate::scenarios::exact_mysql_native_oracle::{
    PrefixOracle, RowInput, RowWireOracle, interrupted_terminal,
};
use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::io;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};

const SMALL: usize = 4096;
const MAX_PACKET: u32 = 67_108_864;
const REQUESTED_RECV: u32 = 4096;
const MAX_APPLIED_RECV: u32 = 65_536;
const ORIGINAL_X_SQL: &str = "SELECT REPEAT('x', 1048576) AS payload FROM generate_series(1, 1)";
const ORIGINAL_WIDE_SQL: &str = "SELECT REPEAT('q', 1048575 + generate_series) AS c0, REPEAT('q', 1048575 + generate_series) AS c1, REPEAT('q', 1048575 + generate_series) AS c2, REPEAT('q', 1048575 + generate_series) AS c3, REPEAT('q', 1048575 + generate_series) AS c4, REPEAT('q', 1048575 + generate_series) AS c5, REPEAT('q', 1048575 + generate_series) AS c6, REPEAT('q', 1048575 + generate_series) AS c7, REPEAT('q', 1048575 + generate_series) AS c8, REPEAT('q', 1048575 + generate_series) AS c9, REPEAT('q', 1048575 + generate_series) AS c10, REPEAT('q', 1048575 + generate_series) AS c11, REPEAT('q', 1048575 + generate_series) AS c12, REPEAT('q', 1048575 + generate_series) AS c13, REPEAT('q', 1048575 + generate_series) AS c14, REPEAT('q', 1048575 + generate_series) AS c15, REPEAT('q', 1048575 + generate_series) AS c16 FROM generate_series(1, 1)";
const NEW_TINY_SQL: &str = "SELECT 'abc' AS payload FROM generate_series(1, 1)";
const ORIGINAL_HEALTH_SQL: &str =
    "SELECT SUM(generate_series) AS total FROM generate_series(1, 100)";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExactPhase {
    Connect,
    Handshake,
    Authenticate,
    Connected,
    QueryWrite,
    Metadata,
    CutPrefix,
    RowTail,
    Terminal,
    MissingTail,
    MissingEof,
    Interrupted,
    HealthWrite,
    HealthRead,
    FollowupWrite,
    FollowupRead,
    FollowupEof,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExactReadSnapshot {
    pub phase: ExactPhase,
    pub connection_id: u32,
    pub applied_receive_buffer_bytes: Option<u32>,
    pub received_bytes: u64,
    pub sent_bytes: u64,
    pub handshake_received_bytes: u64,
    pub target_command_written_bytes: u64,
    pub metadata_wire_bytes: u64,
    pub row_wire_bytes: u64,
    pub terminal_wire_bytes: u64,
    pub health_wire_bytes: u64,
    pub followup_written_bytes: u64,
    pub followup_response_bytes: u64,
    pub received_sha256: [u8; 32],
    pub metadata_wire_sha256: [u8; 32],
    pub metadata_payload_sha256: [u8; 32],
    pub row_wire_sha256: [u8; 32],
    pub row_payload_prefix_sha256: [u8; 32],
    pub complete_row_sha256: Option<[u8; 32]>,
    pub terminal_wire_sha256: [u8; 32],
    pub pending_header: [u8; 4],
    pub pending_header_bytes: u8,
    pub pending_payload_length: Option<u32>,
    pub pending_payload_received: u32,
    pub metadata_complete: bool,
    pub cut_complete: bool,
    pub row_complete: bool,
    pub interrupted_complete: bool,
    pub missing_tail_read_eof: bool,
    pub followup_write_complete: bool,
    pub followup_read_eof: bool,
    pub last_read_eof: bool,
    pub failed: bool,
}
pub(crate) struct ExactReadFailure {
    pub observation: ExactReadSnapshot,
    pub cause: anyhow::Error,
}
impl std::fmt::Display for ExactReadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Source remains owned for typed inspection; never render its potentially secret text.
        let value = &self.observation;
        write!(
            f,
            "exact original MySQL reader failed phase={:?} received={} sent={} metadata={} row={} terminal={} header_bytes={} payload_received={} eof={} row_sha256=",
            value.phase,
            value.received_bytes,
            value.sent_bytes,
            value.metadata_wire_bytes,
            value.row_wire_bytes,
            value.terminal_wire_bytes,
            value.pending_header_bytes,
            value.pending_payload_received,
            value.last_read_eof
        )?;
        for byte in value.row_wire_sha256 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
impl std::fmt::Debug for ExactReadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for ExactReadFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}
pub(crate) type ExactReadResult<T> = std::result::Result<T, ExactReadFailure>;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ExactMetadata {
    pub columns: u8,
    pub actual_next_sequence: u8,
    pub wire_bytes: u64,
    pub wire_sha256: [u8; 32],
    pub payload_sha256: [u8; 32],
}
/// Fixed-size packet for the original one-column health oracle, not target row assembly.
pub(crate) struct ExactSmallPacket {
    pub sequence: u8,
    payload: [u8; SMALL],
    length: usize,
}
impl ExactSmallPacket {
    pub fn payload(&self) -> &[u8] {
        &self.payload[..self.length]
    }
}

#[derive(Clone, Copy)]
enum ReadKind {
    Handshake,
    Metadata,
    Row,
    Terminal,
    Health,
    Followup,
}
#[derive(Clone, Copy)]
enum WriteKind {
    Authenticate,
    Target,
    Health,
    Followup,
}
struct Recorder {
    state: ExactReadSnapshot,
    received: Sha256,
    metadata_wire: Sha256,
    metadata_payload: Sha256,
    row_wire: Sha256,
    row_payload: Sha256,
    terminal: Sha256,
}
impl Recorder {
    fn new() -> Self {
        Self {
            state: ExactReadSnapshot {
                phase: ExactPhase::Connect,
                connection_id: 0,
                applied_receive_buffer_bytes: None,
                received_bytes: 0,
                sent_bytes: 0,
                handshake_received_bytes: 0,
                target_command_written_bytes: 0,
                metadata_wire_bytes: 0,
                row_wire_bytes: 0,
                terminal_wire_bytes: 0,
                health_wire_bytes: 0,
                followup_written_bytes: 0,
                followup_response_bytes: 0,
                received_sha256: [0; 32],
                metadata_wire_sha256: [0; 32],
                metadata_payload_sha256: [0; 32],
                row_wire_sha256: [0; 32],
                row_payload_prefix_sha256: [0; 32],
                complete_row_sha256: None,
                terminal_wire_sha256: [0; 32],
                pending_header: [0; 4],
                pending_header_bytes: 0,
                pending_payload_length: None,
                pending_payload_received: 0,
                metadata_complete: false,
                cut_complete: false,
                row_complete: false,
                interrupted_complete: false,
                missing_tail_read_eof: false,
                followup_write_complete: false,
                followup_read_eof: false,
                last_read_eof: false,
                failed: false,
            },
            received: Sha256::new(),
            metadata_wire: Sha256::new(),
            metadata_payload: Sha256::new(),
            row_wire: Sha256::new(),
            row_payload: Sha256::new(),
            terminal: Sha256::new(),
        }
    }
    fn append_packet_header(&mut self, bytes: &[u8]) -> Result<()> {
        let at = self.state.pending_header_bytes as usize;
        ensure!(
            bytes.len() <= 4 - at,
            "actual small header read exceeds offered prefix"
        );
        self.state.pending_header[at..at + bytes.len()].copy_from_slice(bytes);
        self.state.pending_header_bytes += bytes.len() as u8;
        if self.state.pending_header_bytes == 4 {
            let header = self.state.pending_header;
            self.state.pending_payload_length =
                Some(u32::from(header[0]) | u32::from(header[1]) << 8 | u32::from(header[2]) << 16);
        }
        Ok(())
    }
    fn snapshot(&self) -> ExactReadSnapshot {
        let mut value = self.state;
        value.received_sha256 = self.received.clone().finalize().into();
        value.metadata_wire_sha256 = self.metadata_wire.clone().finalize().into();
        value.metadata_payload_sha256 = self.metadata_payload.clone().finalize().into();
        value.row_wire_sha256 = self.row_wire.clone().finalize().into();
        value.row_payload_prefix_sha256 = self.row_payload.clone().finalize().into();
        value.terminal_wire_sha256 = self.terminal.clone().finalize().into();
        value
    }
    fn observe_read(&mut self, kind: ReadKind, bytes: &[u8]) {
        self.state.received_bytes += bytes.len() as u64;
        self.received.update(bytes);
        match kind {
            ReadKind::Handshake => self.state.handshake_received_bytes += bytes.len() as u64,
            ReadKind::Metadata => {
                self.state.metadata_wire_bytes += bytes.len() as u64;
                self.metadata_wire.update(bytes);
            }
            ReadKind::Row => {
                const U24_FRAME: u64 = 0x00ff_ffff + 4;
                let mut at = self.state.row_wire_bytes;
                let mut remaining = bytes;
                while !remaining.is_empty() {
                    let local = at % U24_FRAME;
                    let count = if local < 4 {
                        (4 - local).min(remaining.len() as u64)
                    } else {
                        (U24_FRAME - local).min(remaining.len() as u64)
                    } as usize;
                    if local < 4 {
                        if local == 0 {
                            self.state.pending_header = [0; 4];
                            self.state.pending_header_bytes = 0;
                            self.state.pending_payload_length = None;
                            self.state.pending_payload_received = 0;
                        }
                        let offset = local as usize;
                        self.state.pending_header[offset..offset + count]
                            .copy_from_slice(&remaining[..count]);
                        self.state.pending_header_bytes = (offset + count) as u8;
                        if self.state.pending_header_bytes == 4 {
                            let header = self.state.pending_header;
                            self.state.pending_payload_length = Some(
                                u32::from(header[0])
                                    | u32::from(header[1]) << 8
                                    | u32::from(header[2]) << 16,
                            );
                        }
                    } else {
                        self.state.pending_payload_received += count as u32;
                        self.row_payload.update(&remaining[..count]);
                    }
                    at += count as u64;
                    remaining = &remaining[count..];
                }
                self.state.row_wire_bytes += bytes.len() as u64;
                self.row_wire.update(bytes);
            }
            ReadKind::Terminal => {
                self.state.terminal_wire_bytes += bytes.len() as u64;
                self.terminal.update(bytes);
            }
            ReadKind::Health => self.state.health_wire_bytes += bytes.len() as u64,
            ReadKind::Followup => self.state.followup_response_bytes += bytes.len() as u64,
        }
    }
    fn observe_write(&mut self, kind: WriteKind, count: usize) {
        self.state.sent_bytes += count as u64;
        match kind {
            WriteKind::Target => self.state.target_command_written_bytes += count as u64,
            WriteKind::Followup => self.state.followup_written_bytes += count as u64,
            WriteKind::Authenticate | WriteKind::Health => {}
        }
    }
    fn failure(&mut self, cause: anyhow::Error) -> ExactReadFailure {
        self.state.failed = true;
        ExactReadFailure {
            observation: self.snapshot(),
            cause,
        }
    }
}
fn check(deadline: Instant) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "original exact MySQL absolute clock expired"
    );
    Ok(())
}
async fn read_some(
    stream: &mut TcpStream,
    bytes: &mut [u8],
    deadline: Instant,
    recorder: &mut Recorder,
    kind: ReadKind,
) -> Result<usize> {
    check(deadline)?;
    let count =
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), stream.read(bytes))
            .await
            .context("original exact MySQL read deadline")??;
    recorder.state.last_read_eof = count == 0;
    if count != 0 {
        recorder.observe_read(kind, &bytes[..count]);
    }
    // Caller records pending-frame facts before its post-read clock check.
    Ok(count)
}
async fn write_all_original(
    stream: &mut TcpStream,
    mut bytes: &[u8],
    deadline: Instant,
    recorder: &mut Recorder,
    kind: WriteKind,
) -> Result<()> {
    while !bytes.is_empty() {
        check(deadline)?;
        let count = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            stream.write(bytes),
        )
        .await
        .context("original exact MySQL write deadline")??;
        ensure!(count != 0, io::Error::from(io::ErrorKind::WriteZero));
        recorder.observe_write(kind, count);
        bytes = &bytes[count..];
        check(deadline)?;
    }
    Ok(())
}
async fn small_packet(
    stream: &mut TcpStream,
    sequence: u8,
    maximum: usize,
    deadline: Instant,
    recorder: &mut Recorder,
    kind: ReadKind,
) -> Result<ExactSmallPacket> {
    ensure!(maximum <= SMALL, "small packet bound exceeds fixed scratch");
    recorder.state.pending_header = [0; 4];
    recorder.state.pending_header_bytes = 0;
    recorder.state.pending_payload_length = None;
    recorder.state.pending_payload_received = 0;
    while recorder.state.pending_header_bytes < 4 {
        let at = recorder.state.pending_header_bytes as usize;
        let mut scratch = [0; 4];
        let count = read_some(stream, &mut scratch[..4 - at], deadline, recorder, kind).await?;
        ensure!(count != 0, "original exact MySQL packet header ended early");
        // Keep the mechanical declaration from actual bytes before deadline refusal.
        recorder.append_packet_header(&scratch[..count])?;
        check(deadline)?;
    }
    let header = recorder.state.pending_header;
    let length =
        usize::from(header[0]) | usize::from(header[1]) << 8 | usize::from(header[2]) << 16;
    recorder.state.pending_payload_length = Some(length as u32);
    ensure!(
        header[3] == sequence,
        "original exact MySQL packet sequence differs"
    );
    ensure!(
        (1..=maximum).contains(&length),
        "original exact MySQL packet exceeds fixed preallocation bound"
    );
    if matches!(kind, ReadKind::Metadata) {
        ensure!(
            recorder.state.metadata_wire_bytes + length as u64 <= SMALL as u64,
            "original exact MySQL metadata exceeds total fixed bound"
        );
    }
    if matches!(kind, ReadKind::Health) {
        ensure!(
            recorder.state.health_wire_bytes + length as u64 <= SMALL as u64,
            "original health result exceeds total fixed bound"
        );
    }
    let mut packet = ExactSmallPacket {
        sequence,
        payload: [0; SMALL],
        length,
    };
    while recorder.state.pending_payload_received < length as u32 {
        let at = recorder.state.pending_payload_received as usize;
        let count = read_some(
            stream,
            &mut packet.payload[at..length],
            deadline,
            recorder,
            kind,
        )
        .await?;
        ensure!(
            count != 0,
            "original exact MySQL packet payload ended early"
        );
        recorder.state.pending_payload_received += count as u32;
        if matches!(kind, ReadKind::Metadata) {
            recorder
                .metadata_payload
                .update(&packet.payload[at..at + count]);
        }
        check(deadline)?;
    }
    Ok(packet)
}
async fn write_command(
    stream: &mut TcpStream,
    sql: &str,
    deadline: Instant,
    recorder: &mut Recorder,
    kind: WriteKind,
) -> Result<()> {
    let length = sql
        .len()
        .checked_add(1)
        .context("original command length overflow")?;
    ensure!(
        length <= SMALL,
        "original command exceeds fixed input bound"
    );
    let header = [length as u8, (length >> 8) as u8, (length >> 16) as u8, 0];
    write_all_original(stream, &header, deadline, recorder, kind).await?;
    write_all_original(stream, &[3], deadline, recorder, kind).await?;
    write_all_original(stream, sql.as_bytes(), deadline, recorder, kind).await
}

fn column(payload: &[u8], input: RowInput, index: u8) -> Result<()> {
    let mut expected = [0u8; 64];
    let prefix = b"\x03def\x00\x00\x00";
    expected[..prefix.len()].copy_from_slice(prefix);
    let mut at = prefix.len();
    if input.columns == 1 {
        expected[at] = 7;
        at += 1;
        expected[at..at + 7].copy_from_slice(b"payload");
        at += 7;
    } else {
        ensure!(index < 17, "column ordinal exceeds frozen wide schema");
        let name = if index < 10 {
            [b'c', b'0' + index, 0]
        } else {
            [b'c', b'1', b'0' + index - 10]
        };
        let length = if index < 10 { 2 } else { 3 };
        expected[at] = length as u8;
        at += 1;
        expected[at..at + length].copy_from_slice(&name[..length]);
        at += length;
    }
    expected[at] = 0;
    at += 1;
    let fixed = [12, 33, 0, 0, 4, 0, 0, 253, 0, 0, 0, 0, 0];
    expected[at..at + fixed.len()].copy_from_slice(&fixed);
    at += fixed.len();
    ensure!(
        payload.len() == at,
        "original exact MySQL column length differs"
    );
    let flags = at - 5;
    ensure!(
        payload[flags] <= 1,
        "original exact MySQL column flags differ"
    );
    expected[flags] = payload[flags]; // Original nullable/NOT_NULL is not a name/type oracle.
    ensure!(
        payload == &expected[..at],
        "original exact MySQL literal column schema differs"
    );
    Ok(())
}
fn original_sql(input: RowInput) -> Result<&'static str> {
    input.payload_bytes()?;
    Ok(
        match (input.columns, input.value_bytes, input.repeated_byte) {
            (1, 3, b'a') => NEW_TINY_SQL,
            (1, 1_048_576, b'x') => ORIGINAL_X_SQL,
            (17, 1_048_576, b'q') => ORIGINAL_WIDE_SQL,
            _ => bail!("SQL is outside original exact matrix"),
        },
    )
}

/// One original connection and one original target. Never opens another target or Root RPC.
pub(crate) struct ExactResultReader {
    client: AsyncMysqlStream,
    input: RowInput,
    deadline: Instant,
    recorder: Recorder,
    phase: ExactPhase,
    row: Option<RowWireOracle>,
    prefix: Option<PrefixOracle>,
    health_packets: u8,
}
impl ExactResultReader {
    pub async fn connect(
        user: &str,
        port: u16,
        input: RowInput,
        original_deadline: Instant,
    ) -> ExactReadResult<Self> {
        let mut recorder = Recorder::new();
        let result = async {
            check(original_deadline)?;
            input.payload_bytes()?;
            ensure!(
                !user.as_bytes().contains(&0),
                "original MySQL user contains NUL"
            );
            // Header+body are constructed only in a fixed stack buffer below.
            ensure!(
                user.len() <= SMALL
                    && user.len() + 32 + 2 + b"mysql_native_password".len() + 1 <= SMALL,
                "original auth user exceeds fixed preallocation bound"
            );
            let socket = TcpSocket::new_v4()?;
            socket.set_recv_buffer_size(REQUESTED_RECV)?;
            let actual = socket.recv_buffer_size()?;
            recorder.state.applied_receive_buffer_bytes = Some(actual);
            ensure!(
                actual > 0 && actual <= MAX_APPLIED_RECV,
                "OS applied receive buffer exceeds freeze"
            );
            let address = SocketAddr::from(([127, 0, 0, 1], port));
            let mut stream = tokio::time::timeout_at(
                tokio::time::Instant::from_std(original_deadline),
                socket.connect(address),
            )
            .await
            .context("original exact MySQL connect deadline")??;
            check(original_deadline)?;
            recorder.state.phase = ExactPhase::Handshake;
            let handshake = small_packet(
                &mut stream,
                0,
                SMALL,
                original_deadline,
                &mut recorder,
                ReadKind::Handshake,
            )
            .await?;
            let cid = handshake_connection_id(handshake.payload())?;
            recorder.state.connection_id = cid;
            drop(handshake);
            recorder.state.phase = ExactPhase::Authenticate;
            const FLAGS: u32 =
                0x0000_0001 | 0x0000_0004 | 0x0000_0200 | 0x0000_2000 | 0x0000_8000 | 0x0008_0000;
            let mut response = [0u8; SMALL];
            response[..4].copy_from_slice(&FLAGS.to_le_bytes());
            response[4..8].copy_from_slice(&MAX_PACKET.to_le_bytes());
            response[8] = 45;
            let mut at = 32;
            response[at..at + user.len()].copy_from_slice(user.as_bytes());
            at += user.len();
            at += 2; // NUL user and zero-length password, as the original actor.
            let plugin = b"mysql_native_password";
            response[at..at + plugin.len()].copy_from_slice(plugin);
            at += plugin.len() + 1;
            let header = [at as u8, (at >> 8) as u8, (at >> 16) as u8, 1];
            write_all_original(
                &mut stream,
                &header,
                original_deadline,
                &mut recorder,
                WriteKind::Authenticate,
            )
            .await?;
            write_all_original(
                &mut stream,
                &response[..at],
                original_deadline,
                &mut recorder,
                WriteKind::Authenticate,
            )
            .await?;
            let auth = small_packet(
                &mut stream,
                2,
                SMALL,
                original_deadline,
                &mut recorder,
                ReadKind::Handshake,
            )
            .await?;
            ensure!(
                auth.payload().first() == Some(&0),
                "original exact MySQL authentication refused or changed protocol"
            );
            check(original_deadline)?;
            Ok::<_, anyhow::Error>(AsyncMysqlStream {
                stream,
                timeout: original_deadline.saturating_duration_since(Instant::now()),
                receive_buffer_bytes: Some(actual),
                connection_id: cid,
            })
        }
        .await;
        match result.and_then(|client| {
            check(original_deadline)?;
            Ok(client)
        }) {
            Err(cause) => Err(recorder.failure(cause)),
            Ok(client) => {
                recorder.state.phase = ExactPhase::Connected;
                Ok(Self {
                    client,
                    input,
                    deadline: original_deadline,
                    recorder,
                    phase: ExactPhase::Connected,
                    row: None,
                    prefix: None,
                    health_packets: 0,
                })
            }
        }
    }
    pub fn connection_id(&self) -> u32 {
        self.client.connection_id
    }
    pub fn receive_buffer_bytes(&self) -> Option<u32> {
        self.client.receive_buffer_bytes
    }
    pub fn snapshot(&self) -> ExactReadSnapshot {
        self.recorder.snapshot()
    }
    fn require(&self, phase: ExactPhase) -> Result<()> {
        ensure!(
            !self.recorder.state.failed && self.phase == phase,
            "original exact reader phase is invalid or sealed"
        );
        check(self.deadline)
    }
    fn settle<T>(&mut self, result: Result<T>) -> ExactReadResult<T> {
        match result.and_then(|value| {
            check(self.deadline)?;
            Ok(value)
        }) {
            Ok(value) => Ok(value),
            Err(cause) => Err(self.recorder.failure(cause)),
        }
    }
    pub async fn send_original_query(&mut self, sql: &str) -> ExactReadResult<()> {
        let result = async {
            self.require(ExactPhase::Connected)?;
            ensure!(
                sql == original_sql(self.input)?,
                "target SQL differs from frozen original command"
            );
            self.phase = ExactPhase::QueryWrite;
            self.recorder.state.phase = self.phase;
            write_command(
                &mut self.client.stream,
                sql,
                self.deadline,
                &mut self.recorder,
                WriteKind::Target,
            )
            .await?;
            self.phase = ExactPhase::Metadata;
            self.recorder.state.phase = self.phase;
            Ok(())
        }
        .await;
        self.settle(result)
    }
    pub async fn read_metadata(&mut self) -> ExactReadResult<ExactMetadata> {
        let result = async {
            self.require(ExactPhase::Metadata)?;
            let count = small_packet(
                &mut self.client.stream,
                1,
                SMALL,
                self.deadline,
                &mut self.recorder,
                ReadKind::Metadata,
            )
            .await?;
            ensure!(
                count.payload() == [self.input.columns],
                "original literal result column count differs"
            );
            for ordinal in 0..self.input.columns {
                let packet = small_packet(
                    &mut self.client.stream,
                    ordinal + 2,
                    SMALL,
                    self.deadline,
                    &mut self.recorder,
                    ReadKind::Metadata,
                )
                .await?;
                column(packet.payload(), self.input, ordinal)?;
            }
            let eof = small_packet(
                &mut self.client.stream,
                self.input.columns + 2,
                SMALL,
                self.deadline,
                &mut self.recorder,
                ReadKind::Metadata,
            )
            .await?;
            ensure!(
                eof.payload() == [0xfe, 0, 0, 0, 0],
                "original metadata EOF differs"
            );
            let actual_next_sequence = eof.sequence.wrapping_add(1);
            self.row = Some(RowWireOracle::new(self.input, actual_next_sequence)?);
            self.prefix = Some(PrefixOracle::new(self.input)?);
            self.recorder.state.metadata_complete = true;
            self.phase = ExactPhase::CutPrefix;
            self.recorder.state.phase = self.phase;
            let state = self.snapshot();
            Ok(ExactMetadata {
                columns: self.input.columns,
                actual_next_sequence,
                wire_bytes: state.metadata_wire_bytes,
                wire_sha256: state.metadata_wire_sha256,
                payload_sha256: state.metadata_payload_sha256,
            })
        }
        .await;
        self.settle(result)
    }
    pub async fn read_cut_prefix(&mut self) -> ExactReadResult<ExactReadSnapshot> {
        let result = async {
            self.require(ExactPhase::CutPrefix)?;
            let mut scratch = [0u8; SMALL];
            while self.recorder.state.row_wire_bytes < self.input.cut {
                let maximum = (self.input.cut - self.recorder.state.row_wire_bytes)
                    .min(SMALL as u64) as usize;
                let count = read_some(
                    &mut self.client.stream,
                    &mut scratch[..maximum],
                    self.deadline,
                    &mut self.recorder,
                    ReadKind::Row,
                )
                .await?;
                ensure!(count != 0, "original row ended before exact cut prefix");
                self.prefix
                    .as_mut()
                    .context("original prefix oracle missing")?
                    .consume(&scratch[..count])?;
                self.row
                    .as_mut()
                    .context("original row oracle missing")?
                    .consume(&scratch[..count])?;
                check(self.deadline)?;
            }
            let actual = self.snapshot().row_wire_sha256;
            self.prefix
                .take()
                .context("original prefix oracle already consumed")?
                .finish(actual)?;
            self.recorder.state.cut_complete = true;
            self.phase = ExactPhase::RowTail;
            self.recorder.state.phase = self.phase;
            Ok(self.snapshot())
        }
        .await;
        self.settle(result)
    }
    pub async fn complete_row_then_interrupted(
        &mut self,
        expected_row_sha256: [u8; 32],
    ) -> ExactReadResult<ExactReadSnapshot> {
        let result = async {
            self.require(ExactPhase::RowTail)?;
            ensure!(
                self.input.expect_complete_tail,
                "missing-tail input cannot claim a complete row"
            );
            let mut scratch = [0u8; SMALL];
            let total = self
                .row
                .as_ref()
                .context("original row oracle missing")?
                .wire_length()?;
            while self.recorder.state.row_wire_bytes < total {
                let maximum =
                    (total - self.recorder.state.row_wire_bytes).min(SMALL as u64) as usize;
                let count = read_some(
                    &mut self.client.stream,
                    &mut scratch[..maximum],
                    self.deadline,
                    &mut self.recorder,
                    ReadKind::Row,
                )
                .await?;
                ensure!(count != 0, "original complete row ended early");
                self.row
                    .as_mut()
                    .context("original row oracle missing")?
                    .consume(&scratch[..count])?;
                check(self.deadline)?;
            }
            self.row
                .take()
                .context("original row oracle already consumed")?
                .finish(expected_row_sha256)?;
            let mut hash = self.recorder.row_payload.clone();
            hash.update(self.input.payload_bytes()?.to_le_bytes());
            let actual: [u8; 32] = hash.finalize().into();
            ensure!(
                actual == expected_row_sha256,
                "actual original payload hash differs"
            );
            self.recorder.state.complete_row_sha256 = Some(actual);
            self.recorder.state.row_complete = true;
            self.phase = ExactPhase::Terminal;
            self.recorder.state.phase = self.phase;
            let sequence = (self.input.columns + 3)
                .wrapping_add((self.input.payload_bytes()? / 0x00ff_ffff + 1) as u8);
            let packet = small_packet(
                &mut self.client.stream,
                sequence,
                SMALL - 4,
                self.deadline,
                &mut self.recorder,
                ReadKind::Terminal,
            )
            .await?;
            let mut wire = [0u8; SMALL];
            wire[..4].copy_from_slice(&self.recorder.state.pending_header);
            wire[4..4 + packet.length].copy_from_slice(packet.payload());
            interrupted_terminal(self.input, &wire[..4 + packet.length])?;
            self.recorder.state.interrupted_complete = true;
            self.phase = ExactPhase::Interrupted;
            self.recorder.state.phase = self.phase;
            Ok(self.snapshot())
        }
        .await;
        self.settle(result)
    }
    pub async fn read_missing_tail_to_eof(&mut self) -> ExactReadResult<ExactReadSnapshot> {
        let result = async {
            self.require(ExactPhase::RowTail)?;
            ensure!(
                !self.input.expect_complete_tail,
                "complete-tail input cannot borrow missing-tail EOF"
            );
            self.phase = ExactPhase::MissingTail;
            self.recorder.state.phase = self.phase;
            let total = self
                .row
                .as_ref()
                .context("original row oracle missing")?
                .wire_length()?;
            let mut scratch = [0u8; SMALL];
            loop {
                ensure!(
                    self.recorder.state.row_wire_bytes < total,
                    "missing-tail path received a complete original row"
                );
                let maximum =
                    (total - self.recorder.state.row_wire_bytes).min(SMALL as u64) as usize;
                let count = read_some(
                    &mut self.client.stream,
                    &mut scratch[..maximum],
                    self.deadline,
                    &mut self.recorder,
                    ReadKind::Row,
                )
                .await?;
                if count == 0 {
                    check(self.deadline)?;
                    self.recorder.state.missing_tail_read_eof = true;
                    self.phase = ExactPhase::MissingEof;
                    self.recorder.state.phase = self.phase;
                    return Ok(self.snapshot());
                }
                self.row
                    .as_mut()
                    .context("original row oracle missing")?
                    .consume(&scratch[..count])?;
                check(self.deadline)?;
            }
        }
        .await;
        self.settle(result)
    }
    /// This probe never waives a BrokenPipe/reset/write timeout; every write must complete.
    pub async fn probe_zero_response_followup(
        &mut self,
        sql: &str,
    ) -> ExactReadResult<ExactReadSnapshot> {
        let result = async {
            self.require(ExactPhase::MissingEof)?;
            ensure!(
                sql == ORIGINAL_HEALTH_SQL,
                "followup SQL differs from original health command"
            );
            self.phase = ExactPhase::FollowupWrite;
            self.recorder.state.phase = self.phase;
            write_command(
                &mut self.client.stream,
                sql,
                self.deadline,
                &mut self.recorder,
                WriteKind::Followup,
            )
            .await?;
            self.recorder.state.followup_write_complete = true;
            self.phase = ExactPhase::FollowupRead;
            self.recorder.state.phase = self.phase;
            let mut byte = [0u8; 1];
            let count = read_some(
                &mut self.client.stream,
                &mut byte,
                self.deadline,
                &mut self.recorder,
                ReadKind::Followup,
            )
            .await?;
            ensure!(
                count == 0,
                "original missing-tail followup received response bytes"
            );
            self.recorder.state.followup_read_eof = true;
            self.phase = ExactPhase::FollowupEof;
            self.recorder.state.phase = self.phase;
            Ok(self.snapshot())
        }
        .await;
        self.settle(result)
    }
    /// Same original socket, original health SQL, original clock; caller owns health oracle.
    pub async fn begin_original_health_query(&mut self, sql: &str) -> ExactReadResult<()> {
        let result = async {
            self.require(ExactPhase::Interrupted)?;
            ensure!(
                sql == ORIGINAL_HEALTH_SQL,
                "health SQL differs from original frozen command"
            );
            self.phase = ExactPhase::HealthWrite;
            self.recorder.state.phase = self.phase;
            write_command(
                &mut self.client.stream,
                sql,
                self.deadline,
                &mut self.recorder,
                WriteKind::Health,
            )
            .await?;
            self.phase = ExactPhase::HealthRead;
            self.recorder.state.phase = self.phase;
            Ok(())
        }
        .await;
        self.settle(result)
    }
    /// At most the five small packets of the original one-column health result.
    /// Driver must independently validate count, literal total/type, row5050 and EOF.
    pub async fn read_original_health_packet(&mut self) -> ExactReadResult<ExactSmallPacket> {
        let result = async {
            self.require(ExactPhase::HealthRead)?;
            ensure!(
                self.health_packets < 5,
                "original health packet count exceeds frozen one-result bound"
            );
            let sequence = self.health_packets + 1;
            let packet = small_packet(
                &mut self.client.stream,
                sequence,
                SMALL,
                self.deadline,
                &mut self.recorder,
                ReadKind::Health,
            )
            .await?;
            self.health_packets += 1;
            Ok(packet)
        }
        .await;
        self.settle(result)
    }
    /// Cleanup method only: takes no new clock and preserves existing failure observations.
    /// Dropping this owner also drops the same original socket; no child task is spawned here.
    pub async fn shutdown_original_socket(&mut self) -> ExactReadResult<()> {
        let result = async {
            check(self.deadline)?;
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(self.deadline),
                self.client.stream.shutdown(),
            )
            .await
            .context("original exact MySQL shutdown deadline")??;
            check(self.deadline)
        }
        .await;
        self.settle(result)
    }
}

#[cfg(test)]
#[path = "exact_result_reader_tests.rs"]
mod tests;
