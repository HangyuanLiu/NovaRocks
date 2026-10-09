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

//! Minimal raw MySQL stream actor for protocol-boundary scenarios.
//!
//! The synchronous MySQL client is useful for ordinary SQL assertions, but it
//! deliberately owns response draining. T14 protocol scenarios need exact
//! control of when a schema, a row packet, or a socket close is observed, so
//! they use this actor instead of a second ad-hoc handshake implementation.

use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream as AsyncTcpStream;
use tokio::time::timeout as async_timeout;

#[cfg(unix)]
#[path = "mysql_stream/exact_result_reader.rs"]
pub(crate) mod exact_result_reader;

pub struct MysqlStream {
    stream: TcpStream,
}

/// An async form of [`MysqlStream`] for scenarios whose logical client count
/// deliberately exceeds the test process's fixed thread budget. It exposes
/// only the raw protocol operations used by those scenarios; the production
/// MySQL protocol assertions continue to use the synchronous actor above.
pub struct AsyncMysqlStream {
    stream: AsyncTcpStream,
    timeout: Duration,
    receive_buffer_bytes: Option<u32>,
    connection_id: u32,
}

pub struct MysqlPacket {
    sequence: u8,
    payload: Vec<u8>,
}

/// A bounded wire observation. Row payloads are hashed as they arrive rather
/// than accumulated, including rows spanning multiple U24 packets.
#[derive(Debug, Default, Serialize)]
pub struct TextResultObservation {
    pub rows: u64,
    pub row_payload_bytes: u64,
    pub wire_bytes: u64,
    pub packets: u64,
    pub columns: u64,
    pub schema: Vec<TextColumnObservation>,
    pub metadata_sha256: String,
    pub wire_prefix_sha256: String,
    pub first_payload_chunk_micros: Option<u128>,
    pub first_row_micros: Option<u128>,
    pub elapsed_micros: u128,
    pub row_sha256: String,
    pub error: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextColumnObservation {
    pub name: String,
    pub mysql_type: u8,
}

/// Opt-in scenario result; arbitrary actual sources are never serialized.
pub(crate) struct OwnedTextResultObservation {
    pub observation: TextResultObservation,
    pub actual_failure: Option<anyhow::Error>,
    pub server_result_error_code: Option<u16>,
}

/// A full bounded ERR packet; no dynamic error message is frozen as an oracle.
#[derive(Debug, Serialize)]
pub struct BoundedMysqlError {
    pub sequence: u8,
    pub payload_bytes: usize,
    pub code: u16,
    pub sqlstate: String,
    pub message: String,
    pub payload_hex: String,
}

/// Exactly three metadata packets followed by one complete ERR, never rows/EOF.
/// All allocations are capped before reading their payload (4 KiB per packet).
#[derive(Debug, Default, Serialize)]
pub struct BoundedNegativeTextObservation {
    pub text: TextResultObservation,
    pub metadata_payload_hex: Vec<String>,
    pub terminal_payload_hex: Option<String>,
    pub terminal: Option<BoundedMysqlError>,
    pub completed_metadata: bool,
    pub pending_packet: Option<BoundedPartialMysqlPacket>,
}

/// Last unfinished packet, bounded to a 4-byte header and 4 KiB payload.
#[derive(Debug, Default, Serialize)]
pub struct BoundedPartialMysqlPacket {
    pub phase: String,
    pub header_hex: String,
    pub expected_payload_bytes: Option<usize>,
    pub payload_prefix_hex: String,
    pub received_payload_bytes: usize,
}

impl AsyncMysqlStream {
    pub async fn observe_bounded_contract_error(
        &mut self,
        sql: &str,
        ready: tokio::sync::oneshot::Sender<()>,
        deadline: std::time::Instant,
    ) -> BoundedNegativeTextObservation {
        let started = std::time::Instant::now();
        let mut observed = BoundedNegativeTextObservation::default();
        let mut wire = Sha256::new();
        let mut metadata = Sha256::new();
        let result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            ensure!(
                std::time::Instant::now() < deadline,
                "absolute client deadline elapsed before query send"
            );
            self.send_query(sql).await?;
            let mut sequence = 1u8;
            for ordinal in 0..3 {
                let phase = ["metadata-count", "metadata-column", "metadata-eof"][ordinal];
                let payload = self
                    .negative_packet(&mut sequence, &mut observed, &mut wire, phase)
                    .await?;
                metadata.update(&payload);
                observed
                    .metadata_payload_hex
                    .push(bounded_packet_hex(&payload));
                match ordinal {
                    0 => {
                        ensure!(
                            payload == [1],
                            "negative result requires one complete metadata column"
                        );
                        observed.text.columns = 1;
                    }
                    1 => {
                        let column = parse_text_column(&payload)?;
                        ensure!(
                            column.name == "payload" && column.mysql_type == 253,
                            "negative result metadata schema differs from scalar SQL"
                        );
                        // Full encoding other than nullable flag is independent of runtime output.
                        let mut expected = vec![3, b'd', b'e', b'f', 0, 0, 0, 7];
                        expected.extend_from_slice(b"payload");
                        expected.extend_from_slice(&[0, 12, 33, 0, 0, 4, 0, 0, 253, 0, 0, 0, 0, 0]);
                        ensure!(
                            payload.len() == expected.len(),
                            "unexpected metadata byte length"
                        );
                        let flag_index = expected.len() - 5;
                        ensure!(payload[flag_index] <= 1, "unexpected scalar metadata flags");
                        expected[flag_index] = payload[flag_index];
                        ensure!(payload == expected, "unexpected complete metadata encoding");
                        observed.text.schema.push(column);
                    }
                    _ => ensure!(
                        payload == [0xfe, 0, 0, 0, 0],
                        "unexpected metadata EOF encoding"
                    ),
                }
            }
            observed.completed_metadata = true;
            ready
                .send(())
                .map_err(|_| anyhow::anyhow!("metadata controller disappeared"))?;
            let payload = self
                .negative_packet(&mut sequence, &mut observed, &mut wire, "terminal")
                .await?;
            let length = payload.len();
            observed.terminal_payload_hex = Some(bounded_packet_hex(&payload));
            ensure!(length >= 9, "terminal ERR shorter than protocol-41 prefix");
            ensure!(
                payload[0] == 0xff,
                "negative query leaked a Data/OK/EOF terminal instead of ERR"
            );
            ensure!(payload[3] == b'#', "ERR has no protocol-41 SQLSTATE");
            let sqlstate = std::str::from_utf8(&payload[4..9])?.to_owned();
            let message = std::str::from_utf8(&payload[9..])?.to_owned();
            observed.terminal = Some(BoundedMysqlError {
                sequence: sequence - 1,
                payload_bytes: length,
                code: u16::from_le_bytes([payload[1], payload[2]]),
                sqlstate,
                message,
                payload_hex: bounded_packet_hex(&payload),
            });
            observed.pending_packet = None;
            Ok::<(), anyhow::Error>(())
        })
        .await;
        observed.text.error = match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(format!("{error:#}").chars().take(512).collect()),
            Err(_) => Some("absolute negative query deadline exceeded".to_owned()),
        };
        observed.text.elapsed_micros = started.elapsed().as_micros();
        observed.text.metadata_sha256 = format!("{:x}", metadata.finalize());
        observed.text.wire_prefix_sha256 = format!("{:x}", wire.finalize());
        observed
    }
    async fn negative_packet(
        &mut self,
        expected: &mut u8,
        observed: &mut BoundedNegativeTextObservation,
        wire: &mut Sha256,
        phase: &str,
    ) -> Result<Vec<u8>> {
        observed.pending_packet = Some(BoundedPartialMysqlPacket {
            phase: phase.to_owned(),
            ..Default::default()
        });
        let mut header = [0u8; 4];
        let mut received = 0;
        while received < header.len() {
            let count = self.stream.read(&mut header[received..]).await?;
            ensure!(count != 0, "truncated negative packet header");
            wire.update(&header[received..received + count]);
            observed.text.wire_bytes += count as u64;
            received += count;
            observed
                .pending_packet
                .as_mut()
                .expect("pending packet")
                .header_hex = bounded_packet_hex(&header[..received]);
        }
        observed.text.packets += 1;
        ensure!(
            header[3] == *expected,
            "negative response packet sequence mismatch"
        );
        *expected = expected.wrapping_add(1);
        let length =
            usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
        observed
            .pending_packet
            .as_mut()
            .expect("pending packet")
            .expected_payload_bytes = Some(length);
        ensure!(
            (1..=4096).contains(&length),
            "negative packet exceeds 4 KiB cap"
        );
        let mut payload = vec![0u8; length];
        let mut received = 0;
        while received < length {
            let count = self.stream.read(&mut payload[received..]).await?;
            ensure!(count != 0, "truncated negative packet payload");
            wire.update(&payload[received..received + count]);
            observed.text.wire_bytes += count as u64;
            received += count;
            let pending = observed.pending_packet.as_mut().expect("pending packet");
            pending.received_payload_bytes = received;
            pending.payload_prefix_hex = bounded_packet_hex(&payload[..received]);
        }
        Ok(payload)
    }
}

fn bounded_packet_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut result, "{byte:02x}").expect("write String");
    }
    result
}

impl MysqlPacket {
    pub const fn sequence(&self) -> u8 {
        self.sequence
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn is_error(&self) -> bool {
        self.payload.first().copied() == Some(0xff)
    }

    pub fn is_result_terminator(&self) -> bool {
        is_mysql_result_terminator(&self.payload)
    }

    pub fn has_more_results(&self) -> bool {
        mysql_status_flags(&self.payload)
            .map(|flags| flags & 0x0008 != 0)
            .unwrap_or(false)
    }
}

impl MysqlStream {
    pub fn connect(user: &str, port: u16, timeout: Duration) -> Result<Self> {
        Self::connect_with_capabilities(user, port, timeout, false)
    }

    pub fn connect_with_multi_results(user: &str, port: u16, timeout: Duration) -> Result<Self> {
        Self::connect_with_capabilities(user, port, timeout, true)
    }

    fn connect_with_capabilities(
        user: &str,
        port: u16,
        timeout: Duration,
        multi_results: bool,
    ) -> Result<Self> {
        const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
        const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
        const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
        const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
        const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
        const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
        const CLIENT_MULTI_STATEMENTS: u32 = 0x0001_0000;
        const CLIENT_MULTI_RESULTS: u32 = 0x0002_0000;

        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let mut stream = TcpStream::connect_timeout(&address, timeout)
            .with_context(|| format!("connect raw public MySQL client at {address}"))?;
        stream
            .set_read_timeout(Some(timeout))
            .context("set raw MySQL read timeout")?;
        stream
            .set_write_timeout(Some(timeout))
            .context("set raw MySQL write timeout")?;

        let (_, handshake) = read_wire_packet(&mut stream).context("read MySQL handshake")?;
        ensure!(
            handshake.first().copied() == Some(10),
            "expected MySQL protocol v10 handshake, got payload={handshake:?}"
        );

        let mut client_flags = CLIENT_LONG_PASSWORD
            | CLIENT_LONG_FLAG
            | CLIENT_PROTOCOL_41
            | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH;
        if multi_results {
            client_flags |= CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS;
        }
        let mut response = Vec::with_capacity(user.len() + 64);
        response.extend_from_slice(&client_flags.to_le_bytes());
        response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        response.push(45);
        response.extend_from_slice(&[0u8; 23]);
        response.extend_from_slice(user.as_bytes());
        response.push(0);
        response.push(0);
        response.extend_from_slice(b"mysql_native_password");
        response.push(0);
        write_packet(&mut stream, 1, &response).context("write MySQL handshake response")?;

        let (_, auth_result) =
            read_wire_packet(&mut stream).context("read MySQL authentication result")?;
        if auth_result.first().copied() == Some(0xff) {
            bail!(
                "raw public MySQL authentication failed: {}",
                mysql_error_text(&auth_result)?
            );
        }
        ensure!(
            auth_result.first().copied() == Some(0),
            "unexpected raw MySQL authentication response: {auth_result:?}"
        );
        Ok(Self { stream })
    }

    pub fn query(user: &str, port: u16, sql: &str, timeout: Duration) -> Result<Self> {
        let mut stream = Self::connect(user, port, timeout)?;
        stream.send_query(sql)?;
        Ok(stream)
    }

    pub fn send_query(&mut self, sql: &str) -> Result<()> {
        let mut payload = Vec::with_capacity(sql.len() + 1);
        payload.push(0x03);
        payload.extend_from_slice(sql.as_bytes());
        write_packet(&mut self.stream, 0, &payload).context("write MySQL COM_QUERY packet")
    }

    pub fn expect_ok_packet(&mut self, operation: &str) -> Result<()> {
        let (_, response) = read_wire_packet(&mut self.stream)
            .with_context(|| format!("read response for {operation}"))?;
        if response.first().copied() == Some(0xff) {
            bail!("{operation} failed: {}", mysql_error_text(&response)?);
        }
        ensure!(
            response.first().copied() == Some(0),
            "{operation} expected a MySQL OK packet, got payload={response:?}"
        );
        Ok(())
    }

    /// Reads exactly one server response packet. Protocol scenarios own the
    /// packet sequence and response framing checks above this raw boundary.
    pub fn read_packet(&mut self, operation: &str) -> Result<MysqlPacket> {
        let (sequence, payload) =
            read_wire_packet(&mut self.stream).with_context(|| format!("read {operation}"))?;
        Ok(MysqlPacket { sequence, payload })
    }

    /// Reads the terminal failure of a one-column query after a possible
    /// metadata prefix. A protocol error may occur before schema start, or
    /// after the schema was made visible, but rows and success EOF are never
    /// accepted on this path.
    pub fn read_timeout_query_error(&mut self) -> Result<String> {
        let first = self.read_packet("timed query first response")?;
        if first.is_error() {
            return mysql_error_text(first.payload());
        }

        ensure!(
            first.payload() == [1],
            "expected timed query to begin with one-column metadata or ERR, got payload={:?}",
            first.payload()
        );
        let column = self.read_packet("timed query column metadata")?;
        ensure!(
            !column.is_result_terminator() && !column.is_error(),
            "expected timed query column definition, got payload={:?}",
            column.payload()
        );
        let metadata_end = self.read_packet("timed query metadata terminator")?;
        ensure!(
            metadata_end.is_result_terminator(),
            "expected timed query metadata terminator, got payload={:?}",
            metadata_end.payload()
        );
        let terminal = self.read_packet("timed query terminal error")?;
        mysql_error_text(terminal.payload())
    }

    pub fn shutdown(self) -> Result<()> {
        self.stream
            .shutdown(Shutdown::Both)
            .context("close raw public MySQL client connection")
    }
}

impl AsyncMysqlStream {
    /// Observes one text result using 64 KiB scratch and an absolute deadline.
    /// The actor does not negotiate deprecated EOF, so 0x00 is a valid empty
    /// first cell in a row; only the short 0xfe packet terminates row delivery.
    pub async fn observe_text_query(
        &mut self,
        sql: &str,
        read_delay: Duration,
    ) -> TextResultObservation {
        self.observe_text_query_with_metadata_pause(sql, read_delay, None)
            .await
    }

    /// Stops socket reads exactly after validated metadata. The same absolute
    /// query budget covers the pause, subsequent row reads and terminal EOF.
    pub async fn observe_text_query_with_metadata_pause(
        &mut self,
        sql: &str,
        read_delay: Duration,
        pause: Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    ) -> TextResultObservation {
        self.observe_text_query_kernel_until(
            sql,
            read_delay,
            pause,
            std::time::Instant::now() + self.timeout,
            false,
        )
        .await
        .observation
    }
    /// One caller-owned absolute clock covers metadata pause, rows and terminal.
    pub(crate) async fn observe_text_query_owned_until(
        &mut self,
        sql: &str,
        read_delay: Duration,
        pause: Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
        original_deadline: std::time::Instant,
    ) -> OwnedTextResultObservation {
        self.observe_text_query_kernel_until(sql, read_delay, pause, original_deadline, true)
            .await
    }
    async fn observe_text_query_kernel_until(
        &mut self,
        sql: &str,
        read_delay: Duration,
        pause: Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
        original_deadline: std::time::Instant,
        strict_owned: bool,
    ) -> OwnedTextResultObservation {
        let started = std::time::Instant::now();
        let mut observation = TextResultObservation::default();
        let mut digest = Sha256::new();
        let mut committed_digest = digest.clone();
        let mut wire_digest = Sha256::new();
        let mut metadata_digest = Sha256::new();
        let mut server_result_error_code = None;
        let result = tokio::time::timeout_at(original_deadline.into(), async {
            if strict_owned {
                ensure!(
                    std::time::Instant::now() < original_deadline,
                    "original absolute deadline expired before query send"
                );
            }
            self.send_query(sql).await?;
            let mut expected_sequence = 1u8;
            let first = self
                .observation_metadata_packet(
                    &mut expected_sequence,
                    &mut observation,
                    &mut wire_digest,
                )
                .await?;
            ensure!(
                first.first() != Some(&0xff),
                "server error {}",
                observation_error(&first)
            );
            let columns = decode_column_count(&first)?;
            ensure!(
                (1..=4096).contains(&columns),
                "invalid text-result column count"
            );
            if strict_owned {
                ensure!(
                    columns == 1,
                    "owned original observer requires one frozen column"
                );
            }
            observation.columns = columns;
            metadata_digest.update(&first);
            for _ in 0..columns {
                let column = self
                    .observation_metadata_packet(
                        &mut expected_sequence,
                        &mut observation,
                        &mut wire_digest,
                    )
                    .await?;
                metadata_digest.update(&column);
                ensure!(
                    column.first() != Some(&0xff),
                    "server metadata error {}",
                    observation_error(&column)
                );
                observation.schema.push(parse_text_column(&column)?);
            }
            let end = self
                .observation_metadata_packet(
                    &mut expected_sequence,
                    &mut observation,
                    &mut wire_digest,
                )
                .await?;
            validate_observation_eof(&end)?;
            metadata_digest.update(&end);
            if let Some((ready, resume)) = pause {
                ready
                    .send(())
                    .map_err(|_| anyhow::anyhow!("metadata observer exited"))?;
                resume.await.context("metadata read pause canceled")?;
            }
            let mut scratch = vec![0u8; 65536];
            let mut row_bytes = 0u64;
            let mut continuation = false;
            let mut row_validator = TextRowValidator::default();
            loop {
                let length = self
                    .observation_header(&mut expected_sequence, &mut observation, &mut wire_digest)
                    .await?;
                if length == 0 {
                    ensure!(continuation, "unexpected zero-length row packet");
                }
                let mut remaining = length;
                let mut first_chunk = true;
                while remaining != 0 {
                    let count = remaining.min(scratch.len());
                    read_observed(
                        &mut self.stream,
                        &mut scratch[..count],
                        &mut observation,
                        &mut wire_digest,
                    )
                    .await?;
                    if first_chunk && !continuation {
                        if scratch[0] == 0xff {
                            if strict_owned {
                                ensure!(
                                    length <= 4096
                                        && count == length
                                        && count >= 9
                                        && scratch[3] == b'#'
                                        && scratch[4..9].iter().all(u8::is_ascii_alphanumeric),
                                    "original result ERR packet is not complete bounded protocol-41"
                                );
                                server_result_error_code =
                                    Some(u16::from_le_bytes([scratch[1], scratch[2]]));
                                bail!("original server result ERR retained");
                            }
                            bail!(
                                "server result error {}",
                                observation_error(&scratch[..count])
                            );
                        }
                        if scratch[0] == 0xfe && length < 9 {
                            validate_observation_eof(&scratch[..count])?;
                            return Ok::<(), anyhow::Error>(());
                        }
                        observation
                            .first_payload_chunk_micros
                            .get_or_insert_with(|| started.elapsed().as_micros());
                    }
                    first_chunk = false;
                    row_validator.consume(&scratch[..count], columns)?;
                    digest.update(&scratch[..count]);
                    row_bytes += count as u64;
                    observation.row_payload_bytes += count as u64;
                    remaining -= count;
                    if !read_delay.is_zero() {
                        tokio::time::sleep(read_delay).await;
                    }
                }
                ensure!(
                    row_bytes <= 1024 * 1024 * 1024,
                    "logical row exceeds probe bound"
                );
                continuation = length == 0x00ff_ffff;
                if !continuation {
                    row_validator.finish(columns)?;
                    row_validator = TextRowValidator::default();
                    observation
                        .first_row_micros
                        .get_or_insert_with(|| started.elapsed().as_micros());
                    digest.update(row_bytes.to_le_bytes());
                    committed_digest = digest.clone();
                    observation.rows += 1;
                    row_bytes = 0;
                }
            }
        })
        .await;
        let actual_failure = match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => {
                observation.error = Some(if strict_owned {
                    "original result failure; actual source retained".to_string()
                } else {
                    format!("{error:#}").chars().take(512).collect()
                });
                Some(error)
            }
            Err(error) => {
                observation.error = Some("absolute query deadline exceeded".to_string());
                Some(error.into())
            }
        };
        observation.elapsed_micros = started.elapsed().as_micros();
        observation.row_sha256 = format!("{:x}", committed_digest.finalize());
        observation.wire_prefix_sha256 = format!("{:x}", wire_digest.finalize());
        observation.metadata_sha256 = format!("{:x}", metadata_digest.finalize());
        OwnedTextResultObservation {
            observation,
            actual_failure,
            server_result_error_code,
        }
    }

    async fn observation_header(
        &mut self,
        expected: &mut u8,
        observation: &mut TextResultObservation,
        wire_digest: &mut Sha256,
    ) -> Result<usize> {
        let mut header = [0u8; 4];
        read_observed(&mut self.stream, &mut header, observation, wire_digest).await?;
        observation.packets += 1;
        ensure!(header[3] == *expected, "response packet sequence mismatch");
        *expected = expected.wrapping_add(1);
        Ok(usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16))
    }

    async fn observation_metadata_packet(
        &mut self,
        expected: &mut u8,
        observation: &mut TextResultObservation,
        wire_digest: &mut Sha256,
    ) -> Result<Vec<u8>> {
        let length = self
            .observation_header(expected, observation, wire_digest)
            .await?;
        ensure!(length <= 1024 * 1024, "metadata packet exceeds probe bound");
        let mut payload = vec![0u8; length];
        read_observed(&mut self.stream, &mut payload, observation, wire_digest).await?;
        Ok(payload)
    }

    pub async fn connect(user: &str, port: u16, timeout: Duration) -> Result<Self> {
        Self::connect_with_max_packet_bytes(user, port, timeout, 16 * 1024 * 1024).await
    }

    /// Advertise the scenario's explicit logical packet allowance, including
    /// rows that span multiple physical U24 packets.
    pub async fn connect_with_max_packet_bytes(
        user: &str,
        port: u16,
        timeout: Duration,
        max_packet_bytes: u32,
    ) -> Result<Self> {
        Self::connect_settings(user, port, timeout, max_packet_bytes, None).await
    }

    /// Fix the client receive window before connect, without modifying any
    /// server profile. Record the OS-applied buffer instead of assuming it.
    pub async fn connect_with_receive_buffer(
        user: &str,
        port: u16,
        timeout: Duration,
        max_packet_bytes: u32,
        receive_buffer_bytes: u32,
    ) -> Result<Self> {
        ensure!(
            (1024..=65536).contains(&receive_buffer_bytes),
            "invalid probe receive buffer"
        );
        Self::connect_settings(
            user,
            port,
            timeout,
            max_packet_bytes,
            Some(receive_buffer_bytes),
        )
        .await
    }

    pub fn connection_id(&self) -> Result<u32> {
        ensure!(
            self.connection_id != 0,
            "MySQL connection identity is unavailable"
        );
        Ok(self.connection_id)
    }

    pub fn receive_buffer_bytes(&self) -> Option<u32> {
        self.receive_buffer_bytes
    }

    async fn connect_settings(
        user: &str,
        port: u16,
        timeout: Duration,
        max_packet_bytes: u32,
        receive_buffer_bytes: Option<u32>,
    ) -> Result<Self> {
        ensure!(
            (1..=1 << 30).contains(&max_packet_bytes),
            "invalid client packet allowance"
        );
        const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
        const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
        const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
        const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
        const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
        const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;

        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let (mut stream, receive_buffer_bytes) = if let Some(bytes) = receive_buffer_bytes {
            let socket = tokio::net::TcpSocket::new_v4()?;
            socket.set_recv_buffer_size(bytes)?;
            let actual = socket.recv_buffer_size()?;
            let stream = async_timeout(timeout, socket.connect(address))
                .await
                .context("time out connecting bounded receive-window client")??;
            (stream, Some(actual))
        } else {
            let stream = async_timeout(timeout, AsyncTcpStream::connect(address))
                .await
                .context("time out connecting raw async public MySQL client")??;
            (stream, None)
        };

        let (_, handshake) = read_wire_packet_async(&mut stream, timeout)
            .await
            .context("read async MySQL handshake")?;
        ensure!(
            handshake.first().copied() == Some(10),
            "expected MySQL protocol v10 handshake, got payload={handshake:?}"
        );
        let connection_id = handshake_connection_id(&handshake)?;

        let client_flags = CLIENT_LONG_PASSWORD
            | CLIENT_LONG_FLAG
            | CLIENT_PROTOCOL_41
            | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH;
        let mut response = Vec::with_capacity(user.len() + 64);
        response.extend_from_slice(&client_flags.to_le_bytes());
        response.extend_from_slice(&max_packet_bytes.to_le_bytes());
        response.push(45);
        response.extend_from_slice(&[0u8; 23]);
        response.extend_from_slice(user.as_bytes());
        response.push(0);
        response.push(0);
        response.extend_from_slice(b"mysql_native_password");
        response.push(0);
        write_packet_async(&mut stream, 1, &response, timeout)
            .await
            .context("write async MySQL handshake response")?;

        let (_, auth_result) = read_wire_packet_async(&mut stream, timeout)
            .await
            .context("read async MySQL authentication result")?;
        if auth_result.first().copied() == Some(0xff) {
            bail!(
                "raw async public MySQL authentication failed: {}",
                mysql_error_text(&auth_result)?
            );
        }
        ensure!(
            auth_result.first().copied() == Some(0),
            "unexpected raw async MySQL authentication response: {auth_result:?}"
        );
        Ok(Self {
            stream,
            timeout,
            receive_buffer_bytes,
            connection_id,
        })
    }

    pub async fn send_query(&mut self, sql: &str) -> Result<()> {
        let mut payload = Vec::with_capacity(sql.len() + 1);
        payload.push(0x03);
        payload.extend_from_slice(sql.as_bytes());
        write_packet_async(&mut self.stream, 0, &payload, self.timeout)
            .await
            .context("write async MySQL COM_QUERY packet")
    }

    pub async fn expect_ok_packet(&mut self, operation: &str) -> Result<()> {
        let (_, response) = read_wire_packet_async(&mut self.stream, self.timeout)
            .await
            .with_context(|| format!("read async response for {operation}"))?;
        if response.first().copied() == Some(0xff) {
            bail!("{operation} failed: {}", mysql_error_text(&response)?);
        }
        ensure!(
            response.first().copied() == Some(0),
            "{operation} expected a MySQL OK packet, got payload={response:?}"
        );
        Ok(())
    }

    pub async fn read_timeout_query_error(&mut self) -> Result<String> {
        let (_, first) = read_wire_packet_async(&mut self.stream, self.timeout)
            .await
            .context("read async timed query first response")?;
        if first.first().copied() == Some(0xff) {
            return mysql_error_text(&first);
        }

        ensure!(
            first == [1],
            "expected timed query to begin with one-column metadata or ERR, got payload={first:?}"
        );
        let (_, column) = read_wire_packet_async(&mut self.stream, self.timeout)
            .await
            .context("read async timed query column metadata")?;
        ensure!(
            !is_mysql_result_terminator(&column) && column.first().copied() != Some(0xff),
            "expected timed query column definition, got payload={column:?}"
        );
        let (_, metadata_end) = read_wire_packet_async(&mut self.stream, self.timeout)
            .await
            .context("read async timed query metadata terminator")?;
        ensure!(
            is_mysql_result_terminator(&metadata_end),
            "expected timed query metadata terminator, got payload={metadata_end:?}"
        );
        let (_, terminal) = read_wire_packet_async(&mut self.stream, self.timeout)
            .await
            .context("read async timed query terminal error")?;
        mysql_error_text(&terminal)
    }
}

fn handshake_connection_id(handshake: &[u8]) -> Result<u32> {
    ensure!(
        handshake.first() == Some(&10),
        "invalid MySQL handshake version"
    );
    let version = &handshake[1..];
    let end = version
        .iter()
        .position(|byte| *byte == 0)
        .context("missing MySQL server version terminator")?;
    ensure!(end > 0, "empty MySQL server version");
    let bytes = version
        .get(end + 1..end + 5)
        .context("truncated MySQL connection identity")?;
    let identity = u32::from_le_bytes(bytes.try_into()?);
    ensure!(identity != 0, "zero MySQL connection identity");
    Ok(identity)
}

fn mysql_error_text(payload: &[u8]) -> Result<String> {
    ensure!(
        payload.first().copied() == Some(0xff),
        "expected a MySQL error packet, got payload={payload:?}"
    );
    ensure!(
        payload.len() >= 3,
        "truncated MySQL error packet: {payload:?}"
    );
    let message_offset = if payload.get(3).copied() == Some(b'#') {
        9
    } else {
        3
    };
    Ok(String::from_utf8_lossy(&payload[message_offset..]).into_owned())
}

async fn read_observed(
    stream: &mut AsyncTcpStream,
    mut buffer: &mut [u8],
    observation: &mut TextResultObservation,
    wire_digest: &mut Sha256,
) -> Result<()> {
    while !buffer.is_empty() {
        let count = stream.read(buffer).await?;
        ensure!(count != 0, "truncated server response");
        observation.wire_bytes += count as u64;
        wire_digest.update(&buffer[..count]);
        buffer = &mut buffer[count..];
    }
    Ok(())
}

fn validate_observation_eof(payload: &[u8]) -> Result<()> {
    ensure!(
        payload.len() == 5 && payload[0] == 0xfe,
        "invalid protocol-41 EOF"
    );
    let status = u16::from_le_bytes([payload[3], payload[4]]);
    ensure!(status & 0x0008 == 0, "unexpected additional result set");
    Ok(())
}

/// Parse ColumnDefinition41 independently of the server metadata encoder.
/// These probes use ordinary COM_QUERY, whose definition has no default-value
/// suffix. Lengths are checked before any field copy.
fn parse_text_column(mut payload: &[u8]) -> Result<TextColumnObservation> {
    fn field<'a>(payload: &mut &'a [u8]) -> Result<&'a [u8]> {
        let marker = *payload.first().context("truncated column definition")?;
        let prefix = match marker {
            0..=250 => 1,
            0xfc => 3,
            0xfd => 4,
            0xfe => 9,
            _ => bail!("invalid column definition string length"),
        };
        let length = usize::try_from(decode_column_count(
            payload
                .get(..prefix)
                .context("truncated column definition length")?,
        )?)?;
        ensure!(
            length <= 65536,
            "column definition string exceeds probe bound"
        );
        *payload = &payload[prefix..];
        let value = payload
            .get(..length)
            .context("truncated column definition string")?;
        *payload = &payload[length..];
        Ok(value)
    }
    ensure!(
        field(&mut payload)? == b"def",
        "invalid column definition catalog"
    );
    for _ in 0..3 {
        field(&mut payload)?;
    }
    let name = field(&mut payload)?;
    field(&mut payload)?;
    ensure!(
        payload.len() == 13 && payload[0] == 0x0c && payload[11..] == [0, 0],
        "invalid column definition fixed section or trailing bytes"
    );
    Ok(TextColumnObservation {
        name: std::str::from_utf8(name)
            .context("invalid column name UTF-8")?
            .to_owned(),
        mysql_type: payload[7],
    })
}

#[derive(Default)]
struct TextRowValidator {
    cells: u64,
    remaining: u64,
    prefix_bytes: usize,
    prefix_offset: usize,
    prefix_value: u64,
}

impl TextRowValidator {
    fn consume(&mut self, mut bytes: &[u8], columns: u64) -> Result<()> {
        while !bytes.is_empty() {
            if self.remaining != 0 {
                let count = self.remaining.min(bytes.len() as u64) as usize;
                self.remaining -= count as u64;
                bytes = &bytes[count..];
                if self.remaining == 0 {
                    self.cells += 1;
                }
            } else if self.prefix_bytes != 0 {
                self.prefix_value |= u64::from(bytes[0]) << (8 * self.prefix_offset);
                self.prefix_offset += 1;
                self.prefix_bytes -= 1;
                bytes = &bytes[1..];
                if self.prefix_bytes == 0 {
                    ensure!(
                        self.prefix_value <= 1024 * 1024 * 1024,
                        "cell exceeds probe bound"
                    );
                    self.remaining = self.prefix_value;
                    if self.remaining == 0 {
                        self.cells += 1;
                    }
                }
            } else {
                ensure!(self.cells < columns, "too many text row cells");
                let first = bytes[0];
                bytes = &bytes[1..];
                match first {
                    0 | 0xfb => self.cells += 1,
                    1..=250 => self.remaining = u64::from(first),
                    0xfc..=0xfe => {
                        self.prefix_bytes = match first {
                            0xfc => 2,
                            0xfd => 3,
                            _ => 8,
                        };
                        self.prefix_offset = 0;
                        self.prefix_value = 0;
                    }
                    _ => bail!("invalid text cell length prefix"),
                }
            }
        }
        Ok(())
    }

    fn finish(&self, columns: u64) -> Result<()> {
        ensure!(
            self.cells == columns && self.remaining == 0 && self.prefix_bytes == 0,
            "truncated text row or column-count mismatch"
        );
        Ok(())
    }
}

fn observation_error(payload: &[u8]) -> String {
    let offset = if payload.get(3) == Some(&b'#') { 9 } else { 3 };
    let diagnostic = payload.get(offset..).unwrap_or_default();
    // The complete error body need not be copied or retained by the probe.
    let diagnostic = &diagnostic[..diagnostic.len().min(512)];
    format!(
        "code {}: {}",
        error_code(payload),
        String::from_utf8_lossy(diagnostic)
    )
}

fn error_code(payload: &[u8]) -> u16 {
    payload
        .get(1..3)
        .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
        .unwrap_or(0)
}

fn decode_column_count(payload: &[u8]) -> Result<u64> {
    let first = *payload.first().context("empty result header")?;
    match first {
        0..=250 if payload.len() == 1 => Ok(u64::from(first)),
        0xfc if payload.len() == 3 => Ok(u64::from(u16::from_le_bytes([payload[1], payload[2]]))),
        0xfd if payload.len() == 4 => Ok(u64::from(payload[1])
            | (u64::from(payload[2]) << 8)
            | (u64::from(payload[3]) << 16)),
        0xfe if payload.len() == 9 => Ok(u64::from_le_bytes(payload[1..9].try_into()?)),
        _ => bail!("invalid column-count encoding"),
    }
}

fn is_mysql_result_terminator(payload: &[u8]) -> bool {
    matches!(payload.first().copied(), Some(0xfe) if payload.len() < 9)
        || payload.first().copied() == Some(0)
}

fn mysql_status_flags(payload: &[u8]) -> Option<u16> {
    match payload.first().copied() {
        Some(0xfe) if payload.len() >= 5 => Some(u16::from_le_bytes([payload[3], payload[4]])),
        Some(0) if payload.len() >= 5 => Some(u16::from_le_bytes([payload[3], payload[4]])),
        _ => None,
    }
}

fn read_wire_packet(stream: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .context("read MySQL packet header")?;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0u8; length];
    stream
        .read_exact(&mut payload)
        .context("read MySQL packet payload")?;
    Ok((header[3], payload))
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) -> Result<()> {
    let length = u32::try_from(payload.len()).context("MySQL packet payload length fits u32")?;
    ensure!(length <= 0x00ff_ffff, "MySQL packet payload is too large");
    let header = [
        (length & 0xff) as u8,
        ((length >> 8) & 0xff) as u8,
        ((length >> 16) & 0xff) as u8,
        sequence,
    ];
    stream
        .write_all(&header)
        .context("write MySQL packet header")?;
    stream
        .write_all(payload)
        .context("write MySQL packet payload")?;
    stream.flush().context("flush MySQL packet")
}

async fn read_wire_packet_async(
    stream: &mut AsyncTcpStream,
    timeout: Duration,
) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    async_timeout(timeout, stream.read_exact(&mut header))
        .await
        .context("time out reading async MySQL packet header")??;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0u8; length];
    async_timeout(timeout, stream.read_exact(&mut payload))
        .await
        .context("time out reading async MySQL packet payload")??;
    Ok((header[3], payload))
}

async fn write_packet_async(
    stream: &mut AsyncTcpStream,
    sequence: u8,
    payload: &[u8],
    timeout: Duration,
) -> Result<()> {
    let length = u32::try_from(payload.len()).context("MySQL packet payload length fits u32")?;
    ensure!(length <= 0x00ff_ffff, "MySQL packet payload is too large");
    let header = [
        (length & 0xff) as u8,
        ((length >> 8) & 0xff) as u8,
        ((length >> 16) & 0xff) as u8,
        sequence,
    ];
    async_timeout(timeout, stream.write_all(&header))
        .await
        .context("time out writing async MySQL packet header")??;
    async_timeout(timeout, stream.write_all(payload))
        .await
        .context("time out writing async MySQL packet payload")??;
    async_timeout(timeout, stream.flush())
        .await
        .context("time out flushing async MySQL packet")??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AsyncMysqlStream, handshake_connection_id, mysql_error_text, read_wire_packet_async,
        write_packet_async,
    };
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn metadata_pause_resumes_or_expires_within_the_original_query_budget() {
        for resume_reads in [true, false] {
            let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .unwrap();
            let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let peer = tokio::spawn(async move {
                let (mut peer, _) = listener.accept().await.unwrap();
                let budget = Duration::from_secs(2);
                read_wire_packet_async(&mut peer, budget).await.unwrap();
                for (sequence, payload) in [
                    (1, vec![1]),
                    (2, column_definition()),
                    (3, vec![0xfe, 0, 0, 0, 0]),
                    (4, vec![1, b'7']),
                    (5, vec![0xfe, 0, 0, 0, 0]),
                ] {
                    write_packet_async(&mut peer, sequence, &payload, budget)
                        .await
                        .unwrap();
                }
            });
            let (ready, metadata) = tokio::sync::oneshot::channel();
            let (resume, resume_read) = tokio::sync::oneshot::channel();
            let job = tokio::spawn(async move {
                AsyncMysqlStream {
                    stream: client,
                    timeout: Duration::from_millis(200),
                    receive_buffer_bytes: None,
                    connection_id: 0,
                }
                .observe_text_query_with_metadata_pause(
                    "SELECT 7",
                    Duration::ZERO,
                    Some((ready, resume_read)),
                )
                .await
            });
            tokio::time::timeout(Duration::from_secs(2), metadata)
                .await
                .unwrap()
                .unwrap();
            assert!(!job.is_finished());
            if resume_reads {
                resume.send(()).unwrap();
            }
            let observation = tokio::time::timeout(Duration::from_secs(2), job)
                .await
                .unwrap()
                .unwrap();
            peer.await.unwrap();
            if resume_reads {
                assert_eq!(observation.error, None);
                assert_eq!((observation.rows, observation.packets), (1, 5));
            } else {
                assert_eq!(
                    observation.error.as_deref(),
                    Some("absolute query deadline exceeded")
                );
                assert_eq!((observation.rows, observation.packets), (0, 3));
            }
        }
    }

    #[test]
    fn handshake_identity_is_exact_and_never_guessed() {
        let mut handshake = vec![10, b'8', b'.', b'0', 0];
        handshake.extend_from_slice(&12345u32.to_le_bytes());
        assert_eq!(handshake_connection_id(&handshake).unwrap(), 12345);
        for length in 0..handshake.len() {
            assert!(handshake_connection_id(&handshake[..length]).is_err());
        }
        handshake[0] = 9;
        assert!(handshake_connection_id(&handshake).is_err());
        handshake[0] = 10;
        handshake[5..9].copy_from_slice(&0u32.to_le_bytes());
        assert!(handshake_connection_id(&handshake).is_err());
    }

    fn column_definition() -> Vec<u8> {
        // Six length-encoded strings, followed by the exact fixed 12 bytes.
        let mut column = b"\x03def\x00\x00\x00\x01v\x00".to_vec();
        column.extend_from_slice(&[0x0c, 33, 0, 0xff, 0xff, 0xff, 0xff, 253, 0, 0, 0, 0, 0]);
        column
    }

    #[test]
    fn observation_rejects_malformed_column_definitions() {
        let column = column_definition();
        let parsed = super::parse_text_column(&column).unwrap();
        assert_eq!(parsed.name, "v");
        assert_eq!(parsed.mysql_type, 253);
        for truncated in 0..column.len() {
            assert!(super::parse_text_column(&column[..truncated]).is_err());
        }
        for invalid in [vec![3], vec![0xfe, 0, 0, 0, 0], vec![0xfb], vec![0xfe; 9]] {
            assert!(super::parse_text_column(&invalid).is_err());
        }
        let mut extra = column.clone();
        extra.push(0);
        assert!(super::parse_text_column(&extra).is_err());
        let mut wrong_fixed_length = column.clone();
        wrong_fixed_length[10] = 11;
        assert!(super::parse_text_column(&wrong_fixed_length).is_err());
        let mut wrong_filler = column;
        *wrong_filler.last_mut().unwrap() = 1;
        assert!(super::parse_text_column(&wrong_filler).is_err());
    }

    #[tokio::test]
    async fn observation_rejects_bad_metadata_even_with_valid_row_and_eof() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let peer = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.unwrap();
            read_wire_packet_async(&mut peer, Duration::from_secs(2))
                .await
                .unwrap();
            // The old observer accepted this malformed [3] definition as a
            // complete one-column result, despite a correct row and EOF.
            let mut wire = Vec::new();
            for (sequence, payload) in [
                (1, &[1][..]),
                (2, &[3][..]),
                (3, &[0xfe, 0, 0, 0, 0][..]),
                (4, &[1, b'7'][..]),
                (5, &[0xfe, 0, 0, 0, 0][..]),
            ] {
                wire.extend_from_slice(&[payload.len() as u8, 0, 0, sequence]);
                wire.extend_from_slice(payload);
            }
            peer.write_all(&wire).await.unwrap();
        });
        let observation = AsyncMysqlStream {
            stream: client,
            timeout: Duration::from_secs(2),
            receive_buffer_bytes: None,
            connection_id: 0,
        }
        .observe_text_query("SELECT 7", Duration::ZERO)
        .await;
        peer.await.unwrap();
        assert!(
            observation
                .error
                .as_deref()
                .unwrap()
                .contains("truncated column definition")
        );
        assert!(observation.schema.is_empty());
        assert_eq!(observation.rows, 0);
    }

    #[test]
    fn observation_error_retains_the_server_reason_with_bounded_diagnostics() {
        let mut payload = vec![0xff, 0x51, 0x04, b'#', b'H', b'Y', b'0', b'0', b'0'];
        payload.extend_from_slice(b"result decode queue is full");
        assert_eq!(
            super::observation_error(&payload),
            "code 1105: result decode queue is full"
        );
        payload.truncate(9);
        payload.extend_from_slice(&[b'x'; 4096]);
        let diagnostic = super::observation_error(&payload);
        assert_eq!(diagnostic.len(), "code 1105: ".len() + 512);
        assert_eq!(super::observation_error(&[0xff]), "code 0: ");
    }

    #[tokio::test]
    async fn observation_counts_empty_first_cells_and_wrapped_sequences() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listen");
        let client = tokio::net::TcpStream::connect(listener.local_addr().expect("address"))
            .await
            .expect("connect");
        let peer = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.expect("accept");
            let timeout = Duration::from_secs(2);
            read_wire_packet_async(&mut peer, timeout)
                .await
                .expect("query");
            for (sequence, payload) in [
                (1, vec![1]),
                (2, column_definition()),
                (3, vec![0xfe, 0, 0, 0, 0]),
            ] {
                write_packet_async(&mut peer, sequence, &payload, timeout)
                    .await
                    .expect("metadata");
            }
            for row in 0..260u16 {
                write_packet_async(&mut peer, (4 + row) as u8, &[0], timeout)
                    .await
                    .expect("empty cell row");
            }
            write_packet_async(&mut peer, 8, &[0xfe, 0, 0, 0, 0], timeout)
                .await
                .expect("EOF");
        });
        let observation = AsyncMysqlStream {
            stream: client,
            timeout: Duration::from_secs(2),
            receive_buffer_bytes: None,
            connection_id: 0,
        }
        .observe_text_query("SELECT ''", Duration::ZERO)
        .await;
        peer.await.expect("peer");
        assert_eq!(observation.error, None);
        assert_eq!(observation.rows, 260);
        assert_eq!(observation.row_payload_bytes, 260);
        assert_eq!(observation.packets, 264);
    }

    #[tokio::test]
    async fn observation_hashes_u24_continuations_without_row_assembly() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listen");
        let client = tokio::net::TcpStream::connect(listener.local_addr().expect("address"))
            .await
            .expect("connect");
        let peer = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.expect("accept");
            let timeout = Duration::from_secs(5);
            read_wire_packet_async(&mut peer, timeout)
                .await
                .expect("query");
            for (sequence, payload) in [
                (1, vec![1]),
                (2, column_definition()),
                (3, vec![0xfe, 0, 0, 0, 0]),
            ] {
                write_packet_async(&mut peer, sequence, &payload, timeout)
                    .await
                    .expect("metadata");
            }
            peer.write_all(&[255, 255, 255, 4])
                .await
                .expect("U24 header");
            let scratch = [7u8; 65536];
            peer.write_all(&[0xfe]).await.expect("cell marker");
            peer.write_all(&(0x00ff_ffffu64 - 9).to_le_bytes())
                .await
                .expect("cell length");
            let mut remaining = 0x00ff_ffff - 9;
            while remaining != 0 {
                let count = remaining.min(scratch.len());
                peer.write_all(&scratch[..count])
                    .await
                    .expect("bounded slice");
                remaining -= count;
            }
            write_packet_async(&mut peer, 5, &[], timeout)
                .await
                .expect("zero terminal");
            write_packet_async(&mut peer, 6, &[0xfe, 0, 0, 0, 0], timeout)
                .await
                .expect("EOF");
        });
        let observation = AsyncMysqlStream {
            stream: client,
            timeout: Duration::from_secs(5),
            receive_buffer_bytes: None,
            connection_id: 0,
        }
        .observe_text_query("SELECT large_value", Duration::ZERO)
        .await;
        peer.await.expect("peer");
        assert_eq!(observation.error, None);
        assert_eq!(observation.rows, 1);
        assert_eq!(observation.row_payload_bytes, 0x00ff_ffff);
    }

    #[tokio::test]
    async fn observation_retains_truncated_response_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listen");
        let client = tokio::net::TcpStream::connect(listener.local_addr().expect("address"))
            .await
            .expect("connect");
        let peer = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.expect("accept");
            read_wire_packet_async(&mut peer, Duration::from_secs(2))
                .await
                .expect("query");
            peer.write_all(&[1, 0, 0, 1]).await.expect("header");
        });
        let observation = AsyncMysqlStream {
            stream: client,
            timeout: Duration::from_secs(2),
            receive_buffer_bytes: None,
            connection_id: 0,
        }
        .observe_text_query("SELECT 1", Duration::ZERO)
        .await;
        peer.await.expect("peer");
        assert!(observation.error.is_some());
        assert_eq!(observation.wire_bytes, 4);
        assert_eq!(observation.rows, 0);
    }

    #[tokio::test]
    async fn observation_retains_short_header_prefix_before_eof() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listen");
        let client = tokio::net::TcpStream::connect(listener.local_addr().expect("address"))
            .await
            .expect("connect");
        let peer = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.expect("accept");
            read_wire_packet_async(&mut peer, Duration::from_secs(2))
                .await
                .expect("query");
            peer.write_all(&[1, 0, 0]).await.expect("short header");
        });
        let observation = AsyncMysqlStream {
            stream: client,
            timeout: Duration::from_secs(2),
            receive_buffer_bytes: None,
            connection_id: 0,
        }
        .observe_text_query("SELECT 1", Duration::ZERO)
        .await;
        peer.await.expect("peer");
        assert!(
            observation
                .error
                .as_deref()
                .is_some_and(|error| error.contains("truncated server response"))
        );
        assert_eq!(observation.wire_bytes, 3);
        assert_eq!(observation.packets, 0);
        assert_eq!(observation.columns, 0);
        assert_eq!(observation.rows, 0);
    }

    #[test]
    fn mysql_error_text_reads_sqlstate_and_plain_packets() {
        assert_eq!(
            mysql_error_text(&[0xff, 0x01, 0x00, b'#', b'H', b'Y', b'0', b'0', b'0', b'x'])
                .expect("sqlstate packet"),
            "x"
        );
        assert_eq!(
            mysql_error_text(&[0xff, 0x01, 0x00, b'x']).expect("plain packet"),
            "x"
        );
    }
}

/// Host-only scripted MySQL fixtures. They exercise the observer and refusal
/// predicate; they are not native FE/BE proof and mint no Native authority.
#[cfg(test)]
pub(crate) mod root_reply_test_peer {
    use super::*;
    use std::time::Instant;

    const BUDGET: Duration = Duration::from_secs(1);
    const WATCHDOG: Duration = Duration::from_secs(3);
    const REASON: &str = "unsupported root-result profile";
    const SQL: &str = "SELECT REPEAT('x',64) AS payload";

    #[derive(Clone, Copy, Debug)]
    pub(crate) enum Mode {
        HeaderPrefix,
        BodyPrefix,
        Oversize,
        WrongSequence,
        Interrupted,
        Eof,
        Ok,
        Valid,
    }
    pub(crate) struct ScriptResult {
        pub observed: BoundedNegativeTextObservation,
        pub response_wire: Vec<u8>,
        pub metadata_wire_bytes: usize,
        pub metadata_sha256: String,
        pub handshake_elapsed: Duration,
    }
    fn metadata_payloads() -> [Vec<u8>; 3] {
        let mut column = vec![3, b'd', b'e', b'f', 0, 0, 0, 7];
        column.extend_from_slice(b"payload");
        column.extend_from_slice(&[0, 12, 33, 0, 0, 4, 0, 0, 253, 0, 0, 0, 0, 0]);
        [vec![1], column, vec![0xfe, 0, 0, 0, 0]]
    }
    fn error_payload(code: u16) -> Vec<u8> {
        let mut payload = vec![0xff];
        payload.extend_from_slice(&code.to_le_bytes());
        payload.extend_from_slice(b"#HY000");
        payload.extend_from_slice(REASON.as_bytes());
        payload
    }
    fn framed(sequence: u8, payload: &[u8]) -> Vec<u8> {
        assert!(payload.len() <= 4097);
        let length = payload.len();
        let mut wire = vec![
            length as u8,
            (length >> 8) as u8,
            (length >> 16) as u8,
            sequence,
        ];
        wire.extend_from_slice(payload);
        wire
    }
    pub(crate) async fn script(mode: Mode, handshake_delay: Duration) -> Result<ScriptResult> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let started = Instant::now();
        let deadline = started + BUDGET;
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            // This watchdog bounds the fixture itself and always lets its
            // JoinHandle exit; it is not a scene deadline or fallback oracle.
            tokio::time::timeout(WATCHDOG, async move {
                let (mut peer,_)=listener.accept().await?;
                tokio::time::sleep(handshake_delay).await;
                let mut handshake=vec![10,b'8',b'.',b'0',0];
                handshake.extend_from_slice(&12345u32.to_le_bytes());
                write_packet_async(&mut peer,0,&handshake,WATCHDOG).await?;
                let (sequence,response)=read_wire_packet_async(&mut peer,WATCHDOG).await?;
                ensure!(sequence==1 && response.len() < 256, "unexpected scripted handshake response");
                write_packet_async(&mut peer,2,&[0],WATCHDOG).await?;
                let (sequence,query)=read_wire_packet_async(&mut peer,WATCHDOG).await?;
                ensure!(sequence==0 && query.first()==Some(&3) && &query[1..]==SQL.as_bytes(),
                    "unexpected scripted COM_QUERY");
                let mut response_wire=Vec::with_capacity(512);
                let mut metadata_digest=Sha256::new();
                for (ordinal,payload) in metadata_payloads().into_iter().enumerate() {
                    metadata_digest.update(&payload);
                    let wire=framed(ordinal as u8+1,&payload);
                    peer.write_all(&wire).await?;
                    response_wire.extend_from_slice(&wire);
                }
                let metadata_wire_bytes=response_wire.len();
                let terminal=match mode {
                    Mode::Oversize=>vec![1,16,0,4], // 4097 announced, no payload is sent.
                    Mode::WrongSequence=>framed(5,&error_payload(1105)),
                    Mode::Interrupted=>framed(4,&error_payload(1317)),
                    Mode::Eof=>framed(4,&[0xfe,0,0,0,0]),
                    Mode::Ok=>framed(4,&[0,0,0,0,0,0,0,0,0]),
                    _=>framed(4,&error_payload(1105)),
                };
                let sent=match mode {Mode::HeaderPrefix=>2,Mode::BodyPrefix=>7,_=>terminal.len()};
                peer.write_all(&terminal[..sent]).await?;
                peer.flush().await?;
                response_wire.extend_from_slice(&terminal[..sent]);
                if matches!(mode,Mode::HeaderPrefix|Mode::BodyPrefix) {
                    // A renewed observer clock would accept the late complete
                    // ERR and fail this regression. The original clock must
                    // return its partial observation before this legal tail.
                    tokio::select! {
                        _=release_rx=>{},
                        _=tokio::time::sleep_until(tokio::time::Instant::from_std(deadline+Duration::from_millis(100)))=>{
                            peer.write_all(&terminal[sent..]).await?;
                            peer.flush().await?;
                            response_wire.extend_from_slice(&terminal[sent..]);
                            // Dropping the receiver after this tail is harmless;
                            // the fixture has no unjoined background producer.
                        }
                    }
                } else {
                    // Keep the socket open: oversize/sequence rejection must
                    // not be an EOF/truncate disguised as a parser refusal.
                    let _=release_rx.await;
                }
                Ok::<_,anyhow::Error>((response_wire,metadata_wire_bytes,format!("{:x}",metadata_digest.finalize())))
            }).await.context("scripted peer watchdog exceeded")?
        });
        let observed = (async {
            let mut stream = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                AsyncMysqlStream::connect("root", port, BUDGET),
            )
            .await
            .context("scripted connect absolute deadline")??;
            let handshake_elapsed = started.elapsed();
            let (ready_tx, _ready_rx) = tokio::sync::oneshot::channel();
            let observation = stream
                .observe_bounded_contract_error(SQL, ready_tx, deadline)
                .await;
            drop(stream);
            Ok::<_, anyhow::Error>((observation, handshake_elapsed))
        })
        .await;
        let _ = release_tx.send(());
        let joined = peer.await.context("scripted peer actual join")?;
        // Propagate failures only after the peer task has actually exited.
        let (response_wire, metadata_wire_bytes, metadata_sha256) = joined?;
        let (observed, handshake_elapsed) = observed?;
        Ok(ScriptResult {
            observed,
            response_wire,
            metadata_wire_bytes,
            metadata_sha256,
            handshake_elapsed,
        })
    }
    #[tokio::test]
    async fn consumed_handshake_budget_preserves_partial_header_and_body_at_original_deadline() {
        for mode in [Mode::HeaderPrefix, Mode::BodyPrefix] {
            let result = script(mode, Duration::from_millis(300))
                .await
                .expect("joined scripted peer");
            assert!(result.handshake_elapsed >= Duration::from_millis(300));
            let observed = &result.observed;
            assert_eq!(
                observed.text.error.as_deref(),
                Some("absolute negative query deadline exceeded")
            );
            assert!(observed.completed_metadata);
            assert!(observed.terminal.is_none());
            let pending = observed
                .pending_packet
                .as_ref()
                .expect("actual unfinished packet");
            assert_eq!(pending.phase, "terminal");
            let count = match mode {
                Mode::HeaderPrefix => 2,
                _ => 7,
            };
            assert_eq!(
                observed.text.wire_bytes as usize,
                result.metadata_wire_bytes + count
            );
            assert_eq!(
                result.response_wire.len(),
                result.metadata_wire_bytes + count,
                "late tail must not become the successful fixture outcome"
            );
            assert_eq!(
                observed.text.wire_prefix_sha256,
                format!("{:x}", Sha256::digest(&result.response_wire))
            );
            let expected = &result.response_wire[result.metadata_wire_bytes..];
            assert_eq!(
                pending.header_hex,
                bounded_packet_hex(&expected[..count.min(4)])
            );
            assert_eq!(pending.received_payload_bytes, count.saturating_sub(4));
            assert_eq!(
                pending.payload_prefix_hex,
                bounded_packet_hex(&expected[count.min(4)..])
            );
            match mode {
                Mode::HeaderPrefix => {
                    assert_eq!(pending.expected_payload_bytes, None);
                    assert_eq!(observed.text.packets, 3);
                }
                _ => {
                    assert_eq!(
                        pending.expected_payload_bytes,
                        Some(error_payload(1105).len())
                    );
                    assert_eq!(observed.text.packets, 4);
                }
            }
        }
    }
    #[tokio::test]
    async fn announced_4097_bytes_is_rejected_while_peer_is_open_and_has_sent_no_payload() {
        let result = script(Mode::Oversize, Duration::ZERO)
            .await
            .expect("joined scripted peer");
        let observed = &result.observed;
        assert!(
            observed
                .text
                .error
                .as_deref()
                .is_some_and(|e| e.contains("negative packet exceeds 4 KiB cap"))
        );
        let pending = observed.pending_packet.as_ref().expect("oversize header");
        assert_eq!(pending.expected_payload_bytes, Some(4097));
        assert_eq!(pending.received_payload_bytes, 0);
        assert!(pending.payload_prefix_hex.is_empty());
        assert_eq!(pending.header_hex, "01100004");
        assert_eq!(
            observed.text.wire_bytes as usize,
            result.metadata_wire_bytes + 4
        );
        assert_eq!(
            observed.text.wire_prefix_sha256,
            format!("{:x}", Sha256::digest(&result.response_wire))
        );
        assert!(observed.terminal.is_none());
    }
}

#[cfg(test)]
#[path = "mysql_stream_owned_until_tests.rs"]
mod owned_until_tests;
