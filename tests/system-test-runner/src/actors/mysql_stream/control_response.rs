// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! A single bounded protocol-41 control response on the original MySQL socket.
use super::{AsyncMysqlStream, BoundedMysqlError, bounded_packet_hex};
use anyhow::{Result, bail, ensure};
use std::{fmt, time::Instant};
use tokio::io::AsyncReadExt;

const PAYLOAD_LIMIT: usize = 4096;

/// An ERR remains an error verdict, distinct from a successfully parsed OK.
#[derive(Debug)]
pub enum BoundedCommandResponse {
    Ok(BoundedCommandOk),
    Error(BoundedCommandError),
}
#[derive(Debug)]
pub struct BoundedCommandOk {
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status_flags: u16,
    pub warnings: u16,
    /// Untracked protocol-41 OK information, after the fixed status fields.
    pub info: String,
    pub original_payload: Vec<u8>,
}
#[derive(Debug)]
pub struct BoundedCommandError {
    pub error: BoundedMysqlError,
    pub original_payload: Vec<u8>,
}

/// Actual observed bytes and original cause. This is no framing-resume right.
/// Presentation never formats an arbitrary cause or the received payload.
pub struct CommandResponseFailure {
    pub header: [u8; 4],
    pub header_received: usize,
    pub expected_payload_bytes: Option<usize>,
    pub payload_prefix: Vec<u8>,
    pub actual_cause: anyhow::Error,
}
impl CommandResponseFailure {
    pub fn original_deadline_expired(&self) -> bool {
        self.actual_cause.is::<tokio::time::error::Elapsed>()
            || self.actual_cause.is::<OriginalCommandDeadlineExpired>()
    }
}
#[derive(Debug)]
struct OriginalCommandDeadlineExpired {
    phase: &'static str,
}
impl fmt::Display for OriginalCommandDeadlineExpired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "original command deadline expired: {}", self.phase)
    }
}
impl std::error::Error for OriginalCommandDeadlineExpired {}
fn on_time(deadline: Instant, phase: &'static str) -> Result<()> {
    if Instant::now() >= deadline {
        return Err(anyhow::Error::new(OriginalCommandDeadlineExpired { phase }));
    }
    Ok(())
}
impl fmt::Debug for CommandResponseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandResponseFailure")
            .field("header_received", &self.header_received)
            .field("expected_payload_bytes", &self.expected_payload_bytes)
            .field("payload_received", &self.payload_prefix.len())
            .field("actual_cause_retained", &true)
            .finish()
    }
}
impl fmt::Display for CommandResponseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded original MySQL command response failed")
    }
}
// Keep the raw cause in the owned object, without generic chain formatting.
impl std::error::Error for CommandResponseFailure {}

#[derive(Default)]
struct Partial {
    header: [u8; 4],
    header_received: usize,
    expected_payload_bytes: Option<usize>,
    payload: Vec<u8>,
}
impl Partial {
    fn failure(self, actual_cause: anyhow::Error) -> anyhow::Error {
        anyhow::Error::new(CommandResponseFailure {
            header: self.header,
            header_received: self.header_received,
            expected_payload_bytes: self.expected_payload_bytes,
            payload_prefix: self.payload,
            actual_cause,
        })
    }
}

impl AsyncMysqlStream {
    /// Read one complete OK or ERR under one caller-owned absolute deadline.
    /// This API supports control SQL only (COM_QUERY payload <= 4 KiB). It
    /// advertises no SESSION_TRACK, DEPRECATE_EOF, cursor or multi-result mode.
    /// A malformed response, timeout or cancelled borrowed call leaves the
    /// original socket with an unknown cursor. The caller must drop that socket
    /// rather than issue another command; Drop is not a server/role join fact.
    pub async fn command_response_until(
        &mut self,
        sql: &str,
        original_deadline: Instant,
    ) -> Result<BoundedCommandResponse> {
        let mut observed = Partial::default();
        // Check before constructing/polling send, even when tokio would poll a
        // ready inner future at an expired timeout_at deadline.
        if let Err(cause) = on_time(original_deadline, "before-send") {
            return Err(observed.failure(cause));
        }
        let result = tokio::time::timeout_at(original_deadline.into(), async {
            ensure!(
                !sql.is_empty() && sql.len() < PAYLOAD_LIMIT,
                "bounded control SQL payload exceeds its limit"
            );
            on_time(original_deadline, "before-send")?;
            self.send_query(sql).await?;
            while observed.header_received < observed.header.len() {
                on_time(original_deadline, "header")?;
                let count = self
                    .stream
                    .read(&mut observed.header[observed.header_received..])
                    .await?;
                ensure!(count != 0, "original command response header is truncated");
                observed.header_received += count;
            }
            let length = usize::from(observed.header[0])
                | (usize::from(observed.header[1]) << 8)
                | (usize::from(observed.header[2]) << 16);
            observed.expected_payload_bytes = Some(length);
            ensure!(
                observed.header[3] == 1,
                "original command response sequence differs"
            );
            ensure!(
                (1..=PAYLOAD_LIMIT).contains(&length),
                "original command response exceeds its 4 KiB limit"
            );
            // Check the header before allocating or reading any response body.
            observed.payload.reserve_exact(length);
            let mut scratch = [0u8; PAYLOAD_LIMIT];
            while observed.payload.len() < length {
                on_time(original_deadline, "payload")?;
                let remaining = length - observed.payload.len();
                let count = self.stream.read(&mut scratch[..remaining]).await?;
                ensure!(count != 0, "original command response payload is truncated");
                observed.payload.extend_from_slice(&scratch[..count]);
            }
            let mut response = decode(&observed.payload)?;
            on_time(original_deadline, "complete-response")?;
            let original_payload = std::mem::take(&mut observed.payload);
            match &mut response {
                BoundedCommandResponse::Ok(value) => value.original_payload = original_payload,
                BoundedCommandResponse::Error(value) => value.original_payload = original_payload,
            }
            Ok::<_, anyhow::Error>(response)
        })
        .await;
        match result {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(cause)) => Err(observed.failure(cause)),
            Err(cause) => Err(observed.failure(cause.into())),
        }
    }
}

fn lenenc(payload: &[u8], cursor: &mut usize) -> Result<u64> {
    let first = *payload
        .get(*cursor)
        .ok_or_else(|| anyhow::anyhow!("truncated OK integer"))?;
    *cursor += 1;
    let (width, minimum) = match first {
        0..=250 => return Ok(u64::from(first)),
        252 => (2, 251),
        253 => (3, 65_536),
        254 => (8, 16_777_216),
        _ => bail!("OK integer is not a non-null unsigned length encoding"),
    };
    let bytes = payload
        .get(*cursor..*cursor + width)
        .ok_or_else(|| anyhow::anyhow!("truncated OK integer payload"))?;
    let mut expanded = [0u8; 8];
    expanded[..width].copy_from_slice(bytes);
    *cursor += width;
    let value = u64::from_le_bytes(expanded);
    ensure!(
        value >= minimum,
        "OK integer has noncanonical length encoding"
    );
    Ok(value)
}
fn decode(payload: &[u8]) -> Result<BoundedCommandResponse> {
    ensure!(
        (1..=PAYLOAD_LIMIT).contains(&payload.len()),
        "command response payload bound differs"
    );
    match payload[0] {
        0 => {
            let mut cursor = 1;
            let affected_rows = lenenc(payload, &mut cursor)?;
            let last_insert_id = lenenc(payload, &mut cursor)?;
            let fields = payload
                .get(cursor..cursor + 4)
                .ok_or_else(|| anyhow::anyhow!("OK protocol-41 status fields are truncated"))?;
            let status_flags = u16::from_le_bytes([fields[0], fields[1]]);
            let warnings = u16::from_le_bytes([fields[2], fields[3]]);
            // Reserved bits, MORE_RESULTS, CURSOR_EXISTS and SESSION_TRACK
            // cannot be a terminal control response in this actor's negotiated mode.
            ensure!(
                status_flags & (0x8004 | 0x0008 | 0x0040 | 0x4000) == 0,
                "OK has unsupported or nonterminal status flags"
            );
            let info = std::str::from_utf8(&payload[cursor + 4..])?.to_owned();
            Ok(BoundedCommandResponse::Ok(BoundedCommandOk {
                affected_rows,
                last_insert_id,
                status_flags,
                warnings,
                info,
                original_payload: Vec::new(),
            }))
        }
        0xff => {
            ensure!(
                payload.len() >= 9 && payload[3] == b'#',
                "ERR lacks complete protocol-41 fields"
            );
            let code = u16::from_le_bytes([payload[1], payload[2]]);
            ensure!(code != 0, "ERR code cannot be zero");
            let state = &payload[4..9];
            ensure!(
                state
                    .iter()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                    && state != b"00000",
                "ERR SQLSTATE is invalid"
            );
            Ok(BoundedCommandResponse::Error(BoundedCommandError {
                error: BoundedMysqlError {
                    sequence: 1,
                    payload_bytes: payload.len(),
                    code,
                    sqlstate: std::str::from_utf8(state)?.to_owned(),
                    message: std::str::from_utf8(&payload[9..])?.to_owned(),
                    payload_hex: bounded_packet_hex(payload),
                },
                original_payload: Vec::new(),
            }))
        }
        _ => bail!("control command did not return a terminal OK or ERR"),
    }
}

#[cfg(test)]
#[path = "control_response_tests.rs"]
mod tests;
