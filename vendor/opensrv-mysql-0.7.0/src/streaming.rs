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

//! Carrier-neutral streaming MySQL framing. No Arrow, query, or Native capability.
use crate::{ErrorKind, ProtocolLimits, U24_MAX};
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WritePhase {
    Boundary,
    Row,
    Metadata,
    Terminal,
    Ended,
    Poisoned,
}

/// Committed progress, updated in the same poll that accepts socket bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FramingCursor {
    pub phase: WritePhase,
    pub sequence: u8,
    pub logical_total: u32,
    pub logical_written: u32,
    pub packet_payload_length: u32,
    pub packet_payload_written: u32,
    pub header: [u8; 4],
    pub header_written: u8,
    pub zero_terminal_pending: bool,
    pub committed_wire_bytes: u64,
    /// Rows whose complete logical payload and required zero terminal reached IO.
    /// This local counter disambiguates coalesced rows even when u8 sequences wrap.
    pub rows_completed: u64,
}
impl FramingCursor {
    fn new(sequence: u8) -> Self {
        Self {
            phase: WritePhase::Boundary,
            sequence,
            logical_total: 0,
            logical_written: 0,
            packet_payload_length: 0,
            packet_payload_written: 0,
            header: [0; 4],
            header_written: 0,
            zero_terminal_pending: false,
            committed_wire_bytes: 0,
            rows_completed: 0,
        }
    }
    fn packet(&mut self, length: usize) {
        self.packet_payload_length = length as u32;
        self.packet_payload_written = 0;
        self.header_written = 0;
        self.header = [
            (length & 255) as u8,
            ((length >> 8) & 255) as u8,
            ((length >> 16) & 255) as u8,
            self.sequence,
        ];
    }
    pub fn row_has_started(&self) -> bool {
        self.phase == WritePhase::Row && (self.logical_written != 0 || self.header_written != 0)
    }
    pub fn logical_remaining(&self) -> usize {
        (self.logical_total - self.logical_written) as usize
    }
}

/// Exact backing, including bytes outside the exposed range. The adapter may
/// transfer an existing Arc without copying, or cover a bounded compaction first.
#[derive(Clone, Debug)]
pub struct ResidentTailPart {
    backing: Arc<[u8]>,
    start: usize,
    end: usize,
}
impl ResidentTailPart {
    pub fn new(backing: Arc<[u8]>, range: std::ops::Range<usize>) -> io::Result<Self> {
        if range.start > range.end || range.end > backing.len() {
            return Err(invalid("invalid resident tail range"));
        }
        Ok(Self {
            backing,
            start: range.start,
            end: range.end,
        })
    }
    pub fn backing_bytes(&self) -> usize {
        self.backing.len()
    }
    pub fn bytes(&self) -> &[u8] {
        &self.backing[self.start..self.end]
    }
}

/// Pre-encoded metadata packets. All bytes and the packet index are retained by
/// one response writer. There is no planner callback during metadata completion.
#[derive(Debug)]
pub struct FrozenMetadata {
    packets: Vec<Arc<[u8]>>,
    index: usize,
    offset: usize,
    limits: ProtocolLimits,
    backing_bytes: usize,
}
impl FrozenMetadata {
    /// Validate a previously protected metadata owner, including unused index
    /// capacity and each complete Arc allocation, before accepting ownership.
    pub fn new(packets: Vec<Arc<[u8]>>, limits: ProtocolLimits) -> io::Result<Self> {
        let limits = limits.validate()?;
        if packets.is_empty() || packets.capacity() > limits.columns + 2 {
            return Err(invalid("metadata packet count exceeds limit"));
        }
        let mut backing_bytes = packets
            .capacity()
            .checked_mul(std::mem::size_of::<Arc<[u8]>>())
            .ok_or_else(|| invalid("metadata index overflow"))?;
        for packet in &packets {
            backing_bytes = Self::checked_packet_bytes(backing_bytes, packet, limits)?;
        }
        Ok(Self {
            packets,
            index: 0,
            offset: 0,
            limits,
            backing_bytes,
        })
    }
    /// Reserve one bounded index before encoding any packet. Use try_push for
    /// growth-before-check prevention; packet construction remains caller-owned.
    pub fn builder(limits: ProtocolLimits) -> io::Result<Self> {
        let limits = limits.validate()?;
        let count = limits.columns + 2;
        let backing_bytes = count * std::mem::size_of::<Arc<[u8]>>();
        if backing_bytes > limits.metadata_bytes {
            return Err(invalid("metadata index exceeds byte limit"));
        }
        Ok(Self {
            packets: Vec::with_capacity(count),
            index: 0,
            offset: 0,
            limits,
            backing_bytes,
        })
    }
    fn checked_packet_bytes(
        total: usize,
        packet: &[u8],
        limits: ProtocolLimits,
    ) -> io::Result<usize> {
        if packet.is_empty() || packet.len() >= U24_MAX {
            return Err(invalid("invalid metadata packet length"));
        }
        // Arc's two reference counters and each wire header are covered in
        // addition to the exact slice backing and the separately counted index.
        total
            .checked_add(packet.len())
            .and_then(|n| n.checked_add(2 * std::mem::size_of::<usize>() + 4))
            .filter(|n| *n <= limits.metadata_bytes)
            .ok_or_else(|| invalid("metadata bytes exceed limit"))
    }
    pub fn try_push(&mut self, packet: Arc<[u8]>) -> io::Result<()> {
        if self.packets.len() >= self.limits.columns + 2
            || self.packets.len() == self.packets.capacity()
        {
            return Err(invalid("metadata packet count exceeds limit"));
        }
        let bytes = Self::checked_packet_bytes(self.backing_bytes, &packet, self.limits)?;
        self.packets.push(packet);
        self.backing_bytes = bytes;
        Ok(())
    }
    /// Check both budgets before allocating a new immutable packet backing.
    pub fn try_push_bytes(&mut self, packet: &[u8]) -> io::Result<()> {
        if self.packets.len() >= self.limits.columns + 2
            || self.packets.len() == self.packets.capacity()
        {
            return Err(invalid("metadata packet count exceeds limit"));
        }
        let bytes = Self::checked_packet_bytes(self.backing_bytes, packet, self.limits)?;
        self.packets.push(Arc::from(packet));
        self.backing_bytes = bytes;
        Ok(())
    }
    pub fn backing_bytes(&self) -> usize {
        self.backing_bytes
    }
    pub fn packet_count(&self) -> usize {
        self.packets.len()
    }
}

/// Exclusive IO owner with a fixed coalescing buffer. Slices are accepted only
/// while a declared row is active; full logical rows are never reconstructed.
/// Dropping a flush future preserves its actual progress in this owner.
pub struct OwnedStreamingMysqlWriter<W> {
    io: W,
    limits: ProtocolLimits,
    cursor: FramingCursor,
    buffer: Box<[u8]>,
    used: usize,
    sent: usize,
    metadata: Option<FrozenMetadata>,
    queued_rows: std::collections::VecDeque<u32>,
    coalesced_rows: bool,
}
impl<W> OwnedStreamingMysqlWriter<W> {
    pub fn new(io: W, limits: ProtocolLimits, sequence: u8) -> io::Result<Self> {
        let limits = limits.validate()?;
        Ok(Self {
            io,
            limits,
            cursor: FramingCursor::new(sequence),
            buffer: vec![0; limits.coalescing_bytes].into_boxed_slice(),
            used: 0,
            sent: 0,
            metadata: None,
            queued_rows: std::collections::VecDeque::with_capacity(1024),
            coalesced_rows: false,
        })
    }
    pub fn receipt(&self) -> FramingCursor {
        self.cursor
    }
    pub fn buffer_capacity(&self) -> usize {
        self.buffer.len()
    }
    pub fn buffered_bytes(&self) -> usize {
        self.used - self.sent
    }
    pub fn buffered_row_bytes(&self) -> usize {
        self.buffered_bytes().min(self.cursor.logical_remaining())
    }
    /// Coalesce complete small rows into the same fixed buffer. The bounded
    /// row-length index tracks actual packet boundaries and permits cancel-cut
    /// to discard every later, uncommitted row without sending its header.
    /// False means flush first; no shared queue or hidden allocation is created.
    pub fn queue_small_row(&mut self, bytes: &[u8]) -> io::Result<bool> {
        if bytes.is_empty()
            || bytes.len() > self.limits.row_bytes
            || bytes.len() > self.buffer.len()
        {
            return Err(invalid("small row exceeds coalescing limit"));
        }
        if self.metadata.is_some()
            || (self.cursor.phase != WritePhase::Boundary && !self.coalesced_rows)
        {
            return Err(invalid("cannot coalesce into a streaming row"));
        }
        if self.sent != 0 {
            self.buffer.copy_within(self.sent..self.used, 0);
            self.used -= self.sent;
            self.sent = 0;
        }
        if bytes.len() > self.buffer.len() - self.used || self.queued_rows.len() >= 1024 {
            return Ok(false);
        }
        if self.cursor.phase == WritePhase::Boundary {
            self.start_row(bytes.len() as u32)?;
            self.coalesced_rows = true;
        } else {
            self.queued_rows.push_back(bytes.len() as u32);
        }
        self.buffer[self.used..self.used + bytes.len()].copy_from_slice(bytes);
        self.used += bytes.len();
        Ok(true)
    }
    pub fn poison(&mut self) {
        self.cursor.phase = WritePhase::Poisoned;
    }
    pub fn into_inner(self) -> W {
        self.io
    }
    pub fn start_row(&mut self, total: u32) -> io::Result<()> {
        self.start_payload(WritePhase::Row, total as usize, self.limits.row_bytes)
    }
    fn start_payload(&mut self, phase: WritePhase, total: usize, limit: usize) -> io::Result<()> {
        if self.cursor.phase != WritePhase::Boundary
            || self.buffered_bytes() != 0
            || (self.metadata.is_some() && phase != WritePhase::Metadata)
        {
            return Err(invalid("MySQL response is not at a packet boundary"));
        }
        if total == 0 || total > limit {
            return Err(invalid("MySQL logical payload exceeds limit"));
        }
        self.cursor.phase = phase;
        self.cursor.logical_total = total as u32;
        self.cursor.logical_written = 0;
        self.cursor.zero_terminal_pending = total.is_multiple_of(U24_MAX);
        self.cursor.packet(total.min(U24_MAX));
        Ok(())
    }
    /// Copies only as many bytes as fit the fixed buffer. Caller retains any
    /// unaccepted suffix and resumes after flush_pending. The returned count is
    /// buffer acceptance, not socket delivery: only receipt() records delivery.
    /// No implicit queue.
    pub fn push_slice(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !matches!(self.cursor.phase, WritePhase::Row | WritePhase::Terminal) {
            return Err(invalid("no active MySQL payload"));
        }
        if self.coalesced_rows {
            return Err(invalid("cannot append a slice to coalesced rows"));
        }
        if bytes.len() > self.cursor.logical_remaining() - self.buffered_bytes() {
            self.poison();
            return Err(invalid("slice exceeds declared logical payload"));
        }
        if self.sent != 0 {
            self.buffer.copy_within(self.sent..self.used, 0);
            self.used -= self.sent;
            self.sent = 0;
        }
        let count = bytes.len().min(self.buffer.len() - self.used);
        self.buffer[self.used..self.used + count].copy_from_slice(&bytes[..count]);
        self.used += count;
        Ok(count)
    }
    pub fn start_terminal(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() > self.limits.diagnostic_bytes + 9 {
            return Err(invalid("terminal packet exceeds limit"));
        }
        self.start_payload(
            WritePhase::Terminal,
            payload.len(),
            self.limits.diagnostic_bytes + 9,
        )?;
        let n = self.push_slice(payload)?;
        if n != payload.len() {
            self.poison();
            return Err(invalid("terminal does not fit coalescing buffer"));
        }
        Ok(())
    }
    pub fn start_metadata(&mut self, metadata: FrozenMetadata) -> io::Result<()> {
        if self.metadata.is_some() {
            return Err(invalid("metadata is already active"));
        }
        // Revalidate against this writer's exact profile before publication.
        let metadata = FrozenMetadata::new(metadata.packets, self.limits)?;
        self.start_payload(
            WritePhase::Metadata,
            metadata.packets[0].len(),
            self.limits.metadata_bytes,
        )?;
        self.metadata = Some(metadata);
        Ok(())
    }
    /// The tail excludes already accepted bytes in the fixed coalescer. Its
    /// length is an exact coverage assertion by the adapter, not a fetch request.
    // Preserve the exclusive IO owner on rejection without allocating an error box.
    #[allow(clippy::result_large_err)]
    pub fn into_closing(
        mut self,
        tail: Vec<ResidentTailPart>,
        kind: ErrorKind,
        message: &[u8],
    ) -> Result<ClosingMysqlWriter<W>, (Self, io::Error)> {
        // A prepared header with no committed byte is still a legal boundary.
        // Drop unsent rows, including any later row in the coalescer, at cut.
        if self.cursor.phase == WritePhase::Row && !self.cursor.row_has_started() {
            self.cursor.phase = WritePhase::Boundary;
            self.used = 0;
            self.sent = 0;
        }
        if self.cursor.phase == WritePhase::Row {
            self.used = self.sent + self.buffered_row_bytes();
        }
        self.queued_rows.clear();
        self.coalesced_rows = false;
        let checked = (|| {
            if tail.len() > 2 || tail.capacity() > 2 || message.len() > self.limits.diagnostic_bytes
            {
                return Err(invalid("closing payload exceeds limit"));
            }
            let mut bytes = 0usize;
            let mut backings = 0usize;
            for part in &tail {
                bytes = bytes
                    .checked_add(part.bytes().len())
                    .ok_or_else(|| invalid("closing coverage overflow"))?;
                backings = backings
                    .checked_add(part.backing_bytes())
                    .ok_or_else(|| invalid("closing backing overflow"))?;
            }
            if backings > 4 * 1024 * 1024 {
                return Err(invalid("closing backing exceeds limit"));
            }
            let expected = match self.cursor.phase {
                WritePhase::Row => self.cursor.logical_remaining() - self.buffered_bytes(),
                WritePhase::Boundary | WritePhase::Metadata => 0,
                _ => return Err(invalid("cannot close this MySQL protocol state")),
            };
            if bytes != expected {
                return Err(invalid("current row tail is not fully resident"));
            }
            Ok(())
        })();
        if let Err(error) = checked {
            self.poison();
            return Err((self, error));
        }
        let mut error_payload = Vec::with_capacity(message.len() + 9);
        error_payload.push(0xff);
        error_payload.extend_from_slice(&(kind as u16).to_le_bytes());
        error_payload.push(b'#');
        error_payload.extend_from_slice(kind.sqlstate());
        error_payload.extend_from_slice(message);
        Ok(ClosingMysqlWriter {
            writer: self,
            tail,
            error_payload,
        })
    }
}
impl<W: AsyncWrite + Unpin> OwnedStreamingMysqlWriter<W> {
    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut quantum = 64 * 1024usize;
        loop {
            if self.cursor.phase == WritePhase::Poisoned {
                return Poll::Ready(Err(invalid("poisoned MySQL writer")));
            }
            if !matches!(
                self.cursor.phase,
                WritePhase::Row | WritePhase::Metadata | WritePhase::Terminal
            ) {
                return Poll::Ready(Ok(()));
            }
            let zero = self.cursor.packet_payload_length == 0;
            if self.buffered_bytes() == 0 && !zero {
                self.used = 0;
                self.sent = 0;
                return Poll::Ready(Ok(()));
            }
            if quantum == 0 {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let header = &self.cursor.header[self.cursor.header_written as usize..];
            let payload_len = ((self.cursor.packet_payload_length
                - self.cursor.packet_payload_written) as usize)
                .min(self.buffered_bytes())
                .min(quantum);
            let payload = &self.buffer[self.sent..self.sent + payload_len];
            let mut headers = [[0u8; 4]; 15];
            let mut following = 0usize;
            let mut offered = header.len() + payload.len();
            if self.coalesced_rows && self.io.is_write_vectored() {
                for (i, &len) in self.queued_rows.iter().take(15).enumerate() {
                    if offered + 4 + len as usize > quantum {
                        break;
                    }
                    headers[i] = [
                        (len & 255) as u8,
                        ((len >> 8) & 255) as u8,
                        ((len >> 16) & 255) as u8,
                        self.cursor.sequence.wrapping_add(i as u8 + 1),
                    ];
                    offered += 4 + len as usize;
                    following += 1;
                }
            }
            let written = if self.io.is_write_vectored() {
                let mut slices = [IoSlice::new(&[]); 32];
                slices[0] = IoSlice::new(header);
                slices[1] = IoSlice::new(payload);
                let mut offset = self.sent + payload_len;
                for (i, &len) in self.queued_rows.iter().take(following).enumerate() {
                    slices[2 + i * 2] = IoSlice::new(&headers[i]);
                    slices[3 + i * 2] = IoSlice::new(&self.buffer[offset..offset + len as usize]);
                    offset += len as usize;
                }
                Pin::new(&mut self.io).poll_write_vectored(cx, &slices[..2 + following * 2])
            } else if !header.is_empty() {
                offered = header.len();
                Pin::new(&mut self.io).poll_write(cx, header)
            } else {
                offered = payload.len();
                Pin::new(&mut self.io).poll_write(cx, payload)
            };
            let mut n = match written {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.poison();
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(0)) => {
                    self.poison();
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "MySQL socket accepted zero bytes",
                    )));
                }
                Poll::Ready(Ok(n)) if n <= offered => n,
                Poll::Ready(Ok(_)) => {
                    self.poison();
                    return Poll::Ready(Err(invalid("socket reported invalid write length")));
                }
            };
            quantum = quantum.saturating_sub(n);
            while n != 0 {
                let header_n = n.min(4 - self.cursor.header_written as usize);
                n -= header_n;
                let payload_n = n.min(
                    (self.cursor.packet_payload_length - self.cursor.packet_payload_written)
                        as usize,
                );
                n -= payload_n;
                self.cursor.header_written += header_n as u8;
                self.cursor.packet_payload_written += payload_n as u32;
                self.cursor.logical_written += payload_n as u32;
                self.cursor.committed_wire_bytes += (header_n + payload_n) as u64;
                self.sent += payload_n;
                if self.cursor.header_written == 4
                    && self.cursor.packet_payload_written == self.cursor.packet_payload_length
                {
                    self.cursor.sequence = self.cursor.sequence.wrapping_add(1);
                    let remaining = self.cursor.logical_remaining();
                    if remaining != 0 {
                        self.cursor.packet(remaining.min(U24_MAX));
                    } else if self.cursor.zero_terminal_pending && !zero {
                        self.cursor.zero_terminal_pending = false;
                        self.cursor.packet(0);
                    } else {
                        if self.cursor.phase == WritePhase::Row {
                            self.cursor.rows_completed += 1;
                        }
                        if self.cursor.phase == WritePhase::Row && self.coalesced_rows {
                            if let Some(total) = self.queued_rows.pop_front() {
                                self.cursor.logical_total = total;
                                self.cursor.logical_written = 0;
                                self.cursor.zero_terminal_pending = false;
                                self.cursor.packet(total as usize);
                                continue;
                            }
                            self.coalesced_rows = false;
                        }
                        self.cursor.phase = if self.cursor.phase == WritePhase::Terminal {
                            WritePhase::Ended
                        } else {
                            WritePhase::Boundary
                        };
                        self.used = 0;
                        self.sent = 0;
                        return Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }
    /// Write one terminal payload through the same fixed scratch and receipt.
    pub async fn write_terminal(&mut self, payload: &[u8]) -> io::Result<()> {
        self.start_payload(
            WritePhase::Terminal,
            payload.len(),
            self.limits.diagnostic_bytes + 9,
        )?;
        self.write_slice(payload).await
    }
    pub async fn flush_pending(&mut self) -> io::Result<()> {
        std::future::poll_fn(|cx| self.poll_pending(cx)).await
    }
    pub async fn write_slice(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            let n = self.push_slice(bytes)?;
            bytes = &bytes[n..];
            self.flush_pending().await?;
        }
        Ok(())
    }
    pub async fn finish_metadata(&mut self) -> io::Result<()> {
        while let Some(metadata) = self.metadata.as_mut() {
            let index = metadata.index;
            let offset = metadata.offset;
            let remaining = &metadata.packets[index][offset..];
            let count = remaining.len().min(self.buffer.len() - self.used);
            // Metadata has a protected independent backing. Copying into the
            // fixed coalescer never grows that backing or loses the source cursor.
            let start = self.used;
            self.buffer[start..start + count].copy_from_slice(&remaining[..count]);
            self.used += count;
            metadata.offset += count;
            self.flush_pending().await?;
            let metadata = self.metadata.as_mut().unwrap();
            if metadata.offset == metadata.packets[index].len()
                && self.cursor.phase == WritePhase::Boundary
            {
                metadata.index += 1;
                metadata.offset = 0;
                if metadata.index == metadata.packets.len() {
                    self.metadata = None;
                    break;
                }
                let len = metadata.packets[metadata.index].len();
                self.start_payload(WritePhase::Metadata, len, self.limits.metadata_bytes)?;
            }
        }
        Ok(())
    }
    pub async fn flush_socket(&mut self) -> io::Result<()> {
        if self.cursor.phase == WritePhase::Poisoned {
            return Err(invalid("poisoned MySQL writer"));
        }
        if let Err(error) = self.io.flush().await {
            self.poison();
            return Err(error);
        }
        Ok(())
    }
}

/// This owner can only consume its frozen metadata/current row and emit ERR.
/// The adapter retains its closing position until this owner and all IO aliases
/// actually exit. There is deliberately no automatic completion in Drop.
pub struct ClosingMysqlWriter<W> {
    writer: OwnedStreamingMysqlWriter<W>,
    tail: Vec<ResidentTailPart>,
    error_payload: Vec<u8>,
}
impl<W> ClosingMysqlWriter<W> {
    pub fn receipt(&self) -> FramingCursor {
        self.writer.receipt()
    }
    pub fn into_inner(self) -> W {
        self.writer.into_inner()
    }
}
impl<W: AsyncWrite + Unpin> ClosingMysqlWriter<W> {
    pub async fn finish(mut self) -> io::Result<W> {
        self.writer.finish_metadata().await?;
        self.writer.flush_pending().await?;
        for part in &self.tail {
            self.writer.write_slice(part.bytes()).await?;
        }
        if self.writer.cursor.phase != WritePhase::Boundary {
            self.writer.poison();
            return Err(invalid("closing row is incomplete"));
        }
        self.writer.start_payload(
            WritePhase::Terminal,
            self.error_payload.len(),
            self.writer.limits.diagnostic_bytes + 9,
        )?;
        self.writer.write_slice(&self.error_payload).await?;
        self.writer.flush_socket().await?;
        Ok(self.writer.into_inner())
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// A temporary loan of the response slot with exclusive ownership of its IO.
/// The slot remains Detached until a complete terminal packet and socket flush
/// succeed. Moving into closing releases the loan, without restoring the slot.
/// No borrowed query/session/slot capability escapes into the closing owner.
pub struct StreamingResponseLease<'a, W> {
    slot: &'a mut crate::packet_writer::PacketWriter<W>,
    owned: OwnedStreamingMysqlWriter<W>,
    is_bin: bool,
    capabilities: crate::CapabilityFlags,
}
impl<'a, W> StreamingResponseLease<'a, W> {
    pub(crate) fn new(
        slot: &'a mut crate::packet_writer::PacketWriter<W>,
        owned: OwnedStreamingMysqlWriter<W>,
        is_bin: bool,
        capabilities: crate::CapabilityFlags,
    ) -> Self {
        Self {
            slot,
            owned,
            is_bin,
            capabilities,
        }
    }
    pub fn writer(&mut self) -> &mut OwnedStreamingMysqlWriter<W> {
        &mut self.owned
    }
    pub fn receipt(&self) -> FramingCursor {
        self.owned.receipt()
    }
    pub fn client_capabilities(&self) -> crate::CapabilityFlags {
        self.capabilities
    }
    #[allow(clippy::result_large_err)]
    pub fn into_closing(
        self,
        tail: Vec<ResidentTailPart>,
        kind: ErrorKind,
        message: &[u8],
    ) -> Result<ClosingMysqlWriter<W>, (OwnedStreamingMysqlWriter<W>, io::Error)> {
        self.owned.into_closing(tail, kind, message)
    }
}
impl<'a, W: AsyncWrite + Unpin> StreamingResponseLease<'a, W> {
    /// Flush a completed terminal response before returning the exact next
    /// sequence to the legacy command slot. On error the slot stays Detached.
    pub async fn restore(mut self) -> io::Result<crate::QueryResultWriter<'a, W>> {
        if self.owned.cursor.phase != WritePhase::Ended
            || self.owned.metadata.is_some()
            || self.owned.buffered_bytes() != 0
        {
            self.owned.poison();
            return Err(invalid("streaming response has not ended"));
        }
        self.owned.flush_socket().await?;
        let sequence = self.owned.receipt().sequence;
        self.slot.restore(self.owned.into_inner(), sequence)?;
        Ok(crate::QueryResultWriter::new(
            self.slot,
            self.is_bin,
            self.capabilities,
        ))
    }
    pub async fn finish(
        mut self,
        terminal_payload: &[u8],
    ) -> io::Result<crate::QueryResultWriter<'a, W>> {
        self.owned.write_terminal(terminal_payload).await?;
        self.restore().await
    }
}
