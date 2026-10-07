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

use byteorder::{ByteOrder, LittleEndian};
use std::io;
use std::io::prelude::*;
use std::io::IoSlice;

use crate::U24_MAX;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// The writer of mysql packet.
/// - behaves as a sync writer, while build the packet
///   so that trivial async writes could be avoided
/// - behaves like a async writer, while writing data to the output stream
pub struct PacketWriter<W> {
    packet_builder: PacketBuilder,
    output_stream: Option<W>,
    limits: crate::ProtocolLimits,
    pending_io: bool,
    response_timeout: Option<std::time::Duration>,
    response_deadline: Option<tokio::time::Instant>,
}

// exports the internal builder as sync Write
impl<W> Write for PacketWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.is_poisoned() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "legacy MySQL IO is poisoned",
            ));
        }
        if self.is_detached() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "MySQL writer is detached",
            ));
        }
        if !buf.is_empty() {
            self.start_response()?;
        }
        self.packet_builder.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.packet_builder.flush()
    }
}

impl<W> PacketWriter<W> {
    pub fn with_limits(output_stream: W, limits: crate::ProtocolLimits) -> Self {
        Self {
            packet_builder: PacketBuilder::new(limits.row_bytes),
            output_stream: Some(output_stream),
            limits,
            pending_io: false,
            response_timeout: None,
            response_deadline: None,
        }
    }
    pub(crate) fn set_response_timeout(&mut self, timeout: Option<std::time::Duration>) {
        self.response_timeout = timeout;
    }
    pub(crate) fn reset_response_deadline(&mut self) {
        self.response_deadline = None;
    }
    fn start_response(&mut self) -> io::Result<()> {
        if self.response_deadline.is_none() {
            self.response_deadline = self
                .response_timeout
                .map(|timeout| tokio::time::Instant::now() + timeout);
        }
        if self
            .response_deadline
            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MySQL response write deadline expired",
            ));
        }
        Ok(())
    }
    pub(crate) fn is_poisoned(&self) -> bool {
        self.pending_io
    }
    pub(crate) fn is_detached(&self) -> bool {
        self.output_stream.is_none()
    }
    pub(crate) fn detach(&mut self) -> io::Result<W> {
        if self.is_poisoned() || !self.packet_builder.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "legacy packet has unpublished bytes",
            ));
        }
        self.output_stream
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "MySQL writer is detached"))
    }
    pub(crate) fn restore(&mut self, io: W, sequence: u8) -> io::Result<()> {
        if self.is_poisoned() || !self.is_detached() || !self.packet_builder.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid MySQL writer restore boundary",
            ));
        }
        self.output_stream = Some(io);
        // The owned writer completed its separately timed response before restoration.
        self.reset_response_deadline();
        self.set_seq(sequence);
        Ok(())
    }
    pub fn limits(&self) -> crate::ProtocolLimits {
        self.limits
    }
    pub fn next_sequence(&self) -> u8 {
        self.packet_builder.seq()
    }
    pub fn set_seq(&mut self, seq: u8) {
        self.packet_builder.set_seq(seq)
    }
}

const PACKET_HEADER_SIZE: usize = 4;
impl<W: AsyncWrite + Unpin> PacketWriter<W> {
    /// Build packet(s) and write them to the output stream
    pub async fn end_packet(&mut self) -> io::Result<()> {
        if self.is_poisoned() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "legacy MySQL IO is poisoned",
            ));
        }
        self.start_response()?;
        let deadline = self.response_deadline;
        let output_stream = self.output_stream.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "MySQL writer is detached")
        })?;
        let builder = &mut self.packet_builder;
        if !builder.is_empty() {
            // Cancellation/error leaves this latch set; legacy IO cannot resume.
            self.pending_io = true;
            let raw_packet = builder.take_buffer();

            let write = async {
                // split the rww buffer at the boundary of size U24_MAX
                let chunks = raw_packet.chunks(U24_MAX);
                let mut header = [0; PACKET_HEADER_SIZE];
                for chunk in chunks {
                    // prepare the header
                    LittleEndian::write_u24(&mut header, chunk.len() as u32);
                    header[3] = builder.seq();
                    builder.increase_seq();

                    // write out the header and payload.
                    //
                    // depends on the AsyncWrite provided, this may trigger
                    // real system call or not (for example, if AsyncWrite is buffered stream)
                    let written = output_stream
                        .write_vectored(&[IoSlice::new(&header), IoSlice::new(chunk)])
                        .await?;

                    // if write buffer is not drained, fall back to write_all
                    if written > PACKET_HEADER_SIZE + chunk.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "socket reported invalid write length",
                        ));
                    }
                    if written != PACKET_HEADER_SIZE + chunk.len() {
                        if written == 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "MySQL socket accepted zero bytes",
                            ));
                        }
                        if written < PACKET_HEADER_SIZE {
                            output_stream.write_all(&header[written..]).await?;
                        }
                        let payload_written = written.saturating_sub(PACKET_HEADER_SIZE);
                        output_stream.write_all(&chunk[payload_written..]).await?
                    }
                }
                if raw_packet.len().is_multiple_of(U24_MAX) {
                    let header = [0, 0, 0, builder.seq()];
                    builder.increase_seq();
                    output_stream.write_all(&header).await?;
                }
                Ok::<_, io::Error>(())
            };
            match deadline {
                Some(deadline) => {
                    tokio::time::timeout_at(deadline, write)
                        .await
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::TimedOut,
                                "MySQL response write deadline expired",
                            )
                        })??
                }
                None => write.await?,
            }
            if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "MySQL response write deadline expired",
                ));
            }
            self.pending_io = false;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn flush_all(&mut self) -> io::Result<()> {
        if self.is_poisoned() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "legacy MySQL IO is poisoned",
            ));
        }
        self.start_response()?;
        let deadline = self.response_deadline;
        let io = self.output_stream.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "MySQL writer is detached")
        })?;
        self.pending_io = true;
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, io.flush())
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "MySQL response write deadline expired",
                    )
                })??,
            None => io.flush().await?,
        }
        if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MySQL response write deadline expired",
            ));
        }
        self.pending_io = false;
        Ok(())
    }
}

// Builder that exports as sync `Write`, so that  trivial scattered async writes
// could be avoided during constructing the packet, especially the writes in mod [writers]
struct PacketBuilder {
    buffer: Vec<u8>,
    seq: u8,
    limit: usize,
}

impl Write for PacketBuilder {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Here we take them all, and split them into raw packets later in `end_packet` if the size
        // of buffer is larger than max payload size (16MB)
        let total = self
            .buffer
            .len()
            .checked_add(buf.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "legacy MySQL packet exceeds limit",
                )
            })?;
        self.buffer
            .try_reserve_exact(total.saturating_sub(self.buffer.len()))
            .map_err(|_| {
                io::Error::new(io::ErrorKind::OutOfMemory, "MySQL output allocation failed")
            })?;
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl PacketBuilder {
    pub fn new(limit: usize) -> Self {
        PacketBuilder {
            buffer: vec![],
            seq: 0,
            limit,
        }
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn take_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }

    fn set_seq(&mut self, seq: u8) {
        self.seq = seq;
    }

    fn increase_seq(&mut self) {
        self.seq = self.seq.wrapping_add(1);
    }

    fn seq(&self) -> u8 {
        self.seq
    }
}

/// A bounded adapter for the legacy binary-row staging API.
pub(crate) struct BoundedVecWriter<'a> {
    bytes: &'a mut Vec<u8>,
    limit: usize,
}
impl<'a> BoundedVecWriter<'a> {
    pub fn new(bytes: &'a mut Vec<u8>, limit: usize) -> Self {
        Self { bytes, limit }
    }
}
impl Write for BoundedVecWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let total = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "binary MySQL row exceeds limit")
            })?;
        self.bytes
            .try_reserve_exact(total - self.bytes.len())
            .map_err(|_| {
                io::Error::new(io::ErrorKind::OutOfMemory, "MySQL output allocation failed")
            })?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
