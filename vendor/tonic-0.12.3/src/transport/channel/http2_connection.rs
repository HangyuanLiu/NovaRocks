//! Fresh owned HTTP/2 builder settings for one actual connection attempt.

use hyper::client::conn::http2::Builder;
use std::{io, sync::Arc};

pub(crate) type Http2ConnectionFactory =
    Arc<dyn Fn() -> Result<Http2ConnectionConfig, crate::Error> + Send + Sync>;

/// Owned configuration returned afresh for one physical connection attempt.
///
/// A factory is shared by Endpoint clones; these once-bound pools are not.
/// Obtain their original allocation grants before constructing them. This
/// carrier neither funds socket/TLS/task/queue metadata nor proves a complete
/// connection budget or deadline. Default options preserve existing settings.
#[derive(Debug, Default)]
pub struct Http2ConnectionConfig {
    /// Local maximum inbound frame payload. This is not an outbound ceiling.
    pub max_frame_size: Option<u32>,
    /// Local maximum inbound decoded header list; overrides the Endpoint value.
    pub max_header_list_size: Option<u32>,
    /// Complete HPACK block and literal allocation precheck limit.
    pub max_receive_header_block_size: Option<usize>,
    /// Advertised incoming HPACK table size; the pre-ACK table starts at 4096.
    pub header_table_size: Option<u32>,
    /// Local outbound HPACK table ceiling; zero disables dynamic storage.
    pub max_send_header_table_size: Option<u32>,
    /// Fresh fixed whole HPACK block; requires explicit zero outbound table cap.
    pub send_header_block_pool: Option<h2::SendHeaderBlockPool>,
    /// Connection-wide count of buffered DATA/header/trailer events.
    pub max_receive_buffered_events: Option<usize>,
    /// Maximum per-stream outbound DATA write buffering.
    pub max_send_buffer_size: Option<usize>,
    /// Keep original DATA objects through successful upstream flush.
    pub retain_data_payloads: bool,
    /// Fresh fixed escaped DATA backing, requiring an explicit event count.
    pub receive_buffer_pool: Option<h2::ReceiveBufferPool>,
    /// Fresh fixed raw frame input, independent of decoded frame copies.
    pub receive_frame_buffer: Option<h2::ReceiveFrameBuffer>,
    /// Fresh original encoded input, requiring fixed raw input and block maximum.
    pub receive_header_block_buffer: Option<h2::ReceiveHeaderBlockBuffer>,
    /// Fresh fixed decoded field backing, requiring raw/encoded input and a
    /// block maximum. HeaderMap storage and dynamic tables remain separate.
    pub receive_header_field_pool: Option<h2::ReceiveHeaderFieldPool>,
    /// Fresh fixed incoming HPACK slots, requiring the decoded field pool.
    /// Capacity covers the initial table and the advertised incoming limit.
    pub receive_header_table_buffer: Option<h2::ReceiveHeaderTableBuffer>,
    /// Fresh fixed outbound storage and local frame cap; HPACK is separate.
    pub send_frame_buffer: Option<h2::SendFrameBuffer>,
    /// Fresh independent GOAWAY debug backing through the last error alias.
    pub receive_goaway_buffer_pool: Option<h2::ReceiveBufferPool>,
}

impl Http2ConnectionConfig {
    pub(crate) fn apply<E: Clone>(
        self,
        builder: &mut Builder<E>,
        inherited_max_header_list_size: Option<u32>,
    ) -> io::Result<()> {
        if self.send_header_block_pool.is_some() && self.max_send_header_table_size != Some(0) {
            return Err(invalid(
                "per-connection send header block pool requires explicit max_send_header_table_size(0)",
            ));
        }
        if let Some(buffer) = &self.receive_header_block_buffer {
            if self.receive_frame_buffer.is_none()
                || self
                    .max_receive_header_block_size
                    .is_none_or(|max| max == 0 || max > buffer.max_encoded_bytes())
            {
                return Err(invalid("per-connection encoded header buffer requires fixed raw input and a valid explicit block maximum"));
            }
        }
        let field_header_limit = if let Some(pool) = &self.receive_header_field_pool {
            // Hyper's client default is 16 KiB. Endpoint settings are already
            // installed on the base builder; an attempt override takes priority.
            let max = self
                .max_header_list_size
                .or(inherited_max_header_list_size)
                .unwrap_or(16384);
            if self.receive_frame_buffer.is_none()
                || self.receive_header_block_buffer.is_none()
                || self.max_receive_header_block_size.is_none()
                || max < 32
                || pool.max_field_bytes() < (max as usize - 32)
            {
                return Err(invalid("per-connection header field pool requires fixed raw/encoded input, an explicit block maximum and sufficient decoded field capacity"));
            }
            Some(max)
        } else {
            None
        };
        if let Some(buffer) = &self.receive_header_table_buffer {
            if self.receive_header_field_pool.is_none()
                || self.header_table_size.unwrap_or(4096) as usize > buffer.max_table_bytes()
            {
                return Err(invalid("per-connection header table buffer requires a decoded field pool and fitting advertised incoming table size"));
            }
        }
        let max_frame = self.max_frame_size.unwrap_or(16384) as usize;
        if !(16384..=16777215).contains(&max_frame) {
            return Err(invalid("invalid per-connection HTTP/2 frame maximum"));
        }
        if self.max_receive_buffered_events == Some(0) {
            return Err(invalid(
                "per-connection HTTP/2 event count must be positive",
            ));
        }
        if let Some(max) = self.max_receive_header_block_size {
            if max == 0 || max > u32::MAX as usize {
                return Err(invalid(
                    "invalid per-connection HTTP/2 header block maximum",
                ));
            }
        }
        if self
            .max_send_buffer_size
            .is_some_and(|max| max > u32::MAX as usize)
        {
            return Err(invalid("invalid per-connection HTTP/2 send buffer maximum"));
        }
        if self.receive_buffer_pool.is_some() && self.max_receive_buffered_events.is_none() {
            return Err(invalid("per-connection DATA pool requires an event count"));
        }
        for pool in [
            self.receive_buffer_pool.as_ref(),
            self.receive_goaway_buffer_pool.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if max_frame > pool.buffer_capacity_bytes() {
                return Err(invalid(
                    "per-connection frame maximum exceeds pool capacity",
                ));
            }
        }
        if self
            .receive_frame_buffer
            .as_ref()
            .is_some_and(|raw| max_frame > raw.max_payload_bytes())
        {
            return Err(invalid(
                "per-connection frame maximum exceeds raw input capacity",
            ));
        }
        // Validate every scalar/geometry before changing the builder or calling
        // the connector. Reuse is checked by h2 bind before its handshake I/O.
        if let Some(max) = self.max_frame_size {
            builder.max_frame_size(max);
        }
        if let Some(max) = field_header_limit.or(self.max_header_list_size) {
            builder.max_header_list_size(max);
        }
        if let Some(max) = self.max_receive_header_block_size {
            builder.max_receive_header_block_size(max);
        }
        if let Some(size) = self.header_table_size {
            builder.header_table_size(size);
        }
        if let Some(max) = self.max_send_header_table_size {
            builder.max_send_header_table_size(max);
        }
        if let Some(pool) = self.send_header_block_pool {
            builder.send_header_block_pool(pool);
        }
        if let Some(max) = self.max_receive_buffered_events {
            builder.max_receive_buffered_events(max);
        }
        if let Some(max) = self.max_send_buffer_size {
            builder.max_send_buf_size(max);
        }
        builder.retain_data_payloads(self.retain_data_payloads);
        if let Some(pool) = self.receive_buffer_pool {
            builder.receive_buffer_pool(pool);
        }
        if let Some(buffer) = self.receive_header_block_buffer {
            builder.receive_header_block_buffer(buffer);
        }
        if let Some(pool) = self.receive_header_field_pool {
            builder.receive_header_field_pool(pool);
        }
        if let Some(buffer) = self.receive_header_table_buffer {
            builder.receive_header_table_buffer(buffer);
        }
        if let Some(raw) = self.receive_frame_buffer {
            builder.receive_frame_buffer(raw);
        }
        if let Some(buffer) = self.send_frame_buffer {
            builder.send_frame_buffer(buffer);
        }
        if let Some(pool) = self.receive_goaway_buffer_pool {
            builder.receive_goaway_buffer_pool(pool);
        }
        Ok(())
    }
}

fn invalid(detail: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, detail)
}
