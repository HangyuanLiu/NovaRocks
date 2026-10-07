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

use crate::{
    ErrorKind, FrozenMetadata, OwnedStreamingMysqlWriter, ProtocolLimits, ResidentTailPart,
    WritePhase, U24_MAX,
};
use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

#[derive(Default)]
struct State {
    bytes: Vec<u8>,
    budget: Option<usize>,
    zero: bool,
    fail: bool,
    calls: usize,
    flush_error: bool,
    flush_blocked: bool,
}
#[derive(Clone, Default)]
struct Socket(Arc<Mutex<State>>);
impl Socket {
    fn write(&self, bufs: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        let mut state = self.0.lock().unwrap();
        if state.fail {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected socket failure",
            )));
        }
        if state.zero {
            return Poll::Ready(Ok(0));
        }
        if state.budget == Some(0) {
            return Poll::Pending;
        }
        let mut remaining = state.budget.unwrap_or(usize::MAX);
        let mut written = 0;
        state.calls += 1;
        for buf in bufs {
            let n = buf.len().min(remaining);
            state.bytes.extend_from_slice(&buf[..n]);
            written += n;
            remaining -= n;
            if remaining == 0 {
                break;
            }
        }
        if state.budget.is_some() {
            state.budget = Some(remaining);
        }
        Poll::Ready(Ok(written))
    }
}
impl AsyncWrite for Socket {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.write(&[IoSlice::new(bytes)])
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.write(bytes)
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let state = self.0.lock().unwrap();
        if state.flush_error {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected flush error",
            )))
        } else if state.flush_blocked {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
async fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx))).await
}
fn packets(bytes: &[u8]) -> Vec<(u8, &[u8])> {
    let mut rest = bytes;
    let mut result = Vec::new();
    while !rest.is_empty() {
        let len = usize::from(rest[0]) | usize::from(rest[1]) << 8 | usize::from(rest[2]) << 16;
        result.push((rest[3], &rest[4..4 + len]));
        rest = &rest[4 + len..];
    }
    result
}

#[tokio::test]
async fn slices_cross_u24_and_exact_multiples_have_zero_terminal() {
    for total in [U24_MAX - 1, U24_MAX, U24_MAX + 1, 2 * U24_MAX] {
        let socket = Socket::default();
        let evidence = socket.clone();
        let mut writer =
            OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 255).unwrap();
        writer.start_row(total as u32).unwrap();
        let chunk = [0x61u8; 32749];
        let mut remaining = total;
        while remaining != 0 {
            let n = remaining.min(chunk.len());
            writer.write_slice(&chunk[..n]).await.unwrap();
            remaining -= n;
            assert_eq!(writer.buffer_capacity(), 65536);
        }
        assert_eq!(writer.receipt().phase, WritePhase::Boundary);
        let state = evidence.0.lock().unwrap();
        let frames = packets(&state.bytes);
        assert_eq!(frames.iter().map(|(_, b)| b.len()).sum::<usize>(), total);
        for (index, (seq, bytes)) in frames.iter().enumerate() {
            assert_eq!(*seq, 255u8.wrapping_add(index as u8));
            assert!(bytes.iter().all(|v| *v == 0x61));
        }
        if total.is_multiple_of(U24_MAX) {
            assert!(frames.last().unwrap().1.is_empty());
        }
    }
}

#[tokio::test]
async fn actual_partial_header_and_payload_survive_closing_handoff() {
    for cut in [1, 2, 3, 4, 5, 6] {
        let socket = Socket::default();
        let evidence = socket.clone();
        evidence.0.lock().unwrap().budget = Some(cut);
        let mut writer =
            OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 255).unwrap();
        writer.start_row(3).unwrap();
        assert_eq!(writer.push_slice(b"abc").unwrap(), 3);
        {
            let mut flush = Box::pin(writer.flush_pending());
            assert!(poll_once(&mut flush).await.is_pending());
        }
        let receipt = writer.receipt();
        assert_eq!(receipt.committed_wire_bytes, cut as u64);
        assert_eq!(receipt.header_written, cut.min(4) as u8);
        assert!(receipt.row_has_started());
        let closing = writer
            .into_closing(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
            .map_err(|(_, e)| e)
            .unwrap();
        evidence.0.lock().unwrap().budget = None;
        closing.finish().await.unwrap();
        let state = evidence.0.lock().unwrap();
        let frames = packets(&state.bytes);
        assert_eq!(frames[0], (255, b"abc".as_slice()));
        assert_eq!(frames[1].0, 0);
        assert_eq!(frames[1].1[0], 0xff);
    }
}

#[tokio::test]
async fn missing_tail_poison_and_exact_resident_tail_complete() {
    let socket = Socket::default();
    let evidence = socket.clone();
    evidence.0.lock().unwrap().budget = Some(1);
    let mut writer = OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 1).unwrap();
    writer.start_row(4).unwrap();
    writer.push_slice(b"a").unwrap();
    {
        let mut f = Box::pin(writer.flush_pending());
        assert!(poll_once(&mut f).await.is_pending());
    }
    let (writer, error) = writer
        .into_closing(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
        .err()
        .unwrap();
    assert_eq!(writer.receipt().phase, WritePhase::Poisoned);
    assert!(error.to_string().contains("resident"));

    let socket = Socket::default();
    let evidence = socket.clone();
    evidence.0.lock().unwrap().budget = Some(1);
    let mut writer = OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 1).unwrap();
    writer.start_row(4).unwrap();
    writer.push_slice(b"a").unwrap();
    {
        let mut f = Box::pin(writer.flush_pending());
        assert!(poll_once(&mut f).await.is_pending());
    }
    let part = ResidentTailPart::new(Arc::from(b"bcde".as_slice()), 0..3).unwrap();
    let closing = writer
        .into_closing(vec![part], ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
        .map_err(|(_, e)| e)
        .unwrap();
    evidence.0.lock().unwrap().budget = None;
    closing.finish().await.unwrap();
    assert_eq!(packets(&evidence.0.lock().unwrap().bytes)[0].1, b"abcd");
}

#[tokio::test]
async fn frozen_metadata_partial_write_finishes_before_err() {
    let socket = Socket::default();
    let evidence = socket.clone();
    evidence.0.lock().unwrap().budget = Some(2);
    let mut writer = OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 1).unwrap();
    writer
        .start_metadata(
            FrozenMetadata::new(
                vec![
                    Arc::from(b"a".as_slice()),
                    Arc::from(b"col".as_slice()),
                    Arc::from(b"end".as_slice()),
                ],
                ProtocolLimits::default(),
            )
            .unwrap(),
        )
        .unwrap();
    {
        let mut f = Box::pin(writer.finish_metadata());
        assert!(poll_once(&mut f).await.is_pending());
    }
    assert_eq!(writer.receipt().phase, WritePhase::Metadata);
    assert_eq!(writer.receipt().header_written, 2);
    let closing = writer
        .into_closing(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
        .map_err(|(_, e)| e)
        .unwrap();
    evidence.0.lock().unwrap().budget = None;
    closing.finish().await.unwrap();
    let state = evidence.0.lock().unwrap();
    let frames = packets(&state.bytes);
    assert_eq!(
        frames.iter().map(|p| p.1).collect::<Vec<_>>()[..3],
        [b"a".as_slice(), b"col", b"end"]
    );
    assert_eq!(frames[3].1[0], 0xff);
}

#[tokio::test]
async fn coalescing_batches_writev_and_discards_later_rows_at_cut() {
    let socket = Socket::default();
    let evidence = socket.clone();
    let mut writer = OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 1).unwrap();
    for row in [b"aa", b"bb", b"cc"] {
        assert!(writer.queue_small_row(row).unwrap());
    }
    writer.flush_pending().await.unwrap();
    assert_eq!(evidence.0.lock().unwrap().calls, 1);
    assert_eq!(packets(&evidence.0.lock().unwrap().bytes).len(), 3);
    let socket = Socket::default();
    let evidence = socket.clone();
    evidence.0.lock().unwrap().budget = Some(5);
    let mut writer = OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 1).unwrap();
    for row in [b"aa", b"bb", b"cc"] {
        writer.queue_small_row(row).unwrap();
    }
    {
        let mut f = Box::pin(writer.flush_pending());
        assert!(poll_once(&mut f).await.is_pending());
    }
    let closing = writer
        .into_closing(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
        .map_err(|(_, e)| e)
        .unwrap();
    evidence.0.lock().unwrap().budget = None;
    closing.finish().await.unwrap();
    let state = evidence.0.lock().unwrap();
    let frames = packets(&state.bytes);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].1, b"aa");
    assert_eq!(frames[1].1[0], 0xff);
}

#[tokio::test]
async fn write_zero_and_io_error_poison_exact_progress() {
    for zero in [true, false] {
        let socket = Socket::default();
        let evidence = socket.clone();
        {
            let mut state = evidence.0.lock().unwrap();
            state.zero = zero;
            state.fail = !zero;
        }
        let mut writer =
            OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 1).unwrap();
        writer.start_row(1).unwrap();
        writer.push_slice(b"a").unwrap();
        assert!(writer.flush_pending().await.is_err());
        assert_eq!(writer.receipt().phase, WritePhase::Poisoned);
        assert_eq!(writer.receipt().committed_wire_bytes, 0);
    }
}

#[test]
fn limits_reject_growth_and_whole_metadata_backing() {
    let limits = ProtocolLimits::default();
    let mut writer = OwnedStreamingMysqlWriter::new(Socket::default(), limits, 1).unwrap();
    assert!(writer.start_row(0).is_err());
    assert!(writer.start_row(limits.row_bytes as u32 + 1).is_err());
    writer.start_row(1).unwrap();
    assert!(writer.push_slice(b"too long").is_err());
    assert_eq!(writer.receipt().phase, WritePhase::Poisoned);
    assert!(FrozenMetadata::new(
        vec![Arc::from(vec![0u8; limits.metadata_bytes + 1])],
        limits
    )
    .is_err());
}

#[tokio::test]
async fn unsent_row_is_withdrawn_without_emitting_header() {
    let socket = Socket::default();
    let evidence = socket.clone();
    let mut writer = OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 1).unwrap();
    writer.start_row(64 * 1024 * 1024).unwrap();
    writer.push_slice(b"a").unwrap();
    let closing = writer
        .into_closing(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
        .map_err(|(_, e)| e)
        .unwrap();
    closing.finish().await.unwrap();
    let state = evidence.0.lock().unwrap();
    let frames = packets(&state.bytes);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].1[0], 0xff);
}

#[tokio::test]
async fn every_coalesced_writev_cut_preserves_only_started_rows() {
    for cut in 1..18 {
        let socket = Socket::default();
        let evidence = socket.clone();
        evidence.0.lock().unwrap().budget = Some(cut);
        let mut writer =
            OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 254).unwrap();
        for row in [b"aa", b"bb", b"cc"] {
            writer.queue_small_row(row).unwrap();
        }
        {
            let mut f = Box::pin(writer.flush_pending());
            assert!(poll_once(&mut f).await.is_pending());
        }
        assert_eq!(writer.receipt().committed_wire_bytes, cut as u64);
        assert_eq!(writer.receipt().rows_completed, (cut / 6) as u64);
        let closing = writer
            .into_closing(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
            .map_err(|(_, e)| e)
            .unwrap();
        evidence.0.lock().unwrap().budget = None;
        closing.finish().await.unwrap();
        let state = evidence.0.lock().unwrap();
        let frames = packets(&state.bytes);
        let rows = cut.div_ceil(6);
        assert_eq!(frames.len(), rows + 1);
        for (index, row) in [b"aa", b"bb", b"cc"].iter().take(rows).enumerate() {
            assert_eq!(
                frames[index],
                (254u8.wrapping_add(index as u8), row.as_slice())
            );
        }
        assert_eq!(frames[rows].1[0], 0xff);
        assert_eq!(frames[rows].0, 254u8.wrapping_add(rows as u8));
    }
}

#[tokio::test]
async fn zero_and_errors_preserve_previously_committed_header_bytes() {
    for zero in [true, false] {
        let socket = Socket::default();
        let evidence = socket.clone();
        evidence.0.lock().unwrap().budget = Some(2);
        let mut writer =
            OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 1).unwrap();
        writer.start_row(2).unwrap();
        writer.push_slice(b"ab").unwrap();
        {
            let mut f = Box::pin(writer.flush_pending());
            assert!(poll_once(&mut f).await.is_pending());
        }
        {
            let mut state = evidence.0.lock().unwrap();
            state.zero = zero;
            state.fail = !zero;
        }
        assert!(writer.flush_pending().await.is_err());
        assert_eq!(writer.receipt().phase, WritePhase::Poisoned);
        assert_eq!(writer.receipt().header_written, 2);
        assert_eq!(writer.receipt().committed_wire_bytes, 2);
    }
}

#[test]
fn metadata_index_and_full_tail_backings_are_bounded_before_growth() {
    let limits = ProtocolLimits {
        columns: 2,
        metadata_bytes: 128,
        ..ProtocolLimits::default()
    };
    let mut metadata = FrozenMetadata::builder(limits).unwrap();
    assert_eq!(
        metadata.backing_bytes(),
        4 * std::mem::size_of::<Arc<[u8]>>()
    );
    metadata.try_push_bytes(b"a").unwrap();
    let before = metadata.backing_bytes();
    assert!(metadata.try_push_bytes(&[0; 64]).is_err());
    assert_eq!(metadata.backing_bytes(), before);
    assert_eq!(metadata.packet_count(), 1);
    let oversized_index = Vec::<Arc<[u8]>>::with_capacity(limits.columns + 3);
    assert!(FrozenMetadata::new(oversized_index, limits).is_err());
    assert!(ResidentTailPart::new(Arc::from(b"a".as_slice()), usize::MAX..usize::MAX).is_err());

    let mut writer =
        OwnedStreamingMysqlWriter::new(Socket::default(), ProtocolLimits::default(), 1).unwrap();
    writer
        .start_metadata(
            FrozenMetadata::new(vec![Arc::from(b"a".as_slice())], ProtocolLimits::default())
                .unwrap(),
        )
        .unwrap();
    assert!(writer.push_slice(b"a").is_err());
    assert!(writer.queue_small_row(b"a").is_err());
    assert!(writer.start_row(1).is_err());
    assert!(writer.start_terminal(b"a").is_err());
}

#[tokio::test]
async fn tail_range_cannot_hide_oversized_owned_allocation() {
    let socket = Socket::default();
    let evidence = socket.clone();
    evidence.0.lock().unwrap().budget = Some(1);
    let mut writer = OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 1).unwrap();
    writer.start_row(2).unwrap();
    writer.push_slice(b"a").unwrap();
    {
        let mut f = Box::pin(writer.flush_pending());
        assert!(poll_once(&mut f).await.is_pending());
    }
    let part = ResidentTailPart::new(Arc::from(vec![b'b'; 4 * 1024 * 1024 + 1]), 0..1).unwrap();
    let (writer, error) = writer
        .into_closing(vec![part], ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
        .err()
        .unwrap();
    assert_eq!(writer.receipt().phase, WritePhase::Poisoned);
    assert!(error.to_string().contains("backing"));
}

struct ScalarSocket(Socket);
impl AsyncWrite for ScalarSocket {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.write(&[IoSlice::new(bytes)])
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn scalar_socket_small_buffer_streams_row_and_closing_diagnostic() {
    let socket = Socket::default();
    let evidence = socket.clone();
    let limits = ProtocolLimits {
        coalescing_bytes: 3,
        ..ProtocolLimits::default()
    };
    let mut writer = OwnedStreamingMysqlWriter::new(ScalarSocket(socket), limits, 255).unwrap();
    writer.start_row(7).unwrap();
    writer.write_slice(b"abcdefg").await.unwrap();
    let closing = writer
        .into_closing(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"diagnostic")
        .map_err(|(_, e)| e)
        .unwrap();
    closing.finish().await.unwrap();
    let state = evidence.0.lock().unwrap();
    let frames = packets(&state.bytes);
    assert_eq!(frames[0], (255, b"abcdefg".as_slice()));
    assert_eq!(frames[1].0, 0);
    assert_eq!(frames[1].1[0], 0xff);
}

#[tokio::test]
async fn lease_restores_only_after_terminal_and_exact_next_sequence() {
    let socket = Socket::default();
    let evidence = socket.clone();
    let mut slot =
        crate::packet_writer::PacketWriter::with_limits(socket, ProtocolLimits::default());
    slot.set_seq(255);
    let results = crate::QueryResultWriter::new(&mut slot, false, crate::CapabilityFlags::empty());
    let mut lease = results.into_streaming().map_err(|(_, e)| e).unwrap();
    lease.writer().start_row(2).unwrap();
    lease.writer().write_slice(b"ab").await.unwrap();
    lease
        .finish(&[0xfe, 0, 0, 0, 0])
        .await
        .unwrap()
        .no_more_results()
        .await
        .unwrap();
    assert!(!slot.is_detached());
    assert_eq!(slot.next_sequence(), 1);
    // The next command's sequence is independent of the previous response.
    slot.set_seq(1);
    let results = crate::QueryResultWriter::new(&mut slot, false, crate::CapabilityFlags::empty());
    let lease = results.into_streaming().map_err(|(_, e)| e).unwrap();
    assert_eq!(lease.receipt().sequence, 1);
    lease
        .finish(&[0xff, 1, 0, b'#', b'H', b'Y', b'0', b'0', b'0'])
        .await
        .unwrap()
        .no_more_results()
        .await
        .unwrap();
    assert_eq!(
        packets(&evidence.0.lock().unwrap().bytes)
            .iter()
            .map(|p| p.0)
            .collect::<Vec<_>>(),
        [255, 0, 1]
    );
}

#[tokio::test]
async fn lease_refuses_unpublished_legacy_bytes_and_pending_finalizer() {
    let mut slot = crate::packet_writer::PacketWriter::with_limits(
        Socket::default(),
        ProtocolLimits::default(),
    );
    std::io::Write::write_all(&mut slot, b"pending").unwrap();
    let results = crate::QueryResultWriter::new(&mut slot, false, crate::CapabilityFlags::empty());
    assert!(results.into_streaming().is_err());
    assert!(!slot.is_detached());
    slot.end_packet().await.unwrap();
    let results = crate::QueryResultWriter::new(&mut slot, false, crate::CapabilityFlags::empty())
        .complete_one(crate::OkResponse::default())
        .await
        .unwrap();
    assert!(results.into_streaming().is_err());
    assert!(!slot.is_detached());
}

#[tokio::test]
async fn cancelled_lease_cannot_reenter_legacy_slot() {
    let socket = Socket::default();
    let evidence = socket.clone();
    evidence.0.lock().unwrap().budget = Some(1);
    let mut slot =
        crate::packet_writer::PacketWriter::with_limits(socket, ProtocolLimits::default());
    let mut lease =
        crate::QueryResultWriter::new(&mut slot, false, crate::CapabilityFlags::empty())
            .into_streaming()
            .map_err(|(_, e)| e)
            .unwrap();
    lease.writer().start_row(1).unwrap();
    lease.writer().push_slice(b"a").unwrap();
    {
        let mut f = Box::pin(lease.writer().flush_pending());
        assert!(poll_once(&mut f).await.is_pending());
    }
    let closing = lease
        .into_closing(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
        .map_err(|(_, e)| e)
        .unwrap();
    assert!(slot.is_detached());
    assert!(
        crate::QueryResultWriter::new(&mut slot, false, crate::CapabilityFlags::empty())
            .into_streaming()
            .is_err()
    );
    evidence.0.lock().unwrap().budget = None;
    closing.finish().await.unwrap();
    assert!(slot.is_detached());
    assert!(slot.flush_all().await.is_err());
}

struct CommandReader {
    wire: std::io::Cursor<Vec<u8>>,
    consumed: Arc<Mutex<usize>>,
}
impl tokio::io::AsyncRead for CommandReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let n = std::io::Read::read(&mut self.wire, buf.initialize_unfilled())?;
        buf.advance(n);
        *self.consumed.lock().unwrap() += n;
        Poll::Ready(Ok(()))
    }
}
struct LeaseShim {
    detached: bool,
    seen: Arc<Mutex<Vec<String>>>,
    closing: Arc<Mutex<Option<crate::ClosingMysqlWriter<Socket>>>>,
}
#[async_trait::async_trait]
impl crate::AsyncMysqlShim<Socket> for LeaseShim {
    type Error = io::Error;
    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        _: crate::StatementMetaWriter<'a, Socket>,
    ) -> io::Result<()> {
        unreachable!()
    }
    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: crate::ParamParser<'a>,
        _: crate::QueryResultWriter<'a, Socket>,
    ) -> io::Result<()> {
        unreachable!()
    }
    async fn on_close<'a>(&'a mut self, _: u32) {}
    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: crate::QueryResultWriter<'a, Socket>,
    ) -> io::Result<()> {
        self.seen.lock().unwrap().push(query.to_owned());
        let mut lease = results.into_streaming().map_err(|(_, e)| e)?;
        lease.writer().start_row(1)?;
        lease.writer().write_slice(b"a").await?;
        if self.detached {
            *self.closing.lock().unwrap() = Some(
                lease
                    .into_closing(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
                    .map_err(|(_, e)| e)?,
            );
        } else {
            lease
                .finish(&[0xfe, 0, 0, 0, 0])
                .await?
                .no_more_results()
                .await?;
        }
        Ok(())
    }
}
#[tokio::test]
async fn intermediary_detached_callback_never_reads_next_command() {
    for detached in [true, false] {
        let wire = vec![2, 0, 0, 0, 3, b'a', 2, 0, 0, 0, 3, b'b'];
        let consumed = Arc::new(Mutex::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let closing = Arc::new(Mutex::new(None));
        let socket = Socket::default();
        let evidence = socket.clone();
        let mi = crate::AsyncMysqlIntermediary {
            client_capabilities: crate::CapabilityFlags::empty(),
            process_use_statement_on_query: true,
            reject_connection_on_dbname_absence: false,
            shim: LeaseShim {
                detached,
                seen: seen.clone(),
                closing: closing.clone(),
            },
            reader: crate::packet_reader::PacketReader::new(CommandReader {
                wire: std::io::Cursor::new(wire),
                consumed: consumed.clone(),
            }),
            writer: crate::packet_writer::PacketWriter::with_limits(
                socket,
                ProtocolLimits::default(),
            ),
        };
        mi.run().await.unwrap();
        assert_eq!(*consumed.lock().unwrap(), if detached { 6 } else { 12 });
        assert_eq!(seen.lock().unwrap().len(), if detached { 1 } else { 2 });
        let finish = closing.lock().unwrap().take();
        if let Some(finish) = finish {
            finish.finish().await.unwrap();
        }
        assert_eq!(
            packets(&evidence.0.lock().unwrap().bytes)
                .iter()
                .map(|p| p.0)
                .collect::<Vec<_>>(),
            if detached {
                vec![1, 2]
            } else {
                vec![1, 2, 1, 2]
            }
        );
    }
}

#[tokio::test]
async fn unsuccessful_terminal_flush_never_restores_slot() {
    for cancelled in [true, false] {
        let socket = Socket::default();
        let evidence = socket.clone();
        {
            let mut state = evidence.0.lock().unwrap();
            state.flush_error = !cancelled;
            state.flush_blocked = cancelled;
        }
        let mut slot =
            crate::packet_writer::PacketWriter::with_limits(socket, ProtocolLimits::default());
        let lease =
            crate::QueryResultWriter::new(&mut slot, false, crate::CapabilityFlags::empty())
                .into_streaming()
                .map_err(|(_, e)| e)
                .unwrap();
        if cancelled {
            let mut f = Box::pin(lease.finish(&[0xfe, 0, 0, 0, 0]));
            assert!(poll_once(&mut f).await.is_pending());
        } else {
            assert!(lease.finish(&[0xfe, 0, 0, 0, 0]).await.is_err());
        }
        assert!(slot.is_detached());
        assert_eq!(packets(&evidence.0.lock().unwrap().bytes).len(), 1);
    }
    let mut slot = crate::packet_writer::PacketWriter::with_limits(
        Socket::default(),
        ProtocolLimits::default(),
    );
    let lease = crate::QueryResultWriter::new(&mut slot, false, crate::CapabilityFlags::empty())
        .into_streaming()
        .map_err(|(_, e)| e)
        .unwrap();
    assert!(lease.restore().await.is_err());
    assert!(slot.is_detached());
}

#[tokio::test]
async fn cancelled_legacy_send_poison_rejects_new_writes_and_detach() {
    let socket = Socket::default();
    let evidence = socket.clone();
    evidence.0.lock().unwrap().budget = Some(1);
    let mut slot =
        crate::packet_writer::PacketWriter::with_limits(socket, ProtocolLimits::default());
    std::io::Write::write_all(&mut slot, b"abc").unwrap();
    {
        let mut send = Box::pin(slot.end_packet());
        assert!(poll_once(&mut send).await.is_pending());
    }
    assert!(slot.is_poisoned());
    assert!(!slot.is_detached());
    assert_eq!(evidence.0.lock().unwrap().bytes.len(), 1);
    assert!(std::io::Write::write_all(&mut slot, b"later").is_err());
    assert!(slot.end_packet().await.is_err());
    assert!(slot.flush_all().await.is_err());
    assert!(
        crate::QueryResultWriter::new(&mut slot, false, crate::CapabilityFlags::empty())
            .into_streaming()
            .is_err()
    );
}
struct LegacyCancelShim {
    seen: Arc<Mutex<Vec<String>>>,
}
#[async_trait::async_trait]
impl crate::AsyncMysqlShim<Socket> for LegacyCancelShim {
    type Error = io::Error;
    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        _: crate::StatementMetaWriter<'a, Socket>,
    ) -> io::Result<()> {
        unreachable!()
    }
    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: crate::ParamParser<'a>,
        _: crate::QueryResultWriter<'a, Socket>,
    ) -> io::Result<()> {
        unreachable!()
    }
    async fn on_close<'a>(&'a mut self, _: u32) {}
    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: crate::QueryResultWriter<'a, Socket>,
    ) -> io::Result<()> {
        self.seen.lock().unwrap().push(query.to_owned());
        let mut response = Box::pin(results.completed(crate::OkResponse::default()));
        assert!(poll_once(&mut response).await.is_pending());
        // Even a shim swallowing cancellation must not permit another command.
        Ok(())
    }
}
#[tokio::test]
async fn intermediary_cancelled_legacy_callback_never_reads_next_command() {
    let consumed = Arc::new(Mutex::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let socket = Socket::default();
    let evidence = socket.clone();
    evidence.0.lock().unwrap().budget = Some(1);
    let mi = crate::AsyncMysqlIntermediary {
        client_capabilities: crate::CapabilityFlags::empty(),
        process_use_statement_on_query: true,
        reject_connection_on_dbname_absence: false,
        shim: LegacyCancelShim { seen: seen.clone() },
        reader: crate::packet_reader::PacketReader::new(CommandReader {
            wire: std::io::Cursor::new(vec![2, 0, 0, 0, 3, b'a', 2, 0, 0, 0, 3, b'b']),
            consumed: consumed.clone(),
        }),
        writer: crate::packet_writer::PacketWriter::with_limits(socket, ProtocolLimits::default()),
    };
    mi.run().await.unwrap();
    assert_eq!(*consumed.lock().unwrap(), 6);
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(evidence.0.lock().unwrap().bytes.len(), 1);
}
#[tokio::test]
async fn legacy_zero_error_and_cancelled_flush_permanently_poison_io() {
    for zero in [true, false] {
        let socket = Socket::default();
        let evidence = socket.clone();
        {
            let mut state = evidence.0.lock().unwrap();
            state.zero = zero;
            state.fail = !zero;
        }
        let mut slot =
            crate::packet_writer::PacketWriter::with_limits(socket, ProtocolLimits::default());
        std::io::Write::write_all(&mut slot, b"abc").unwrap();
        assert!(slot.end_packet().await.is_err());
        assert!(slot.is_poisoned());
        assert!(std::io::Write::write_all(&mut slot, b"later").is_err());
    }
    let socket = Socket::default();
    socket.0.lock().unwrap().flush_blocked = true;
    let mut slot =
        crate::packet_writer::PacketWriter::with_limits(socket, ProtocolLimits::default());
    {
        let mut flush = Box::pin(slot.flush_all());
        assert!(poll_once(&mut flush).await.is_pending());
    }
    assert!(slot.is_poisoned());
    assert!(slot.detach().is_err());
}

struct PreparedCallbackShim {
    column: crate::Column,
    executions: Arc<Mutex<usize>>,
}
#[async_trait::async_trait]
impl crate::AsyncMysqlShim<Socket> for PreparedCallbackShim {
    type Error = io::Error;
    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        info: crate::StatementMetaWriter<'a, Socket>,
    ) -> io::Result<()> {
        info.reply(1, std::iter::once(&self.column), std::iter::empty())
            .await
    }
    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: crate::ParamParser<'a>,
        results: crate::QueryResultWriter<'a, Socket>,
    ) -> io::Result<()> {
        *self.executions.lock().unwrap() += 1;
        results.completed(crate::OkResponse::default()).await
    }
    async fn on_close<'a>(&'a mut self, _: u32) {}
    async fn on_query<'a>(
        &'a mut self,
        _: &'a str,
        _: crate::QueryResultWriter<'a, Socket>,
    ) -> io::Result<()> {
        unreachable!()
    }
}
#[tokio::test]
async fn temporal_preflight_rejects_before_execute_callback_and_next_read() {
    let consumed = Arc::new(Mutex::new(0));
    let executions = Arc::new(Mutex::new(0));
    let mut wire = vec![2, 0, 0, 0, 0x16, b's'];
    let payload = [0x17, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 10, 0, 1, 0];
    wire.extend_from_slice(&[payload.len() as u8, 0, 0, 0]);
    wire.extend_from_slice(&payload);
    let end_of_bad = wire.len();
    wire.extend_from_slice(&[1, 0, 0, 0, 0x0e]);
    let mi = crate::AsyncMysqlIntermediary {
        client_capabilities: crate::CapabilityFlags::empty(),
        process_use_statement_on_query: true,
        reject_connection_on_dbname_absence: false,
        shim: PreparedCallbackShim {
            column: crate::Column {
                table: String::new(),
                column: "date".to_owned(),
                coltype: crate::ColumnType::MYSQL_TYPE_DATE,
                colflags: crate::ColumnFlags::empty(),
            },
            executions: executions.clone(),
        },
        reader: crate::packet_reader::PacketReader::new(CommandReader {
            wire: std::io::Cursor::new(wire),
            consumed: consumed.clone(),
        }),
        writer: crate::packet_writer::PacketWriter::with_limits(
            Socket::default(),
            ProtocolLimits::default(),
        ),
    };
    assert!(mi.run().await.is_err());
    assert_eq!(*executions.lock().unwrap(), 0);
    assert_eq!(*consumed.lock().unwrap(), end_of_bad);
}

#[tokio::test]
async fn closing_lease_restores_the_connection_only_after_err_flush() {
    let socket = Socket::default();
    let evidence = socket.clone();
    let mut slot =
        crate::packet_writer::PacketWriter::with_limits(socket, ProtocolLimits::default());
    slot.set_seq(254);
    let result =
        crate::QueryResultWriter::new(&mut slot, false, crate::CapabilityFlags::CLIENT_PROTOCOL_41);
    let mut lease = result.into_streaming_result().await.unwrap();
    lease.writer().start_row(3).unwrap();
    lease.writer().write_slice(b"a").await.unwrap();
    let tail = vec![ResidentTailPart::new(Arc::from(b"bc".as_slice()), 0..2).unwrap()];
    let mut closing = lease
        .into_closing_lease(tail, ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
        .map_err(|(_, error)| error)
        .unwrap();
    evidence.0.lock().unwrap().flush_blocked = true;
    {
        let mut future = Box::pin(closing.finish());
        assert!(poll_once(&mut future).await.is_pending());
        // Dropping the pending closing operation closes its IO. The inert
        // connection slot must stay detached, rather than become reusable.
    }
    drop(closing);
    assert!(slot.is_detached());
    let state = evidence.0.lock().unwrap();
    let frames = packets(&state.bytes);
    assert_eq!(frames[0], (254, b"abc".as_slice()));
    assert_eq!(frames[1].0, 255);
    assert_eq!(frames[1].1[0], 0xff);
    drop(state);

    let socket = Socket::default();
    let mut slot =
        crate::packet_writer::PacketWriter::with_limits(socket, ProtocolLimits::default());
    slot.set_seq(255);
    let result =
        crate::QueryResultWriter::new(&mut slot, false, crate::CapabilityFlags::CLIENT_PROTOCOL_41);
    let lease = result.into_streaming_result().await.unwrap();
    let mut closing = lease
        .into_closing_lease(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"cancelled")
        .map_err(|(_, error)| error)
        .unwrap();
    closing
        .finish()
        .await
        .unwrap()
        .no_more_results()
        .await
        .unwrap();
    drop(closing);
    assert!(!slot.is_detached());
    assert_eq!(slot.next_sequence(), 0);
}
