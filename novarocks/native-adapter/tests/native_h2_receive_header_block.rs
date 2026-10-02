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

//! Actual fixed encoded-header input consumption. Decoded fields, dynamic table,
//! Huffman output and whole Native connection funding are separate authorities.

use bytes::Bytes;
use h2::{ReceiveFrameBuffer, ReceiveHeaderBlockBuffer};
use hyper::body::{Body, Frame as BodyFrame};
use hyper::http::{Request, Response};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

const FRAME: usize = 16384;
const DEADLINE: Duration = Duration::from_secs(5);
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static MAX_ALLOCATION: Cell<usize> = const { Cell::new(0) };
}
struct ProbeAllocator;
fn record(size: usize) {
    let _ = ACTIVE.try_with(|active| {
        if active.get() {
            MAX_ALLOCATION.with(|maximum| maximum.set(maximum.get().max(size)));
        }
    });
}
// SAFETY: every operation forwards the unchanged pointer and Layout to System;
// the fixed thread-local counters neither allocate nor affect ownership.
unsafe impl GlobalAlloc for ProbeAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // SAFETY: the caller's valid Layout is forwarded unchanged.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // SAFETY: the caller's valid Layout is forwarded unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        // SAFETY: the same System pointer and original Layout are forwarded.
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the live pointer and its matching Layout are forwarded.
        unsafe { System.dealloc(pointer, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: ProbeAllocator = ProbeAllocator;
struct ProbeReset;
impl Drop for ProbeReset {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.set(false));
    }
}
fn measured<R>(work: impl FnOnce() -> R) -> (R, usize) {
    MAX_ALLOCATION.with(|maximum| maximum.set(0));
    ACTIVE.with(|active| assert!(!active.replace(true)));
    let reset = ProbeReset;
    let result = work();
    drop(reset);
    (result, MAX_ALLOCATION.with(Cell::get))
}
struct IoState {
    reads: usize,
    writes: usize,
    output: Vec<u8>,
}
struct SpyIo {
    inner: DuplexStream,
    state: Arc<Mutex<IoState>>,
}
impl AsyncRead for SpyIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.state.lock().unwrap().reads += 1;
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}
impl AsyncWrite for SpyIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state.lock().unwrap();
        state.writes += 1;
        assert!(state.output.len() + bytes.len() <= state.output.capacity());
        state.output.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
fn frame(wire: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    wire.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&[kind, flags]);
    wire.extend_from_slice(&stream.to_be_bytes());
    wire.extend_from_slice(payload);
}
fn prefix(server: bool) -> Vec<u8> {
    let mut wire = if server { PREFACE.to_vec() } else { Vec::new() };
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    wire
}
async fn make_io(wire: &[u8]) -> (SpyIo, DuplexStream, Arc<Mutex<IoState>>) {
    let (inner, mut peer) = tokio::io::duplex(65536);
    peer.write_all(wire).await.unwrap();
    let state = Arc::new(Mutex::new(IoState {
        reads: 0,
        writes: 0,
        output: Vec::with_capacity(65536),
    }));
    (
        SpyIo {
            inner,
            state: state.clone(),
        },
        peer,
        state,
    )
}
struct Marker;
impl AsRef<[u8]> for Marker {
    fn as_ref(&self) -> &[u8] {
        &[]
    }
}
struct Funding {
    raw: ReceiveFrameBuffer,
    header: ReceiveHeaderBlockBuffer,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
}
fn funded(max: usize) -> Funding {
    let carrier = Bytes::owner_with_exit_guard_metadata_size::<Marker, ResultWriteCredit>();
    let sizes = [
        ReceiveFrameBuffer::allocation_capacity_bound(FRAME).unwrap() + carrier,
        ReceiveHeaderBlockBuffer::allocation_capacity_bound(max).unwrap() + carrier,
    ];
    let total = sizes.iter().sum();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let mut owners = sizes.map(|size| {
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(size).unwrap()
        else {
            panic!("original process pregrant");
        };
        Bytes::from_owner_with_exit_guard(Marker, credit)
    });
    Funding {
        raw: ReceiveFrameBuffer::new(FRAME, std::mem::take(&mut owners[0])).unwrap(),
        header: ReceiveHeaderBlockBuffer::new(max, std::mem::take(&mut owners[1])).unwrap(),
        budget,
        total,
    }
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(
        matches!(
            budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ),
        "both original backing grants must remain held"
    );
}
fn reused(budget: &Arc<ResultRetainedBudget>, total: usize) -> ResultWriteCredit {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap() else {
        panic!("encoded input must physically exit independently of decoded aliases");
    };
    credit
}
fn server_builder(
    funding: Funding,
    max: usize,
) -> (h2::server::Builder, Arc<ResultRetainedBudget>, usize) {
    let Funding {
        raw,
        header,
        budget,
        total,
    } = funding;
    let mut builder = h2::server::Builder::new();
    builder
        .receive_frame_buffer(raw)
        .receive_header_block_buffer(header)
        .max_receive_header_block_size(max)
        .max_header_list_size(4096);
    (builder, budget, total)
}
fn client_builder(
    funding: Funding,
    max: usize,
) -> (h2::client::Builder, Arc<ResultRetainedBudget>, usize) {
    let Funding {
        raw,
        header,
        budget,
        total,
    } = funding;
    let mut builder = h2::client::Builder::new();
    builder
        .enable_push(false)
        .receive_frame_buffer(raw)
        .receive_header_block_buffer(header)
        .max_receive_header_block_size(max)
        .max_header_list_size(4096);
    (builder, budget, total)
}
fn literal(block: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    assert!(name.len() < 127 && value.len() < 127);
    block.push(0x40);
    block.push(name.len() as u8);
    block.extend_from_slice(name);
    block.push(value.len() as u8);
    block.extend_from_slice(value);
}
fn pseudo(path: &[u8]) -> Vec<u8> {
    let mut block = b"\x82\x86\x04".to_vec();
    block.push(path.len() as u8);
    block.extend_from_slice(path);
    block.extend_from_slice(b"\x01\x09localhost");
    block
}
fn fields(block: &mut Vec<u8>) {
    literal(block, b"x-plain", b"alpha");
    block.extend_from_slice(b"\x40\x09x-huffman\x8c");
    // RFC HPACK example: Huffman bytes for www.example.com.
    block.extend_from_slice(&[
        0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
    ]);
}
fn assert_fields(headers: &hyper::http::HeaderMap) {
    assert_eq!(headers["x-plain"], "alpha");
    assert_eq!(headers["x-huffman"], "www.example.com");
    assert_eq!(headers.get_all("x-plain").iter().count(), 1);
    assert_eq!(headers.get_all("x-huffman").iter().count(), 1);
}
fn send(
    sender: &mut h2::client::SendRequest<Bytes>,
    path: &str,
) -> (h2::client::ResponseFuture, h2::SendStream<Bytes>) {
    sender
        .send_request(Request::builder().uri(path).body(()).unwrap(), true)
        .unwrap()
}
fn response(
    connection: &mut h2::client::Connection<SpyIo, Bytes>,
    response: &mut h2::client::ResponseFuture,
) -> Response<h2::RecvStream> {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..8 {
        assert!(Pin::new(&mut *connection).poll(&mut cx).is_pending());
        if let Poll::Ready(result) = Pin::new(&mut *response).poll(&mut cx) {
            return result.unwrap();
        }
    }
    panic!("complete response must be published at a finite pump point");
}

#[tokio::test]
async fn server_every_byte_split_commits_once_and_reuses_dynamic_table_without_input_aliases() {
    let mut block = pseudo(b"/first?x=1");
    fields(&mut block);
    for split in 1..block.len() {
        // Each independent connection gets a fresh Tokio cooperative turn.
        // Synchronous noop polls do not replenish the task poll budget.
        tokio::task::yield_now().await;
        let (builder, budget, total) = server_builder(funded(256), 256);
        let mut wire = prefix(true);
        frame(&mut wire, 1, 1, 1, &block[..split]);
        let (io, mut peer, _) = make_io(&wire).await;
        let mut connection = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
            .await
            .unwrap()
            .unwrap();
        drop(builder);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            connection.poll_accept(&mut cx).is_pending(),
            "unfinished block at {split}"
        );
        held(&budget);
        let mut tail = Vec::new();
        frame(&mut tail, 9, 4, 1, &block[split..]);
        peer.write_all(&tail).await.unwrap();
        let Poll::Ready(Some(Ok((request, first_send)))) = connection.poll_accept(&mut cx) else {
            panic!("split {split} did not finish");
        };
        assert_fields(request.headers());
        assert_eq!(request.uri().path_and_query().unwrap(), "/first?x=1");
        let uri = request.uri().clone();
        let plain = request.headers()["x-plain"].clone();
        let huffman = request.headers()["x-huffman"].clone();
        let mut second = pseudo(b"/second?y=2");
        second.extend_from_slice(&[0xbe, 0xbf]);
        let mut wire = Vec::new();
        frame(&mut wire, 1, 5, 3, &second);
        peer.write_all(&wire).await.unwrap();
        let Poll::Ready(Some(Ok((second_request, second_send)))) = connection.poll_accept(&mut cx)
        else {
            panic!("second stream after split {split}");
        };
        assert_fields(second_request.headers());
        assert_eq!(
            second_request.uri().path_and_query().unwrap(),
            "/second?y=2"
        );
        assert_eq!(uri.path_and_query().unwrap(), "/first?x=1");
        assert_eq!(plain, "alpha");
        assert_eq!(huffman, "www.example.com");
        drop(first_send);
        drop(second_send);
        drop(request);
        drop(second_request);
        held(&budget);
        drop(connection);
        let credit = reused(&budget, total);
        // Retain decoded aliases beyond the actual encoded workspace exit.
        assert_eq!(uri.path(), "/first");
        assert_eq!(plain, "alpha");
        assert_eq!(huffman, "www.example.com");
        drop(credit);
    }
}

#[tokio::test]
async fn client_every_byte_split_commits_once_and_reuses_dynamic_table_without_input_aliases() {
    let mut block = vec![0x88];
    fields(&mut block);
    for split in 1..block.len() {
        // Each independent connection gets a fresh Tokio cooperative turn.
        // Synchronous noop polls do not replenish the task poll budget.
        tokio::task::yield_now().await;
        let (builder, budget, total) = client_builder(funded(256), 256);
        let (io, mut peer, _) = make_io(&prefix(false)).await;
        let (mut sender, mut connection) =
            tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
                .await
                .unwrap()
                .unwrap();
        drop(builder);
        let (mut first, first_send) = send(&mut sender, "http://localhost/first");
        let mut wire = Vec::new();
        frame(&mut wire, 1, 1, 1, &block[..split]);
        peer.write_all(&wire).await.unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut first).poll(&mut cx).is_pending());
        held(&budget);
        let mut wire = Vec::new();
        frame(&mut wire, 9, 4, 1, &block[split..]);
        peer.write_all(&wire).await.unwrap();
        let first_response = response(&mut connection, &mut first);
        assert_fields(first_response.headers());
        let plain = first_response.headers()["x-plain"].clone();
        let huffman = first_response.headers()["x-huffman"].clone();
        assert!(matches!(sender.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        let (mut second, second_send) = send(&mut sender, "http://localhost/second");
        let mut wire = Vec::new();
        frame(&mut wire, 1, 5, 3, &[0x88, 0xbe, 0xbf]);
        peer.write_all(&wire).await.unwrap();
        let second_response = response(&mut connection, &mut second);
        assert_fields(second_response.headers());
        drop(first_response);
        drop(second_response);
        drop(first);
        drop(second);
        drop(first_send);
        drop(second_send);
        drop(sender);
        held(&budget);
        drop(connection);
        let credit = reused(&budget, total);
        assert_eq!(plain, "alpha");
        assert_eq!(huffman, "www.example.com");
        drop(credit);
    }
}

fn large_declaration(server: bool) -> Vec<u8> {
    let mut block = if server { pseudo(b"/") } else { vec![0x88] };
    block.extend_from_slice(b"\x00\x01x\x7f\x81\xff\x3f"); // 1MiB plain value length, no body
    block
}
#[tokio::test]
async fn both_sides_refuse_declared_large_literal_before_end_headers_or_body() {
    let (mut builder, budget, total) = server_builder(funded(4096), 4096);
    builder.max_header_list_size(256);
    let mut wire = prefix(true);
    frame(&mut wire, 1, 1, 1, &large_declaration(true));
    let (io, _peer, _) = make_io(&wire).await;
    let mut connection = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
        .await
        .unwrap()
        .unwrap();
    drop(builder);
    let Poll::Ready(Some(Err(error))) =
        connection.poll_accept(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("server delayed declared-length rejection until END_HEADERS");
    };
    assert_eq!(error.reason(), Some(h2::Reason::COMPRESSION_ERROR));
    held(&budget);
    drop(connection);
    drop(reused(&budget, total));
    let (mut builder, budget, total) = client_builder(funded(4096), 4096);
    builder.max_header_list_size(256);
    let mut wire = prefix(false);
    frame(&mut wire, 1, 0, 1, &large_declaration(false));
    let (io, _peer, _) = make_io(&wire).await;
    let (mut sender, mut connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let (future, stream) = send(&mut sender, "http://localhost/");
    let Poll::Ready(Err(error)) =
        Pin::new(&mut connection).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("client delayed declared-length rejection until END_HEADERS");
    };
    assert_eq!(error.reason(), Some(h2::Reason::COMPRESSION_ERROR));
    drop(future);
    drop(stream);
    drop(sender);
    held(&budget);
    drop(connection);
    drop(reused(&budget, total));
}

#[tokio::test]
async fn exact_encoded_limit_counts_consumed_representations_and_excludes_padding_priority() {
    let first = pseudo(b"/padded");
    let rest = b"\x00\x01x\x03abc";
    let max = first.len() + rest.len();
    for limit in [max, max - 1] {
        let (builder, budget, total) = server_builder(funded(max), limit);
        let mut payload = vec![2, 0, 0, 0, 3, 15]; // padding and legal dependency on stream 3
        payload.extend_from_slice(&first);
        payload.extend_from_slice(&[0x7e, 0x7e]);
        let mut wire = prefix(true);
        frame(&mut wire, 1, 1 | 8 | 32, 1, &payload);
        let (io, mut peer, _) = make_io(&wire).await;
        let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
        drop(builder);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(connection.poll_accept(&mut cx).is_pending());
        let mut wire = Vec::new();
        frame(&mut wire, 9, 4, 1, rest);
        peer.write_all(&wire).await.unwrap();
        match connection.poll_accept(&mut cx) {
            Poll::Ready(Some(Ok((request, stream)))) if limit == max => {
                assert_eq!(request.uri().path(), "/padded");
                assert_eq!(request.headers()["x"], "abc");
                drop(request);
                drop(stream);
            }
            Poll::Ready(Some(Err(error))) if limit < max => {
                assert_eq!(error.reason(), Some(h2::Reason::COMPRESSION_ERROR))
            }
            _ => panic!("exact encoded boundary {limit}/{max}"),
        }
        held(&budget);
        drop(connection);
        drop(reused(&budget, total));
    }
}

#[tokio::test]
async fn malformed_stream_reset_preserves_dynamic_table_for_next_workspace_block() {
    let mut bad = pseudo(b"/bad");
    literal(&mut bad, b"x-plain", b"alpha");
    literal(&mut bad, b"connection", b"keep-alive");
    let mut good = pseudo(b"/good");
    good.push(0xbf); // x-plain remains index 63 after malformed insertion
    let mut wire = prefix(true);
    frame(&mut wire, 1, 5, 1, &bad);
    frame(&mut wire, 1, 5, 3, &good);
    let (builder, budget, total) = server_builder(funded(256), 256);
    let (io, _peer, _) = make_io(&wire).await;
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let Poll::Ready(Some(Ok((request, stream)))) =
        connection.poll_accept(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("malformed stream must reset without destroying next-block table");
    };
    assert_eq!(request.uri().path(), "/good");
    assert_eq!(request.headers()["x-plain"], "alpha");
    drop(request);
    drop(stream);
    held(&budget);
    drop(connection);
    drop(reused(&budget, total));
}

#[tokio::test]
async fn partial_cancel_and_wrong_continuation_stream_release_only_after_connection_drop() {
    for wrong in [false, true] {
        let (builder, budget, total) = server_builder(funded(256), 256);
        let mut wire = prefix(true);
        frame(&mut wire, 1, 1, 1, b"\x82\x86\x04\x05/a");
        let (io, mut peer, _) = make_io(&wire).await;
        let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
        drop(builder);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(connection.poll_accept(&mut cx).is_pending());
        held(&budget);
        if wrong {
            let mut wire = Vec::new();
            frame(&mut wire, 9, 4, 3, b"bcd");
            peer.write_all(&wire).await.unwrap();
            let Poll::Ready(Some(Err(error))) = connection.poll_accept(&mut cx) else {
                panic!("wrong continuation stream must fail");
            };
            assert_eq!(error.reason(), Some(h2::Reason::PROTOCOL_ERROR));
            held(&budget);
        }
        drop(connection);
        drop(reused(&budget, total));
    }
}

#[tokio::test]
async fn client_actual_static_decoder_consumption_has_no_independent_full_frame_allocation() {
    let (mut builder, budget, total) = client_builder(funded(FRAME), FRAME);
    builder
        .max_receive_buffered_events(8)
        .max_header_list_size(FRAME as u32);
    let (io, mut peer, _) = make_io(&prefix(false)).await;
    let (mut sender, mut connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let (mut future, stream) = send(&mut sender, "http://localhost/");
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    assert!(Pin::new(&mut future).poll(&mut cx).is_pending());
    // Specific accepted decoder stress input, not a claim that repeated size
    // updates are a canonical HPACK block. Table zero and static status produce
    // no long decoded strings; wrapper/stream allocations remain in scope.
    let mut payload = vec![0x20; FRAME];
    payload[FRAME - 1] = 0x88;
    let mut wire = Vec::with_capacity(FRAME + 9);
    frame(&mut wire, 1, 5, 1, &payload);
    peer.write_all(&wire).await.unwrap();
    let (poll, largest) = measured(|| Pin::new(&mut connection).poll(&mut cx));
    assert!(poll.is_pending());
    assert!(
        largest < FRAME,
        "independent whole-frame/header copy requested {largest} bytes"
    );
    let Poll::Ready(Ok(response)) = Pin::new(&mut future).poll(&mut cx) else {
        panic!("measured turn did not consume static HEADERS");
    };
    assert_eq!(response.status(), 200);
    assert!(response.headers().is_empty());
    drop(response);
    drop(future);
    drop(stream);
    drop(sender);
    held(&budget);
    drop(connection);
    drop(reused(&budget, total));
}

#[tokio::test]
async fn both_sides_validate_geometry_before_io_and_header_once_bind_survives_drop() {
    for server in [false, true] {
        let Funding {
            raw,
            header,
            budget,
            total,
        } = funded(256);
        assert_eq!(header.max_encoded_bytes(), 256);
        for bad in 0..3 {
            let (io, _peer, state) = make_io(&prefix(server)).await;
            let failed = if server {
                let mut builder = h2::server::Builder::new();
                builder.receive_header_block_buffer(header.clone());
                if bad != 0 {
                    builder.receive_frame_buffer(raw.clone());
                }
                if bad != 1 {
                    builder.max_receive_header_block_size(if bad == 2 { 257 } else { 256 });
                }
                tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
                    .await
                    .unwrap()
                    .is_err()
            } else {
                let mut builder = h2::client::Builder::new();
                builder.receive_header_block_buffer(header.clone());
                if bad != 0 {
                    builder.receive_frame_buffer(raw.clone());
                }
                if bad != 1 {
                    builder.max_receive_header_block_size(if bad == 2 { 257 } else { 256 });
                }
                tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
                    .await
                    .unwrap()
                    .is_err()
            };
            assert!(failed);
            let state = state.lock().unwrap();
            assert_eq!((state.reads, state.writes), (0, 0));
        }
        // Valid configuration still binds after all refused geometries.
        let (io, _peer, _) = make_io(&prefix(server)).await;
        if server {
            let mut builder = h2::server::Builder::new();
            builder
                .receive_frame_buffer(raw.clone())
                .receive_header_block_buffer(header.clone())
                .max_receive_header_block_size(256);
            drop(builder.handshake::<_, Bytes>(io).await.unwrap());
            drop(builder);
        } else {
            let mut builder = h2::client::Builder::new();
            builder
                .receive_frame_buffer(raw.clone())
                .receive_header_block_buffer(header.clone())
                .max_receive_header_block_size(256);
            drop(builder.handshake::<_, Bytes>(io).await.unwrap());
            drop(builder);
        }
        let (io, _peer, state) = make_io(&prefix(server)).await;
        // Fresh raw buffer isolates the header buffer's one-bind latch.
        let fresh_raw = ReceiveFrameBuffer::new(FRAME, Bytes::new()).unwrap();
        let failed = if server {
            let mut builder = h2::server::Builder::new();
            builder
                .receive_frame_buffer(fresh_raw)
                .receive_header_block_buffer(header.clone())
                .max_receive_header_block_size(256);
            builder.handshake::<_, Bytes>(io).await.is_err()
        } else {
            let mut builder = h2::client::Builder::new();
            builder
                .receive_frame_buffer(fresh_raw)
                .receive_header_block_buffer(header.clone())
                .max_receive_header_block_size(256);
            builder.handshake::<_, Bytes>(io).await.is_err()
        };
        assert!(failed);
        let observed = state.lock().unwrap();
        assert_eq!((observed.reads, observed.writes), (0, 0));
        drop(observed);
        held(&budget);
        drop(raw);
        drop(header);
        drop(reused(&budget, total));
    }
}

fn assert_reset(output: &[u8], stream: u32, reason: h2::Reason) {
    let mut offset = 0;
    while offset + 9 <= output.len() {
        let length = (usize::from(output[offset]) << 16)
            | (usize::from(output[offset + 1]) << 8)
            | usize::from(output[offset + 2]);
        let end = offset + 9 + length;
        assert!(end <= output.len());
        let id = u32::from_be_bytes(output[offset + 5..offset + 9].try_into().unwrap());
        if output[offset + 3] == 3 && id == stream {
            assert_eq!(length, 4);
            assert_eq!(
                u32::from_be_bytes(output[offset + 9..end].try_into().unwrap()),
                u32::from(reason)
            );
            return;
        }
        offset = end;
    }
    panic!("actual RST_STREAM {stream} was not written");
}

#[tokio::test]
async fn fragmented_malformed_header_remains_sticky_in_fixed_and_default_modes() {
    for fixed in [false, true] {
        let mut first = pseudo(b"/must-reset");
        literal(&mut first, b"connection", b"close");
        first.extend_from_slice(b"\x40\x04x-ok\x03a"); // unfinished valid literal after the illegal field
        let mut wire = prefix(true);
        frame(&mut wire, 1, 1, 1, &first);
        let (io, mut peer, state) = make_io(&wire).await;
        let (builder, budget, total) = if fixed {
            server_builder(funded(256), 256)
        } else {
            (
                h2::server::Builder::new(),
                ResultRetainedBudget::new(NonZeroUsize::new(1).unwrap()),
                0,
            )
        };
        let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
        drop(builder);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(connection.poll_accept(&mut cx).is_pending());
        let mut good = pseudo(b"/after-reset");
        good.push(0xbe); // completed x-ok must be dynamic index 62
        let mut wire = Vec::new();
        frame(&mut wire, 9, 4, 1, b"bc");
        frame(&mut wire, 1, 5, 3, &good);
        peer.write_all(&wire).await.unwrap();
        let Poll::Ready(Some(Ok((request, response)))) = connection.poll_accept(&mut cx) else {
            panic!("strict invalid block must reset while stream3 progresses in fixed={fixed}");
        };
        assert_eq!(
            request.uri().path(),
            "/after-reset",
            "invalid stream1 must never publish"
        );
        assert_eq!(request.headers()["x-ok"], "abc");
        drop(request);
        drop(response);
        // A finite extra poll flushes the real queued reset; no EOF oracle.
        assert!(connection.poll_accept(&mut cx).is_pending());
        assert_reset(&state.lock().unwrap().output, 1, h2::Reason::PROTOCOL_ERROR);
        if fixed {
            held(&budget);
        }
        drop(connection);
        if fixed {
            drop(reused(&budget, total));
        }
    }
}

#[tokio::test]
async fn padded_push_promise_continuation_retains_policy_and_encoded_input_exits_without_request_aliases()
 {
    for pad in [0u8, 2] {
        tokio::task::yield_now().await;
        let mut block = pseudo(b"/promised?x=1");
        fields(&mut block);
        let (mut builder, budget, total) = client_builder(funded(block.len()), block.len());
        // Fixed encoded/raw input alone must preserve upstream enabled push.
        // Native bounded event/payload policy is intentionally not installed.
        builder.enable_push(true);
        let (io, mut peer, _) = make_io(&prefix(false)).await;
        let (mut sender, mut connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
        drop(builder);
        let (mut parent, send_stream) = send(&mut sender, "http://localhost/parent");
        let mut pushes = parent.push_promises();
        let split = block.len() - 5;
        let mut payload = vec![pad];
        payload.extend_from_slice(&2u32.to_be_bytes());
        payload.extend_from_slice(&block[..split]);
        payload.resize(payload.len() + usize::from(pad), 0x7e);
        let mut wire = Vec::new();
        frame(&mut wire, 5, 8, 1, &payload);
        peer.write_all(&wire).await.unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
        assert!(pushes.poll_push_promise(&mut cx).is_pending());
        held(&budget);
        let mut wire = Vec::new();
        frame(&mut wire, 9, 4, 1, &block[split..]);
        peer.write_all(&wire).await.unwrap();
        assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
        let Poll::Ready(Some(Ok(promise))) = pushes.poll_push_promise(&mut cx) else {
            panic!("padded promised request did not finish at CONTINUATION");
        };
        assert_fields(promise.request().headers());
        assert_eq!(
            promise.request().uri().path_and_query().unwrap(),
            "/promised?x=1"
        );
        let uri = promise.request().uri().clone();
        let huffman = promise.request().headers()["x-huffman"].clone();
        let (request, pushed_response) = promise.into_parts();
        assert_eq!(pushed_response.stream_id().as_u32(), 2);
        // A regular response and following DATA still progress after workspace
        // reuse. DATA backing/flow authority is outside this header proof.
        let mut wire = Vec::new();
        frame(&mut wire, 1, 4, 1, &[0x88]);
        frame(&mut wire, 0, 1, 1, b"z");
        peer.write_all(&wire).await.unwrap();
        let mut parent_response = response(&mut connection, &mut parent);
        let Poll::Ready(Some(Ok(data))) = parent_response.body_mut().poll_data(&mut cx) else {
            panic!("DATA after promised block must remain readable");
        };
        assert_eq!(data.as_ref(), b"z");
        drop(data);
        assert!(matches!(
            parent_response.body_mut().poll_data(&mut cx),
            Poll::Ready(None)
        ));
        drop(parent_response);
        drop(parent);
        drop(send_stream);
        drop(pushed_response);
        drop(pushes);
        drop(request);
        drop(sender);
        held(&budget);
        drop(connection);
        let credit = reused(&budget, total);
        assert_eq!(uri.path(), "/promised");
        assert_eq!(huffman, "www.example.com");
        drop(credit);
    }
}

#[tokio::test]
async fn self_dependency_resets_before_decode_and_next_stream_reuses_fixed_workspace() {
    let block = pseudo(b"/self-dependent");
    let mut payload = vec![0, 0, 0, 1, 15];
    payload.extend_from_slice(&block);
    let mut wire = prefix(true);
    frame(&mut wire, 1, 1 | 4 | 32, 1, &payload);
    let mut good = pseudo(b"/after-priority-reset");
    fields(&mut good);
    frame(&mut wire, 1, 5, 3, &good);
    let (builder, budget, total) = server_builder(funded(256), 256);
    let (io, _peer, state) = make_io(&wire).await;
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(Ok((request, response)))) = connection.poll_accept(&mut cx) else {
        panic!("valid stream must progress after priority self-dependency");
    };
    assert_eq!(request.uri().path(), "/after-priority-reset");
    assert_fields(request.headers());
    drop(request);
    drop(response);
    assert!(connection.poll_accept(&mut cx).is_pending());
    assert_reset(&state.lock().unwrap().output, 1, h2::Reason::PROTOCOL_ERROR);
    held(&budget);
    drop(connection);
    drop(reused(&budget, total));
}

struct EmptyBody;
impl Body for EmptyBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<BodyFrame<Bytes>, Infallible>>> {
        Poll::Ready(None)
    }
    fn is_end_stream(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn cloned_hyper_server_installs_original_encoded_buffer_and_decoded_alias_is_independent() {
    tokio::time::timeout(DEADLINE, async {
        let (server_io, client_io) = tokio::io::duplex(65536);
        let Funding {
            raw,
            header,
            budget,
            total,
        } = funded(4096);
        let bind_witness = header.clone();
        let retained = Arc::new(Mutex::new(None));
        let observed = retained.clone();
        let service = service_fn(move |request: Request<hyper::body::Incoming>| {
            assert_eq!(request.uri().path(), "/hyper-server");
            assert_eq!(request.headers()["x-retained"], "independent");
            *observed.lock().unwrap() = Some(request.headers()["x-retained"].clone());
            async { Ok::<_, Infallible>(Response::new(EmptyBody)) }
        });
        let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
        builder
            .receive_frame_buffer(raw)
            .receive_header_block_buffer(header)
            .max_receive_header_block_size(4096);
        let connection = builder
            .clone()
            .serve_connection(TokioIo::new(server_io), service);
        drop(builder);
        let server = tokio::spawn(connection);
        let (mut sender, connection) = h2::client::handshake(client_io).await.unwrap();
        let client = tokio::spawn(connection);
        let (future, stream) = sender
            .send_request(
                Request::builder()
                    .uri("http://localhost/hyper-server")
                    .header("x-retained", "independent")
                    .body(())
                    .unwrap(),
                true,
            )
            .unwrap();
        drop(stream);
        assert_eq!(future.await.unwrap().status(), 200);
        // Configuration handles have exited and the real protocol has decoded.
        // A missing Hyper forwarding seam prematurely releases part of total.
        held(&budget);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        assert_original_header_was_bound(&bind_witness, true).await;
        assert!(matches!(
            budget.try_reserve_process(total).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        drop(bind_witness);
        let credit = reused(&budget, total);
        assert_eq!(retained.lock().unwrap().as_ref().unwrap(), "independent");
        drop(credit);
        drop(sender);
        let _ = client.await.unwrap();
    })
    .await
    .expect("Hyper server fixed encoded input fixture stalled");
}

#[tokio::test]
async fn cloned_hyper_client_installs_original_encoded_buffer_and_decoded_alias_is_independent() {
    tokio::time::timeout(DEADLINE, async {
        let (server_io, client_io) = tokio::io::duplex(65536);
        let server = tokio::spawn(async move {
            let mut connection = h2::server::handshake(server_io).await.unwrap();
            let (request, mut response) = connection.accept().await.unwrap().unwrap();
            assert_eq!(request.uri().path(), "/hyper-client");
            drop(request);
            drop(
                response
                    .send_response(
                        Response::builder()
                            .header("x-retained", "independent")
                            .body(())
                            .unwrap(),
                        true,
                    )
                    .unwrap(),
            );
            while let Some(result) = connection.accept().await {
                if result.is_err() {
                    break;
                }
            }
        });
        let Funding {
            raw,
            header,
            budget,
            total,
        } = funded(4096);
        let bind_witness = header.clone();
        let mut builder = hyper::client::conn::http2::Builder::new(TokioExecutor::new());
        builder
            .receive_frame_buffer(raw)
            .receive_header_block_buffer(header)
            .max_receive_header_block_size(4096);
        let (mut sender, connection) = builder
            .clone()
            .handshake::<_, EmptyBody>(TokioIo::new(client_io))
            .await
            .unwrap();
        drop(builder);
        let client = tokio::spawn(connection);
        let response = sender
            .send_request(
                Request::builder()
                    .uri("http://localhost/hyper-client")
                    .body(EmptyBody)
                    .unwrap(),
            )
            .await
            .unwrap();
        let alias = response.headers()["x-retained"].clone();
        assert_eq!(alias, "independent");
        drop(response);
        held(&budget);
        drop(sender);
        client.await.unwrap().unwrap();
        assert_original_header_was_bound(&bind_witness, false).await;
        assert!(matches!(
            budget.try_reserve_process(total).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        drop(bind_witness);
        let credit = reused(&budget, total);
        assert_eq!(alias, "independent");
        drop(credit);
        server.await.unwrap();
    })
    .await
    .expect("Hyper client fixed encoded input fixture stalled");
}

async fn assert_original_header_was_bound(header: &ReceiveHeaderBlockBuffer, server: bool) {
    let (io, _peer, state) = make_io(&prefix(server)).await;
    // This independent fixture raw buffer isolates the actual encoded input
    // latch; it is not included in the original connection funding claim.
    let raw = ReceiveFrameBuffer::new(FRAME, Bytes::new()).unwrap();
    let error = if server {
        let mut builder = h2::server::Builder::new();
        builder
            .receive_frame_buffer(raw)
            .receive_header_block_buffer(header.clone())
            .max_receive_header_block_size(header.max_encoded_bytes());
        match builder.handshake::<_, Bytes>(io).await {
            Err(error) => error,
            Ok(_) => panic!("Hyper server failed to bind the original encoded input"),
        }
    } else {
        let mut builder = h2::client::Builder::new();
        builder
            .receive_frame_buffer(raw)
            .receive_header_block_buffer(header.clone())
            .max_receive_header_block_size(header.max_encoded_bytes());
        match builder.handshake::<_, Bytes>(io).await {
            Err(error) => error,
            Ok(_) => panic!("Hyper client failed to bind the original encoded input"),
        }
    };
    assert_eq!(error.get_io().unwrap().kind(), io::ErrorKind::InvalidInput);
    assert!(
        error
            .get_io()
            .unwrap()
            .to_string()
            .contains("encoded header buffer already bound")
    );
    let state = state.lock().unwrap();
    assert_eq!((state.reads, state.writes), (0, 0));
}
