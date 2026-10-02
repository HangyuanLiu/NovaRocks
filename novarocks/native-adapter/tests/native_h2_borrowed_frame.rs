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

//! Actual borrowed fixed-frame DATA/control parsing and original pool exits.
//! HEADERS/PUSH/CONT owned fallback and other connection allocations remain open.
//! Padding flow/content-length deliberately retain upstream unpadded-body semantics.

use bytes::Bytes;
use h2::{ReceiveBufferPool, ReceiveFrameBuffer};
use hyper::http::{Request, Response};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

const FRAME: usize = 16384;
const DEADLINE: Duration = Duration::from_secs(5);
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static ALLOCATED_BYTES: Cell<usize> = const { Cell::new(0) };
}
struct ProbeAllocator;
fn record(size: usize) {
    let _ = ACTIVE.try_with(|active| {
        if active.get() {
            ALLOCATIONS.with(|count| count.set(count.get() + 1));
            ALLOCATED_BYTES.with(|bytes| bytes.set(bytes.get() + size));
        }
    });
}
// SAFETY: every operation delegates the exact pointer/Layout to System. The
// thread-local fixed counters neither allocate nor change allocator ownership.
unsafe impl GlobalAlloc for ProbeAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // SAFETY: this allocator receives the caller's valid Layout unchanged.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // SAFETY: this allocator receives the caller's valid Layout unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        // SAFETY: the live System pointer, original Layout and requested size
        // are forwarded unchanged to the same underlying allocator.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the live pointer and matching Layout are forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
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
fn measured<R>(work: impl FnOnce() -> R) -> (R, usize, usize) {
    ALLOCATIONS.with(|count| count.set(0));
    ALLOCATED_BYTES.with(|bytes| bytes.set(0));
    ACTIVE.with(|active| assert!(!active.replace(true)));
    let reset = ProbeReset;
    let result = work();
    drop(reset);
    (
        result,
        ALLOCATIONS.with(Cell::get),
        ALLOCATED_BYTES.with(Cell::get),
    )
}

#[derive(Clone, Copy, Default)]
struct ReadEvent {
    position: usize,
    requested: usize,
    count: usize,
    pointer: usize,
}
struct IoState {
    position: usize,
    reads: [ReadEvent; 128],
    count: usize,
    output: Vec<u8>,
}
impl IoState {
    fn new() -> Self {
        Self {
            position: 0,
            reads: [ReadEvent::default(); 128],
            count: 0,
            output: Vec::with_capacity(65536),
        }
    }
}
struct SpyIo {
    inner: DuplexStream,
    state: Arc<Mutex<IoState>>,
}
impl AsyncRead for SpyIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let requested = buf.remaining();
        let pointer = buf.filled().as_ptr() as usize;
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        let count = buf.filled().len() - before;
        if count != 0 {
            let mut state = self.state.lock().unwrap();
            let index = state.count;
            assert!(index < state.reads.len());
            state.reads[index] = ReadEvent {
                position: state.position,
                requested,
                count,
                pointer,
            };
            state.position += count;
            state.count += 1;
        }
        result
    }
}
impl AsyncWrite for SpyIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state.lock().unwrap();
        assert!(state.output.len() + buf.len() <= state.output.capacity());
        state.output.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
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
    if server {
        frame(&mut wire, 1, 4, 1, b"\x83\x86\x84\x01\x09localhost");
    } else {
        frame(&mut wire, 1, 4, 1, b"\x88");
    }
    wire
}
async fn io(wire: &[u8]) -> (SpyIo, DuplexStream, Arc<Mutex<IoState>>) {
    let (inner, mut peer) = tokio::io::duplex(65536);
    peer.write_all(wire).await.unwrap();
    let state = Arc::new(Mutex::new(IoState::new()));
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
    data: ReceiveBufferPool,
    goaway: ReceiveBufferPool,
    budget: Arc<ResultRetainedBudget>,
    sizes: [usize; 3],
}
fn funded() -> Funding {
    let carrier = Bytes::owner_with_exit_guard_metadata_size::<Marker, ResultWriteCredit>();
    let sizes = [
        ReceiveFrameBuffer::allocation_capacity_bound(FRAME).unwrap() + carrier,
        ReceiveBufferPool::allocation_capacity_bound(1, FRAME).unwrap() + carrier,
        ReceiveBufferPool::allocation_capacity_bound(1, FRAME).unwrap() + carrier,
    ];
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(sizes.iter().sum()).unwrap());
    let mut owners = sizes.map(|size| {
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(size).unwrap()
        else {
            panic!("original existing process pregrant");
        };
        Bytes::from_owner_with_exit_guard(Marker, credit)
    });
    Funding {
        raw: ReceiveFrameBuffer::new(FRAME, std::mem::take(&mut owners[0])).unwrap(),
        data: ReceiveBufferPool::new(1, FRAME, std::mem::take(&mut owners[1])).unwrap(),
        goaway: ReceiveBufferPool::new(1, FRAME, std::mem::take(&mut owners[2])).unwrap(),
        budget,
        sizes,
    }
}
fn reserve(budget: &Arc<ResultRetainedBudget>, size: usize) -> ResultWriteCredit {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(size).unwrap() else {
        panic!("exact physical backing must exit before original grant reuse");
    };
    credit
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn data_wire(payload: &[u8], pad: Option<u8>, eos: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    if let Some(pad) = pad {
        bytes.push(pad);
    }
    bytes.extend_from_slice(payload);
    if let Some(pad) = pad {
        bytes.resize(bytes.len() + usize::from(pad), 0x7e);
    }
    let mut wire = Vec::new();
    frame(
        &mut wire,
        0,
        u8::from(eos) | if pad.is_some() { 8 } else { 0 },
        1,
        &bytes,
    );
    wire
}
fn assert_fixed_read(state: &Arc<Mutex<IoState>>, first: usize, wire_len: usize) {
    let state = state.lock().unwrap();
    let reads = &state.reads[first..state.count];
    assert_eq!(reads.len(), if wire_len == 9 { 1 } else { 2 });
    assert_eq!((reads[0].requested, reads[0].count), (9, 9));
    if wire_len != 9 {
        assert_eq!(
            (reads[1].requested, reads[1].count),
            (wire_len - 9, wire_len - 9)
        );
        assert_eq!(reads[1].pointer, reads[0].pointer + 9);
        assert_eq!(reads[1].position, reads[0].position + 9);
    }
}
fn assert_only_funded_wrapper(allocations: usize, bytes: usize) {
    // PoolBuffer/BufferExit use one Bytes owner allocation under the pool's
    // original bound. The public bound covers all metadata in addition to the
    // fixed payload, so no private struct-layout guess is needed here.
    assert_eq!(
        allocations, 1,
        "DATA must allocate only its pregranted owner wrapper, never an intermediate full frame"
    );
    assert!(
        bytes > 0
            && bytes <= ReceiveBufferPool::allocation_capacity_bound(1, FRAME).unwrap() - FRAME
    );
}
fn take_data(body: &mut h2::RecvStream, cx: &mut Context<'_>, expected: &[u8]) -> Bytes {
    let Poll::Ready(Some(Ok(bytes))) = body.poll_data(cx) else {
        panic!("real DATA body must be ready");
    };
    assert_eq!(&bytes[..], expected);
    assert_eq!(body.flow_control().used_capacity(), expected.len());
    body.flow_control()
        .release_capacity(expected.len())
        .unwrap();
    assert_eq!(body.flow_control().used_capacity(), 0);
    bytes
}

async fn server_data(payload: &[u8], pad: Option<u8>) {
    let (io, mut peer, state) = io(&prefix(true)).await;
    let Funding {
        raw,
        data,
        goaway,
        budget,
        sizes,
    } = funded();
    let mut builder = h2::server::Builder::new();
    builder
        .max_receive_buffered_events(8)
        .receive_frame_buffer(raw)
        .receive_buffer_pool(data.clone())
        .receive_goaway_buffer_pool(goaway);
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let (request, response) = connection.accept().await.unwrap().unwrap();
    let mut body = request.into_body();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert_eq!(data.available_buffers(), 1);
    let first = state.lock().unwrap().count;
    let wire = data_wire(payload, pad, false);
    peer.write_all(&wire).await.unwrap();
    let (result, allocations, bytes) = measured(|| connection.poll_closed(&mut cx));
    assert!(result.is_pending());
    assert_only_funded_wrapper(allocations, bytes);
    assert_fixed_read(&state, first, wire.len());
    let original = take_data(&mut body, &mut cx, payload);
    let alias = original.clone();
    drop(original);
    assert_eq!(data.available_buffers(), 0);
    held(&budget);
    let second = data_wire(b"z", None, true);
    peer.write_all(&second).await.unwrap();
    let before = state.lock().unwrap().position;
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert_eq!(
        state.lock().unwrap().position,
        before,
        "full pool must block before the next header read"
    );
    assert!(body.poll_data(&mut cx).is_pending());
    drop(alias);
    assert_eq!(data.available_buffers(), 1);
    assert!(connection.poll_closed(&mut cx).is_pending());
    let last = take_data(&mut body, &mut cx, b"z");
    assert!(matches!(body.poll_data(&mut cx), Poll::Ready(None)));
    let surviving = last.slice(..);
    drop(last);
    drop(body);
    drop(response);
    drop(data);
    drop(connection);
    drop(peer);
    let exited = reserve(&budget, sizes[0] + sizes[2]);
    held(&budget);
    assert_eq!(&surviving[..], b"z");
    drop(surviving);
    let final_credit = reserve(&budget, sizes[1]);
    drop(final_credit);
    drop(exited);
}
#[tokio::test]
async fn server_zero_exact_padded_and_some_zero_preserve_body_flow_eos_and_alias_exit() {
    tokio::time::timeout(DEADLINE, async {
        for (len, pad) in [
            (0, None),
            (1, None),
            (FRAME, None),
            (0, Some(0)),
            (3, Some(0)),
            (3, Some(2)),
            (FRAME - 3, Some(2)),
        ] {
            server_data(&vec![0xa5; len], pad).await;
        }
    })
    .await
    .expect("borrowed server DATA matrix stalled");
}

async fn client_data(payload: &[u8], pad: Option<u8>) {
    let (io, mut peer, state) = io(&prefix(false)).await;
    let Funding {
        raw,
        data,
        goaway,
        budget,
        sizes,
    } = funded();
    let mut builder = h2::client::Builder::new();
    builder
        .enable_push(false)
        .max_receive_buffered_events(8)
        .receive_frame_buffer(raw)
        .receive_buffer_pool(data.clone())
        .receive_goaway_buffer_pool(goaway);
    let (mut sender, mut connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let (mut response, stream) = sender
        .send_request(
            Request::builder()
                .uri("http://localhost/")
                .body(())
                .unwrap(),
            true,
        )
        .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let Poll::Ready(Ok(response)) = Pin::new(&mut response).poll(&mut cx) else {
        panic!("real response headers");
    };
    let mut body = response.into_body();
    let first = state.lock().unwrap().count;
    let wire = data_wire(payload, pad, true);
    peer.write_all(&wire).await.unwrap();
    let (result, allocations, bytes) = measured(|| Pin::new(&mut connection).poll(&mut cx));
    assert!(result.is_pending());
    assert_only_funded_wrapper(allocations, bytes);
    assert_fixed_read(&state, first, wire.len());
    let last = take_data(&mut body, &mut cx, payload);
    assert!(matches!(body.poll_data(&mut cx), Poll::Ready(None)));
    let alias = last.clone();
    drop(last);
    drop(body);
    drop(stream);
    drop(sender);
    drop(data);
    drop(connection);
    drop(peer);
    let exited = reserve(&budget, sizes[0] + sizes[2]);
    held(&budget);
    assert_eq!(&alias[..], payload);
    drop(alias);
    drop(reserve(&budget, sizes[1]));
    drop(exited);
}
#[tokio::test]
async fn client_zero_exact_and_padded_preserve_original_alias_until_exit() {
    tokio::time::timeout(DEADLINE, async {
        for (len, pad) in [(0, None), (FRAME, None), (0, Some(0)), (3, Some(2))] {
            client_data(&vec![0x5a; len], pad).await;
        }
    })
    .await
    .expect("borrowed client DATA matrix stalled");
}

#[tokio::test]
async fn valid_controls_and_large_unknown_frame_are_borrowed_before_body() {
    tokio::time::timeout(DEADLINE, async {
        let (io, mut peer, state) = io(&prefix(true)).await;
        let Funding {
            raw,
            data,
            goaway,
            budget,
            sizes,
        } = funded();
        let mut builder = h2::server::Builder::new();
        builder
            .max_receive_buffered_events(8)
            .receive_frame_buffer(raw)
            .receive_buffer_pool(data.clone())
            .receive_goaway_buffer_pool(goaway);
        let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
        drop(builder);
        let (request, mut response) = connection.accept().await.unwrap().unwrap();
        let mut body = request.into_body();
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(connection.poll_closed(&mut cx).is_pending());
        let mut wire = Vec::new();
        frame(&mut wire, 4, 0, 0, &[]);
        frame(&mut wire, 6, 0, 0, b"12345678");
        frame(&mut wire, 8, 0, 0, &4096u32.to_be_bytes());
        frame(&mut wire, 8, 0, 1, &4096u32.to_be_bytes());
        frame(&mut wire, 2, 0, 1, &[0, 0, 0, 3, 1]);
        frame(&mut wire, 0xf0, 0, 0, &vec![0xa5; FRAME]);
        peer.write_all(&wire).await.unwrap();
        let (result, allocations, bytes) = measured(|| connection.poll_closed(&mut cx));
        assert!(result.is_pending());
        assert_eq!(
            (allocations, bytes),
            (0, 0),
            "controls/unknown must not allocate intermediate raw copies"
        );
        peer.write_all(&data_wire(b"body", None, true))
            .await
            .unwrap();
        let (result, allocations, bytes) = measured(|| connection.poll_closed(&mut cx));
        assert!(result.is_pending());
        assert_only_funded_wrapper(allocations, bytes);
        let last = take_data(&mut body, &mut cx, b"body");
        assert!(matches!(body.poll_data(&mut cx), Poll::Ready(None)));
        drop(last);
        assert_eq!(data.available_buffers(), 1);
        response.send_response(Response::new(()), true).unwrap();
        assert!(connection.poll_closed(&mut cx).is_pending());
        assert!(
            state
                .lock()
                .unwrap()
                .output
                .windows(17)
                .any(|frame| frame == b"\x00\x00\x08\x06\x01\x00\x00\x00\x0012345678"),
            "actual PING ACK must precede successful body completion"
        );
        drop(body);
        drop(response);
        drop(data);
        drop(connection);
        drop(peer);
        drop(reserve(&budget, sizes.iter().sum()));
    })
    .await
    .expect("borrowed control protocol fixture stalled");
}

#[tokio::test]
async fn invalid_data_stream_and_padding_produce_no_body_and_leave_pool_free() {
    tokio::time::timeout(DEADLINE, async {
        for (flags, stream, payload) in [
            (0, 0, &b"x"[..]),
            (8, 1, &b""[..]),
            (8, 1, &b"\x01"[..]),
            (8, 1, &b"\x02x"[..]),
        ] {
            let (io, mut peer, _) = io(&prefix(true)).await;
            let Funding {
                raw,
                data,
                goaway,
                budget,
                sizes,
            } = funded();
            let mut builder = h2::server::Builder::new();
            builder
                .max_receive_buffered_events(8)
                .receive_frame_buffer(raw)
                .receive_buffer_pool(data.clone())
                .receive_goaway_buffer_pool(goaway);
            let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
            drop(builder);
            let (request, response) = connection.accept().await.unwrap().unwrap();
            let mut body = request.into_body();
            let mut wire = Vec::new();
            frame(&mut wire, 0, flags, stream, payload);
            peer.write_all(&wire).await.unwrap();
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(matches!(
                connection.poll_closed(&mut cx),
                Poll::Ready(Err(_))
            ));
            assert_eq!(data.available_buffers(), 1);
            assert!(!matches!(body.poll_data(&mut cx), Poll::Ready(Some(Ok(_)))));
            drop(body);
            drop(response);
            drop(data);
            drop(connection);
            drop(peer);
            drop(reserve(&budget, sizes.iter().sum()));
        }
    })
    .await
    .expect("invalid borrowed DATA fixture stalled");
}

#[tokio::test]
async fn oversized_header_refuses_before_reading_its_body() {
    let (io, mut peer, state) = io(&prefix(true)).await;
    let Funding {
        raw,
        data,
        goaway,
        budget,
        sizes,
    } = funded();
    let mut builder = h2::server::Builder::new();
    builder
        .max_receive_buffered_events(8)
        .receive_frame_buffer(raw)
        .receive_buffer_pool(data.clone())
        .receive_goaway_buffer_pool(goaway);
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let (request, response) = connection.accept().await.unwrap().unwrap();
    drop(request);
    let before = state.lock().unwrap().position;
    let mut wire = Vec::new();
    frame(&mut wire, 0, 1, 1, &vec![0xa5; FRAME + 1]);
    peer.write_all(&wire).await.unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        connection.poll_closed(&mut cx),
        Poll::Ready(Err(_))
    ));
    assert_eq!(state.lock().unwrap().position - before, 9);
    assert_eq!(data.available_buffers(), 1);
    drop(response);
    drop(data);
    drop(connection);
    drop(peer);
    drop(reserve(&budget, sizes.iter().sum()));
}

#[tokio::test]
async fn borrowed_goaway_diagnostic_alias_keeps_only_its_original_pool_grant() {
    let (io, mut peer, _) = io(&prefix(false)).await;
    let Funding {
        raw,
        data,
        goaway,
        budget,
        sizes,
    } = funded();
    let mut builder = h2::client::Builder::new();
    builder
        .enable_push(false)
        .max_receive_buffered_events(8)
        .receive_frame_buffer(raw)
        .receive_buffer_pool(data)
        .receive_goaway_buffer_pool(goaway.clone());
    let (mut sender, mut connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let (response, stream) = sender
        .send_request(
            Request::builder()
                .uri("http://localhost/")
                .body(())
                .unwrap(),
            true,
        )
        .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let mut payload = 1u32.to_be_bytes().to_vec();
    payload.extend_from_slice(&u32::from(h2::Reason::NO_ERROR).to_be_bytes());
    payload.extend_from_slice(b"original-borrowed-diagnostic");
    let mut wire = Vec::new();
    frame(&mut wire, 7, 0, 0, &payload);
    peer.write_all(&wire).await.unwrap();
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let error = sender
        .send_request(
            Request::builder()
                .uri("http://localhost/")
                .body(())
                .unwrap(),
            true,
        )
        .unwrap_err();
    assert!(error.is_remote() && error.is_go_away());
    assert!(error.to_string().contains("original-borrowed-diagnostic"));
    assert_eq!(goaway.available_buffers(), 0);
    let alias = sender
        .send_request(
            Request::builder()
                .uri("http://localhost/")
                .body(())
                .unwrap(),
            true,
        )
        .unwrap_err();
    drop(error);
    drop(response);
    drop(stream);
    drop(sender);
    drop(goaway);
    drop(connection);
    drop(peer);
    let exited = reserve(&budget, sizes[0] + sizes[1]);
    held(&budget);
    assert!(alias.to_string().contains("original-borrowed-diagnostic"));
    drop(alias);
    drop(reserve(&budget, sizes[2]));
    drop(exited);
}

#[tokio::test]
async fn self_dependent_priority_still_resets_the_real_stream() {
    let (io, mut peer, _) = io(&prefix(true)).await;
    let Funding {
        raw,
        data,
        goaway,
        budget,
        sizes,
    } = funded();
    let mut builder = h2::server::Builder::new();
    builder
        .max_receive_buffered_events(8)
        .receive_frame_buffer(raw)
        .receive_buffer_pool(data.clone())
        .receive_goaway_buffer_pool(goaway);
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let (request, response) = connection.accept().await.unwrap().unwrap();
    let mut body = request.into_body();
    let mut wire = Vec::new();
    frame(&mut wire, 2, 0, 1, &[0, 0, 0, 1, 0]);
    peer.write_all(&wire).await.unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(connection.poll_closed(&mut cx).is_pending());
    let Poll::Ready(Some(Err(error))) = body.poll_data(&mut cx) else {
        panic!("self-dependency must reset the stream");
    };
    assert_eq!(error.reason(), Some(h2::Reason::PROTOCOL_ERROR));
    assert_eq!(data.available_buffers(), 1);
    drop(error);
    drop(body);
    drop(response);
    drop(data);
    drop(connection);
    drop(peer);
    drop(reserve(&budget, sizes.iter().sum()));
}

#[tokio::test]
async fn control_frame_cannot_bypass_an_unfinished_headers_block() {
    let mut wire = PREFACE.to_vec();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 0, 1, b"\x83\x86\x84\x01\x09localhost");
    frame(&mut wire, 6, 0, 0, b"12345678");
    let (io, peer, state) = io(&wire).await;
    let Funding {
        raw,
        data,
        goaway,
        budget,
        sizes,
    } = funded();
    let mut builder = h2::server::Builder::new();
    builder
        .max_receive_buffered_events(8)
        .receive_frame_buffer(raw)
        .receive_buffer_pool(data.clone())
        .receive_goaway_buffer_pool(goaway);
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let result = tokio::time::timeout(DEADLINE, connection.accept())
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.is_err(),
        "PING inside pending CONT must fail before delivery"
    );
    assert_eq!(data.available_buffers(), 1);
    assert!(
        !state
            .lock()
            .unwrap()
            .output
            .windows(17)
            .any(|frame| frame == b"\x00\x00\x08\x06\x01\x00\x00\x00\x0012345678")
    );
    drop(result);
    drop(data);
    drop(connection);
    drop(peer);
    drop(reserve(&budget, sizes.iter().sum()));
}

#[tokio::test]
async fn default_owned_reader_preserves_some_zero_and_nonzero_padding_body_semantics() {
    tokio::time::timeout(DEADLINE, async {
        for pad in [Some(0), Some(2)] {
            let (io, mut peer, _) = io(&prefix(true)).await;
            let mut connection = h2::server::handshake(io).await.unwrap();
            let (request, response) = connection.accept().await.unwrap().unwrap();
            let mut body = request.into_body();
            peer.write_all(&data_wire(b"same", pad, true))
                .await
                .unwrap();
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(connection.poll_closed(&mut cx).is_pending());
            let bytes = take_data(&mut body, &mut cx, b"same");
            assert!(matches!(body.poll_data(&mut cx), Poll::Ready(None)));
            drop(bytes);
            drop(body);
            drop(response);
            drop(connection);
            drop(peer);
        }
    })
    .await
    .expect("default padding parity fixture stalled");
}
