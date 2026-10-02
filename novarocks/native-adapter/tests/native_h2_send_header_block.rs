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

//! Whole encoded-header original funding across actual H2/Hyper ownership.
//! Writer backing, caller HeaderMaps, HTTP decoding and tasks are separate.

use bytes::Bytes;
use h2::{SendFrameBuffer, SendHeaderBlockPool};
use hyper::body::{Body, Frame};
use hyper::http::{HeaderValue, Request, Response};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::convert::Infallible;
use std::future::Future;
use std::io::{self, IoSlice};
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

const CAPACITY: usize = 65536;
const FRAME: usize = 16384;
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const DEADLINE: Duration = Duration::from_secs(5);
const MAX_LIST: usize = 32768;

struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}

struct GrantMarker;
impl AsRef<[u8]> for GrantMarker {
    fn as_ref(&self) -> &[u8] {
        &[]
    }
}

fn funded(max: usize) -> (SendHeaderBlockPool, Arc<ResultRetainedBudget>, usize) {
    let total = SendHeaderBlockPool::allocation_capacity_bound(max).unwrap()
        + Bytes::owner_with_exit_guard_metadata_size::<GrantMarker, ResultWriteCredit>();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap() else {
        panic!("whole header block original pregrant");
    };
    let pool =
        SendHeaderBlockPool::new(max, Bytes::from_owner_with_exit_guard(GrantMarker, credit))
            .unwrap();
    assert_eq!(pool.max_header_list_size(), max);
    assert_eq!(pool.buffer_capacity_bytes(), max * 4 + 20);
    (pool, budget, total)
}
// Whole-block and writer backing are independently pregranted from the same
// process budget. Losing either forwarding seam makes a byte reusable early.
fn funded_with_writer(
    max: usize,
) -> (
    SendHeaderBlockPool,
    SendFrameBuffer,
    Arc<ResultRetainedBudget>,
    usize,
) {
    let carrier = Bytes::owner_with_exit_guard_metadata_size::<GrantMarker, ResultWriteCredit>();
    let header_bytes = SendHeaderBlockPool::allocation_capacity_bound(max).unwrap() + carrier;
    let writer_bytes =
        SendFrameBuffer::allocation_capacity_bound(CAPACITY, FRAME).unwrap() + carrier;
    let total = header_bytes.checked_add(writer_bytes).unwrap();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let ResultWriteAdmission::Granted(header_credit) =
        budget.try_reserve_process(header_bytes).unwrap()
    else {
        panic!("whole header pregrant");
    };
    let ResultWriteAdmission::Granted(writer_credit) =
        budget.try_reserve_process(writer_bytes).unwrap()
    else {
        panic!("independent writer pregrant");
    };
    let pool = SendHeaderBlockPool::new(
        max,
        Bytes::from_owner_with_exit_guard(GrantMarker, header_credit),
    )
    .unwrap();
    let writer = SendFrameBuffer::new(
        CAPACITY,
        FRAME,
        Bytes::from_owner_with_exit_guard(GrantMarker, writer_credit),
    )
    .unwrap();
    (pool, writer, budget, total)
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn released(budget: &Arc<ResultRetainedBudget>, total: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap() else {
        panic!("all actual connection/config/header-pool owners must exit");
    };
    drop(credit);
}

struct Writes {
    wire: Vec<u8>,
    allowance: usize,
    write_limit: usize,
    block_flush: bool,
    write_error: bool,
    flush_error: bool,
    write_zero: bool,
    zero_calls: usize,
    write_calls: usize,
    read_calls: usize,
    deadline: Instant,
}

impl Writes {
    fn new() -> Self {
        Self {
            wire: Vec::new(),
            allowance: usize::MAX,
            write_limit: usize::MAX,
            block_flush: false,
            write_error: false,
            flush_error: false,
            write_zero: false,
            zero_calls: 0,
            write_calls: 0,
            read_calls: 0,
            deadline: Instant::now() + DEADLINE,
        }
    }

    fn watch(&self) {
        assert!(
            Instant::now() < self.deadline,
            "H2 fixture watchdog elapsed"
        );
    }

    fn write(&mut self, slices: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        self.watch();
        self.write_calls += 1;
        if self.write_error {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected fixed writer failure",
            )));
        }
        if self.write_zero {
            self.zero_calls += 1;
            assert_eq!(
                self.zero_calls, 1,
                "H2 retried a zero write without progress"
            );
            return Poll::Ready(Ok(0));
        }
        let mut count = 0;
        for bytes in slices {
            let available = bytes
                .len()
                .min(self.write_limit.saturating_sub(count))
                .min(self.allowance);
            if available == 0 && !bytes.is_empty() {
                break;
            }
            self.wire.extend_from_slice(&bytes[..available]);
            count += available;
            if self.allowance != usize::MAX {
                self.allowance -= available;
            }
            if available != bytes.len() {
                break;
            }
        }
        if count == 0 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(count))
        }
    }

    fn frames(&self, client: bool) -> Vec<(u8, u8, u32, Vec<u8>)> {
        let mut wire = self.wire.as_slice();
        if client {
            assert!(wire.starts_with(PREFACE));
            wire = &wire[PREFACE.len()..];
        }
        let mut frames = Vec::new();
        while !wire.is_empty() {
            assert!(wire.len() >= 9, "incomplete frame header");
            let len = ((wire[0] as usize) << 16) | ((wire[1] as usize) << 8) | wire[2] as usize;
            assert!(wire.len() >= len + 9, "incomplete frame body");
            let stream = u32::from_be_bytes(wire[5..9].try_into().unwrap()) & 0x7fff_ffff;
            frames.push((wire[3], wire[4], stream, wire[9..len + 9].to_vec()));
            wire = &wire[len + 9..];
        }
        frames
    }
}

struct SpyIo {
    read: DuplexStream,
    writes: Arc<Mutex<Writes>>,
    vectored: bool,
}

impl AsyncRead for SpyIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        {
            let mut state = self.writes.lock().unwrap();
            state.watch();
            state.read_calls += 1;
        }
        Pin::new(&mut self.read).poll_read(cx, bytes)
    }
}

impl AsyncWrite for SpyIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.writes.lock().unwrap().write(&[IoSlice::new(bytes)])
    }

    fn is_write_vectored(&self) -> bool {
        self.vectored
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.writes.lock().unwrap().write(bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let state = self.writes.lock().unwrap();
        state.watch();
        if state.flush_error {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected fixed writer flush failure",
            )))
        } else if state.block_flush {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.writes.lock().unwrap().watch();
        Poll::Ready(Ok(()))
    }
}

fn frame(wire: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, bytes: &[u8]) {
    wire.extend_from_slice(&(bytes.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&[kind, flags]);
    wire.extend_from_slice(&stream.to_be_bytes());
    wire.extend_from_slice(bytes);
}

async fn fixture(
    server: bool,
    requests: usize,
    vectored: bool,
) -> (SpyIo, DuplexStream, Arc<Mutex<Writes>>) {
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut wire = Vec::new();
    if server {
        wire.extend_from_slice(PREFACE);
    }
    // A legal large peer value must not replace the local outbound bound.
    let settings = [0, 1, 255, 255, 255, 255, 0, 8, 0, 0, 0, 1];
    frame(&mut wire, 4, 0, 0, &settings);
    frame(&mut wire, 4, 1, 0, &[]);
    if server {
        for index in 0..requests {
            let stream = u32::try_from(index * 2 + 1).unwrap();
            frame(&mut wire, 1, 5, stream, b"\x83\x86\x84\x01\x09localhost");
        }
    }
    tokio::time::timeout(DEADLINE, peer.write_all(&wire))
        .await
        .expect("bounded peer fixture write")
        .unwrap();
    let writes = Arc::new(Mutex::new(Writes::new()));
    (
        SpyIo {
            read: io,
            writes: writes.clone(),
            vectored,
        },
        peer,
        writes,
    )
}

fn large_value() -> HeaderValue {
    // Each '~' Huffman code has thirteen bits. This forces multiple actual
    // CONTINUATION frames despite the decoded list fitting within 32 KiB.
    HeaderValue::from_bytes(&vec![b'~'; 24000]).unwrap()
}
fn assert_continuations(writes: &Arc<Mutex<Writes>>, client: bool) {
    let state = writes.lock().unwrap();
    let frames = state.frames(client);
    let headers: Vec<_> = frames
        .iter()
        .filter(|frame| frame.0 == 1 || frame.0 == 9)
        .collect();
    assert_eq!(headers.len(), 3);
    assert_eq!(
        headers.iter().map(|frame| frame.0).collect::<Vec<_>>(),
        [1, 9, 9]
    );
    assert_eq!(
        headers.iter().map(|frame| frame.1 & 4).collect::<Vec<_>>(),
        [0, 0, 4]
    );
    assert!(
        headers
            .iter()
            .all(|frame| frame.2 == 1 && frame.3.len() <= FRAME)
    );
    let block: Vec<_> = headers
        .iter()
        .flat_map(|frame| frame.3.iter().copied())
        .collect();
    assert_eq!(
        block[0], 0x20,
        "fixed pool requires actual table zero update"
    );
    assert!(block.len() > 2 * FRAME);
}

#[tokio::test]
async fn server_original_grant_survives_partial_continuations_and_flush() {
    for vectored in [false, true] {
        let (io, peer, writes) = fixture(true, 1, vectored).await;
        let (pool, writer, budget, total) = funded_with_writer(MAX_LIST);
        let mut builder = h2::server::Builder::new();
        builder
            .max_send_header_table_size(0)
            .send_header_block_pool(pool)
            .send_frame_buffer(writer);
        let mut connection = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
            .await
            .unwrap()
            .unwrap();
        drop(builder);
        let (request, mut response) = tokio::time::timeout(DEADLINE, connection.accept())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(request);
        let stream = response
            .send_response(
                Response::builder()
                    .header("x-block", large_value())
                    .body(())
                    .unwrap(),
                true,
            )
            .unwrap();
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        let initial = writes.lock().unwrap().wire.len();
        {
            let mut state = writes.lock().unwrap();
            state.allowance = FRAME + 9 + 7;
            state.write_limit = 3;
            state.block_flush = true;
        }
        assert!(connection.poll_closed(&mut cx).is_pending());
        assert_eq!(
            writes.lock().unwrap().wire.len() - initial,
            FRAME + 9 + 7,
            "must suspend inside a real CONT frame"
        );
        held(&budget);
        let partial = writes.lock().unwrap().wire.len();
        for _ in 0..3 {
            assert!(connection.poll_closed(&mut cx).is_pending());
            assert_eq!(writes.lock().unwrap().wire.len(), partial);
            held(&budget);
        }
        writes.lock().unwrap().allowance = usize::MAX;
        assert!(connection.poll_closed(&mut cx).is_pending());
        assert_continuations(&writes, false);
        let full = writes.lock().unwrap().wire.len();
        for _ in 0..3 {
            assert!(connection.poll_closed(&mut cx).is_pending());
            assert_eq!(writes.lock().unwrap().wire.len(), full);
            held(&budget);
        }
        drop(stream);
        drop(response);
        writes.lock().unwrap().block_flush = false;
        assert!(connection.poll_closed(&mut cx).is_pending());
        held(&budget);
        drop(connection);
        drop(peer);
        released(&budget, total);
    }
}

#[tokio::test]
async fn client_original_grant_survives_partial_continuations_and_flush() {
    let (io, peer, writes) = fixture(false, 0, true).await;
    let (pool, writer, budget, total) = funded_with_writer(MAX_LIST);
    let mut builder = h2::client::Builder::new();
    builder
        .max_send_header_table_size(0)
        .send_header_block_pool(pool)
        .send_frame_buffer(writer);
    let (mut sender, mut connection) =
        tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
            .await
            .unwrap()
            .unwrap();
    drop(builder);
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let (response, stream) = sender
        .send_request(
            Request::builder()
                .uri("http://localhost/")
                .header("x-block", large_value())
                .body(())
                .unwrap(),
            true,
        )
        .unwrap();
    let initial = writes.lock().unwrap().wire.len();
    {
        let mut state = writes.lock().unwrap();
        state.allowance = FRAME + 9 + 7;
        state.write_limit = 3;
        state.block_flush = true;
    }
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    assert_eq!(writes.lock().unwrap().wire.len() - initial, FRAME + 9 + 7);
    held(&budget);
    writes.lock().unwrap().allowance = usize::MAX;
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    assert_continuations(&writes, true);
    let full = writes.lock().unwrap().wire.len();
    for _ in 0..3 {
        assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
        assert_eq!(writes.lock().unwrap().wire.len(), full);
        held(&budget);
    }
    drop(response);
    drop(stream);
    drop(sender);
    held(&budget);
    drop(connection);
    drop(peer);
    released(&budget, total);
}

#[tokio::test]
async fn once_bound_and_explicit_zero_policy_refuse_before_any_io() {
    for server in [false, true] {
        for wrong_cap in [None, Some(1)] {
            let (io, peer, writes) = fixture(server, 0, true).await;
            let (pool, budget, total) = funded(MAX_LIST);
            if server {
                let mut builder = h2::server::Builder::new();
                builder.send_header_block_pool(pool);
                if let Some(cap) = wrong_cap {
                    builder.max_send_header_table_size(cap);
                }
                assert!(
                    tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
                        .await
                        .unwrap()
                        .is_err()
                );
                drop(builder);
            } else {
                let mut builder = h2::client::Builder::new();
                builder.send_header_block_pool(pool);
                if let Some(cap) = wrong_cap {
                    builder.max_send_header_table_size(cap);
                }
                assert!(
                    tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
                        .await
                        .unwrap()
                        .is_err()
                );
                drop(builder);
            }
            let state = writes.lock().unwrap();
            assert_eq!(
                (state.read_calls, state.write_calls, state.wire.len()),
                (0, 0, 0)
            );
            drop(state);
            drop(peer);
            released(&budget, total);
        }
        let (io, peer, _) = fixture(server, 0, true).await;
        let (second_io, second_peer, writes) = fixture(server, 0, true).await;
        let (pool, budget, total) = funded(MAX_LIST);
        if server {
            let mut builder = h2::server::Builder::new();
            builder
                .max_send_header_table_size(0)
                .send_header_block_pool(pool);
            let first = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
                .await
                .unwrap()
                .unwrap();
            assert!(
                tokio::time::timeout(DEADLINE, builder.clone().handshake::<_, Bytes>(second_io))
                    .await
                    .unwrap()
                    .is_err()
            );
            drop(first);
            held(&budget);
            drop(builder);
        } else {
            let mut builder = h2::client::Builder::new();
            builder
                .max_send_header_table_size(0)
                .send_header_block_pool(pool);
            let first = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
                .await
                .unwrap()
                .unwrap();
            assert!(
                tokio::time::timeout(DEADLINE, builder.clone().handshake::<_, Bytes>(second_io))
                    .await
                    .unwrap()
                    .is_err()
            );
            drop(first);
            held(&budget);
            drop(builder);
        }
        let state = writes.lock().unwrap();
        assert_eq!(
            (state.read_calls, state.write_calls, state.wire.len()),
            (0, 0, 0)
        );
        drop(state);
        drop(peer);
        drop(second_peer);
        released(&budget, total);
    }
}

async fn response_bound(max: usize, duplicates: usize, accepted: bool) {
    let (io, peer, writes) = fixture(true, 1, true).await;
    let (pool, writer, budget, total) = funded_with_writer(max);
    let mut builder = h2::server::Builder::new();
    builder
        .max_send_header_table_size(0)
        .send_header_block_pool(pool)
        .send_frame_buffer(writer);
    let mut connection = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
        .await
        .unwrap()
        .unwrap();
    drop(builder);
    let (request, mut response) = connection.accept().await.unwrap().unwrap();
    drop(request);
    let mut head = Response::new(());
    for _ in 0..duplicates {
        head.headers_mut().append(
            "x",
            HeaderValue::from_static("123456789012345678901234567890"),
        );
    }
    let stream = response.send_response(head, true).unwrap();
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    let result = catch_unwind(AssertUnwindSafe(|| connection.poll_closed(&mut cx)))
        .expect("header refusal must not panic the real connection");
    if accepted {
        assert!(result.is_pending());
    } else {
        let Poll::Ready(Err(error)) = result else {
            panic!("oversize complete decoded list must fail the actual connection");
        };
        assert_eq!(error.get_io().unwrap().kind(), io::ErrorKind::InvalidInput);
    }
    let state = writes.lock().unwrap();
    assert_eq!(
        state
            .frames(false)
            .iter()
            .filter(|frame| frame.0 == 1 || frame.0 == 9)
            .count(),
        usize::from(accepted)
    );
    drop(state);
    held(&budget);
    drop(stream);
    drop(response);
    drop(connection);
    drop(peer);
    released(&budget, total);
}
#[tokio::test]
async fn exact_decoded_boundary_includes_status_and_each_duplicate() {
    tokio::time::timeout(DEADLINE, async {
        // :status=42; each x=30-byte-value costs 1+30+32=63.
        for duplicates in [1, 2] {
            let exact = 42 + duplicates * 63;
            response_bound(exact, duplicates, true).await;
            response_bound(exact - 1, duplicates, false).await;
        }
    })
    .await
    .expect("decoded response boundary fixture stalled");
}

#[tokio::test]
async fn extended_connect_protocol_is_counted_before_encoding() {
    let (io, peer, writes) = fixture(false, 0, true).await;
    // CONNECT + scheme + authority + path = 178, :protocol adds 50.
    let (pool, writer, budget, total) = funded_with_writer(220);
    let mut builder = h2::client::Builder::new();
    builder
        .max_send_header_table_size(0)
        .send_header_block_pool(pool)
        .send_frame_buffer(writer);
    let (mut sender, mut connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    assert!(sender.is_extended_connect_protocol_enabled());
    let mut request = Request::builder()
        .method("CONNECT")
        .uri("http://localhost/")
        .body(())
        .unwrap();
    request
        .extensions_mut()
        .insert(h2::ext::Protocol::from_static("websocket"));
    let (response, stream) = sender.send_request(request, true).unwrap();
    let result = catch_unwind(AssertUnwindSafe(|| Pin::new(&mut connection).poll(&mut cx)))
        .expect("protocol bound refusal must not panic");
    let Poll::Ready(Err(error)) = result else {
        panic!("complete protocol pseudo list must refuse before encode");
    };
    assert_eq!(error.get_io().unwrap().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        writes
            .lock()
            .unwrap()
            .frames(true)
            .iter()
            .filter(|frame| frame.0 == 1 || frame.0 == 9)
            .count(),
        0
    );
    held(&budget);
    drop(response);
    drop(stream);
    drop(sender);
    drop(connection);
    drop(peer);
    released(&budget, total);
}

#[tokio::test]
async fn original_grant_waits_for_actual_drop_after_write_flush_errors_and_cancel() {
    for exit in 0..4 {
        let (io, peer, writes) = fixture(true, 1, true).await;
        let (pool, writer, budget, total) = funded_with_writer(MAX_LIST);
        let mut builder = h2::server::Builder::new();
        builder
            .max_send_header_table_size(0)
            .send_header_block_pool(pool)
            .send_frame_buffer(writer);
        let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
        drop(builder);
        let (request, mut response) = connection.accept().await.unwrap().unwrap();
        drop(request);
        let stream = response
            .send_response(
                Response::builder()
                    .header("x-block", large_value())
                    .body(())
                    .unwrap(),
                true,
            )
            .unwrap();
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        {
            let mut state = writes.lock().unwrap();
            state.allowance = 1;
            state.block_flush = true;
        }
        assert!(connection.poll_closed(&mut cx).is_pending());
        held(&budget);
        if exit != 0 {
            {
                let mut state = writes.lock().unwrap();
                state.allowance = usize::MAX;
                match exit {
                    1 => state.write_error = true,
                    2 => state.flush_error = true,
                    3 => state.write_zero = true,
                    _ => unreachable!(),
                }
            }
            assert!(matches!(
                connection.poll_closed(&mut cx),
                Poll::Ready(Err(_))
            ));
            held(&budget);
        }
        drop(stream);
        drop(response);
        held(&budget);
        drop(connection);
        drop(peer);
        released(&budget, total);
    }
}

struct EmptyBody;
impl Body for EmptyBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(None)
    }
    fn is_end_stream(&self) -> bool {
        true
    }
}
#[tokio::test]
async fn cloned_hyper_server_retains_original_pool_and_real_peer_decodes_continuations() {
    tokio::time::timeout(DEADLINE, async {
        let (server_io, client_io) = tokio::io::duplex(65536);
        let (pool, writer, budget, total) = funded_with_writer(MAX_LIST);
        let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
        builder
            .max_send_header_table_size(0)
            .send_header_block_pool(pool)
            .send_frame_buffer(writer);
        let service = service_fn(|_: Request<hyper::body::Incoming>| async {
            Ok::<_, Infallible>(
                Response::builder()
                    .header("x-block", large_value())
                    .body(EmptyBody)
                    .unwrap(),
            )
        });
        let connection = builder
            .clone()
            .serve_connection(TokioIo::new(server_io), service);
        drop(builder);
        let server = tokio::spawn(connection);
        held(&budget);
        let (mut sender, connection) = h2::client::handshake(client_io).await.unwrap();
        let client = tokio::spawn(connection);
        let (response, stream) = sender
            .send_request(
                Request::builder()
                    .uri("http://localhost/")
                    .body(())
                    .unwrap(),
                true,
            )
            .unwrap();
        drop(stream);
        assert_eq!(response.await.unwrap().headers()["x-block"], large_value());
        held(&budget);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        released(&budget, total);
        drop(sender);
        client.await.unwrap().unwrap();
    })
    .await
    .expect("Hyper server original header pool fixture stalled");
}
#[tokio::test]
async fn cloned_hyper_client_retains_original_pool_and_real_peer_decodes_continuations() {
    tokio::time::timeout(DEADLINE, async {
        let (server_io, client_io) = tokio::io::duplex(65536);
        let server = tokio::spawn(async move {
            let mut connection = h2::server::handshake(server_io).await.unwrap();
            let (request, mut response) = connection.accept().await.unwrap().unwrap();
            assert_eq!(request.headers()["x-block"], large_value());
            drop(request);
            drop(response.send_response(Response::new(()), true).unwrap());
            assert!(connection.accept().await.is_none());
        });
        let (pool, writer, budget, total) = funded_with_writer(MAX_LIST);
        let mut builder = hyper::client::conn::http2::Builder::new(TokioExecutor::new());
        builder
            .max_send_header_table_size(0)
            .send_header_block_pool(pool)
            .send_frame_buffer(writer);
        let (mut sender, connection) = builder
            .clone()
            .handshake::<_, EmptyBody>(TokioIo::new(client_io))
            .await
            .unwrap();
        drop(builder);
        let client = tokio::spawn(connection);
        held(&budget);
        let response = sender
            .send_request(
                Request::builder()
                    .uri("http://localhost/")
                    .header("x-block", large_value())
                    .body(EmptyBody)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        drop(response);
        held(&budget);
        drop(sender);
        client.await.unwrap().unwrap();
        released(&budget, total);
        server.await.unwrap();
    })
    .await
    .expect("Hyper client original header pool fixture stalled");
}

#[tokio::test]
async fn absent_header_pool_keeps_the_unbounded_upstream_header_path() {
    let (io, peer, writes) = fixture(true, 1, true).await;
    let mut builder = h2::server::Builder::new();
    builder
        .max_send_header_table_size(0)
        .send_frame_buffer(SendFrameBuffer::new(CAPACITY, FRAME, Bytes::new()).unwrap());
    let mut connection = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
        .await
        .unwrap()
        .unwrap();
    drop(builder);
    let (request, mut response) = tokio::time::timeout(DEADLINE, connection.accept())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(request);
    // The decoded value alone exceeds the opt-in profile. Default None must
    // retain upstream semantics while the separately fixed writer frames it.
    let stream = response
        .send_response(
            Response::builder()
                .header(
                    "x-block",
                    HeaderValue::from_bytes(&vec![b'~'; 40000]).unwrap(),
                )
                .body(())
                .unwrap(),
            true,
        )
        .unwrap();
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    assert!(connection.poll_closed(&mut cx).is_pending());
    let state = writes.lock().unwrap();
    let frames = state.frames(false);
    let headers: Vec<_> = frames
        .iter()
        .filter(|frame| frame.0 == 1 || frame.0 == 9)
        .collect();
    assert_eq!(
        headers.iter().map(|frame| frame.0).collect::<Vec<_>>(),
        [1, 9, 9, 9]
    );
    assert_eq!(
        headers.iter().map(|frame| frame.1 & 4).collect::<Vec<_>>(),
        [0, 0, 0, 4]
    );
    assert!(headers.iter().all(|frame| frame.3.len() <= FRAME));
    assert!(headers.iter().map(|frame| frame.3.len()).sum::<usize>() > MAX_LIST);
    drop(state);
    drop(stream);
    drop(response);
    drop(connection);
    drop(peer);
}
