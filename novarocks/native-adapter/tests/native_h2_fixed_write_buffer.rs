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

//! Actual h2 writes from one fixed contiguous allocation and original credit.
//! HPACK blocks, HeaderMap/stream/task storage and original DATA are separate.
//! The pointer oracle proves no writer relocation/growth, not Vec deallocation.

use bytes::Bytes;
use h2::SendFrameBuffer;
use hyper::http::{HeaderValue, Request, Response};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::future::Future;
use std::io::{self, IoSlice};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

const CAPACITY: usize = 65536;
const FRAME: usize = 16384;
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const DEADLINE: Duration = Duration::from_secs(5);

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

fn funded() -> (SendFrameBuffer, Arc<ResultRetainedBudget>, usize) {
    let backing = SendFrameBuffer::allocation_capacity_bound(CAPACITY, FRAME).unwrap();
    let carrier = Bytes::owner_with_exit_guard_metadata_size::<GrantMarker, ResultWriteCredit>();
    let total = backing.checked_add(carrier).unwrap();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap() else {
        panic!("fixed writer pregrant");
    };
    let owner = Bytes::from_owner_with_exit_guard(GrantMarker, credit);
    let buffer = SendFrameBuffer::new(CAPACITY, FRAME, owner).unwrap();
    assert_eq!(buffer.capacity_bytes(), CAPACITY);
    assert_eq!(buffer.max_payload_bytes(), FRAME);
    (buffer, budget, total)
}

fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}

fn released(budget: &Arc<ResultRetainedBudget>, total: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap() else {
        panic!("fixed writer carrier must leave with the actual owner");
    };
    drop(credit);
}

struct Writes {
    wire: Vec<u8>,
    fixed_base: Option<usize>,
    enforce_fixed: bool,
    payload: Option<(usize, usize)>,
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
    fn new(enforce_fixed: bool) -> Self {
        Self {
            wire: Vec::new(),
            fixed_base: None,
            enforce_fixed,
            payload: None,
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

    fn check_storage(&mut self, bytes: &[u8]) {
        if bytes.is_empty() || bytes == PREFACE || !self.enforce_fixed {
            return;
        }
        let pointer = bytes.as_ptr() as usize;
        if self
            .payload
            .is_some_and(|(start, len)| pointer >= start && pointer < start + len)
        {
            return;
        }
        // The first real frame write starts at the writer's byte-zero address.
        // Later partial writes may start inside the same full allocation.
        let base = *self.fixed_base.get_or_insert(pointer);
        assert!(pointer >= base, "fixed writer changed its allocation base");
        let end = pointer.checked_add(bytes.len()).unwrap();
        assert!(
            end <= base + CAPACITY,
            "fixed writer escaped its full backing"
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
            self.check_storage(bytes);
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
    enforce_fixed: bool,
) -> (SpyIo, DuplexStream, Arc<Mutex<Writes>>) {
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut wire = Vec::new();
    if server {
        wire.extend_from_slice(PREFACE);
    }
    // A legal large peer value must not replace the local outbound bound.
    let settings = [0, 5, 0, 255, 255, 255, 0, 1, 255, 255, 255, 255];
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
    let writes = Arc::new(Mutex::new(Writes::new(enforce_fixed)));
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

fn large_header(index: usize, length: usize) -> HeaderValue {
    // The peer permits a huge dynamic table, so use distinct full values to
    // prevent indexed compression from hiding actual writer-buffer batching.
    let mut value = vec![b'z'; length];
    let suffix = format!("{index:08x}");
    value[length - 8..].copy_from_slice(suffix.as_bytes());
    HeaderValue::from_bytes(&value).unwrap()
}

#[tokio::test]
async fn server_many_headers_share_one_fixed_write_allocation() {
    for header_len in [15200, 24000] {
        for vectored in [false, true] {
            let (io, peer, writes) = fixture(true, 64, vectored, true).await;
            let (buffer, budget, total) = funded();
            let mut builder = h2::server::Builder::new();
            builder.send_frame_buffer(buffer);
            let mut connection = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
                .await
                .expect("bounded server handshake")
                .unwrap();
            drop(builder);
            let mut responses = Vec::new();
            for _ in 0..64 {
                let (request, response) = tokio::time::timeout(DEADLINE, connection.accept())
                    .await
                    .expect("bounded request accept")
                    .unwrap()
                    .unwrap();
                drop(request);
                responses.push(response);
            }
            for (index, mut response) in responses.into_iter().enumerate() {
                let mut head = Response::new(());
                head.headers_mut()
                    .insert("x-writer-batch", large_header(index, header_len));
                drop(response.send_response(head, true).unwrap());
            }
            let waker = Waker::from(Arc::new(Noop));
            let mut cx = Context::from_waker(&waker);
            assert!(connection.poll_closed(&mut cx).is_pending());
            held(&budget);
            let state = writes.lock().unwrap();
            let frames = state.frames(false);
            assert_eq!(frames.iter().filter(|f| f.0 == 1).count(), 64);
            assert_eq!(
                frames.iter().filter(|f| f.0 == 9).count(),
                if header_len == 24000 { 64 } else { 0 }
            );
            assert!(
                frames
                    .iter()
                    .filter(|f| f.0 == 1)
                    .all(|f| f.1 & 4 == if header_len == 24000 { 0 } else { 4 })
            );
            assert!(frames.iter().filter(|f| f.0 == 9).all(|f| f.1 & 4 == 4));
            assert!(frames.iter().all(|f| f.3.len() <= FRAME));
            assert!(state.fixed_base.is_some());
            assert!(
                state.wire.len() > CAPACITY * 8,
                "exercise repeated buffer reuse"
            );
            drop(state);
            drop(connection);
            drop(peer);
            released(&budget, total);
        }
    }
}

#[tokio::test]
async fn client_many_headers_share_one_fixed_write_allocation() {
    for header_len in [15200, 24000] {
        let (io, peer, writes) = fixture(false, 0, true, true).await;
        let (buffer, budget, total) = funded();
        let mut builder = h2::client::Builder::new();
        builder.send_frame_buffer(buffer);
        let (mut sender, mut connection) =
            tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
                .await
                .expect("bounded client handshake")
                .unwrap();
        drop(builder);
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
        let mut responses = Vec::new();
        for index in 0..64 {
            assert!(matches!(sender.poll_ready(&mut cx), Poll::Ready(Ok(()))));
            let mut request = Request::builder()
                .uri("http://localhost/")
                .body(())
                .unwrap();
            request
                .headers_mut()
                .insert("x-writer-batch", large_header(index, header_len));
            let (response, stream) = sender.send_request(request, true).unwrap();
            responses.push(response);
            drop(stream);
        }
        assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
        held(&budget);
        let state = writes.lock().unwrap();
        let frames = state.frames(true);
        assert_eq!(frames.iter().filter(|f| f.0 == 1).count(), 64);
        assert_eq!(
            frames.iter().filter(|f| f.0 == 9).count(),
            if header_len == 24000 { 64 } else { 0 }
        );
        assert!(
            frames
                .iter()
                .filter(|f| f.0 == 1)
                .all(|f| f.1 & 4 == if header_len == 24000 { 0 } else { 4 })
        );
        assert!(frames.iter().filter(|f| f.0 == 9).all(|f| f.1 & 4 == 4));
        assert!(frames.iter().all(|f| f.3.len() <= FRAME));
        assert!(state.wire.len() > CAPACITY * 8);
        drop(state);
        drop(responses);
        drop(sender);
        drop(connection);
        drop(peer);
        released(&budget, total);
    }
}

async fn server_data(fixed: bool) {
    let (io, peer, writes) = fixture(true, 1, true, fixed).await;
    let funding = fixed.then(funded);
    let mut builder = h2::server::Builder::new();
    builder.retain_data_payloads(true);
    let budget = funding.map(|(buffer, budget, total)| {
        builder.send_frame_buffer(buffer);
        (budget, total)
    });
    let mut connection = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
        .await
        .expect("bounded data handshake")
        .unwrap();
    drop(builder);
    let (request, mut response) = tokio::time::timeout(DEADLINE, connection.accept())
        .await
        .expect("bounded data request")
        .unwrap()
        .unwrap();
    drop(request);
    let mut stream = response.send_response(Response::new(()), false).unwrap();
    let data = Bytes::from(vec![0xa5; 32769]);
    {
        let mut state = writes.lock().unwrap();
        state.payload = Some((data.as_ptr() as usize, data.len()));
        state.write_limit = 3;
        state.allowance = 1;
        state.block_flush = true;
    }
    stream.send_data(data, true).unwrap();
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    assert!(connection.poll_closed(&mut cx).is_pending());
    if let Some((budget, _)) = &budget {
        held(budget);
    }
    assert!(connection.poll_closed(&mut cx).is_pending());
    {
        let mut state = writes.lock().unwrap();
        state.allowance = usize::MAX;
        state.block_flush = false;
    }
    assert!(connection.poll_closed(&mut cx).is_pending());
    let frames = writes.lock().unwrap().frames(false);
    let data: Vec<_> = frames.iter().filter(|f| f.0 == 0).collect();
    let lengths: Vec<_> = data.iter().map(|f| f.3.len()).collect();
    assert_eq!(
        lengths,
        if fixed {
            vec![FRAME, FRAME, 1]
        } else {
            vec![32769]
        }
    );
    assert_eq!(data.last().unwrap().1, 1);
    assert!(data[..data.len() - 1].iter().all(|f| f.1 == 0));
    assert!(data.iter().all(|f| f.3.iter().all(|&b| b == 0xa5)));
    drop(stream);
    drop(response);
    if let Some((budget, _)) = &budget {
        held(budget);
    }
    drop(connection);
    drop(peer);
    if let Some((budget, total)) = budget {
        released(&budget, total);
    }
}

#[tokio::test]
async fn large_peer_frame_is_clamped_locally_through_partial_writes() {
    server_data(true).await;
}

#[tokio::test]
async fn absent_fixed_buffer_keeps_the_peer_frame_behavior() {
    server_data(false).await;
}

#[derive(Clone, Copy)]
enum Exit {
    Cancel,
    WriteError,
    FlushError,
    WriteZero,
}

#[tokio::test]
async fn pending_flush_errors_and_cancel_keep_credit_until_writer_drop() {
    for exit in [
        Exit::Cancel,
        Exit::WriteError,
        Exit::FlushError,
        Exit::WriteZero,
    ] {
        let (io, peer, writes) = fixture(true, 1, false, true).await;
        let (buffer, budget, total) = funded();
        let mut builder = h2::server::Builder::new();
        builder.send_frame_buffer(buffer);
        let mut connection = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
            .await
            .expect("bounded exit handshake")
            .unwrap();
        drop(builder);
        let (request, mut response) = tokio::time::timeout(DEADLINE, connection.accept())
            .await
            .expect("bounded exit request")
            .unwrap()
            .unwrap();
        drop(request);
        let mut head = Response::new(());
        head.headers_mut()
            .insert("x-writer-batch", large_header(0, 15200));
        let stream = response.send_response(head, true).unwrap();
        writes.lock().unwrap().block_flush = true;
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        assert!(connection.poll_closed(&mut cx).is_pending());
        held(&budget);
        let written = writes.lock().unwrap().wire.len();
        for _ in 0..3 {
            assert!(connection.poll_closed(&mut cx).is_pending());
            assert_eq!(writes.lock().unwrap().wire.len(), written);
            held(&budget);
        }
        match exit {
            Exit::Cancel => (),
            Exit::WriteError | Exit::WriteZero => {
                // Queue a real control frame after the completed response.
                connection.abrupt_shutdown(h2::Reason::CANCEL);
                let mut state = writes.lock().unwrap();
                state.block_flush = false;
                state.write_error = matches!(exit, Exit::WriteError);
                state.write_zero = matches!(exit, Exit::WriteZero);
                drop(state);
                let Poll::Ready(Err(error)) = connection.poll_closed(&mut cx) else {
                    panic!("write failure must terminate the connection poll");
                };
                if matches!(exit, Exit::WriteZero) {
                    assert_eq!(error.get_io().unwrap().kind(), io::ErrorKind::WriteZero);
                }
            }
            Exit::FlushError => {
                writes.lock().unwrap().flush_error = true;
                assert!(matches!(
                    connection.poll_closed(&mut cx),
                    Poll::Ready(Err(_))
                ));
            }
        }
        drop(stream);
        drop(response);
        held(&budget);
        drop(connection);
        drop(peer);
        released(&budget, total);
    }
}

#[tokio::test]
async fn client_and_server_reused_buffer_refuse_before_any_preface_io() {
    for server in [false, true] {
        let (buffer, budget, total) = funded();
        let (first_io, first_peer, _) = fixture(server, 0, true, true).await;
        let (second_io, second_peer, writes) = fixture(server, 0, true, true).await;
        if server {
            let mut builder = h2::server::Builder::new();
            builder.send_frame_buffer(buffer);
            let first = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(first_io))
                .await
                .expect("bounded first server binding")
                .unwrap();
            let second =
                tokio::time::timeout(DEADLINE, builder.clone().handshake::<_, Bytes>(second_io))
                    .await
                    .expect("bounded reused server refusal");
            assert!(second.is_err());
            drop(first);
            held(&budget);
            drop(builder);
        } else {
            let mut builder = h2::client::Builder::new();
            builder.send_frame_buffer(buffer);
            let first = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(first_io))
                .await
                .expect("bounded first client binding")
                .unwrap();
            let second =
                tokio::time::timeout(DEADLINE, builder.clone().handshake::<_, Bytes>(second_io))
                    .await
                    .expect("bounded reused client refusal");
            assert!(second.is_err());
            drop(first);
            held(&budget);
            drop(builder);
        }
        let state = writes.lock().unwrap();
        assert_eq!(state.read_calls, 0);
        assert_eq!(state.write_calls, 0);
        assert!(state.wire.is_empty());
        drop(state);
        drop(first_peer);
        drop(second_peer);
        released(&budget, total);
    }
}
