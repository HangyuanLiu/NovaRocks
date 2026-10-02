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

//! Public H2 kernel acquisition boundaries with actual scripted socket I/O.
//! Short component deadlines are test inputs, not Native's frozen two seconds.
//! The fixture has no whole-connection allocation or original-credit claim.

use bytes::Bytes;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const WATCHDOG: Duration = Duration::from_secs(5);
const COMPONENT_BUDGET: Duration = Duration::from_millis(200);
const UNKNOWN: [u8; 9] = [0, 0, 0, 255, 0, 0, 0, 0, 0];

#[derive(Clone, Copy, Debug)]
enum Side {
    Client,
    Server,
}
#[derive(Clone, Copy)]
enum FlushMode {
    Ready,
    PendingAfterPeerAck,
    LateReadyAfterPeerAck(Instant),
}
struct IoState {
    input: Vec<u8>,
    cursor: usize,
    output: Vec<u8>,
    infinite_unknown: bool,
    unknown_bytes: usize,
    reads: usize,
    writes: usize,
    flushes: usize,
    successful_flushes: usize,
    flush_mode: FlushMode,
}
impl IoState {
    fn operations(&self) -> (usize, usize, usize) {
        (self.reads, self.writes, self.flushes)
    }
    fn has_frame(&self, kind: u8, flags: u8, payload: &[u8]) -> bool {
        let mut offset = usize::from(self.output.starts_with(PREFACE)) * PREFACE.len();
        while self.output.len().saturating_sub(offset) >= 9 {
            let head = &self.output[offset..offset + 9];
            let len =
                (usize::from(head[0]) << 16) | (usize::from(head[1]) << 8) | usize::from(head[2]);
            let end = offset + 9 + len;
            if end > self.output.len() {
                return false;
            }
            if head[3] == kind
                && head[4] == flags
                && head[5..9] == [0, 0, 0, 0]
                && self.output[offset + 9..end] == *payload
            {
                return true;
            }
            offset = end;
        }
        false
    }
}
struct ScriptIo(Arc<Mutex<IoState>>);
impl AsyncRead for ScriptIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.0.lock().unwrap();
        state.reads += 1;
        if state.cursor < state.input.len() {
            let start = state.cursor;
            let count = buf.remaining().min(state.input.len() - start);
            buf.put_slice(&state.input[start..start + count]);
            state.cursor += count;
            return Poll::Ready(Ok(()));
        }
        if state.infinite_unknown {
            let count = buf.remaining();
            for _ in 0..count {
                buf.put_slice(&[UNKNOWN[state.unknown_bytes % UNKNOWN.len()]]);
                state.unknown_bytes += 1;
            }
            return Poll::Ready(Ok(()));
        }
        // Exhausted scripted input leaves a live socket, not EOF.
        Poll::Pending
    }
}
impl AsyncWrite for ScriptIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.0.lock().unwrap();
        state.writes += 1;
        state.output.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.0.lock().unwrap();
        state.flushes += 1;
        if state.has_frame(4, 1, &[]) {
            match state.flush_mode {
                FlushMode::PendingAfterPeerAck => return Poll::Pending,
                FlushMode::LateReadyAfterPeerAck(deadline) => {
                    // This models an I/O poll that returns Ready only after D.
                    // The measured clock at return, not the delay, is the oracle.
                    while Instant::now() < deadline {
                        std::thread::park_timeout(
                            deadline.saturating_duration_since(Instant::now()),
                        );
                    }
                    assert!(Instant::now() >= deadline);
                }
                FlushMode::Ready => (),
            }
        }
        state.successful_flushes += 1;
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
fn frame(kind: u8, flags: u8, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    let mut wire = vec![
        (len >> 16) as u8,
        (len >> 8) as u8,
        len as u8,
        kind,
        flags,
        0,
        0,
        0,
        0,
    ];
    wire.extend_from_slice(payload);
    wire
}
fn settings_seven() -> Vec<u8> {
    // SETTINGS_MAX_CONCURRENT_STREAMS = 7, independently observed below.
    frame(4, 0, &[0, 3, 0, 0, 0, 7])
}
fn make_io(
    side: Side,
    wire: Vec<u8>,
    mode: FlushMode,
    infinite_unknown: bool,
) -> (ScriptIo, Arc<Mutex<IoState>>) {
    let mut input = Vec::with_capacity(PREFACE.len() + wire.len());
    if matches!(side, Side::Server) {
        input.extend_from_slice(PREFACE);
    }
    input.extend(wire);
    let state = Arc::new(Mutex::new(IoState {
        input,
        cursor: 0,
        output: Vec::with_capacity(65536),
        infinite_unknown,
        unknown_bytes: 0,
        reads: 0,
        writes: 0,
        flushes: 0,
        successful_flushes: 0,
        flush_mode: mode,
    }));
    (ScriptIo(state.clone()), state)
}
enum Connection {
    Client(Box<h2::client::Connection<ScriptIo, Bytes>>),
    Server(Box<h2::server::Connection<ScriptIo, Bytes>>),
}
struct Kernel {
    connection: Connection,
    // Retain the real client handle so ordinary post-acquisition polling does
    // not close a reference-free connection before exercising its socket.
    _sender: Option<h2::client::SendRequest<Bytes>>,
}
impl Kernel {
    fn poll_phase(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), h2::Error>> {
        match &mut self.connection {
            Connection::Client(connection) => connection.poll_initial_settings(cx),
            Connection::Server(connection) => connection.poll_initial_settings(cx),
        }
    }
    fn poll_once(&mut self) -> Poll<Result<(), h2::Error>> {
        self.poll_phase(&mut Context::from_waker(Waker::noop()))
    }
}
async fn open(side: Side, io: ScriptIo, deadline: Instant) -> Result<Kernel, h2::Error> {
    tokio::time::timeout(WATCHDOG, async move {
        // Fixed raw input makes the unknown-frame read count an exact wire
        // oracle. Its fixture storage is independent of Native funding.
        let raw = h2::ReceiveFrameBuffer::new(16384, Bytes::new()).unwrap();
        match side {
            Side::Client => {
                let mut builder = h2::client::Builder::new();
                builder
                    .initial_settings_deadline(deadline)
                    .receive_frame_buffer(raw);
                let (sender, connection) = builder.handshake(io).await?;
                Ok(Kernel {
                    connection: Connection::Client(Box::new(connection)),
                    _sender: Some(sender),
                })
            }
            Side::Server => {
                let mut builder = h2::server::Builder::new();
                builder
                    .initial_settings_deadline(deadline)
                    .receive_frame_buffer(raw);
                let connection = builder.handshake::<_, Bytes>(io).await?;
                Ok(Kernel {
                    connection: Connection::Server(Box::new(connection)),
                    _sender: None,
                })
            }
        }
    })
    .await
    .expect("old handshake exceeded fixture watchdog")
}
fn phase_error(kernel: &mut Kernel) -> h2::Error {
    match kernel.poll_once() {
        Poll::Ready(Err(error)) => error,
        other => panic!("expected phase refusal, got {other:?}"),
    }
}
fn assert_io_kind(error: &h2::Error, kind: io::ErrorKind) {
    assert_eq!(error.get_io().expect("expected I/O error").kind(), kind);
}
async fn expire(deadline: Instant) {
    tokio::time::timeout(WATCHDOG, async {
        while Instant::now() < deadline {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
        }
    })
    .await
    .expect("component deadline did not expire");
    assert!(Instant::now() >= deadline);
}

#[tokio::test]
async fn expired_old_handshakes_refuse_before_any_socket_operation() {
    for side in [Side::Client, Side::Server] {
        let (io, state) = make_io(side, settings_seven(), FlushMode::Ready, false);
        let deadline = Instant::now() - Duration::from_millis(1);
        let error = match open(side, io, deadline).await {
            Err(error) => error,
            Ok(_) => panic!("expired {side:?} old handshake published a connection"),
        };
        assert_io_kind(&error, io::ErrorKind::TimedOut);
        assert_eq!(state.lock().unwrap().operations(), (0, 0, 0));
    }
}

#[tokio::test]
async fn peer_ack_and_ping_do_not_establish_initial_settings() {
    for side in [Side::Client, Side::Server] {
        tokio::task::yield_now().await;
        let mut wire = frame(4, 1, &[]);
        wire.extend(frame(6, 0, b"abcdefgh"));
        let (io, state) = make_io(side, wire, FlushMode::Ready, false);
        let mut kernel = open(side, io, Instant::now() + COMPONENT_BUDGET)
            .await
            .unwrap();
        assert!(kernel.poll_once().is_pending(), "{side:?}");
        assert!(kernel.poll_once().is_pending(), "{side:?}");
        let state = state.lock().unwrap();
        assert_eq!(state.cursor, state.input.len());
        assert!(
            state.has_frame(6, 1, b"abcdefgh"),
            "actual PONG missing: {side:?}"
        );
        assert!(state.successful_flushes > 0);
    }
}

#[tokio::test]
async fn malformed_settings_refuse_and_cannot_be_repaired_by_repoll() {
    for side in [Side::Client, Side::Server] {
        tokio::task::yield_now().await;
        // The actual frame loader rejects a window larger than 2^31 - 1.
        // Its existing public error mapping is PROTOCOL_ERROR.
        let wire = frame(4, 0, &[0, 4, 0x80, 0, 0, 0]);
        let (io, _) = make_io(side, wire, FlushMode::Ready, false);
        let mut kernel = open(side, io, Instant::now() + COMPONENT_BUDGET)
            .await
            .unwrap();
        assert_eq!(
            phase_error(&mut kernel).reason(),
            Some(h2::Reason::PROTOCOL_ERROR)
        );
        assert_io_kind(&phase_error(&mut kernel), io::ErrorKind::ConnectionAborted);
    }
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);
impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
#[tokio::test]
async fn always_ready_unknown_frames_yield_at_32_and_expiry_stops_reads() {
    for side in [Side::Client, Side::Server] {
        tokio::task::yield_now().await;
        let deadline = Instant::now() + COMPONENT_BUDGET;
        let (io, state) = make_io(side, Vec::new(), FlushMode::Ready, true);
        let mut kernel = open(side, io, deadline).await.unwrap();
        let wakes = Arc::new(WakeCounter::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        for turn in 1..=2 {
            let reads_before = state.lock().unwrap().reads;
            assert!(
                kernel.poll_phase(&mut cx).is_pending(),
                "{side:?} turn {turn}"
            );
            let state = state.lock().unwrap();
            assert_eq!(state.unknown_bytes, turn * 32 * 9, "{side:?} turn {turn}");
            assert_eq!(state.reads - reads_before, 32, "{side:?} turn {turn}");
        }
        assert!(wakes.0.load(Ordering::Relaxed) >= 2);
        expire(deadline).await;
        let before = state.lock().unwrap().operations();
        assert_io_kind(&phase_error(&mut kernel), io::ErrorKind::TimedOut);
        assert_eq!(state.lock().unwrap().operations(), before);
    }
}

#[tokio::test]
async fn applied_peer_settings_wait_for_real_flush_then_disarm_deadline() {
    let deadline = Instant::now() + COMPONENT_BUDGET;
    let (io, state) = make_io(
        Side::Client,
        settings_seven(),
        FlushMode::PendingAfterPeerAck,
        false,
    );
    let mut kernel = open(Side::Client, io, deadline).await.unwrap();
    assert!(kernel.poll_once().is_pending());
    let Connection::Client(connection) = &kernel.connection else {
        unreachable!()
    };
    assert_eq!(connection.max_concurrent_send_streams(), 7);
    {
        let state = state.lock().unwrap();
        assert!(state.has_frame(4, 1, &[]));
        assert!(state.flushes > state.successful_flushes);
    }
    assert!(kernel.poll_once().is_pending());
    state.lock().unwrap().flush_mode = FlushMode::Ready;
    assert!(matches!(kernel.poll_once(), Poll::Ready(Ok(()))));
    assert!(matches!(kernel.poll_once(), Poll::Ready(Ok(()))));
    expire(deadline).await;
    state.lock().unwrap().input.extend(frame(6, 0, b"longlive"));
    let Connection::Client(connection) = &mut kernel.connection else {
        unreachable!()
    };
    assert!(
        Pin::new(connection.as_mut())
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(state.lock().unwrap().has_frame(6, 1, b"longlive"));
}

#[tokio::test]
async fn pending_ack_flush_cannot_recover_to_ready_after_deadline() {
    for side in [Side::Client, Side::Server] {
        tokio::task::yield_now().await;
        let deadline = Instant::now() + COMPONENT_BUDGET;
        let (io, state) = make_io(
            side,
            settings_seven(),
            FlushMode::PendingAfterPeerAck,
            false,
        );
        let mut kernel = open(side, io, deadline).await.unwrap();
        assert!(kernel.poll_once().is_pending(), "{side:?}");
        assert!(state.lock().unwrap().has_frame(4, 1, &[]));
        expire(deadline).await;
        state.lock().unwrap().flush_mode = FlushMode::Ready;
        let before = state.lock().unwrap().operations();
        assert_io_kind(&phase_error(&mut kernel), io::ErrorKind::TimedOut);
        assert_eq!(state.lock().unwrap().operations(), before);
        assert_io_kind(&phase_error(&mut kernel), io::ErrorKind::ConnectionAborted);
    }
}

#[tokio::test]
async fn actual_ack_flush_returning_ready_late_does_not_publish_acquisition() {
    for side in [Side::Client, Side::Server] {
        tokio::task::yield_now().await;
        let deadline = Instant::now() + COMPONENT_BUDGET;
        let (io, state) = make_io(
            side,
            settings_seven(),
            FlushMode::LateReadyAfterPeerAck(deadline),
            false,
        );
        let mut kernel = open(side, io, deadline).await.unwrap();
        assert_io_kind(&phase_error(&mut kernel), io::ErrorKind::TimedOut);
        let state = state.lock().unwrap();
        assert!(Instant::now() >= deadline);
        assert!(state.has_frame(4, 1, &[]));
        assert!(
            state.successful_flushes >= 2,
            "actual ACK flush did not return Ready: {side:?}"
        );
        drop(state);
        assert_io_kind(&phase_error(&mut kernel), io::ErrorKind::ConnectionAborted);
    }
}
