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

//! Real DATA write pointers and original credit through partial write/flush.
//! Raw codec/header/TLS/task backing and queued Body count remain separate.

use std::convert::Infallible;
use std::future::Future;
use std::io::{self, IoSlice};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

#[derive(Default)]
struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}

struct Payload {
    bytes: [u8; 65536],
    len: usize,
}
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}
fn original(len: usize) -> (Bytes, Arc<ResultRetainedBudget>, usize) {
    let capacity = Bytes::owner_with_exit_guard_metadata_size::<Payload, ResultWriteCredit>();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(capacity).unwrap());
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(capacity).unwrap()
    else {
        panic!("original DATA pregrant");
    };
    let data = Bytes::from_owner_with_exit_guard(
        Payload {
            bytes: [0xa5; 65536],
            len,
        },
        credit,
    );
    (data, budget, capacity)
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn released(budget: &Arc<ResultRetainedBudget>, capacity: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(capacity).unwrap()
    else {
        panic!("original DATA credit must leave with the actual owner");
    };
    drop(credit);
}

struct Writes {
    wire: Vec<u8>,
    original_start: usize,
    original_len: usize,
    original_written: usize,
    payload_allowance: usize,
    write_limit: usize,
    block_flush: bool,
    flush_allowance: usize,
    zero_calls: usize,
    write_zero: bool,
    write_error: bool,
    flush_error: bool,
}
impl Default for Writes {
    fn default() -> Self {
        Self {
            wire: Vec::new(),
            original_start: 0,
            original_len: 0,
            original_written: 0,
            payload_allowance: usize::MAX,
            write_limit: usize::MAX,
            block_flush: false,
            flush_allowance: 0,
            zero_calls: 0,
            write_zero: false,
            write_error: false,
            flush_error: false,
        }
    }
}
impl Writes {
    fn arm(&mut self, data: &Bytes) {
        self.original_start = data.as_ptr() as usize;
        self.original_len = data.len();
        self.payload_allowance = usize::from(!data.is_empty());
        self.write_limit = 3;
        self.block_flush = true;
    }
    fn write(&mut self, slices: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        if self.write_error {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected write failure",
            )));
        }
        if self.write_zero {
            self.zero_calls += 1;
            assert_eq!(
                self.zero_calls, 1,
                "H2 must stop after the first zero write"
            );
            return Poll::Ready(Ok(0));
        }
        let mut written = 0;
        for slice in slices {
            if written == self.write_limit {
                break;
            }
            let start = slice.as_ptr() as usize;
            let original =
                start >= self.original_start && start < self.original_start + self.original_len;
            let mut count = slice.len().min(self.write_limit - written);
            if original {
                count = count.min(self.payload_allowance);
            }
            if count == 0 && !slice.is_empty() {
                break;
            }
            self.wire.extend_from_slice(&slice[..count]);
            if original {
                self.original_written += count;
                if self.payload_allowance != usize::MAX {
                    self.payload_allowance -= count;
                }
            }
            written += count;
            if count < slice.len() {
                break;
            }
        }
        if written == 0 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(written))
        }
    }
    fn data_frames(&self, client: bool) -> Vec<(u8, Vec<u8>)> {
        let mut wire = self.wire.as_slice();
        if client {
            assert!(wire.starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"));
            wire = &wire[24..];
        }
        let mut frames = Vec::new();
        while !wire.is_empty() {
            assert!(wire.len() >= 9);
            let size = ((wire[0] as usize) << 16) | ((wire[1] as usize) << 8) | wire[2] as usize;
            assert!(wire.len() >= 9 + size);
            if wire[3] == 0 {
                frames.push((wire[4], wire[9..9 + size].to_vec()));
            }
            wire = &wire[9 + size..];
        }
        frames
    }
}
struct SpyIo {
    read: tokio::io::DuplexStream,
    writes: Arc<Mutex<Writes>>,
    vectored: bool,
}
impl AsyncRead for SpyIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.read).poll_read(cx, buf)
    }
}
impl AsyncWrite for SpyIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.writes.lock().unwrap().write(&[IoSlice::new(data)])
    }
    fn is_write_vectored(&self) -> bool {
        self.vectored
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        data: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.writes.lock().unwrap().write(data)
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.writes.lock().unwrap();
        if state.flush_error {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected flush failure",
            )))
        } else if state.block_flush {
            if state.flush_allowance == 0 {
                Poll::Pending
            } else {
                state.flush_allowance -= 1;
                Poll::Ready(Ok(()))
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
fn frame(wire: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, body: &[u8]) {
    wire.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&[kind, flags]);
    wire.extend_from_slice(&stream.to_be_bytes());
    wire.extend_from_slice(body);
}
async fn server_io(vectored: bool) -> (SpyIo, tokio::io::DuplexStream, Arc<Mutex<Writes>>) {
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 5, 1, b"\x83\x86\x84\x01\x09localhost");
    peer.write_all(&wire).await.unwrap();
    let writes = Arc::new(Mutex::new(Writes::default()));
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
#[derive(Clone, Copy)]
enum Exit {
    Flush,
    Drop,
    Reset,
    ResetFlush,
    WriteError,
    FlushError,
    WriteZero,
}

async fn server_send(len: usize, vectored: bool, exit: Exit) {
    let (io, peer, writes) = server_io(vectored).await;
    let mut builder = h2::server::Builder::new();
    builder.retain_data_payloads(true);
    let mut connection = builder.clone().handshake::<_, Bytes>(io).await.unwrap();
    let (request, mut response) = connection.accept().await.unwrap().unwrap();
    drop(request);
    let mut stream = response
        .send_response(hyper::http::Response::new(()), false)
        .unwrap();
    let (data, budget, capacity) = original(len);
    writes.lock().unwrap().arm(&data);
    let reset = matches!(exit, Exit::Reset | Exit::ResetFlush);
    stream.send_data(data, !reset).unwrap();
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert_eq!(
        writes.lock().unwrap().original_written,
        usize::from(len != 0)
    );
    held(&budget);
    if matches!(exit, Exit::Flush | Exit::FlushError | Exit::ResetFlush) {
        writes.lock().unwrap().payload_allowance = usize::MAX;
        assert!(connection.poll_closed(&mut cx).is_pending());
        held(&budget);
        assert_eq!(writes.lock().unwrap().original_written, len);
        // Repeated poll_ready/reclaim cannot take the completed frame before flush.
        assert!(connection.poll_closed(&mut cx).is_pending());
        held(&budget);
        let frames = writes.lock().unwrap().data_frames(false);
        assert_eq!(frames, vec![(u8::from(!reset), vec![0xa5; len])]);
    }
    match exit {
        Exit::Flush => {
            writes.lock().unwrap().block_flush = false;
            assert!(connection.poll_closed(&mut cx).is_pending());
            released(&budget, capacity);
        }
        Exit::Drop => (),
        Exit::Reset | Exit::ResetFlush => {
            stream.send_reset(h2::Reason::CANCEL);
            assert!(connection.poll_closed(&mut cx).is_pending());
            held(&budget);
            if matches!(exit, Exit::ResetFlush) {
                writes.lock().unwrap().block_flush = false;
                assert!(connection.poll_closed(&mut cx).is_pending());
                released(&budget, capacity);
                // The send side stayed open, so this is a real RST_STREAM,
                // not the closed/empty early-return in send_reset.
                assert!(
                    writes
                        .lock()
                        .unwrap()
                        .wire
                        .ends_with(&[0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 8,])
                );
            }
        }
        Exit::WriteError => {
            writes.lock().unwrap().write_error = true;
            assert!(matches!(
                connection.poll_closed(&mut cx),
                Poll::Ready(Err(_))
            ));
        }
        Exit::WriteZero => {
            writes.lock().unwrap().write_zero = true;
            let Poll::Ready(Err(error)) = connection.poll_closed(&mut cx) else {
                panic!("zero write must terminate");
            };
            assert_eq!(error.get_io().unwrap().kind(), io::ErrorKind::WriteZero);
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
    if !matches!(exit, Exit::Flush | Exit::ResetFlush) {
        held(&budget);
    }
    drop(connection);
    drop(builder);
    drop(peer);
    released(&budget, capacity);
}

#[tokio::test]
async fn h2_retains_original_empty_tiny_and_full_data_through_vectored_and_scalar_flush() {
    for vectored in [false, true] {
        for len in [0, 1, 255, 256, 1023, 1024, 16384] {
            server_send(len, vectored, Exit::Flush).await;
        }
    }
}
#[tokio::test]
async fn h2_partial_write_cancel_and_io_errors_release_the_actual_owner() {
    for exit in [
        Exit::Drop,
        Exit::Reset,
        Exit::ResetFlush,
        Exit::WriteError,
        Exit::FlushError,
        Exit::WriteZero,
    ] {
        server_send(3, false, exit).await;
    }
}

#[tokio::test]
async fn one_original_owner_follows_multi_frame_requeue_until_the_final_flush() {
    let (io, peer, writes) = server_io(true).await;
    let mut builder = h2::server::Builder::new();
    builder.retain_data_payloads(true);
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    let (request, mut response) = connection.accept().await.unwrap().unwrap();
    drop(request);
    let mut stream = response
        .send_response(hyper::http::Response::new(()), false)
        .unwrap();
    let (data, budget, capacity) = original(32769);
    writes.lock().unwrap().arm(&data);
    writes.lock().unwrap().payload_allowance = usize::MAX;
    stream.send_data(data, true).unwrap();
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    assert!(connection.poll_closed(&mut cx).is_pending());
    held(&budget);
    assert_eq!(writes.lock().unwrap().original_written, 16384);
    for expected in [32768, 32769] {
        writes.lock().unwrap().flush_allowance = 1;
        assert!(connection.poll_closed(&mut cx).is_pending());
        held(&budget);
        assert_eq!(writes.lock().unwrap().original_written, expected);
        assert!(connection.poll_closed(&mut cx).is_pending());
        held(&budget);
    }
    assert_eq!(
        writes.lock().unwrap().data_frames(false),
        vec![
            (0, vec![0xa5; 16384]),
            (0, vec![0xa5; 16384]),
            (1, vec![0xa5])
        ]
    );
    writes.lock().unwrap().block_flush = false;
    assert!(connection.poll_closed(&mut cx).is_pending());
    released(&budget, capacity);
    drop(stream);
    drop(response);
    drop(connection);
    drop(builder);
    drop(peer);
    released(&budget, capacity);
}

#[tokio::test]
async fn h2_default_keeps_upstream_small_payload_copy() {
    let (io, peer, writes) = server_io(false).await;
    let mut connection = h2::server::handshake(io).await.unwrap();
    let (request, mut response) = connection.accept().await.unwrap().unwrap();
    drop(request);
    let mut stream = response
        .send_response(hyper::http::Response::new(()), false)
        .unwrap();
    let (data, budget, capacity) = original(3);
    writes.lock().unwrap().arm(&data);
    stream.send_data(data, true).unwrap();
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert_eq!(writes.lock().unwrap().original_written, 0);
    released(&budget, capacity);
    writes.lock().unwrap().block_flush = false;
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert_eq!(
        writes.lock().unwrap().data_frames(false),
        vec![(1, vec![0xa5; 3])]
    );
    drop(stream);
    drop(response);
    drop(connection);
    drop(peer);
}

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;
#[derive(Clone, Default)]
struct ManualExecutor(Arc<Mutex<Vec<Task>>>);
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for ManualExecutor {
    fn execute(&self, future: F) {
        self.0.lock().unwrap().push(Box::pin(future));
    }
}
impl ManualExecutor {
    fn poll(&self, cx: &mut Context<'_>) {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        for mut task in tasks {
            if task.as_mut().poll(cx).is_pending() {
                self.0.lock().unwrap().push(task);
            }
        }
    }
    fn clear(&self) {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        drop(tasks);
    }
}
struct OneFrame(Option<Bytes>);
impl Body for OneFrame {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.take().map(|data| Ok(Frame::data(data))))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.0.as_ref().map_or(0, |b| b.len()) as u64)
    }
}

#[tokio::test]
async fn hyper_server_forwards_original_payload_and_flush_owner() {
    let (io, peer, writes) = server_io(false).await;
    let (data, budget, capacity) = original(3);
    writes.lock().unwrap().arm(&data);
    // Let the handshake flush before the service gives H2 its original DATA.
    writes.lock().unwrap().block_flush = false;
    let payload = Arc::new(Mutex::new(Some(data)));
    let captured = payload.clone();
    let pending_flush = writes.clone();
    let service = service_fn(move |_| {
        pending_flush.lock().unwrap().block_flush = true;
        let data = captured.lock().unwrap().take().unwrap();
        std::future::ready(Ok::<_, Infallible>(hyper::http::Response::new(OneFrame(
            Some(data),
        ))))
    });
    let exec = ManualExecutor::default();
    let mut builder = hyper::server::conn::http2::Builder::new(exec.clone());
    builder.retain_data_payloads(true);
    let mut connection = Box::pin(builder.clone().serve_connection(TokioIo::new(io), service));
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    for _ in 0..3 {
        assert!(connection.as_mut().poll(&mut cx).is_pending());
        exec.poll(&mut cx);
    }
    assert_eq!(writes.lock().unwrap().original_written, 1);
    held(&budget);
    writes.lock().unwrap().payload_allowance = usize::MAX;
    for _ in 0..3 {
        assert!(connection.as_mut().poll(&mut cx).is_pending());
        exec.poll(&mut cx);
        held(&budget);
    }
    assert_eq!(writes.lock().unwrap().original_written, 3);
    writes.lock().unwrap().block_flush = false;
    for _ in 0..3 {
        assert!(connection.as_mut().poll(&mut cx).is_pending());
        exec.poll(&mut cx);
    }
    released(&budget, capacity);
    assert_eq!(
        writes.lock().unwrap().data_frames(false),
        vec![(1, vec![0xa5; 3])]
    );
    drop(connection);
    drop(builder);
    exec.clear();
    drop(payload);
    drop(peer);
    released(&budget, capacity);
}

#[tokio::test]
async fn hyper_client_forwards_original_payload_and_flush_owner() {
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut wire = Vec::new();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 5, 1, b"\x88");
    peer.write_all(&wire).await.unwrap();
    let writes = Arc::new(Mutex::new(Writes::default()));
    let io = SpyIo {
        read: io,
        writes: writes.clone(),
        vectored: false,
    };
    let exec = ManualExecutor::default();
    let mut builder = hyper::client::conn::http2::Builder::new(exec.clone());
    builder.retain_data_payloads(true);
    let (mut sender, connection) = builder
        .clone()
        .handshake::<_, OneFrame>(TokioIo::new(io))
        .await
        .unwrap();
    let (data, budget, capacity) = original(3);
    writes.lock().unwrap().arm(&data);
    let mut connection = Box::pin(connection);
    let request = hyper::http::Request::builder()
        .uri("http://localhost/")
        .body(OneFrame(Some(data)))
        .unwrap();
    let mut response = Box::pin(sender.send_request(request));
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    let _ = response.as_mut().poll(&mut cx);
    for _ in 0..3 {
        assert!(connection.as_mut().poll(&mut cx).is_pending());
        exec.poll(&mut cx);
    }
    assert_eq!(writes.lock().unwrap().original_written, 1);
    held(&budget);
    writes.lock().unwrap().payload_allowance = usize::MAX;
    for _ in 0..3 {
        assert!(connection.as_mut().poll(&mut cx).is_pending());
        exec.poll(&mut cx);
        held(&budget);
    }
    assert_eq!(writes.lock().unwrap().original_written, 3);
    writes.lock().unwrap().block_flush = false;
    for _ in 0..3 {
        assert!(connection.as_mut().poll(&mut cx).is_pending());
        exec.poll(&mut cx);
    }
    released(&budget, capacity);
    assert_eq!(
        writes.lock().unwrap().data_frames(true),
        vec![(1, vec![0xa5; 3])]
    );
    drop(response);
    drop(sender);
    drop(connection);
    drop(builder);
    exec.clear();
    drop(peer);
    released(&budget, capacity);
}
