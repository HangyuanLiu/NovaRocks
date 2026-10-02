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

//! Fixed raw input pregrant, exact frame reads and actual h2/Hyper setup.
//! Emitted frame copies, headers/GOAWAY and complete Native ownership are separate.
use bytes::Bytes;
use h2::{ReceiveBufferPool, ReceiveFrameBuffer};
use hyper::body::{Body, Frame, SizeHint};
use hyper::http::{Request, Response};
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::convert::Infallible;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

/// The real Hyper tasks run only at explicit pump points, so executor timing
/// cannot conceal additional buffered DATA or resume the parser in the oracle.
#[derive(Clone, Default)]
struct ManualExecutor(Arc<Mutex<Vec<Task>>>);
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for ManualExecutor {
    fn execute(&self, future: F) {
        self.0.lock().unwrap().push(Box::pin(future));
    }
}
impl ManualExecutor {
    fn cancel_all(&self) {
        // Futures may hold executor clones. Remove them before dropping so the
        // test scheduler cannot retain an executor/future reference cycle.
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        drop(tasks);
    }
    fn poll_once(&self, cx: &mut Context<'_>) {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        for mut task in tasks {
            if task.as_mut().poll(cx).is_pending() {
                self.0.lock().unwrap().push(task);
            }
        }
    }
}

struct Empty;
impl Body for Empty {
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
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(0)
    }
}

fn frame(target: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    let length = payload.len() as u32;
    target.extend_from_slice(&length.to_be_bytes()[1..]);
    target.extend_from_slice(&[kind, flags]);
    target.extend_from_slice(&stream.to_be_bytes());
    target.extend_from_slice(payload);
}

#[derive(Debug)]
struct Read {
    position: usize,
    requested: usize,
    count: usize,
    pointer: usize,
}
#[derive(Default, Debug)]
struct Reads {
    position: usize,
    events: Vec<Read>,
}
#[derive(Debug)]
struct SpyIo {
    inner: DuplexStream,
    reads: Arc<Mutex<Reads>>,
}
impl AsyncRead for SpyIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let requested = buf.remaining();
        let before = buf.filled().len();
        let pointer = buf.filled().as_ptr() as usize;
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        let count = buf.filled().len() - before;
        if count != 0 {
            let mut reads = self.reads.lock().unwrap();
            let position = reads.position;
            reads.events.push(Read {
                position,
                requested,
                count,
                pointer,
            });
            reads.position += count;
        }
        result
    }
}
impl AsyncWrite for SpyIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
fn funded_raw() -> (ReceiveFrameBuffer, Arc<ResultRetainedBudget>, usize) {
    let total = ReceiveFrameBuffer::allocation_capacity_bound(16384).unwrap()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap() else {
        panic!("original raw input pregrant");
    };
    let buffer = ReceiveFrameBuffer::new(
        16384,
        Bytes::from_owner_with_exit_guard(Bytes::new(), credit),
    )
    .unwrap();
    (buffer, budget, total)
}
fn funded_data() -> (ReceiveBufferPool, Arc<ResultRetainedBudget>, usize) {
    let total = ReceiveBufferPool::allocation_capacity_bound(1, 16384).unwrap()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap() else {
        panic!("original retained DATA pregrant");
    };
    (
        ReceiveBufferPool::new(
            1,
            16384,
            Bytes::from_owner_with_exit_guard(Bytes::new(), credit),
        )
        .unwrap(),
        budget,
        total,
    )
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn released(budget: &Arc<ResultRetainedBudget>, total: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap() else {
        panic!("original raw input allocation must have physically exited");
    };
    drop(credit);
}
async fn wire_io(wire: &[u8]) -> (SpyIo, DuplexStream, Arc<Mutex<Reads>>) {
    let (io, mut peer) = tokio::io::duplex(65536);
    peer.write_all(wire).await.unwrap();
    let reads = Arc::new(Mutex::new(Reads::default()));
    (
        SpyIo {
            inner: io,
            reads: reads.clone(),
        },
        peer,
        reads,
    )
}
fn server_prefix() -> Vec<u8> {
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 4, 1, b"\x83\x86\x84\x01\x09localhost");
    wire
}
fn assert_fixed_reads(reads: &Arc<Mutex<Reads>>, preface: bool) {
    let reads = reads.lock().unwrap();
    let begin = usize::from(preface);
    if preface {
        assert_eq!(reads.events[0].count, 24);
    }
    let frames = &reads.events[begin..];
    assert!(!frames.is_empty());
    let base = frames[0].pointer;
    assert_eq!(
        frames[0].requested, 9,
        "fixed reader begins with one complete frame header"
    );
    let mut position = if preface { 24 } else { 0 };
    for read in frames {
        assert_eq!(read.position, position);
        assert!(read.count <= read.requested);
        position += read.count;
        assert!(read.requested <= 16384);
        assert!(read.pointer >= base && read.pointer + read.requested <= base + 16393);
    }
}

#[tokio::test]
async fn fixed_raw_input_stops_before_next_frame_when_retained_data_is_full() {
    let mut wire = server_prefix();
    let first_end = wire.len() + 10;
    for i in 0..64 {
        frame(&mut wire, 0, u8::from(i == 63), 1, &[i]);
    }
    let (io, _peer, reads) = wire_io(&wire).await;
    let (raw, raw_budget, raw_total) = funded_raw();
    let (data, data_budget, data_total) = funded_data();
    let mut builder = h2::server::Builder::new();
    builder
        .max_receive_buffered_events(8)
        .receive_buffer_pool(data.clone())
        .receive_frame_buffer(raw.clone())
        .max_header_list_size(16384)
        .max_receive_header_block_size(16384);
    let mut connection = builder.clone().handshake::<_, Bytes>(io).await.unwrap();
    let (request, response) = connection.accept().await.unwrap().unwrap();
    let mut body = request.into_body();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert_eq!(
        reads.lock().unwrap().position,
        first_end,
        "no next header or body read-ahead into raw input"
    );
    assert_fixed_reads(&reads, true);
    let Poll::Ready(Some(Ok(first))) = body.poll_data(&mut cx) else {
        panic!("first DATA");
    };
    assert_eq!(first.as_ref(), [0]);
    body.flow_control().release_capacity(1).unwrap();
    let alias = first.clone();
    drop(first);
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert_eq!(reads.lock().unwrap().position, first_end);
    held(&raw_budget);
    held(&data_budget);
    drop(body);
    drop(response);
    drop(connection);
    drop(builder);
    drop(raw);
    drop(data);
    // The raw input allocation and retained DATA are separate original owners.
    released(&raw_budget, raw_total);
    held(&data_budget);
    drop(alias);
    released(&data_budget, data_total);
}
#[tokio::test]
async fn fixed_raw_input_preserves_zero_and_one_byte_payloads() {
    let mut wire = server_prefix();
    for i in 0..8 {
        frame(&mut wire, 0, 0, 1, &[]);
        frame(&mut wire, 0, u8::from(i == 7), 1, &[i]);
    }
    let (io, _peer, reads) = wire_io(&wire).await;
    let (raw, budget, total) = funded_raw();
    let mut builder = h2::server::Builder::new();
    builder.receive_frame_buffer(raw.clone());
    let mut connection = builder.clone().handshake::<_, Bytes>(io).await.unwrap();
    let (request, response) = connection.accept().await.unwrap().unwrap();
    let mut body = request.into_body();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    assert!(connection.poll_closed(&mut cx).is_pending());
    for i in 0..8 {
        let Poll::Ready(Some(Ok(empty))) = body.poll_data(&mut cx) else {
            panic!("empty DATA");
        };
        assert!(empty.is_empty());
        let Poll::Ready(Some(Ok(bytes))) = body.poll_data(&mut cx) else {
            panic!("one-byte DATA");
        };
        assert_eq!(bytes.as_ref(), [i]);
    }
    assert!(matches!(body.poll_data(&mut cx), Poll::Ready(None)));
    assert_fixed_reads(&reads, true);
    held(&budget);
    drop(body);
    drop(response);
    drop(connection);
    drop(builder);
    drop(raw);
    released(&budget, total);
}
#[tokio::test]
async fn fixed_raw_input_refuses_oversized_header_before_payload_read() {
    let mut wire = server_prefix();
    let start = wire.len();
    wire.extend_from_slice(&[0, 64, 1, 0, 0, 0, 0, 0, 1]); // 16385-byte DATA, body not provided
    let (io, _peer, reads) = wire_io(&wire).await;
    let (raw, budget, total) = funded_raw();
    let mut builder = h2::server::Builder::new();
    builder.receive_frame_buffer(raw.clone());
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    let Poll::Ready(Some(Err(error))) = connection.poll_accept(&mut cx) else {
        panic!("oversized frame must fail without its body");
    };
    assert_eq!(error.reason(), Some(h2::Reason::FRAME_SIZE_ERROR));
    assert_eq!(reads.lock().unwrap().position, start + 9);
    assert_fixed_reads(&reads, true);
    drop(connection);
    drop(builder);
    drop(raw);
    released(&budget, total);
}
#[tokio::test]
async fn fixed_raw_client_preserves_response_frames_and_owner() {
    let mut wire = Vec::new();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 4, 1, &[0x88]);
    frame(&mut wire, 0, 1, 1, b"abc");
    let (io, _peer, reads) = wire_io(&wire).await;
    let (raw, budget, total) = funded_raw();
    let mut builder = h2::client::Builder::new();
    builder.receive_frame_buffer(raw.clone());
    let (mut sender, mut connection) = builder.clone().handshake::<_, Bytes>(io).await.unwrap();
    let (response, stream) = sender
        .send_request(
            Request::builder()
                .uri("http://localhost/")
                .body(())
                .unwrap(),
            true,
        )
        .unwrap();
    let mut response = Box::pin(response);
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let Poll::Ready(Ok(response)) = response.as_mut().poll(&mut cx) else {
        panic!("response headers");
    };
    let mut body = response.into_body();
    let Poll::Ready(Some(Ok(bytes))) = body.poll_data(&mut cx) else {
        panic!("response DATA");
    };
    assert_eq!(bytes.as_ref(), b"abc");
    assert_fixed_reads(&reads, false);
    drop(bytes);
    drop(body);
    drop(stream);
    drop(sender);
    drop(connection);
    drop(builder);
    held(&budget);
    drop(raw);
    released(&budget, total);
}
#[tokio::test]
async fn fixed_raw_reuse_and_geometry_fail_before_client_preface_write() {
    let (raw, budget, total) = funded_raw();
    let (io, mut peer, _) = wire_io(&[]).await;
    let mut wrong = h2::client::Builder::new();
    wrong
        .max_frame_size(32768)
        .receive_frame_buffer(raw.clone());
    assert!(wrong.handshake::<_, Bytes>(io).await.unwrap_err().is_io());
    let mut out = [0u8; 1];
    assert_eq!(
        tokio::io::AsyncReadExt::read(&mut peer, &mut out)
            .await
            .unwrap(),
        0
    );
    let (io, _peer, _) = wire_io(&[]).await;
    let mut builder = h2::client::Builder::new();
    builder.receive_frame_buffer(raw.clone());
    let (sender, connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(sender);
    drop(connection);
    let (io, mut peer, _) = wire_io(&[]).await;
    assert!(
        builder
            .clone()
            .handshake::<_, Bytes>(io)
            .await
            .unwrap_err()
            .is_io()
    );
    assert_eq!(
        tokio::io::AsyncReadExt::read(&mut peer, &mut out)
            .await
            .unwrap(),
        0
    );
    drop(builder);
    drop(wrong);
    held(&budget);
    drop(raw);
    released(&budget, total);
}
#[tokio::test]
async fn cloned_hyper_server_forwards_fixed_raw_input() {
    let mut wire = server_prefix();
    frame(&mut wire, 0, 1, 1, b"abc");
    let (io, _peer, reads) = wire_io(&wire).await;
    let (raw, budget, total) = funded_raw();
    let exec = ManualExecutor::default();
    let service = service_fn(|_request| async { Ok::<_, Infallible>(Response::new(Empty)) });
    let mut builder = hyper::server::conn::http2::Builder::new(exec.clone());
    builder.receive_frame_buffer(raw.clone());
    let mut connection = Box::pin(builder.clone().serve_connection(TokioIo::new(io), service));
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    assert!(connection.as_mut().poll(&mut cx).is_pending());
    exec.poll_once(&mut cx);
    assert_fixed_reads(&reads, true);
    drop(connection);
    exec.cancel_all();
    drop(builder);
    held(&budget);
    drop(raw);
    released(&budget, total);
}
#[tokio::test]
async fn cloned_hyper_client_forwards_fixed_raw_input() {
    let mut wire = Vec::new();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 5, 1, &[0x88]);
    let (io, _peer, reads) = wire_io(&wire).await;
    let (raw, budget, total) = funded_raw();
    let exec = ManualExecutor::default();
    let mut builder = hyper::client::conn::http2::Builder::new(exec.clone());
    builder.receive_frame_buffer(raw.clone());
    let (mut sender, connection) = builder
        .clone()
        .handshake::<_, Empty>(TokioIo::new(io))
        .await
        .unwrap();
    let mut connection = Box::pin(connection);
    let mut response = Box::pin(
        sender.send_request(
            Request::builder()
                .uri("http://localhost/")
                .body(Empty)
                .unwrap(),
        ),
    );
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    let mut done = false;
    for _ in 0..8 {
        let _ = connection.as_mut().poll(&mut cx);
        exec.poll_once(&mut cx);
        if let Poll::Ready(result) = response.as_mut().poll(&mut cx) {
            assert_eq!(result.unwrap().status(), 200);
            done = true;
            break;
        }
    }
    assert!(done);
    assert_fixed_reads(&reads, false);
    drop(response);
    drop(sender);
    drop(connection);
    exec.cancel_all();
    drop(builder);
    held(&budget);
    drop(raw);
    released(&budget, total);
}
