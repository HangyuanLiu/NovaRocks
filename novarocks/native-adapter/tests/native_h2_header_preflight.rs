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

//! Actual opt-in header allocation prechecks through h2 and cloned Hyper configs.
//! Full connection ownership, raw I/O capacity and Native product setup are separate.

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use hyper::http::{Request, Response};
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use tokio::io::{AsyncWriteExt, DuplexStream};

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

fn length(wire: &mut Vec<u8>, n: usize) {
    if n < 127 {
        wire.push(n as u8);
        return;
    }
    wire.push(127);
    let mut rest = n - 127;
    while rest >= 128 {
        wire.push((rest as u8 & 127) | 128);
        rest >>= 7;
    }
    wire.push(rest as u8);
}
fn declared_large_value() -> Vec<u8> {
    let mut wire = b"\x83\x86\x84\x01\x09localhost\x00\x01x".to_vec();
    length(&mut wire, 1 << 20);
    wire
}
async fn server_frames(frames: &[(u8, u8, Vec<u8>)]) -> (DuplexStream, DuplexStream) {
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    for (kind, flags, payload) in frames {
        frame(&mut wire, *kind, *flags, 1, payload);
    }
    peer.write_all(&wire).await.unwrap();
    // The prewritten SETTINGS ACK controls polling; this is a raw protocol
    // fixture, not a native handshake timing or product acceptance claim.
    (io, peer)
}
async fn server_rejects(frames: &[(u8, u8, Vec<u8>)], max: usize, header_list: u32) {
    let (io, _peer) = server_frames(frames).await;
    let mut builder = h2::server::Builder::new();
    builder
        .max_header_list_size(header_list)
        .max_receive_header_block_size(max);
    let mut connection = builder.clone().handshake::<_, Bytes>(io).await.unwrap();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    let Poll::Ready(Some(Err(error))) = connection.poll_accept(&mut cx) else {
        panic!("bounded decoder must fail before requesting more bytes or publishing headers");
    };
    assert_eq!(error.reason(), Some(h2::Reason::COMPRESSION_ERROR));
}

#[tokio::test]
async fn h2_declared_over_limit_field_fails_without_body_in_bounded_server() {
    let frames = [(1, 1, declared_large_value())]; // END_STREAM, incomplete HPACK block
    server_rejects(&frames, 16384, 256).await;
    let (io, _peer) = server_frames(&frames).await;
    let mut builder = h2::server::Builder::new();
    builder.max_header_list_size(256);
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    assert!(
        connection
            .poll_accept(&mut Context::from_waker(&waker))
            .is_pending(),
        "default decoder still waits for an incomplete literal"
    );
}
#[tokio::test]
async fn h2_continuation_counts_already_consumed_fields_before_extend() {
    let first = b"\x83\x86\x84\x01\x09localhost".to_vec();
    let max = first.len();
    let frames = [(1, 1, first), (9, 4, b"\x00\x01x\x03abc".to_vec())];
    server_rejects(&frames, max, 1024).await;
    let (io, _peer) = server_frames(&frames).await;
    let mut connection = h2::server::Builder::new()
        .max_header_list_size(1024)
        .handshake::<_, Bytes>(io)
        .await
        .unwrap();
    let (request, response) = connection.accept().await.unwrap().unwrap();
    assert_eq!(request.headers()["x"], "abc");
    drop(response);
}
#[tokio::test]
async fn h2_incomplete_literal_continuation_is_refused_before_accumulation() {
    let mut first = b"\x83\x86\x84\x01\x09localhost\x00\x01x".to_vec();
    length(&mut first, 200);
    first.extend([b'a'; 100]);
    let frames = [(1, 1, first), (9, 4, vec![b'a'; 100])];
    server_rejects(&frames, 128, 1024).await;
}
#[tokio::test]
async fn h2_exact_block_boundary_and_fragmented_field_preserve_values() {
    let mut first = b"\x83\x86\x84\x01\x09localhost\x00\x01x".to_vec();
    length(&mut first, 200);
    first.extend([b'a'; 100]);
    let max = first.len() + 100;
    let frames = [(1, 1, first), (9, 4, vec![b'a'; 100])];
    let (io, _peer) = server_frames(&frames).await;
    let mut builder = h2::server::Builder::new();
    builder
        .max_header_list_size(1024)
        .max_receive_header_block_size(max);
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    let (request, response) = connection.accept().await.unwrap().unwrap();
    assert_eq!(request.headers()["x"].as_bytes(), &[b'a'; 200]);
    assert_eq!(request.uri().path(), "/");
    drop(response);
}
#[tokio::test]
async fn h2_initial_block_over_limit_never_publishes_request() {
    let payload = b"\x83\x86\x84\x01\x09localhost".to_vec();
    let max = payload.len() - 1;
    server_rejects(&[(1, 5, payload)], max, 1024).await;
}
#[tokio::test]
async fn h2_bounded_client_checks_declared_response_field_without_body() {
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut payload = b"\x88\x00\x01x".to_vec();
    length(&mut payload, 1 << 20);
    let mut wire = Vec::new();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 0, 1, &payload);
    peer.write_all(&wire).await.unwrap();
    let mut builder = h2::client::Builder::new();
    builder
        .enable_push(false)
        .max_header_list_size(256)
        .max_receive_header_block_size(16384);
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
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let Poll::Ready(Err(error)) = Pin::new(&mut connection).poll(&mut Context::from_waker(&waker))
    else {
        panic!("bounded client must fail before waiting for declared body");
    };
    assert_eq!(error.reason(), Some(h2::Reason::COMPRESSION_ERROR));
    drop(response);
    drop(stream);
}
#[tokio::test]
async fn hyper_cloned_server_forwards_header_precheck_and_never_calls_service() {
    let (io, _peer) = server_frames(&[(1, 1, declared_large_value())]).await;
    let called = Arc::new(AtomicUsize::new(0));
    let observed = called.clone();
    let service = service_fn(move |_request| {
        observed.fetch_add(1, Ordering::SeqCst);
        async { Ok::<_, Infallible>(Response::new(Empty)) }
    });
    let exec = ManualExecutor::default();
    let mut builder = hyper::server::conn::http2::Builder::new(exec.clone());
    builder
        .max_header_list_size(256)
        .max_receive_header_block_size(16384);
    let mut connection = Box::pin(builder.clone().serve_connection(TokioIo::new(io), service));
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    assert!(
        matches!(connection.as_mut().poll(&mut cx), Poll::Ready(Err(_))),
        "Hyper must forward the header decoder bound"
    );
    assert_eq!(called.load(Ordering::SeqCst), 0);
    drop(connection);
    exec.cancel_all();
}
#[tokio::test]
async fn hyper_cloned_client_forwards_header_precheck() {
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut payload = b"\x88\x00\x01x".to_vec();
    length(&mut payload, 1 << 20);
    let mut wire = Vec::new();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 0, 1, &payload);
    peer.write_all(&wire).await.unwrap();
    let exec = ManualExecutor::default();
    let mut builder = hyper::client::conn::http2::Builder::new(exec.clone());
    builder
        .max_header_list_size(256)
        .max_receive_header_block_size(16384);
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
    // Hyper's connection actor is hosted by the supplied executor. Pump it
    // explicitly; no timeout or socket EOF can make this oracle pass.
    let mut failed = false;
    for _ in 0..8 {
        let _ = connection.as_mut().poll(&mut cx);
        exec.poll_once(&mut cx);
        if let Poll::Ready(result) = response.as_mut().poll(&mut cx) {
            assert!(result.is_err());
            failed = true;
            break;
        }
    }
    assert!(
        failed,
        "Hyper client must forward and refuse before body arrives"
    );
    drop(response);
    drop(sender);
    drop(connection);
    exec.cancel_all();
}
