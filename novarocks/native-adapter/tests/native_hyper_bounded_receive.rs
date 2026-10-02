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

//! Real Hyper builder propagation and count backpressure under tiny H2 DATA.
//! This does not certify Native listener/lane or full payload backing bounds.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Wake, Waker};

use bytes::Bytes;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::http::{Request, Response};
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::io::AsyncWriteExt;

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

fn data(body: &mut Incoming, cx: &mut Context<'_>, expected: &[u8]) {
    let Poll::Ready(Some(Ok(frame))) = Pin::new(body).poll_frame(cx) else {
        panic!("expected exact buffered DATA");
    };
    assert_eq!(frame.into_data().unwrap().as_ref(), expected);
}

async fn server_receive(payload: &[u8], bounded: bool) {
    const FRAMES: usize = 64;
    let (io, mut peer) = tokio::io::duplex(64 * 1024);
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 4, 1, b"\x83\x86\x84\x01\x09localhost");
    for i in 0..FRAMES {
        frame(&mut wire, 0, u8::from(i == FRAMES - 1), 1, payload);
    }
    peer.write_all(&wire).await.unwrap();

    let exec = ManualExecutor::default();
    let (sender, receiver) = mpsc::channel();
    let service = service_fn(move |request: Request<Incoming>| {
        sender.send(request.into_body()).unwrap();
        // Keep the real response task alive without consuming the body.
        std::future::pending::<Result<Response<Empty>, Infallible>>()
    });
    let mut builder = hyper::server::conn::http2::Builder::new(exec.clone());
    if bounded {
        builder.max_receive_buffered_events(2);
    }
    // Exercise the actual cloned private Config as used by connection pools.
    let mut connection = Box::pin(builder.clone().serve_connection(TokioIo::new(io), service));
    let probe = Arc::new(WakeCount::default());
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(connection.as_mut().poll(&mut cx).is_pending());
    let mut body = receiver.try_recv().expect("actual Hyper request accepted");

    if !bounded {
        for _ in 0..FRAMES {
            data(&mut body, &mut cx, payload);
        }
        assert!(matches!(
            Pin::new(&mut body).poll_frame(&mut cx),
            Poll::Ready(None)
        ));
        drop(body);
        drop(connection);
        exec.cancel_all();
        return;
    }

    for next in (0..FRAMES).step_by(2) {
        if next > 0 {
            assert!(connection.as_mut().poll(&mut cx).is_pending());
        }
        let before = probe.0.load(Ordering::SeqCst);
        data(&mut body, &mut cx, payload);
        data(&mut body, &mut cx, payload);
        if next < FRAMES - 2 {
            assert!(
                Pin::new(&mut body).poll_frame(&mut cx).is_pending(),
                "Hyper server must preserve the configured event count gate"
            );
        }
        assert!(probe.0.load(Ordering::SeqCst) > before);
    }
    assert!(matches!(
        Pin::new(&mut body).poll_frame(&mut cx),
        Poll::Ready(None)
    ));
    drop(body);
    drop(connection);
    exec.cancel_all();
}

#[tokio::test]
async fn hyper_server_one_byte_data_preserves_count_backpressure_and_order() {
    server_receive(&[0xa5], true).await;
}

#[tokio::test]
async fn hyper_server_empty_data_preserves_count_backpressure() {
    server_receive(&[], true).await;
}

#[tokio::test]
async fn hyper_default_server_keeps_upstream_receive_readiness() {
    server_receive(&[0xa5], false).await;
}

async fn client_receive(payload: &[u8]) {
    const FRAMES: usize = 64;
    let (io, mut peer) = tokio::io::duplex(64 * 1024);
    let mut wire = Vec::new();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 4, 1, &[0x88]); // HPACK :status 200
    for i in 0..FRAMES {
        frame(&mut wire, 0, u8::from(i == FRAMES - 1), 1, payload);
    }
    peer.write_all(&wire).await.unwrap();
    let exec = ManualExecutor::default();
    let mut builder = hyper::client::conn::http2::Builder::new(exec.clone());
    builder.max_receive_buffered_events(2);
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
    let probe = Arc::new(WakeCount::default());
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(connection.as_mut().poll(&mut cx).is_pending());
    let mut body = None;
    for _ in 0..8 {
        exec.poll_once(&mut cx);
        if let Poll::Ready(result) = response.as_mut().poll(&mut cx) {
            let response = result.unwrap();
            assert_eq!(response.status(), 200);
            body = Some(response.into_body());
            break;
        }
    }
    let mut body = body.expect("actual Hyper response task completed");
    // The header was an event too. Removing it frees exactly one node. Drain
    // any already buffered DATA before the next explicit connection pump.
    let mut received = 0;
    while let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut body).poll_frame(&mut cx) {
        assert_eq!(frame.into_data().unwrap().as_ref(), payload);
        received += 1;
        assert!(
            received <= 2,
            "header delivery cannot bypass the count bound"
        );
    }
    while received < FRAMES {
        exec.poll_once(&mut cx);
        let count = 2.min(FRAMES - received);
        for _ in 0..count {
            data(&mut body, &mut cx, payload);
            received += 1;
        }
        if received < FRAMES {
            assert!(
                Pin::new(&mut body).poll_frame(&mut cx).is_pending(),
                "Hyper client must preserve the configured event count gate"
            );
        }
    }
    assert!(matches!(
        Pin::new(&mut body).poll_frame(&mut cx),
        Poll::Ready(None)
    ));
    drop(body);
    drop(connection);
    exec.cancel_all();
}

#[tokio::test]
async fn hyper_client_one_byte_data_preserves_count_backpressure() {
    client_receive(&[0x5a]).await;
}

#[tokio::test]
async fn hyper_client_empty_data_preserves_count_backpressure() {
    client_receive(&[]).await;
}

#[test]
fn hyper_both_builders_refuse_zero_before_connection_construction() {
    let client = std::panic::catch_unwind(|| {
        hyper::client::conn::http2::Builder::new(ManualExecutor::default())
            .max_receive_buffered_events(0);
    });
    let server = std::panic::catch_unwind(|| {
        hyper::server::conn::http2::Builder::new(ManualExecutor::default())
            .max_receive_buffered_events(0);
    });
    assert!(client.is_err());
    assert!(server.is_err());
}

#[tokio::test]
async fn hyper_canceling_stalled_body_resumes_the_other_stream() {
    let (io, mut peer) = tokio::io::duplex(64 * 1024);
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 4, 1, b"\x83\x86\x84\x01\x09localhost");
    for i in 0..64 {
        frame(&mut wire, 0, u8::from(i == 63), 1, &[]);
    }
    frame(
        &mut wire,
        1,
        5,
        3,
        b"\x83\x86\x04\x05/next\x01\x09localhost",
    );
    peer.write_all(&wire).await.unwrap();
    let exec = ManualExecutor::default();
    let (sender, receiver) = mpsc::channel();
    let service = service_fn(move |request: Request<Incoming>| {
        let path = request.uri().path().to_owned();
        sender.send((path, request.into_body())).unwrap();
        std::future::pending::<Result<Response<Empty>, Infallible>>()
    });
    let mut builder = hyper::server::conn::http2::Builder::new(exec.clone());
    builder.max_receive_buffered_events(2);
    let mut connection = Box::pin(builder.serve_connection(TokioIo::new(io), service));
    let probe = Arc::new(WakeCount::default());
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(connection.as_mut().poll(&mut cx).is_pending());
    let (path, body) = receiver.try_recv().expect("first real Hyper stream");
    assert_eq!(path, "/");
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    let before = probe.0.load(Ordering::SeqCst);
    // Local cancel owns actual body Drop. Remote RST behind these frames
    // cannot be relied upon to unblock the connection-global read gate.
    drop(body);
    assert!(probe.0.load(Ordering::SeqCst) > before);
    assert!(connection.as_mut().poll(&mut cx).is_pending());
    let (path, mut body) = receiver
        .try_recv()
        .expect("other stream resumes after Drop");
    assert_eq!(path, "/next");
    assert!(matches!(
        Pin::new(&mut body).poll_frame(&mut cx),
        Poll::Ready(None)
    ));
    drop(body);
    drop(connection);
    exec.cancel_all();
}
