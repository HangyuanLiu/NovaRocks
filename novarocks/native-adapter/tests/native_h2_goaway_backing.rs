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

//! Actual GOAWAY debug backing and error aliases through h2 and Hyper builders.
//! The peer is a fixed SETTINGS/ACK wire fixture, not a Native RPC deployment.
//! Only the GOAWAY pool and carrier use the measured original Worker grant;
//! raw input, emitted frames, IO/tasks/streams and error boxes are separate.
//! A pool refusal is a connection error, not proof of deadline or physical exit.

use bytes::Bytes;
use h2::ReceiveBufferPool;
use hyper::body::{Body, Frame, SizeHint};
use hyper::http::{Request, Response};
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::convert::Infallible;
use std::error::Error as StdError;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn frame(wire: &mut Vec<u8>, kind: u8, flags: u8, payload: &[u8]) {
    wire.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&[kind, flags, 0, 0, 0, 0]);
    wire.extend_from_slice(payload);
}
fn settings() -> Vec<u8> {
    let mut wire = Vec::new();
    frame(&mut wire, 4, 0, &[]);
    // This fixed peer acknowledges the initial local SETTINGS. No runtime,
    // network peer, timeout or actual deadline mechanism is being modeled.
    frame(&mut wire, 4, 1, &[]);
    wire
}
fn goaway(last_id: u32, reason: h2::Reason, debug: &[u8]) -> Vec<u8> {
    let mut payload = last_id.to_be_bytes().to_vec();
    payload.extend_from_slice(&u32::from(reason).to_be_bytes());
    payload.extend_from_slice(debug);
    let mut wire = Vec::new();
    frame(&mut wire, 7, 0, &payload);
    wire
}
fn request() -> Request<()> {
    Request::builder()
        .uri("http://localhost/")
        .body(())
        .unwrap()
}
fn funded_pool(slots: usize) -> (ReceiveBufferPool, Arc<ResultRetainedBudget>, usize) {
    let bytes = ReceiveBufferPool::allocation_capacity_bound(slots, 16384).unwrap()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("original GOAWAY process pregrant");
    };
    let pool = ReceiveBufferPool::new(
        slots,
        16384,
        Bytes::from_owner_with_exit_guard(Bytes::new(), credit),
    )
    .unwrap();
    (pool, budget, bytes)
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn released(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("all original GOAWAY backing must physically exit before reuse");
    };
    drop(credit);
}
fn assert_debug(error: &h2::Error, expected: &str, reason: h2::Reason) {
    assert!(error.is_go_away());
    assert!(error.is_remote());
    assert_eq!(error.reason(), Some(reason));
    let display = error.to_string();
    assert!(
        display.contains(expected),
        "missing original debug {expected:?}: {display}"
    );
}
fn h2_cause(error: &hyper::Error) -> &h2::Error {
    let mut current: &(dyn StdError + 'static) = error;
    loop {
        if let Some(error) = current.downcast_ref::<h2::Error>() {
            return error;
        }
        current = current
            .source()
            .expect("Hyper must preserve the h2 GOAWAY error source");
    }
}

#[tokio::test]
async fn third_retained_debug_refuses_the_real_connection_without_waiting_for_pool_return() {
    let (io, mut peer) = tokio::io::duplex(65536);
    peer.write_all(&settings()).await.unwrap();
    let (pool, budget, bytes) = funded_pool(2);
    let mut builder = h2::client::Builder::new();
    builder.receive_goaway_buffer_pool(pool.clone());
    let (mut sender, mut connection) = builder.clone().handshake::<_, Bytes>(io).await.unwrap();
    // A live permitted stream keeps graceful GOAWAY processing open. Equal
    // last-stream IDs on repeated GOAWAY frames are legal and never increase.
    let (response, stream) = sender.send_request(request(), true).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    peer.write_all(&goaway(1, h2::Reason::NO_ERROR, b"first"))
        .await
        .unwrap();
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let first = sender.send_request(request(), true).unwrap_err();
    assert_debug(&first, "first", h2::Reason::NO_ERROR);
    assert_eq!(pool.available_buffers(), 1);
    // Public h2::Error is not Clone. These independently returned errors are
    // real clones of the same internal error/Bytes owner and consume one slot.
    let first_alias = sender.send_request(request(), true).unwrap_err();
    assert_debug(&first_alias, "first", h2::Reason::NO_ERROR);
    assert_eq!(pool.available_buffers(), 1);
    peer.write_all(&goaway(1, h2::Reason::NO_ERROR, b"second"))
        .await
        .unwrap();
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let second = sender.send_request(request(), true).unwrap_err();
    assert_debug(&second, "second", h2::Reason::NO_ERROR);
    assert_eq!(pool.available_buffers(), 0);
    peer.write_all(&goaway(1, h2::Reason::NO_ERROR, b"third"))
        .await
        .unwrap();
    let Poll::Ready(Err(refusal)) = Pin::new(&mut connection).poll(&mut cx) else {
        panic!("exhausted GOAWAY pool must fail the connection instead of parking");
    };
    assert_eq!(refusal.reason(), Some(h2::Reason::ENHANCE_YOUR_CALM));
    assert!(refusal.is_library());
    assert_eq!(pool.available_buffers(), 0);
    drop(refusal);
    drop(response);
    drop(stream);
    drop(sender);
    drop(connection);
    drop(builder);
    drop(pool);
    held(&budget);
    drop(first);
    held(&budget);
    assert_debug(&first_alias, "first", h2::Reason::NO_ERROR);
    drop(second);
    held(&budget);
    drop(first_alias);
    released(&budget, bytes);
}

#[tokio::test]
async fn full_pool_sends_local_refusal_but_preserves_latest_remote_error_priority() {
    let (io, mut peer) = tokio::io::duplex(65536);
    peer.write_all(&settings()).await.unwrap();
    let (pool, budget, bytes) = funded_pool(2);
    let mut builder = h2::client::Builder::new();
    builder.receive_goaway_buffer_pool(pool.clone());
    let (mut sender, mut connection) = builder.clone().handshake::<_, Bytes>(io).await.unwrap();
    let (response, stream) = sender.send_request(request(), true).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    peer.write_all(&goaway(1, h2::Reason::PROTOCOL_ERROR, b"remote-first"))
        .await
        .unwrap();
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let first = sender.send_request(request(), true).unwrap_err();
    assert_debug(&first, "remote-first", h2::Reason::PROTOCOL_ERROR);
    peer.write_all(&goaway(1, h2::Reason::PROTOCOL_ERROR, b"remote-second"))
        .await
        .unwrap();
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let second = sender.send_request(request(), true).unwrap_err();
    assert_debug(&second, "remote-second", h2::Reason::PROTOCOL_ERROR);
    assert_eq!(pool.available_buffers(), 0);
    peer.write_all(&goaway(1, h2::Reason::PROTOCOL_ERROR, b"refused-third"))
        .await
        .unwrap();
    let Poll::Ready(Err(error)) = Pin::new(&mut connection).poll(&mut cx) else {
        panic!("the third debug must refuse without waiting for a pool slot");
    };
    // Preserve h2's existing take_error priority: a remote error predates our
    // local resource refusal, so the public terminal uses that original cause.
    assert_debug(&error, "remote-second", h2::Reason::PROTOCOL_ERROR);
    assert_eq!(pool.available_buffers(), 0);
    // The writable in-memory fixture lets the connection actually flush its
    // local GOAWAY. This is not a bound for blocked writes or deadline exit.
    let mut written = [0u8; 65536];
    let count = peer.read(&mut written).await.unwrap();
    assert!(written[..count].starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"));
    let mut at = 24;
    let mut local_refusal = false;
    while at < count {
        assert!(count - at >= 9, "the flushed frame header is complete");
        let length = ((written[at] as usize) << 16)
            | ((written[at + 1] as usize) << 8)
            | written[at + 2] as usize;
        assert!(
            length <= count - at - 9,
            "the flushed frame body is complete"
        );
        if written[at + 3] == 7 {
            assert!(length >= 8);
            let reason = u32::from_be_bytes(written[at + 13..at + 17].try_into().unwrap());
            local_refusal |= reason == u32::from(h2::Reason::ENHANCE_YOUR_CALM);
        }
        at += 9 + length;
    }
    assert!(
        local_refusal,
        "actual peer bytes must contain the local resource-refusal GOAWAY"
    );
    drop(response);
    drop(stream);
    drop(sender);
    drop(connection);
    drop(builder);
    drop(pool);
    held(&budget);
    drop(first);
    drop(second);
    held(&budget);
    assert_debug(&error, "remote-second", h2::Reason::PROTOCOL_ERROR);
    drop(error);
    released(&budget, bytes);
}

#[tokio::test]
async fn old_error_alias_exit_reuses_slots_for_repeated_graceful_goaway() {
    let (io, mut peer) = tokio::io::duplex(65536);
    peer.write_all(&settings()).await.unwrap();
    // Leave room for the parser's current and protocol-held debug snapshots
    // while acquiring the next frame; logical error replacement is not free.
    let (pool, budget, bytes) = funded_pool(3);
    let mut builder = h2::client::Builder::new();
    builder.receive_goaway_buffer_pool(pool.clone());
    let (mut sender, mut connection) = builder.clone().handshake::<_, Bytes>(io).await.unwrap();
    let (response, stream) = sender.send_request(request(), true).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    for sequence in 0..64 {
        let debug = format!("diagnostic-{sequence:02}");
        peer.write_all(&goaway(1, h2::Reason::NO_ERROR, debug.as_bytes()))
            .await
            .unwrap();
        assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
        let error = sender.send_request(request(), true).unwrap_err();
        assert_debug(&error, &debug, h2::Reason::NO_ERROR);
        let alias = sender.send_request(request(), true).unwrap_err();
        assert_debug(&alias, &debug, h2::Reason::NO_ERROR);
        let available = pool.available_buffers();
        assert!(
            available > 0,
            "three finite slots must permit replacement progress"
        );
        drop(error);
        assert_eq!(pool.available_buffers(), available);
        drop(alias);
        held(&budget);
    }
    drop(response);
    drop(stream);
    drop(sender);
    drop(connection);
    drop(builder);
    drop(pool);
    released(&budget, bytes);
}

#[tokio::test]
async fn empty_debug_never_checks_out_a_retained_position() {
    let (io, mut peer) = tokio::io::duplex(65536);
    peer.write_all(&settings()).await.unwrap();
    let (pool, budget, bytes) = funded_pool(1);
    let mut builder = h2::client::Builder::new();
    builder.receive_goaway_buffer_pool(pool.clone());
    let (mut sender, mut connection) = builder.clone().handshake::<_, Bytes>(io).await.unwrap();
    let (response, stream) = sender.send_request(request(), true).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let mut errors = Vec::new();
    for _ in 0..64 {
        peer.write_all(&goaway(1, h2::Reason::NO_ERROR, &[]))
            .await
            .unwrap();
        assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
        let error = sender.send_request(request(), true).unwrap_err();
        assert_eq!(error.reason(), Some(h2::Reason::NO_ERROR));
        assert!(error.is_remote() && error.is_go_away());
        errors.push(error);
        assert_eq!(pool.available_buffers(), 1);
    }
    drop(response);
    drop(stream);
    drop(sender);
    drop(connection);
    drop(builder);
    drop(pool);
    // Empty error Bytes do not escape with the GOAWAY pool/credit.
    released(&budget, bytes);
    drop(errors);
}

#[tokio::test]
async fn default_none_keeps_upstream_repeated_debug_behavior() {
    let (io, mut peer) = tokio::io::duplex(65536);
    peer.write_all(&settings()).await.unwrap();
    let builder = h2::client::Builder::new();
    let (mut sender, mut connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    let (response, stream) = sender.send_request(request(), true).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let mut errors = Vec::new();
    for sequence in 0..64 {
        let debug = format!("unbounded-default-{sequence:02}");
        peer.write_all(&goaway(1, h2::Reason::NO_ERROR, debug.as_bytes()))
            .await
            .unwrap();
        assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
        let error = sender.send_request(request(), true).unwrap_err();
        assert_debug(&error, &debug, h2::Reason::NO_ERROR);
        errors.push(error);
    }
    assert_eq!(errors.len(), 64);
    drop(response);
    drop(stream);
    drop(sender);
    drop(connection);
    drop(builder);
    for (sequence, error) in errors.iter().enumerate() {
        assert_debug(
            error,
            &format!("unbounded-default-{sequence:02}"),
            h2::Reason::NO_ERROR,
        );
    }
}

#[tokio::test]
async fn malformed_goaway_without_eight_fixed_payload_bytes_does_not_copy_debug() {
    for payload_length in 0..8 {
        let (io, mut peer) = tokio::io::duplex(65536);
        let mut wire = settings();
        frame(&mut wire, 7, 0, &[0; 7][..payload_length]);
        peer.write_all(&wire).await.unwrap();
        let (pool, budget, bytes) = funded_pool(1);
        let mut builder = h2::client::Builder::new();
        builder.receive_goaway_buffer_pool(pool.clone());
        let (sender, mut connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(Err(error)) = Pin::new(&mut connection).poll(&mut cx) else {
            panic!("malformed GOAWAY must fail without retaining a debug position");
        };
        assert_eq!(error.reason(), Some(h2::Reason::PROTOCOL_ERROR));
        assert_eq!(pool.available_buffers(), 1);
        drop(error);
        drop(sender);
        drop(connection);
        drop(builder);
        drop(pool);
        released(&budget, bytes);
    }
}

#[tokio::test]
async fn independent_geometry_and_once_bind_refuse_before_client_preface_io() {
    let (pool, budget, bytes) = funded_pool(1);
    let mut wrong = h2::client::Builder::new();
    wrong
        .max_frame_size(32768)
        .receive_goaway_buffer_pool(pool.clone());
    let (io, mut peer) = tokio::io::duplex(1024);
    let error = wrong.handshake::<_, Bytes>(io).await.unwrap_err();
    assert!(error.is_io());
    assert!(error.to_string().contains("exceeds pool buffer capacity"));
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    drop(error);
    let mut builder = h2::client::Builder::new();
    builder.receive_goaway_buffer_pool(pool.clone());
    let (io, _peer) = tokio::io::duplex(1024);
    let (sender, connection) = builder.clone().handshake::<_, Bytes>(io).await.unwrap();
    drop(sender);
    drop(connection);
    let (io, mut peer) = tokio::io::duplex(1024);
    let error = builder.clone().handshake::<_, Bytes>(io).await.unwrap_err();
    assert!(error.is_io());
    assert!(error.to_string().contains("already bound"));
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    let mut server = h2::server::Builder::new();
    server.receive_goaway_buffer_pool(pool.clone());
    let (io, mut peer) = tokio::io::duplex(1024);
    let error = server.handshake::<_, Bytes>(io).await.unwrap_err();
    assert!(error.is_io());
    assert!(error.to_string().contains("already bound"));
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    drop(error);
    drop(wrong);
    drop(builder);
    drop(server);
    drop(pool);
    released(&budget, bytes);
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
    fn poll_once(&self, cx: &mut Context<'_>) {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        for mut task in tasks {
            if task.as_mut().poll(cx).is_pending() {
                self.0.lock().unwrap().push(task);
            }
        }
    }
    fn cancel_all(&self) {
        drop(std::mem::take(&mut *self.0.lock().unwrap()));
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

#[tokio::test]
async fn cloned_hyper_server_preserves_goaway_error_backing_after_connection_exit() {
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    wire.extend_from_slice(&settings());
    wire.extend_from_slice(&goaway(
        0,
        h2::Reason::INTERNAL_ERROR,
        b"hyper-server-diagnostic",
    ));
    peer.write_all(&wire).await.unwrap();
    let (pool, budget, bytes) = funded_pool(2);
    let exec = ManualExecutor::default();
    let service = service_fn(|_| async { Ok::<_, Infallible>(Response::new(Empty)) });
    let mut builder = hyper::server::conn::http2::Builder::new(exec.clone());
    builder.receive_goaway_buffer_pool(pool.clone());
    let mut connection = Box::pin(builder.clone().serve_connection(TokioIo::new(io), service));
    let mut cx = Context::from_waker(Waker::noop());
    let mut error = None;
    for _ in 0..16 {
        if let Poll::Ready(result) = connection.as_mut().poll(&mut cx) {
            error = Some(result.unwrap_err());
            break;
        }
        exec.poll_once(&mut cx);
    }
    let error = error.expect("real Hyper server must return the remote GOAWAY error");
    assert_debug(
        h2_cause(&error),
        "hyper-server-diagnostic",
        h2::Reason::INTERNAL_ERROR,
    );
    assert_eq!(pool.available_buffers(), 1);
    drop(connection);
    exec.cancel_all();
    drop(builder);
    drop(pool);
    held(&budget);
    assert_debug(
        h2_cause(&error),
        "hyper-server-diagnostic",
        h2::Reason::INTERNAL_ERROR,
    );
    drop(error);
    released(&budget, bytes);
}

#[tokio::test]
async fn cloned_hyper_client_error_aliases_hold_one_original_goaway_position() {
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut wire = settings();
    wire.extend_from_slice(&goaway(
        0,
        h2::Reason::INTERNAL_ERROR,
        b"hyper-client-diagnostic",
    ));
    peer.write_all(&wire).await.unwrap();
    let (pool, budget, bytes) = funded_pool(2);
    let exec = ManualExecutor::default();
    let mut builder = hyper::client::conn::http2::Builder::new(exec.clone());
    builder.receive_goaway_buffer_pool(pool.clone());
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
    let mut cx = Context::from_waker(Waker::noop());
    let mut connection_done = false;
    let mut connection_error = None;
    let mut response_error = None;
    for _ in 0..16 {
        if !connection_done {
            if let Poll::Ready(result) = connection.as_mut().poll(&mut cx) {
                connection_done = true;
                connection_error = Some(result.unwrap_err());
            }
        }
        exec.poll_once(&mut cx);
        if response_error.is_none() {
            if let Poll::Ready(result) = response.as_mut().poll(&mut cx) {
                response_error = Some(result.unwrap_err());
            }
        }
        if connection_error.is_some() && response_error.is_some() {
            break;
        }
    }
    let connection_error = connection_error.expect("Hyper connection must return GOAWAY");
    let response_error = response_error.expect("Hyper request must preserve GOAWAY source");
    assert_debug(
        h2_cause(&connection_error),
        "hyper-client-diagnostic",
        h2::Reason::INTERNAL_ERROR,
    );
    assert_debug(
        h2_cause(&response_error),
        "hyper-client-diagnostic",
        h2::Reason::INTERNAL_ERROR,
    );
    assert_eq!(
        pool.available_buffers(),
        1,
        "multiple error aliases consume one debug slot"
    );
    drop(response);
    drop(sender);
    drop(connection);
    exec.cancel_all();
    drop(builder);
    drop(pool);
    held(&budget);
    drop(connection_error);
    held(&budget);
    assert_debug(
        h2_cause(&response_error),
        "hyper-client-diagnostic",
        h2::Reason::INTERNAL_ERROR,
    );
    drop(response_error);
    released(&budget, bytes);
}
