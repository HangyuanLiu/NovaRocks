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

//! Actual retained DATA pool, escaping aliases and original funding lifetime.
//! This does not cover original codec/HPACK/header/writer or Native lane owners.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use bytes::Bytes;
use h2::ReceiveBufferPool;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
fn frame(wire: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    wire.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&[kind, flags]);
    wire.extend_from_slice(&stream.to_be_bytes());
    wire.extend_from_slice(payload);
}

fn funded_pool(slots: usize) -> (ReceiveBufferPool, Arc<ResultRetainedBudget>, usize) {
    let bytes = ReceiveBufferPool::allocation_capacity_bound(slots, 16384).unwrap()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("original process pregrant");
    };
    let ownership = Bytes::from_owner_with_exit_guard(Bytes::new(), credit);
    let pool = ReceiveBufferPool::new(slots, 16384, ownership).unwrap();
    assert_eq!(pool.buffer_positions(), slots);
    assert_eq!(pool.buffer_capacity_bytes(), 16384);
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
        panic!("original credit must follow last physical backing/owner exit");
    };
    drop(credit);
}

async fn server_aliases(payload: &[u8]) {
    let (io, mut peer) = tokio::io::duplex(64 * 1024);
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 4, 1, b"\x83\x86\x84\x01\x09localhost");
    for i in 0..3 {
        frame(&mut wire, 0, u8::from(i == 2), 1, payload);
    }
    peer.write_all(&wire).await.unwrap();
    let (pool, budget, bytes) = funded_pool(1);
    let mut builder = h2::server::Builder::new();
    builder
        .max_receive_buffered_events(8)
        .receive_buffer_pool(pool.clone());
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    let (request, response) = connection.accept().await.unwrap().unwrap();
    let mut body = request.into_body();
    let probe = Arc::new(WakeCount::default());
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(connection.poll_closed(&mut cx).is_pending());
    let Poll::Ready(Some(Ok(first))) = body.poll_data(&mut cx) else {
        panic!("first DATA");
    };
    assert_eq!(first.as_ref(), payload);
    let alias = if first.is_empty() {
        first.clone()
    } else {
        first.slice(..)
    };
    body.flow_control().release_capacity(first.len()).unwrap();
    drop(first);
    assert_eq!(pool.available_buffers(), 0);
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert!(
        body.poll_data(&mut cx).is_pending(),
        "flow release cannot free retained DATA backing"
    );
    held(&budget);
    let before = probe.0.load(Ordering::SeqCst);
    drop(alias);
    assert_eq!(pool.available_buffers(), 1);
    assert!(probe.0.load(Ordering::SeqCst) > before);
    assert!(connection.poll_closed(&mut cx).is_pending());
    let Poll::Ready(Some(Ok(second))) = body.poll_data(&mut cx) else {
        panic!("second DATA resumes");
    };
    assert_eq!(second.as_ref(), payload);
    body.flow_control().release_capacity(second.len()).unwrap();
    drop(second);
    assert!(connection.poll_closed(&mut cx).is_pending());
    let Poll::Ready(Some(Ok(third))) = body.poll_data(&mut cx) else {
        panic!("third DATA resumes");
    };
    assert_eq!(third.as_ref(), payload);
    assert!(matches!(body.poll_data(&mut cx), Poll::Ready(None)));
    let surviving = if third.is_empty() {
        third.clone()
    } else {
        third.slice(..)
    };
    drop(third);
    drop(body);
    drop(response);
    drop(connection);
    drop(builder);
    drop(pool);
    // The original pregrant remains, including the complete pool and wrapper
    // layout, after EOF, all transport futures and their builders exit.
    held(&budget);
    assert_eq!(surviving.as_ref(), payload);
    drop(surviving);
    released(&budget, bytes);
}

#[tokio::test]
async fn one_byte_data_alias_blocks_the_pool_even_after_flow_credit_release() {
    server_aliases(&[0xa5]).await;
}
#[tokio::test]
async fn empty_data_alias_retains_its_wrapper_position_and_original_credit() {
    server_aliases(&[]).await;
}

#[tokio::test]
async fn client_retained_data_has_the_same_alias_and_funding_lifetime() {
    let (io, mut peer) = tokio::io::duplex(64 * 1024);
    let mut wire = Vec::new();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 4, 1, &[0x88]);
    frame(&mut wire, 0, 0, 1, &[17]);
    frame(&mut wire, 0, 1, 1, &[18]);
    peer.write_all(&wire).await.unwrap();
    let (pool, budget, bytes) = funded_pool(1);
    let mut builder = h2::client::Builder::new();
    builder
        .enable_push(false)
        .max_receive_buffered_events(8)
        .receive_buffer_pool(pool.clone());
    let (mut requests, mut connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    let (mut response, send) = requests
        .send_request(hyper::http::Request::new(()), true)
        .unwrap();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let Poll::Ready(Ok(response)) = Pin::new(&mut response).poll(&mut cx) else {
        panic!("response headers");
    };
    let mut body = response.into_body();
    let Poll::Ready(Some(Ok(first))) = body.poll_data(&mut cx) else {
        panic!("first client DATA");
    };
    body.flow_control().release_capacity(first.len()).unwrap();
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    assert!(body.poll_data(&mut cx).is_pending());
    drop(first);
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let Poll::Ready(Some(Ok(last))) = body.poll_data(&mut cx) else {
        panic!("second client DATA");
    };
    assert_eq!(last.as_ref(), &[18]);
    drop(body);
    drop(send);
    drop(requests);
    drop(connection);
    drop(builder);
    drop(pool);
    held(&budget);
    drop(last);
    released(&budget, bytes);
}

#[tokio::test]
async fn reused_pool_and_oversized_local_frames_fail_before_handshake_io() {
    let (pool, budget, bytes) = funded_pool(1);
    let (io, mut peer) = tokio::io::duplex(1024);
    let mut builder = h2::client::Builder::new();
    builder
        .enable_push(false)
        .max_receive_buffered_events(8)
        .receive_buffer_pool(pool.clone())
        .max_frame_size(32768);
    let error = builder.handshake::<_, Bytes>(io).await.err().unwrap();
    assert!(error.to_string().contains("exceeds pool buffer capacity"));
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    let (io, mut peer) = tokio::io::duplex(1024);
    let mut server = h2::server::Builder::new();
    server
        .max_receive_buffered_events(8)
        .receive_buffer_pool(pool.clone())
        .max_frame_size(32768);
    let error = server.handshake::<_, Bytes>(io).await.err().unwrap();
    assert!(error.to_string().contains("exceeds pool buffer capacity"));
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    // A configuration refusal did not bind the pool. The first valid codec
    // can bind, and a second one is refused even after the first closes.
    builder.max_frame_size(16384);
    let (io, _valid_peer) = tokio::io::duplex(1024);
    let (requests, connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(requests);
    drop(connection);
    let (io, mut peer) = tokio::io::duplex(1024);
    let error = builder.handshake::<_, Bytes>(io).await.err().unwrap();
    assert!(error.to_string().contains("already bound"));
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    server.max_frame_size(16384);
    let (io, mut peer) = tokio::io::duplex(1024);
    let error = server.handshake::<_, Bytes>(io).await.err().unwrap();
    assert!(error.to_string().contains("already bound"));
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    drop(builder);
    drop(server);
    drop(pool);
    released(&budget, bytes);
}

#[tokio::test]
async fn pool_requires_explicit_event_bound_and_disabled_client_push() {
    let (pool, _, _) = funded_pool(1);
    let mut client = h2::client::Builder::new();
    client.receive_buffer_pool(pool.clone());
    let (io, mut peer) = tokio::io::duplex(1024);
    assert!(
        client
            .handshake::<_, Bytes>(io)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("enable_push(false)")
    );
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    client.enable_push(false);
    let (io, mut peer) = tokio::io::duplex(1024);
    assert!(
        client
            .handshake::<_, Bytes>(io)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("explicit buffered event limit")
    );
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    let mut server = h2::server::Builder::new();
    server.receive_buffer_pool(pool);
    let (io, mut peer) = tokio::io::duplex(1024);
    assert!(
        server
            .handshake::<_, Bytes>(io)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("explicit buffered event limit")
    );
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
}

#[test]
fn pool_geometry_and_capacity_arithmetic_are_checked_before_allocating() {
    for (slots, bytes) in [
        (0, 16384),
        (4097, 16384),
        (1, 16383),
        (1, 16777216),
        (usize::MAX, usize::MAX),
    ] {
        assert!(ReceiveBufferPool::allocation_capacity_bound(slots, bytes).is_err());
        assert!(ReceiveBufferPool::new(slots, bytes, Bytes::new()).is_err());
    }
    assert!(ReceiveBufferPool::allocation_capacity_bound(64, 16384).unwrap() < 2 << 20);
}
