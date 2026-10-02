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

//! Real H2 parsing, body ownership and connection wakeups under tiny frames.
//! This does not certify listener/lane wiring or whole connection backing.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use bytes::Bytes;
use hyper::http::Response;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

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
fn frame(target: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    let length = payload.len() as u32;
    target.extend_from_slice(&length.to_be_bytes()[1..]);
    target.extend_from_slice(&[kind, flags]);
    target.extend_from_slice(&stream.to_be_bytes());
    target.extend_from_slice(payload);
}
async fn read_frame(peer: &mut DuplexStream) -> (u8, u8, Vec<u8>) {
    let mut header = [0; 9];
    peer.read_exact(&mut header).await.unwrap();
    let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
    let mut payload = vec![0; length];
    peer.read_exact(&mut payload).await.unwrap();
    (header[3], header[4], payload)
}
async fn run_tiny_frames(payload: &[u8], limit: usize) {
    const FRAMES: usize = 64;
    let (io, mut peer) = tokio::io::duplex(64 * 1024);
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    frame(&mut wire, 4, 0, 0, &[]); // SETTINGS
    frame(&mut wire, 4, 1, 0, &[]); // ACK server's initial SETTINGS
    // HPACK static POST/http/path, then literal authority=localhost.
    let mut headers = vec![0x83, 0x86, 0x84, 0x01, 9];
    headers.extend_from_slice(b"localhost");
    frame(&mut wire, 1, 4, 1, &headers);
    for i in 0..FRAMES {
        frame(&mut wire, 0, u8::from(i == FRAMES - 1), 1, payload);
    }
    // All bytes exist before parsing; TCP scheduling cannot make the queue
    // accidentally small and hide a missing admission check.
    peer.write_all(&wire).await.unwrap();
    let mut builder = h2::server::Builder::new();
    builder.max_receive_buffered_events(limit);
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    let (request, mut response) = connection.accept().await.unwrap().unwrap();
    assert_eq!(request.uri().path(), "/");
    let mut body = request.into_body();
    let probe = Arc::new(WakeCount::default());
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);

    // Removing request headers has freed a node. Fill exactly the data cap.
    assert!(matches!(connection.poll_closed(&mut cx), Poll::Pending));
    let before = probe.0.load(Ordering::SeqCst);
    for _ in 0..limit {
        let Poll::Ready(Some(Ok(bytes))) = body.poll_data(&mut cx) else {
            panic!("expected a buffered tiny DATA frame");
        };
        assert_eq!(bytes.as_ref(), payload);
        body.flow_control().release_capacity(bytes.len()).unwrap();
    }
    assert!(
        body.poll_data(&mut cx).is_pending(),
        "connection must stop decoding when event nodes are full"
    );
    assert!(probe.0.load(Ordering::SeqCst) > before);

    // Full receive queues must not prevent outbound response progress.
    response.send_response(Response::new(()), true).unwrap();
    assert!(matches!(connection.poll_closed(&mut cx), Poll::Pending));
    let mut response_seen = false;
    for _ in 0..4 {
        let (kind, flags, _) = read_frame(&mut peer).await;
        if kind == 1 {
            assert_eq!(flags & 1, 1);
            response_seen = true;
            break;
        }
    }
    assert!(
        response_seen,
        "outbound writes still flush under receive backpressure"
    );

    // Repeated dequeue/refill preserves every frame, including empty DATA.
    let mut received = limit;
    while received < FRAMES {
        assert!(matches!(connection.poll_closed(&mut cx), Poll::Pending));
        for _ in 0..limit.min(FRAMES - received) {
            let Poll::Ready(Some(Ok(bytes))) = body.poll_data(&mut cx) else {
                panic!("consumer removal must resume exact queued DATA");
            };
            assert_eq!(bytes.as_ref(), payload);
            body.flow_control().release_capacity(bytes.len()).unwrap();
            received += 1;
        }
        if received < FRAMES {
            assert!(body.poll_data(&mut cx).is_pending());
        }
    }
    assert!(matches!(body.poll_data(&mut cx), Poll::Ready(None)));
}

#[tokio::test]
async fn one_byte_data_applies_node_backpressure_and_resumes_without_loss() {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run_tiny_frames(&[0xa5], 2),
    )
    .await
    .expect("bounded H2 one-byte progress");
}

#[tokio::test]
async fn empty_nonterminal_data_has_the_same_node_bound() {
    tokio::time::timeout(std::time::Duration::from_secs(5), run_tiny_frames(&[], 2))
        .await
        .expect("bounded H2 empty DATA progress");
}

#[tokio::test]
async fn client_receive_nodes_resume_under_the_same_byte_independent_limit() {
    let (io, mut peer) = tokio::io::duplex(64 * 1024);
    let mut wire = Vec::new();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 4, 1, &[0x88]); // HPACK :status 200
    for i in 0..64 {
        frame(&mut wire, 0, u8::from(i == 63), 1, &[i]);
    }
    peer.write_all(&wire).await.unwrap();
    let mut builder = h2::client::Builder::new();
    builder.enable_push(false);
    builder.max_receive_buffered_events(2);
    let (mut requests, mut connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    let (mut response, _) = requests
        .send_request(hyper::http::Request::new(()), true)
        .unwrap();
    let probe = Arc::new(WakeCount::default());
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let Poll::Ready(Ok(response)) = Pin::new(&mut response).poll(&mut cx) else {
        panic!("exact response headers are buffered");
    };
    assert_eq!(response.status(), 200);
    let mut body = response.into_body();
    for next in (0..64).step_by(2) {
        assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
        let before = probe.0.load(Ordering::SeqCst);
        for expected in next..next + 2 {
            let Poll::Ready(Some(Ok(bytes))) = body.poll_data(&mut cx) else {
                panic!("connection must refill two nodes after consumption");
            };
            assert_eq!(bytes.as_ref(), &[expected as u8]);
            body.flow_control().release_capacity(bytes.len()).unwrap();
        }
        assert!(probe.0.load(Ordering::SeqCst) > before);
        if next < 62 {
            assert!(body.poll_data(&mut cx).is_pending());
        }
    }
    assert!(matches!(body.poll_data(&mut cx), Poll::Ready(None)));
}

#[tokio::test]
async fn dropping_a_full_receive_body_wakes_the_next_request() {
    let (io, mut peer) = tokio::io::duplex(64 * 1024);
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    let headers = b"\x83\x86\x84\x01\x09localhost";
    frame(&mut wire, 1, 4, 1, headers);
    for i in 0..64 {
        frame(&mut wire, 0, u8::from(i == 63), 1, &[]);
    }
    frame(&mut wire, 1, 5, 3, headers);
    peer.write_all(&wire).await.unwrap();
    let mut builder = h2::server::Builder::new();
    builder.max_receive_buffered_events(2);
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    let (request, _response) = connection.accept().await.unwrap().unwrap();
    let body = request.into_body();
    let probe = Arc::new(WakeCount::default());
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(connection.poll_closed(&mut cx).is_pending());
    let before = probe.0.load(Ordering::SeqCst);
    drop(body);
    assert!(probe.0.load(Ordering::SeqCst) > before);
    let (request, _) = tokio::time::timeout(std::time::Duration::from_secs(5), connection.accept())
        .await
        .expect("physical clear must resume the next exact stream")
        .unwrap()
        .unwrap();
    assert_eq!(request.body().stream_id().as_u32(), 3);
}

#[tokio::test]
async fn default_builder_keeps_the_upstream_receive_path() {
    let (io, mut peer) = tokio::io::duplex(64 * 1024);
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 4, 1, b"\x83\x86\x84\x01\x09localhost");
    for i in 0..8 {
        frame(&mut wire, 0, u8::from(i == 7), 1, &[i]);
    }
    peer.write_all(&wire).await.unwrap();
    let mut connection = h2::server::Builder::new()
        .handshake::<_, Bytes>(io)
        .await
        .unwrap();
    let (request, _) = connection.accept().await.unwrap().unwrap();
    let mut body = request.into_body();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    // No connection re-poll: the default path still queues every available
    // event. Only an explicitly installed limit adds count backpressure.
    for expected in 0..8 {
        let Poll::Ready(Some(Ok(bytes))) = body.poll_data(&mut cx) else {
            panic!("default builder must retain upstream readiness");
        };
        assert_eq!(bytes.as_ref(), &[expected]);
    }
    assert!(matches!(body.poll_data(&mut cx), Poll::Ready(None)));
}

#[tokio::test]
async fn dropping_unpolled_headers_only_response_releases_the_last_event_node() {
    let (io, mut peer) = tokio::io::duplex(64 * 1024);
    let mut wire = Vec::new();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    frame(&mut wire, 1, 5, 1, &[0x88]);
    frame(&mut wire, 1, 5, 3, &[0x88]);
    peer.write_all(&wire).await.unwrap();
    let mut builder = h2::client::Builder::new();
    builder.enable_push(false);
    builder.max_receive_buffered_events(1);
    let (mut requests, mut connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    let (unpolled, send) = requests
        .send_request(hyper::http::Request::new(()), true)
        .unwrap();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    // No RecvStream ever exists for this response. Its header event must leave
    // when the last original stream reference exits, even with zero DATA.
    drop(unpolled);
    drop(send);
    let (mut response, _send) = requests
        .send_request(hyper::http::Request::new(()), true)
        .unwrap();
    assert_eq!(response.stream_id().as_u32(), 3);
    assert!(Pin::new(&mut connection).poll(&mut cx).is_pending());
    let Poll::Ready(Ok(response)) = Pin::new(&mut response).poll(&mut cx) else {
        panic!("a canceled headers-only response must not strand the bounded receive node");
    };
    assert_eq!(response.status(), 200);
    assert!(response.body().is_end_stream());
}

#[tokio::test]
async fn bounded_client_refuses_push_before_writing_a_handshake_preface() {
    let (io, mut peer) = tokio::io::duplex(1024);
    let mut builder = h2::client::Builder::new();
    builder.max_receive_buffered_events(1);
    let error = builder.handshake::<_, Bytes>(io).await.err().unwrap();
    assert!(error.to_string().contains("explicit enable_push(false)"));
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
}
