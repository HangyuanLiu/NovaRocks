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

//! Actual peer decoding and HPACK wire evidence for the outbound table ceiling.
//! This does not prove table allocation capacity or a complete connection budget.

use bytes::Bytes;
use hyper::body::{Body, Frame};
use hyper::http::{Request, Response};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::convert::Infallible;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

const VALUES: [&str; 6] = ["alpha", "alpha", "beta", "gamma", "delta", "delta"];
const LARGE_TABLE_SETTINGS: [u8; 15] = [0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255];
type Wire = Arc<Mutex<Vec<u8>>>;

struct SpyIo {
    inner: DuplexStream,
    wire: Wire,
}
impl SpyIo {
    fn new(inner: DuplexStream) -> (Self, Wire) {
        let wire = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner,
                wire: wire.clone(),
            },
            wire,
        )
    }
}
impl AsyncRead for SpyIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for SpyIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, bytes) {
            Poll::Ready(Ok(n)) => {
                self.wire.lock().unwrap().extend_from_slice(&bytes[..n]);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write_vectored(cx, bufs) {
            Poll::Ready(Ok(n)) => {
                let mut remaining = n;
                let mut wire = self.wire.lock().unwrap();
                for buf in bufs {
                    let written = remaining.min(buf.len());
                    wire.extend_from_slice(&buf[..written]);
                    remaining -= written;
                    if remaining == 0 {
                        break;
                    }
                }
                assert_eq!(remaining, 0);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// Replace the real server's first SETTINGS rather than adding a second one:
// the real connection owns exactly one initial ACK obligation. Only this
// peer fixture's advertised allowance changes; its actual HPACK decoder remains
// the ordinary implementation and receives only small legal header blocks.
struct LargePeerIo {
    inner: DuplexStream,
    replace_first_settings: bool,
}
impl AsyncRead for LargePeerIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for LargePeerIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.replace_first_settings {
            assert!(bytes.len() >= 9);
            assert_eq!(
                (bytes[3], bytes[4]),
                (4, 0),
                "first real server frame must be its initial SETTINGS"
            );
            let len = (usize::from(bytes[0]) << 16)
                | (usize::from(bytes[1]) << 8)
                | usize::from(bytes[2]);
            assert!(bytes.len() >= 9 + len);
            self.replace_first_settings = false;
            return Poll::Ready(Ok(9 + len));
        }
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
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

fn header_blocks(wire: &Wire, client: bool) -> Vec<Vec<u8>> {
    let wire = wire.lock().unwrap();
    let mut pos = if client {
        assert_eq!(&wire[..24], b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        24
    } else {
        0
    };
    let mut blocks = Vec::new();
    let mut continuation = None;
    while pos < wire.len() {
        assert!(wire.len() - pos >= 9, "partial frame header");
        let len = (usize::from(wire[pos]) << 16)
            | (usize::from(wire[pos + 1]) << 8)
            | usize::from(wire[pos + 2]);
        let ty = wire[pos + 3];
        let flags = wire[pos + 4];
        let stream = u32::from_be_bytes(wire[pos + 5..pos + 9].try_into().unwrap()) & 0x7fff_ffff;
        let end = pos + 9 + len;
        assert!(end <= wire.len(), "partial frame payload");
        let payload = &wire[pos + 9..end];
        if ty == 1 {
            assert!(continuation.is_none());
            assert_eq!(
                flags & (8 | 32),
                0,
                "fixture has no HEADERS padding or priority"
            );
            blocks.push(payload.to_vec());
            if flags & 4 == 0 {
                continuation = Some(stream);
            }
        } else if ty == 9 {
            assert_eq!(continuation, Some(stream));
            blocks.last_mut().unwrap().extend_from_slice(payload);
            if flags & 4 != 0 {
                continuation = None;
            }
        } else {
            assert!(continuation.is_none(), "interleaved continuation");
        }
        pos = end;
    }
    assert!(continuation.is_none());
    assert_eq!(blocks.len(), VALUES.len());
    blocks
}

fn integer(block: &[u8], pos: &mut usize, bits: u8) -> usize {
    let mask = (1usize << bits) - 1;
    let mut value = usize::from(block[*pos]) & mask;
    *pos += 1;
    if value < mask {
        return value;
    }
    let mut shift = 0;
    loop {
        let byte = block[*pos];
        *pos += 1;
        value += usize::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return value;
        }
        shift += 7;
        assert!(shift <= 28, "fixture HPACK integer exceeds u32");
    }
}
fn skip_string(block: &[u8], pos: &mut usize) {
    let len = integer(block, pos, 7);
    *pos += len;
    assert!(*pos <= block.len(), "truncated HPACK string");
}
fn updates(block: &[u8]) -> (Vec<usize>, usize) {
    let mut sizes = Vec::new();
    let mut pos = 0;
    while pos < block.len() && block[pos] & 0xe0 == 0x20 {
        sizes.push(integer(block, &mut pos, 5));
    }
    (sizes, pos)
}
fn dynamic_indices(block: &[u8], zero: bool) -> usize {
    let (_, mut pos) = updates(block);
    let mut dynamic = 0;
    while pos < block.len() {
        let byte = block[pos];
        if byte & 128 != 0 {
            let index = integer(block, &mut pos, 7);
            if index > 61 {
                dynamic += 1;
            }
            if zero {
                assert!(
                    (1..=61).contains(&index),
                    "zero cap referenced a dynamic entry"
                );
            }
        } else {
            assert_ne!(byte & 0xe0, 0x20, "table update appeared after a field");
            let incremental = byte & 64 != 0;
            if zero {
                assert!(!incremental, "zero cap inserted a dynamic entry");
            }
            let index = integer(block, &mut pos, if incremental { 6 } else { 4 });
            if zero {
                assert!(index <= 61, "zero cap referenced a dynamic field name");
            }
            if index == 0 {
                skip_string(block, &mut pos);
            }
            skip_string(block, &mut pos);
        }
    }
    dynamic
}
fn assert_wire(wire: &Wire, client: bool, cap: Option<u32>, repeated_dynamic: bool) {
    let blocks = header_blocks(wire, client);
    let expected = cap.map(|cap| vec![cap as usize]).unwrap_or_default();
    assert_eq!(
        updates(&blocks[0]).0,
        expected,
        "first actual HPACK table-size update"
    );
    for block in &blocks[1..] {
        assert!(
            updates(block).0.is_empty(),
            "large peer SETTINGS must not reopen the local table"
        );
    }
    for block in &blocks {
        dynamic_indices(block, cap == Some(0));
    }
    if repeated_dynamic {
        assert!(
            dynamic_indices(&blocks[1], false) > 0,
            "default/positive cap lost dynamic indexing"
        );
        assert!(
            blocks[1].len() < blocks[0].len(),
            "repeated header must compress"
        );
    }
}
fn request(value: &str) -> Request<()> {
    Request::builder()
        .uri("http://localhost/table/test")
        .header("x-table-check", value)
        .body(())
        .unwrap()
}
fn response(value: &str) -> Response<()> {
    Response::builder()
        .header("x-table-check", value)
        .body(())
        .unwrap()
}

async fn h2_server_case(cap: Option<u32>) {
    let (server_io, client_io) = tokio::io::duplex(65536);
    let (server_io, wire) = SpyIo::new(server_io);
    let server = tokio::spawn(async move {
        let mut builder = h2::server::Builder::new();
        if let Some(cap) = cap {
            builder.max_send_header_table_size(cap);
        }
        let mut connection = builder
            .clone()
            .handshake::<_, Bytes>(server_io)
            .await
            .unwrap();
        for value in VALUES {
            let (request, mut sender) = connection.accept().await.unwrap().unwrap();
            assert_eq!(request.headers()["x-table-check"], value);
            sender.send_response(response(value), true).unwrap();
        }
        if let Some(other) = connection.accept().await {
            panic!("unexpected extra request: {other:?}");
        }
    });
    let mut peer = h2::client::Builder::new();
    peer.header_table_size(if cap.is_some() { u32::MAX } else { 4096 });
    let (mut sender, mut connection) = peer.handshake::<_, Bytes>(client_io).await.unwrap();
    let mut ping = connection.ping_pong().unwrap();
    let client = tokio::spawn(connection);
    ping.ping(h2::Ping::opaque()).await.unwrap();
    for value in VALUES {
        sender = sender.ready().await.unwrap();
        let (response, stream) = sender.send_request(request(value), true).unwrap();
        drop(stream);
        assert_eq!(response.await.unwrap().headers()["x-table-check"], value);
    }
    assert_wire(&wire, false, cap, cap != Some(0));
    drop(sender);
    client.await.unwrap().unwrap();
    server.await.unwrap();
}

async fn peer_server(mut server_io: DuplexStream, large_allowance: bool) {
    // The server builder has no receive table-size setter. Replace its single
    // initial SETTINGS on the fixture wire, preserving its real ACK obligation.
    if large_allowance {
        server_io.write_all(&LARGE_TABLE_SETTINGS).await.unwrap();
    }
    let io = LargePeerIo {
        inner: server_io,
        replace_first_settings: large_allowance,
    };
    let mut connection = h2::server::handshake(io).await.unwrap();
    for value in VALUES {
        let (request, mut sender) = connection.accept().await.unwrap().unwrap();
        assert_eq!(request.headers()["x-table-check"], value);
        sender.send_response(response(value), true).unwrap();
    }
    if let Some(other) = connection.accept().await {
        panic!("unexpected extra request: {other:?}");
    }
}
async fn h2_client_case(cap: Option<u32>) {
    let (server_io, client_io) = tokio::io::duplex(65536);
    let server = tokio::spawn(peer_server(server_io, cap.is_some()));
    let (client_io, wire) = SpyIo::new(client_io);
    let mut builder = h2::client::Builder::new();
    if let Some(cap) = cap {
        builder.max_send_header_table_size(cap);
    }
    let (mut sender, mut connection) = builder
        .clone()
        .handshake::<_, Bytes>(client_io)
        .await
        .unwrap();
    let mut ping = connection.ping_pong().unwrap();
    let client = tokio::spawn(connection);
    ping.ping(h2::Ping::opaque()).await.unwrap();
    for value in VALUES {
        sender = sender.ready().await.unwrap();
        let (response, stream) = sender.send_request(request(value), true).unwrap();
        drop(stream);
        assert_eq!(response.await.unwrap().headers()["x-table-check"], value);
    }
    assert_wire(&wire, true, cap, cap.is_none());
    drop(sender);
    client.await.unwrap().unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn h2_server_zero_stays_literal_after_large_peer_settings() {
    tokio::time::timeout(Duration::from_secs(5), h2_server_case(Some(0)))
        .await
        .expect("real H2 server zero-cap fixture stalled");
}
#[tokio::test]
async fn h2_server_positive_cap_preserves_dynamic_indexing() {
    tokio::time::timeout(Duration::from_secs(5), h2_server_case(Some(128)))
        .await
        .expect("real H2 server positive-cap fixture stalled");
}
#[tokio::test]
async fn h2_server_default_none_preserves_peer_selected_table() {
    tokio::time::timeout(Duration::from_secs(5), h2_server_case(None))
        .await
        .expect("real H2 server default fixture stalled");
}
#[tokio::test]
async fn h2_client_local_caps_survive_large_peer_and_default_keeps_indexing() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for cap in [Some(0), Some(128), None] {
            h2_client_case(cap).await;
        }
    })
    .await
    .expect("real H2 client cap matrix stalled");
}

#[tokio::test]
async fn cloned_hyper_server_forwards_zero_encoder_cap_to_actual_h2() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (server_io, client_io) = tokio::io::duplex(65536);
        let (server_io, wire) = SpyIo::new(server_io);
        let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
        builder.max_send_header_table_size(0);
        let service = service_fn(|request: Request<hyper::body::Incoming>| async move {
            let value = request.headers()["x-table-check"].clone();
            Ok::<_, Infallible>(
                Response::builder()
                    .header("x-table-check", value)
                    .body(EmptyBody)
                    .unwrap(),
            )
        });
        let server = tokio::spawn(
            builder
                .clone()
                .serve_connection(TokioIo::new(server_io), service),
        );
        let mut peer = h2::client::Builder::new();
        peer.header_table_size(u32::MAX);
        let (mut sender, mut connection) = peer.handshake::<_, Bytes>(client_io).await.unwrap();
        let mut ping = connection.ping_pong().unwrap();
        let client = tokio::spawn(connection);
        ping.ping(h2::Ping::opaque()).await.unwrap();
        for value in VALUES {
            sender = sender.ready().await.unwrap();
            let (response, stream) = sender.send_request(request(value), true).unwrap();
            drop(stream);
            assert_eq!(response.await.unwrap().headers()["x-table-check"], value);
        }
        assert_wire(&wire, false, Some(0), false);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        drop(sender);
        client.await.unwrap().unwrap();
    })
    .await
    .expect("real Hyper server cap forwarding stalled");
}
#[tokio::test]
async fn cloned_hyper_client_forwards_zero_encoder_cap_to_actual_h2() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (server_io, client_io) = tokio::io::duplex(65536);
        let server = tokio::spawn(peer_server(server_io, true));
        let (client_io, wire) = SpyIo::new(client_io);
        let mut builder = hyper::client::conn::http2::Builder::new(TokioExecutor::new());
        builder.max_send_header_table_size(0);
        let (mut sender, connection) = builder
            .clone()
            .handshake::<_, EmptyBody>(TokioIo::new(client_io))
            .await
            .unwrap();
        let client = tokio::spawn(connection);
        // Hyper exposes no PingPong handle. The first complete real response
        // follows the peer's initial large SETTINGS; the next five requests
        // must keep the local zero ceiling. It also applies before request one.
        for value in VALUES {
            let request = request(value).map(|()| EmptyBody);
            assert_eq!(
                sender.send_request(request).await.unwrap().headers()["x-table-check"],
                value
            );
        }
        assert_wire(&wire, true, Some(0), false);
        drop(sender);
        client.await.unwrap().unwrap();
        server.await.unwrap();
    })
    .await
    .expect("real Hyper client cap forwarding stalled");
}
