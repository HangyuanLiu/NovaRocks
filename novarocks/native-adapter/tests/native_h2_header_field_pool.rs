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

//! Actual h2/Hyper decoded field owners through protocol and task exit.
//! Only the fixed raw/block/field owners and their Bytes carriers receive the
//! original grants here. HeaderMap, HPACK tables, URI, peer IO, executor/tasks
//! and test scaffolds remain separate; this is not a whole-connection bound.

use bytes::Bytes;
use h2::{ReceiveFrameBuffer, ReceiveHeaderBlockBuffer, ReceiveHeaderFieldPool};
use hyper::body::{Body, Frame, Incoming};
use hyper::http::{HeaderMap, Request, Response};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const MAX: usize = 16384;
const ARENA: usize = 65536;
const POSITIONS: usize = 1024;
const DEADLINE: Duration = Duration::from_secs(5);
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[derive(Default)]
struct IoCounts {
    reads: AtomicUsize,
    writes: AtomicUsize,
    exits: AtomicUsize,
}
struct ObservedIo {
    inner: DuplexStream,
    counts: Arc<IoCounts>,
}
impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.counts.reads.fetch_add(1, Ordering::SeqCst);
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.counts.writes.fetch_add(1, Ordering::SeqCst);
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
impl Drop for ObservedIo {
    fn drop(&mut self) {
        self.counts.exits.fetch_add(1, Ordering::SeqCst);
    }
}
fn io_pair() -> (ObservedIo, DuplexStream, Arc<IoCounts>) {
    let (inner, peer) = tokio::io::duplex(131072);
    let counts = Arc::new(IoCounts::default());
    (
        ObservedIo {
            inner,
            counts: counts.clone(),
        },
        peer,
        counts,
    )
}
fn frame(wire: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    wire.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&[kind, flags]);
    wire.extend_from_slice(&stream.to_be_bytes());
    wire.extend_from_slice(payload);
}
fn prefix(server: bool) -> Vec<u8> {
    let mut wire = if server { PREFACE.to_vec() } else { Vec::new() };
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 4, 1, 0, &[]);
    wire
}
fn integer(output: &mut Vec<u8>, value: usize) {
    if value < 127 {
        output.push(value as u8);
        return;
    }
    output.push(127);
    let mut rest = value - 127;
    while rest >= 128 {
        output.push((rest as u8 & 127) | 128);
        rest >>= 7;
    }
    output.push(rest as u8);
}
fn literal(block: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    // Without indexing: dynamic table retention cannot mask field alias exit.
    block.push(0);
    integer(block, name.len());
    block.extend_from_slice(name);
    integer(block, value.len());
    block.extend_from_slice(value);
}
fn block(server: bool) -> Vec<u8> {
    let mut block = if server {
        b"\x82\x86\x84\x01\x09localhost".to_vec()
    } else {
        vec![0x88]
    };
    literal(&mut block, b"x-plain", b"alpha");
    block.extend_from_slice(b"\x00\x09x-huffman\x8c");
    // RFC 7541 example: exact Huffman encoding of www.example.com.
    block.extend_from_slice(&[
        0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
    ]);
    block
}
fn assert_fields(headers: &HeaderMap) {
    assert_eq!(headers["x-plain"], "alpha");
    assert_eq!(headers["x-huffman"], "www.example.com");
}
struct Funding {
    raw: ReceiveFrameBuffer,
    encoded: ReceiveHeaderBlockBuffer,
    fields: ReceiveHeaderFieldPool,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
    field_grant: usize,
}
fn funding_sizes(positions: usize) -> [usize; 3] {
    let carrier = Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    [
        ReceiveFrameBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
        ReceiveHeaderBlockBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
        ReceiveHeaderFieldPool::allocation_capacity_bound(ARENA, positions, MAX).unwrap() + carrier,
    ]
}
fn funded(positions: usize) -> Funding {
    let total = funding_sizes(positions).iter().sum();
    funded_on(
        positions,
        ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap()),
    )
}
fn funded_on(positions: usize, budget: Arc<ResultRetainedBudget>) -> Funding {
    let sizes = funding_sizes(positions);
    let total = sizes.iter().sum();
    // Every complete bound is acquired before any owned carrier/backing is
    // constructed. The same real Worker process budget funds all three owners.
    let mut owners = sizes.map(|size| {
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(size).unwrap()
        else {
            panic!("original raw/block/field pregrant");
        };
        Bytes::from_owner_with_exit_guard(Bytes::new(), credit)
    });
    Funding {
        raw: ReceiveFrameBuffer::new(MAX, std::mem::take(&mut owners[0])).unwrap(),
        encoded: ReceiveHeaderBlockBuffer::new(MAX, std::mem::take(&mut owners[1])).unwrap(),
        fields: ReceiveHeaderFieldPool::new(ARENA, positions, MAX, std::mem::take(&mut owners[2]))
            .unwrap(),
        budget,
        total,
        field_grant: sizes[2],
    }
}
fn reserve(budget: &Arc<ResultRetainedBudget>, bytes: usize) -> ResultWriteCredit {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("actual exited owners must make original capacity reusable");
    };
    credit
}
fn finish_aliases(funding: Funding, headers: HeaderMap) {
    assert_fields(&headers);
    let copy = headers.clone(); // HeaderMap scaffolds are separate, unfunded here.
    let plain = copy["x-plain"].clone();
    let huffman = copy["x-huffman"].clone();
    let name = copy
        .keys()
        .find(|name| name.as_str() == "x-huffman")
        .unwrap()
        .clone();
    drop(headers);
    drop(copy);
    let Funding {
        raw,
        encoded,
        fields,
        budget,
        total,
        field_grant,
    } = funding;
    drop(raw);
    drop(encoded);
    assert_eq!(fields.available_positions(), fields.field_positions() - 3);
    drop(fields);
    let transport_credit = reserve(&budget, total - field_grant);
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    assert_eq!(plain, "alpha");
    assert_eq!(huffman, "www.example.com");
    assert_eq!(name, "x-huffman");
    drop(name);
    drop(huffman);
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    drop(plain);
    drop(reserve(&budget, field_grant));
    drop(transport_credit);
    drop(reserve(&budget, total));
}
async fn abort_join<T: Send + 'static>(task: JoinHandle<T>) {
    task.abort();
    let result = tokio::time::timeout(DEADLINE, task).await.unwrap();
    assert!(
        result.is_err_and(|error| error.is_cancelled()),
        "task must actually exit through cancellation"
    );
}
fn configure_client(builder: &mut h2::client::Builder, f: &Funding) {
    builder
        .enable_push(false)
        .max_header_list_size(MAX as u32)
        .max_receive_header_block_size(MAX)
        .receive_frame_buffer(f.raw.clone())
        .receive_header_block_buffer(f.encoded.clone())
        .receive_header_field_pool(f.fields.clone());
}
fn configure_server(builder: &mut h2::server::Builder, f: &Funding) {
    builder
        .max_header_list_size(MAX as u32)
        .max_receive_header_block_size(MAX)
        .receive_frame_buffer(f.raw.clone())
        .receive_header_block_buffer(f.encoded.clone())
        .receive_header_field_pool(f.fields.clone());
}

async fn h2_server_aliases(bounded: bool) {
    let funding = bounded.then(|| funded(POSITIONS));
    let mut builder = h2::server::Builder::new();
    if let Some(f) = &funding {
        configure_server(&mut builder, f);
    }
    let (io, mut peer, counts) = io_pair();
    let mut wire = prefix(true);
    frame(&mut wire, 1, 5, 1, &block(true));
    peer.write_all(&wire).await.unwrap();
    let mut connection = tokio::time::timeout(DEADLINE, builder.handshake::<_, Bytes>(io))
        .await
        .unwrap()
        .unwrap();
    drop(builder);
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut tx = Some(tx);
        while let Some(request) = connection.accept().await {
            let (request, response) = request.unwrap();
            let (parts, body) = request.into_parts();
            drop(body);
            drop(response);
            tx.take().unwrap().send(parts.headers).unwrap();
        }
    });
    let headers = tokio::time::timeout(DEADLINE, rx).await.unwrap().unwrap();
    abort_join(task).await;
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    if let Some(f) = funding {
        finish_aliases(f, headers);
    } else {
        assert_fields(&headers);
    }
    drop(peer);
}
async fn h2_client_aliases(bounded: bool) {
    let funding = bounded.then(|| funded(POSITIONS));
    let mut builder = h2::client::Builder::new();
    if let Some(f) = &funding {
        configure_client(&mut builder, f);
    }
    let (io, mut peer, counts) = io_pair();
    peer.write_all(&prefix(false)).await.unwrap();
    let (mut sender, connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let task = tokio::spawn(connection);
    let (response, stream) = sender
        .send_request(
            Request::builder()
                .uri("http://localhost/")
                .body(())
                .unwrap(),
            true,
        )
        .unwrap();
    let mut wire = Vec::new();
    frame(&mut wire, 1, 5, 1, &block(false));
    peer.write_all(&wire).await.unwrap();
    let response = tokio::time::timeout(DEADLINE, response)
        .await
        .unwrap()
        .unwrap();
    let (parts, body) = response.into_parts();
    drop(body);
    drop(stream);
    drop(sender);
    abort_join(task).await;
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    if let Some(f) = funding {
        finish_aliases(f, parts.headers);
    } else {
        assert_fields(&parts.headers);
    }
    drop(peer);
}
#[tokio::test]
async fn h2_server_plain_huffman_owners_outlive_actual_connection_exit_and_default_none_works() {
    h2_server_aliases(true).await;
    h2_server_aliases(false).await;
}
#[tokio::test]
async fn h2_client_plain_huffman_owners_outlive_actual_connection_exit_and_default_none_works() {
    h2_client_aliases(true).await;
    h2_client_aliases(false).await;
}

#[derive(Clone, Default)]
struct JoinedExecutor(Arc<Mutex<Vec<JoinHandle<()>>>>);
impl<F> hyper::rt::Executor<F> for JoinedExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        let task = tokio::spawn(async move {
            let _ = future.await;
        });
        self.0.lock().unwrap().push(task);
    }
}
impl JoinedExecutor {
    async fn stop_and_join(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.0.lock().unwrap());
            if tasks.is_empty() {
                break;
            }
            for task in &tasks {
                task.abort();
            }
            for task in tasks {
                let _ = tokio::time::timeout(DEADLINE, task).await.unwrap();
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
}
async fn published_request(peer: &mut DuplexStream) {
    tokio::time::timeout(DEADLINE, async {
        let mut preface = [0; 24];
        peer.read_exact(&mut preface).await.unwrap();
        assert_eq!(preface, PREFACE);
        let mut headers_seen = false;
        for _ in 0..16 {
            let mut header = [0; 9];
            peer.read_exact(&mut header).await.unwrap();
            let length = (usize::from(header[0]) << 16)
                | (usize::from(header[1]) << 8)
                | usize::from(header[2]);
            assert!(length <= MAX, "bounded peer request frame");
            let mut payload = vec![0; length];
            peer.read_exact(&mut payload).await.unwrap();
            let stream = u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff;
            if header[3] == 1 && stream == 1 {
                headers_seen = true;
            }
            if headers_seen && stream == 1 && matches!(header[3], 1 | 9) && header[4] & 4 != 0 {
                return;
            }
        }
        panic!("stream 1 request HEADERS were not actually published");
    })
    .await
    .unwrap();
}
async fn hyper_server_aliases(bounded: bool) {
    let funding = bounded.then(|| funded(POSITIONS));
    let executor = JoinedExecutor::default();
    let mut builder = hyper::server::conn::http2::Builder::new(executor.clone());
    if let Some(f) = &funding {
        builder
            .max_receive_header_block_size(MAX)
            .receive_frame_buffer(f.raw.clone())
            .receive_header_block_buffer(f.encoded.clone())
            .receive_header_field_pool(f.fields.clone());
    }
    let (io, mut peer, counts) = io_pair();
    let mut wire = prefix(true);
    frame(&mut wire, 1, 5, 1, &block(true));
    peer.write_all(&wire).await.unwrap();
    let (tx, rx) = oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));
    let service = service_fn(move |request: Request<Incoming>| {
        let (parts, body) = request.into_parts();
        drop(body);
        tx.lock()
            .unwrap()
            .take()
            .unwrap()
            .send(parts.headers)
            .unwrap();
        async { Ok::<_, Infallible>(Response::new(Empty)) }
    });
    let connection = builder.clone().serve_connection(TokioIo::new(io), service);
    drop(builder);
    let task = tokio::spawn(connection);
    let headers = tokio::time::timeout(DEADLINE, rx).await.unwrap().unwrap();
    abort_join(task).await;
    executor.stop_and_join().await;
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    if let Some(f) = funding {
        finish_aliases(f, headers);
    } else {
        assert_fields(&headers);
    }
    drop(peer);
}
async fn hyper_client_aliases(bounded: bool) {
    let funding = bounded.then(|| funded(POSITIONS));
    let executor = JoinedExecutor::default();
    let mut builder = hyper::client::conn::http2::Builder::new(executor.clone());
    if let Some(f) = &funding {
        builder
            .max_receive_header_block_size(MAX)
            .receive_frame_buffer(f.raw.clone())
            .receive_header_block_buffer(f.encoded.clone())
            .receive_header_field_pool(f.fields.clone());
    }
    let (io, mut peer, counts) = io_pair();
    peer.write_all(&prefix(false)).await.unwrap();
    let (mut sender, connection) = builder
        .clone()
        .handshake::<_, Empty>(TokioIo::new(io))
        .await
        .unwrap();
    drop(builder);
    let task = tokio::spawn(connection);
    let response = sender.send_request(
        Request::builder()
            .uri("http://localhost/")
            .body(Empty)
            .unwrap(),
    );
    // Hyper dispatch creates the h2 stream in its executor task. The raw peer
    // must observe that real publication before responding on stream 1.
    published_request(&mut peer).await;
    let mut wire = Vec::new();
    frame(&mut wire, 1, 5, 1, &block(false));
    peer.write_all(&wire).await.unwrap();
    let response = tokio::time::timeout(DEADLINE, response)
        .await
        .unwrap()
        .unwrap();
    let (parts, body) = response.into_parts();
    drop(body);
    drop(sender);
    abort_join(task).await;
    executor.stop_and_join().await;
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    if let Some(f) = funding {
        finish_aliases(f, parts.headers);
    } else {
        assert_fields(&parts.headers);
    }
    drop(peer);
}
#[tokio::test]
async fn hyper_server_forwards_field_owners_through_config_clone_and_actual_task_join() {
    hyper_server_aliases(true).await;
    hyper_server_aliases(false).await;
}
#[tokio::test]
async fn hyper_client_forwards_field_owners_through_config_clone_and_actual_task_join() {
    hyper_client_aliases(true).await;
    hyper_client_aliases(false).await;
}

#[tokio::test]
async fn tonic_actual_factory_forwards_field_owners_until_last_response_alias_exit() {
    use tonic::transport::{Endpoint, Http2ConnectionConfig};
    use tower::ServiceExt;

    let total = funding_sizes(POSITIONS).iter().sum();
    let original_budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let factory_budget = original_budget.clone();
    let observed = Arc::new(Mutex::new(None));
    let factory_observed = observed.clone();
    let factories = Arc::new(AtomicUsize::new(0));
    let factory_calls = factories.clone();
    let executor = JoinedExecutor::default();
    let endpoint = Endpoint::from_static("http://localhost")
        .executor(executor.clone())
        .http2_connection_factory(move || {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            let fresh = funded_on(POSITIONS, factory_budget.clone());
            let config = Http2ConnectionConfig {
                max_header_list_size: Some(MAX as u32),
                max_receive_header_block_size: Some(MAX),
                receive_frame_buffer: Some(fresh.raw.clone()),
                receive_header_block_buffer: Some(fresh.encoded.clone()),
                receive_header_field_pool: Some(fresh.fields.clone()),
                ..Default::default()
            };
            assert!(factory_observed.lock().unwrap().replace(fresh).is_none());
            Ok::<_, io::Error>(config)
        });
    let (io, mut peer, counts) = io_pair();
    peer.write_all(&prefix(false)).await.unwrap();
    let input = Arc::new(Mutex::new(Some(io)));
    let connector = tower::service_fn(move |_: hyper::http::Uri| {
        let io = input.lock().unwrap().take();
        async move {
            io.map(TokioIo::new)
                .ok_or_else(|| io::Error::from(io::ErrorKind::ConnectionRefused))
        }
    });
    let channel = endpoint.connect_with_connector(connector).await.unwrap();
    let request = Request::builder()
        .uri("http://localhost/field-owner")
        .body(tonic::body::empty_body())
        .unwrap();
    let response_task = tokio::spawn(channel.clone().oneshot(request));
    published_request(&mut peer).await;
    let mut wire = Vec::new();
    frame(&mut wire, 1, 5, 1, &block(false));
    peer.write_all(&wire).await.unwrap();
    let response = tokio::time::timeout(DEADLINE, response_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let (parts, body) = response.into_parts();
    drop(body);
    drop(channel);
    drop(endpoint);
    executor.stop_and_join().await;
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    assert_eq!(factories.load(Ordering::SeqCst), 1);
    let funding = observed.lock().unwrap().take().unwrap();
    assert!(Arc::ptr_eq(&funding.budget, &original_budget));
    finish_aliases(funding, parts.headers);
    drop(peer);
}

#[tokio::test]
async fn h2_both_builders_reject_missing_dependencies_and_header_geometry_before_io() {
    for server in [false, true] {
        for case in 0..6 {
            let f = funded(POSITIONS);
            let (io, _peer, counts) = io_pair();
            let error = if server {
                let mut builder = h2::server::Builder::new();
                builder.receive_header_field_pool(f.fields.clone());
                if case != 0 {
                    builder.receive_frame_buffer(f.raw.clone());
                }
                if case != 1 {
                    builder.receive_header_block_buffer(f.encoded.clone());
                }
                if case != 2 {
                    builder.max_receive_header_block_size(MAX);
                }
                if case != 3 {
                    builder.max_header_list_size(if case == 4 {
                        31
                    } else if case == 5 {
                        32768
                    } else {
                        MAX as u32
                    });
                }
                let error = match builder.handshake::<_, Bytes>(io).await {
                    Err(e) => e,
                    Ok(_) => panic!("invalid server field config {case}"),
                };
                drop(builder);
                error
            } else {
                let mut builder = h2::client::Builder::new();
                builder.receive_header_field_pool(f.fields.clone());
                if case != 0 {
                    builder.receive_frame_buffer(f.raw.clone());
                }
                if case != 1 {
                    builder.receive_header_block_buffer(f.encoded.clone());
                }
                if case != 2 {
                    builder.max_receive_header_block_size(MAX);
                }
                if case != 3 {
                    builder.max_header_list_size(if case == 4 {
                        31
                    } else if case == 5 {
                        32768
                    } else {
                        MAX as u32
                    });
                }
                let error = match builder.handshake::<_, Bytes>(io).await {
                    Err(e) => e,
                    Ok(_) => panic!("invalid client field config {case}"),
                };
                drop(builder);
                error
            };
            assert!(error.is_io(), "server={server}, case={case}: {error:?}");
            assert_eq!(counts.reads.load(Ordering::SeqCst), 0);
            assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
            let total = f.total;
            let budget = f.budget.clone();
            drop(f);
            drop(reserve(&budget, total));
        }
    }
}

#[tokio::test]
async fn h2_field_pool_once_binding_refuses_with_fresh_raw_and_encoded_dependencies() {
    for server in [false, true] {
        let first = funded(POSITIONS);
        let (io, mut peer, _) = io_pair();
        if server {
            peer.write_all(&prefix(true)).await.unwrap();
            let mut builder = h2::server::Builder::new();
            configure_server(&mut builder, &first);
            let connection = builder.handshake::<_, Bytes>(io).await.unwrap();
            drop(connection);
        } else {
            let mut builder = h2::client::Builder::new();
            configure_client(&mut builder, &first);
            let (sender, connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
            drop(sender);
            drop(connection);
        }
        let second = funded(POSITIONS);
        let (io, _peer, counts) = io_pair();
        let error = if server {
            let mut builder = h2::server::Builder::new();
            configure_server(&mut builder, &second);
            builder.receive_header_field_pool(first.fields.clone());
            match builder.handshake::<_, Bytes>(io).await {
                Err(e) => e,
                Ok(_) => panic!("field pool rebound"),
            }
        } else {
            let mut builder = h2::client::Builder::new();
            configure_client(&mut builder, &second);
            builder.receive_header_field_pool(first.fields.clone());
            match builder.handshake::<_, Bytes>(io).await {
                Err(e) => e,
                Ok(_) => panic!("field pool rebound"),
            }
        };
        assert!(
            error
                .to_string()
                .contains("header field pool already bound"),
            "{error:?}"
        );
        assert_eq!(counts.reads.load(Ordering::SeqCst), 0);
        assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
        for f in [first, second] {
            let total = f.total;
            let budget = f.budget.clone();
            drop(f);
            drop(reserve(&budget, total));
        }
    }
}

#[tokio::test]
async fn field_position_exhaustion_is_an_actual_connection_error_without_waiting() {
    let f = funded(1);
    let mut builder = h2::client::Builder::new();
    configure_client(&mut builder, &f);
    let (io, mut peer, counts) = io_pair();
    peer.write_all(&prefix(false)).await.unwrap();
    let (mut sender, connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let task = tokio::spawn(connection);
    let (response, stream) = sender
        .send_request(
            Request::builder()
                .uri("http://localhost/")
                .body(())
                .unwrap(),
            true,
        )
        .unwrap();
    let mut block = vec![0x88];
    literal(&mut block, b"x-one", b"value"); // Name fits position 1; value cannot acquire position 2.
    let mut wire = Vec::new();
    frame(&mut wire, 1, 5, 1, &block);
    peer.write_all(&wire).await.unwrap();
    assert!(
        tokio::time::timeout(DEADLINE, response)
            .await
            .unwrap()
            .is_err()
    );
    let error = tokio::time::timeout(DEADLINE, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.reason(), Some(h2::Reason::PROTOCOL_ERROR));
    drop(stream);
    drop(sender);
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    let total = f.total;
    let budget = f.budget.clone();
    drop(f);
    drop(reserve(&budget, total));
}

#[tokio::test]
async fn arena_capacity_exhausts_across_retained_legal_streams_before_positions() {
    let f = funded(POSITIONS);
    let mut builder = h2::client::Builder::new();
    configure_client(&mut builder, &f);
    let (io, mut peer, counts) = io_pair();
    peer.write_all(&prefix(false)).await.unwrap();
    let (mut sender, connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let task = tokio::spawn(connection);
    let payload = vec![b'a'; 8192];
    let mut aliases = Vec::new();
    let mut streams = Vec::new();
    for index in 0..8_u32 {
        let (response, stream) = sender
            .send_request(
                Request::builder()
                    .uri("http://localhost/")
                    .body(())
                    .unwrap(),
                true,
            )
            .unwrap();
        streams.push(stream);
        let mut block = vec![0x88];
        literal(&mut block, b"x-big", &payload);
        assert!(payload.len() + b"x-big".len() + 32 < MAX);
        let mut wire = Vec::new();
        frame(&mut wire, 1, 5, 2 * index + 1, &block);
        peer.write_all(&wire).await.unwrap();
        let response = tokio::time::timeout(DEADLINE, response).await.unwrap();
        if index < 7 {
            let response = response.unwrap();
            assert_eq!(response.headers()["x-big"].as_bytes(), payload);
            aliases.push(response.headers().clone());
        } else {
            assert!(
                response.is_err(),
                "eighth legal header list must exceed aggregate arena bytes"
            );
        }
    }
    let error = tokio::time::timeout(DEADLINE, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.reason(), Some(h2::Reason::PROTOCOL_ERROR));
    assert!(
        f.fields.available_positions() > POSITIONS / 2,
        "this must exhaust bytes, not owner positions"
    );
    drop(streams);
    drop(sender);
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    let Funding {
        raw,
        encoded,
        fields,
        budget,
        total,
        field_grant,
    } = f;
    drop(raw);
    drop(encoded);
    drop(fields);
    let transport_credit = reserve(&budget, total - field_grant);
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    assert_eq!(aliases.len(), 7);
    drop(aliases);
    drop(reserve(&budget, field_grant));
    drop(transport_credit);
    drop(reserve(&budget, total));
}
