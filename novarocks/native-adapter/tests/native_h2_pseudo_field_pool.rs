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

//! Actual server pseudo-field conversion through H2 and Hyper. Only fixed
//! raw/encoded/field/table/map families and their carriers receive original
//! Worker grants. Socket, task, peer, outgoing headers and other request metadata
//! are separate. The last-alias oracle isolates Method and Scheme independently.

use bytes::Bytes;
use h2::{
    ReceiveFrameBuffer, ReceiveHeaderBlockBuffer, ReceiveHeaderFieldPool, ReceiveHeaderTableBuffer,
};
use hyper::body::{Body, Frame, Incoming};
use hyper::http::header::HeaderMapAllocationPool;
use hyper::http::uri::Scheme;
use hyper::http::{Method, Request, Response};
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
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const MAX: usize = 16384;
const DEADLINE: Duration = Duration::from_secs(5);
const METHOD: &str = "aaaaaaaaaaaaaaaa";
const SCHEME: &str = "CuStOm+~1";
const URI: &str = "CuStOm+~1://localhost/opaque?q=1";
const HUFFMAN_METHOD: &[u8] = &[0x18, 0xc6, 0x31, 0x8c, 0x63, 0x18, 0xc6, 0x31, 0x8c, 0x63];

struct ObservedIo {
    inner: DuplexStream,
    exits: Arc<AtomicUsize>,
}
impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}
impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buffer)
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
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Clone, Default)]
struct JoinedExecutor(Arc<Mutex<Vec<JoinHandle<()>>>>);
impl<F: Future + Send + 'static> hyper::rt::Executor<F> for JoinedExecutor
where
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        self.0.lock().unwrap().push(tokio::spawn(async move {
            let _ = future.await;
        }));
    }
}
async fn joined(task: JoinHandle<()>) {
    if let Err(error) = tokio::time::timeout(DEADLINE, task).await.unwrap() {
        assert!(error.is_cancelled(), "connection task panicked: {error}");
    }
}
impl JoinedExecutor {
    async fn stop_and_join(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.0.lock().unwrap());
            if tasks.is_empty() {
                return;
            }
            for task in &tasks {
                task.abort();
            }
            for task in tasks {
                joined(task).await;
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
struct Funding {
    raw: ReceiveFrameBuffer,
    encoded: ReceiveHeaderBlockBuffer,
    fields: ReceiveHeaderFieldPool,
    table: ReceiveHeaderTableBuffer,
    maps: HeaderMapAllocationPool,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
    field_grant: usize,
}
fn reserve(budget: &Arc<ResultRetainedBudget>, bytes: usize) -> ResultWriteCredit {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("complete original family pregrant or actual exited backing");
    };
    credit
}
fn funded() -> Funding {
    let carrier = Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    let amounts = [
        ReceiveFrameBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
        ReceiveHeaderBlockBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
        ReceiveHeaderFieldPool::allocation_capacity_bound(65536, 1024, MAX).unwrap() + carrier,
        ReceiveHeaderTableBuffer::allocation_capacity_bound(4096).unwrap() + carrier,
        HeaderMapAllocationPool::allocation_capacity_bound(8, 16, 8).unwrap() + carrier,
    ];
    let total = amounts.iter().sum();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    // All five complete bounds precede construction of any carrier or backing.
    let mut owners = amounts
        .map(|bytes| Bytes::from_owner_with_exit_guard(Bytes::new(), reserve(&budget, bytes)));
    Funding {
        raw: ReceiveFrameBuffer::new(MAX, std::mem::take(&mut owners[0])).unwrap(),
        encoded: ReceiveHeaderBlockBuffer::new(MAX, std::mem::take(&mut owners[1])).unwrap(),
        fields: ReceiveHeaderFieldPool::new(65536, 1024, MAX, std::mem::take(&mut owners[2]))
            .unwrap(),
        table: ReceiveHeaderTableBuffer::new(4096, std::mem::take(&mut owners[3])).unwrap(),
        maps: HeaderMapAllocationPool::new(8, 16, 8, std::mem::take(&mut owners[4])).unwrap(),
        budget,
        total,
        field_grant: amounts[2],
    }
}
fn frame(wire: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    wire.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&[kind, flags]);
    wire.extend_from_slice(&stream.to_be_bytes());
    wire.extend_from_slice(payload);
}
fn value(block: &mut Vec<u8>, bytes: &[u8], huffman: bool) {
    assert!(bytes.len() < 127);
    block.push(bytes.len() as u8 | if huffman { 128 } else { 0 });
    block.extend_from_slice(bytes);
}
#[derive(Clone, Copy)]
enum Encoding {
    NewName,
    IndexedName,
    HuffmanNewName,
    HuffmanIndexedName,
    Incremental,
}
fn block(encoding: Encoding) -> Vec<u8> {
    let mut block = Vec::new();
    let new_name = matches!(encoding, Encoding::NewName | Encoding::HuffmanNewName);
    let huffman = matches!(
        encoding,
        Encoding::HuffmanNewName | Encoding::HuffmanIndexedName
    );
    let incremental = matches!(encoding, Encoding::Incremental);
    block.push(if incremental {
        0x42
    } else if new_name {
        0
    } else {
        2
    });
    if new_name {
        value(&mut block, b":method", false);
    }
    value(
        &mut block,
        if huffman {
            HUFFMAN_METHOD
        } else {
            METHOD.as_bytes()
        },
        huffman,
    );
    block.push(if incremental {
        0x46
    } else if new_name {
        0
    } else {
        6
    });
    if new_name {
        value(&mut block, b":scheme", false);
    }
    value(&mut block, SCHEME.as_bytes(), false);
    authority_and_path(&mut block);
    block
}
fn authority_and_path(block: &mut Vec<u8>) {
    block.push(1); // Literal without indexing, static :authority name.
    value(block, b"localhost", false);
    block.push(4); // Literal without indexing, static :path name.
    value(block, b"/opaque?q=1", false);
}
#[derive(Clone, Copy)]
enum Side {
    H2,
    Hyper,
}
struct Server {
    peer: DuplexStream,
    requests: mpsc::Receiver<Request<()>>,
    task: JoinHandle<()>,
    executor: JoinedExecutor,
    exits: Arc<AtomicUsize>,
    funding: Funding,
}
impl Server {
    async fn new(side: Side) -> Self {
        let funding = funded();
        let executor = JoinedExecutor::default();
        let (io, mut peer) = tokio::io::duplex(131072);
        let exits = Arc::new(AtomicUsize::new(0));
        let io = ObservedIo {
            inner: io,
            exits: exits.clone(),
        };
        let mut initial = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
        frame(&mut initial, 4, 0, 0, &[]);
        peer.write_all(&initial).await.unwrap();
        let (tx, requests) = mpsc::channel(4);
        let task = match side {
            Side::H2 => {
                let mut builder = h2::server::Builder::new();
                builder
                    .max_header_list_size(MAX as u32)
                    .max_receive_header_block_size(MAX)
                    .receive_frame_buffer(funding.raw.clone())
                    .receive_header_block_buffer(funding.encoded.clone())
                    .receive_header_field_pool(funding.fields.clone())
                    .receive_header_table_buffer(funding.table.clone())
                    .receive_header_map_pool(funding.maps.clone());
                let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
                drop(builder);
                tokio::spawn(async move {
                    while let Some(result) = connection.accept().await {
                        let (request, response) = result.unwrap();
                        let (parts, body) = request.into_parts();
                        drop((body, response));
                        tx.send(Request::from_parts(parts, ())).await.unwrap();
                    }
                })
            }
            Side::Hyper => {
                let mut builder = hyper::server::conn::http2::Builder::new(executor.clone());
                builder
                    .max_header_list_size(MAX as u32)
                    .max_receive_header_block_size(MAX)
                    .receive_frame_buffer(funding.raw.clone())
                    .receive_header_block_buffer(funding.encoded.clone())
                    .receive_header_field_pool(funding.fields.clone())
                    .receive_header_table_buffer(funding.table.clone())
                    .receive_header_map_pool(funding.maps.clone());
                let service = service_fn(move |request: Request<Incoming>| {
                    let tx = tx.clone();
                    async move {
                        let (parts, body) = request.into_parts();
                        drop(body);
                        tx.send(Request::from_parts(parts, ())).await.unwrap();
                        Ok::<_, Infallible>(Response::new(Empty))
                    }
                });
                let connection = builder.serve_connection(TokioIo::new(io), service);
                drop(builder);
                tokio::spawn(async move {
                    let _ = connection.await;
                })
            }
        };
        // Observe actual server SETTINGS before acknowledging or publishing a
        // request. No sleeps infer handshake progress or ownership exit.
        tokio::time::timeout(DEADLINE, async {
            for _ in 0..16 {
                let mut header = [0; 9];
                peer.read_exact(&mut header).await.unwrap();
                let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
                let mut payload = vec![0; length];
                peer.read_exact(&mut payload).await.unwrap();
                if header[3] == 4 && header[4] & 1 == 0 {
                    return;
                }
            }
            panic!("actual server SETTINGS publication gate did not complete");
        })
        .await
        .unwrap();
        let mut ack = Vec::new();
        frame(&mut ack, 4, 1, 0, &[]);
        peer.write_all(&ack).await.unwrap();
        Self {
            peer,
            requests,
            task,
            executor,
            exits,
            funding,
        }
    }
    async fn request(&mut self, stream: u32, block: &[u8]) -> Request<()> {
        let mut wire = Vec::new();
        frame(&mut wire, 1, 5, stream, block);
        self.peer.write_all(&wire).await.unwrap();
        let request = tokio::time::timeout(DEADLINE, self.requests.recv())
            .await
            .unwrap()
            .expect("actual accepted request");
        assert_eq!(request.method().as_str(), METHOD);
        assert_eq!(request.uri().scheme_str(), Some(SCHEME));
        assert_eq!(request.uri().to_string(), URI);
        request
    }
    async fn stop(self) -> Funding {
        let Self {
            peer,
            requests,
            task,
            executor,
            exits,
            funding,
        } = self;
        task.abort();
        joined(task).await;
        executor.stop_and_join().await;
        drop((peer, requests, executor));
        assert_eq!(
            exits.load(Ordering::SeqCst),
            1,
            "original IO physically exited after all connection/service tasks joined"
        );
        funding
    }
}
struct Escaped {
    method: Option<Method>,
    scheme: Option<Scheme>,
}
fn clone_request_fields(request: &Request<()>, method: bool) -> Escaped {
    let copy = request.clone();
    let uri = copy.uri().clone();
    assert_eq!(
        copy.method().as_str().as_ptr(),
        request.method().as_str().as_ptr()
    );
    assert_eq!(
        uri.scheme().unwrap().as_str().as_ptr(),
        request.uri().scheme().unwrap().as_str().as_ptr()
    );
    assert_eq!(uri.to_string(), URI);
    Escaped {
        method: method.then(|| copy.method().clone()),
        scheme: (!method).then(|| uri.scheme().unwrap().clone()),
    }
}
fn assert_escaped(escaped: &Escaped) {
    if let Some(method) = &escaped.method {
        assert_eq!(method.as_str(), METHOD);
    }
    if let Some(scheme) = &escaped.scheme {
        assert_eq!(scheme.as_str(), SCHEME);
    }
}
fn finish_original_owner(funding: Funding, escaped: Escaped) {
    let budget = funding.budget.clone();
    let (total, field_grant) = (funding.total, funding.field_grant);
    let copy = Escaped {
        method: escaped.method.clone(),
        scheme: escaped.scheme.clone(),
    };
    drop(funding); // All public raw/block/field/table/map aliases, including map -> field.
    assert_escaped(&escaped);
    assert!(matches!(
        budget.try_reserve_process(total).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    drop(reserve(&budget, total - field_grant)); // No other funded family remains live.
    drop(escaped);
    assert_escaped(&copy);
    assert!(matches!(
        budget.try_reserve_process(total).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    drop(copy);
    drop(reserve(&budget, total));
}
async fn isolated(side: Side, encoding: Encoding, method: bool) {
    let mut server = Server::new(side).await;
    let request = server.request(1, &block(encoding)).await;
    let escaped = clone_request_fields(&request, method);
    drop(request);
    let funding = server.stop().await;
    finish_original_owner(funding, escaped);
}

#[tokio::test(flavor = "current_thread")]
async fn h2_literal_new_name_long_method_keeps_original_field_owner() {
    isolated(Side::H2, Encoding::NewName, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn indexed_name_and_hyper_huffman_long_method_keep_original_field_owner() {
    isolated(Side::H2, Encoding::IndexedName, true).await;
    isolated(Side::Hyper, Encoding::HuffmanIndexedName, true).await;
    isolated(Side::Hyper, Encoding::HuffmanNewName, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn custom_scheme_is_original_owned_after_h2_and_hyper_request_exit() {
    for side in [Side::H2, Side::Hyper] {
        for encoding in [Encoding::NewName, Encoding::IndexedName] {
            isolated(side, encoding, false).await;
        }
    }
}
#[tokio::test(flavor = "current_thread")]
async fn dynamic_indexed_method_scheme_request_clones_survive_table_exit() {
    for side in [Side::H2, Side::Hyper] {
        let mut server = Server::new(side).await;
        let first = server.request(1, &block(Encoding::Incremental)).await;
        let mut indexed = vec![0xbf, 0xbe]; // Index 63 Method, 62 Scheme after two incremental insertions.
        authority_and_path(&mut indexed);
        let second = server.request(3, &indexed).await;
        assert_eq!(
            first.method().as_str().as_ptr(),
            second.method().as_str().as_ptr()
        );
        assert_eq!(
            first.uri().scheme().unwrap().as_str().as_ptr(),
            second.uri().scheme().unwrap().as_str().as_ptr()
        );
        let method = clone_request_fields(&second, true);
        let scheme = clone_request_fields(&second, false);
        drop((first, second));
        let funding = server.stop().await;
        let budget = funding.budget.clone();
        let (total, field_grant) = (funding.total, funding.field_grant);
        drop(funding);
        drop(reserve(&budget, total - field_grant));
        assert_escaped(&method);
        assert_escaped(&scheme);
        assert!(matches!(
            budget.try_reserve_process(total).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        drop(method);
        assert!(matches!(
            budget.try_reserve_process(total).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        drop(scheme);
        drop(reserve(&budget, total));
    }
}
