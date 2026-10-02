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

//! Actual typed incoming HPACK table ownership through wire and task exit.
//! Only fixed raw/block/field/table owners and their Bytes carriers receive the
//! original grants here. HeaderMap, pseudo-header conversions, URI, peer IO, executor/tasks
//! and test scaffolds remain separate; this is not a whole-connection bound.

use bytes::Bytes;
use h2::{
    ReceiveFrameBuffer, ReceiveHeaderBlockBuffer, ReceiveHeaderFieldPool, ReceiveHeaderTableBuffer,
};
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
use tokio::sync::mpsc;
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

const TABLE: usize = 4096;
struct Funding {
    raw: ReceiveFrameBuffer,
    block: ReceiveHeaderBlockBuffer,
    fields: ReceiveHeaderFieldPool,
    table: ReceiveHeaderTableBuffer,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
    field_grant: usize,
}
fn sizes() -> [usize; 4] {
    let carrier = Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    [
        ReceiveFrameBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
        ReceiveHeaderBlockBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
        ReceiveHeaderFieldPool::allocation_capacity_bound(ARENA, POSITIONS, MAX).unwrap() + carrier,
        ReceiveHeaderTableBuffer::allocation_capacity_bound(TABLE).unwrap() + carrier,
    ]
}
fn funded(budget: Arc<ResultRetainedBudget>) -> Funding {
    let amounts = sizes();
    let mut owners = amounts.map(|bytes| {
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap()
        else {
            panic!("complete original raw/block/field/table pregrant");
        };
        Bytes::from_owner_with_exit_guard(Bytes::new(), credit)
    });
    Funding {
        raw: ReceiveFrameBuffer::new(MAX, std::mem::take(&mut owners[0])).unwrap(),
        block: ReceiveHeaderBlockBuffer::new(MAX, std::mem::take(&mut owners[1])).unwrap(),
        fields: ReceiveHeaderFieldPool::new(ARENA, POSITIONS, MAX, std::mem::take(&mut owners[2]))
            .unwrap(),
        table: ReceiveHeaderTableBuffer::new(TABLE, std::mem::take(&mut owners[3])).unwrap(),
        budget,
        total: amounts.iter().sum(),
        field_grant: amounts[2],
    }
}
fn budget() -> Arc<ResultRetainedBudget> {
    ResultRetainedBudget::new(NonZeroUsize::new(sizes().iter().sum()).unwrap())
}
fn reserve(budget: &Arc<ResultRetainedBudget>, bytes: usize) -> ResultWriteCredit {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("actual exited backing must make original credit available");
    };
    credit
}
fn client_config(builder: &mut h2::client::Builder, f: &Funding, advertised: u32, fixed: bool) {
    builder
        .max_header_list_size(MAX as u32)
        .max_receive_header_block_size(MAX)
        .receive_frame_buffer(f.raw.clone())
        .receive_header_block_buffer(f.block.clone())
        .receive_header_field_pool(f.fields.clone())
        .header_table_size(advertised);
    if fixed {
        builder.receive_header_table_buffer(f.table.clone());
    }
}
fn server_config(builder: &mut h2::server::Builder, f: &Funding, advertised: u32, fixed: bool) {
    builder
        .max_header_list_size(MAX as u32)
        .max_receive_header_block_size(MAX)
        .receive_frame_buffer(f.raw.clone())
        .receive_header_block_buffer(f.block.clone())
        .receive_header_field_pool(f.fields.clone())
        .header_table_size(advertised);
    if fixed {
        builder.receive_header_table_buffer(f.table.clone());
    }
}
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Side {
    H2Client,
    H2Server,
    HyperClient,
    HyperServer,
    Tonic,
}
impl Side {
    fn server(self) -> bool {
        matches!(self, Self::H2Server | Self::HyperServer)
    }
}
enum Requests {
    H2(h2::client::SendRequest<Bytes>),
    Hyper(hyper::client::conn::http2::SendRequest<Empty>),
    Tonic(tonic::transport::Channel),
    Server(mpsc::Receiver<HeaderMap>),
}
struct Peer {
    side: Side,
    requests: Option<Requests>,
    io: DuplexStream,
    counts: Arc<IoCounts>,
    task: Option<JoinHandle<Result<(), String>>>,
    executor: JoinedExecutor,
    funding: Option<Funding>,
    original_budget: Arc<ResultRetainedBudget>,
    total: usize,
    field_grant: usize,
    preface_read: bool,
    settings_read: bool,
    next_stream: u32,
    ack_sent: bool,
    advertised: u32,
}
impl Peer {
    async fn new(side: Side, advertised: u32, fixed: bool) -> Self {
        let original = budget();
        let executor = JoinedExecutor::default();
        let (io, mut wire, counts) = io_pair();
        // The ACK is sent deliberately later by request(), so pre-ACK stock
        // tests never infer acknowledgement from local builder state.
        let mut initial = if side.server() {
            PREFACE.to_vec()
        } else {
            Vec::new()
        };
        frame(&mut initial, 4, 0, 0, &[]);
        wire.write_all(&initial).await.unwrap();
        let (requests, task, funding) = match side {
            Side::H2Client => {
                let f = funded(original);
                let mut builder = h2::client::Builder::new();
                client_config(&mut builder, &f, advertised, fixed);
                let (sender, connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
                drop(builder);
                let task =
                    tokio::spawn(async move { connection.await.map_err(|e| format!("{e:?}")) });
                (Requests::H2(sender), Some(task), f)
            }
            Side::H2Server => {
                let f = funded(original);
                let mut builder = h2::server::Builder::new();
                server_config(&mut builder, &f, advertised, fixed);
                let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
                drop(builder);
                let (tx, rx) = mpsc::channel(1);
                let task = tokio::spawn(async move {
                    while let Some(result) = connection.accept().await {
                        let (request, response) = result.map_err(|e| format!("{e:?}"))?;
                        let (parts, body) = request.into_parts();
                        drop(body);
                        drop(response);
                        tx.send(parts.headers).await.map_err(|e| e.to_string())?;
                    }
                    Ok(())
                });
                (Requests::Server(rx), Some(task), f)
            }
            Side::HyperClient => {
                let f = funded(original);
                let mut builder = hyper::client::conn::http2::Builder::new(executor.clone());
                builder
                    .max_receive_header_block_size(MAX)
                    .receive_frame_buffer(f.raw.clone())
                    .receive_header_block_buffer(f.block.clone())
                    .receive_header_field_pool(f.fields.clone())
                    .header_table_size(advertised);
                if fixed {
                    builder.receive_header_table_buffer(f.table.clone());
                }
                let (sender, connection) = builder
                    .handshake::<_, Empty>(TokioIo::new(io))
                    .await
                    .unwrap();
                drop(builder);
                let task =
                    tokio::spawn(async move { connection.await.map_err(|e| format!("{e:?}")) });
                (Requests::Hyper(sender), Some(task), f)
            }
            Side::HyperServer => {
                let f = funded(original);
                let mut builder = hyper::server::conn::http2::Builder::new(executor.clone());
                builder
                    .max_receive_header_block_size(MAX)
                    .receive_frame_buffer(f.raw.clone())
                    .receive_header_block_buffer(f.block.clone())
                    .receive_header_field_pool(f.fields.clone())
                    .header_table_size(advertised);
                if fixed {
                    builder.receive_header_table_buffer(f.table.clone());
                }
                let (tx, rx) = mpsc::channel(1);
                let service = service_fn(move |request: Request<Incoming>| {
                    let tx = tx.clone();
                    async move {
                        let (parts, body) = request.into_parts();
                        drop(body);
                        tx.send(parts.headers).await.unwrap();
                        Ok::<_, Infallible>(Response::new(Empty))
                    }
                });
                let connection = builder.serve_connection(TokioIo::new(io), service);
                drop(builder);
                let task =
                    tokio::spawn(async move { connection.await.map_err(|e| format!("{e:?}")) });
                (Requests::Server(rx), Some(task), f)
            }
            Side::Tonic => {
                let observed = Arc::new(Mutex::new(None));
                let saved = observed.clone();
                let factory_budget = original;
                let endpoint = tonic::transport::Endpoint::from_static("http://localhost")
                    .executor(executor.clone())
                    .http2_connection_factory(move || {
                        let f = funded(factory_budget.clone());
                        let config = tonic::transport::Http2ConnectionConfig {
                            max_header_list_size: Some(MAX as u32),
                            max_receive_header_block_size: Some(MAX),
                            header_table_size: Some(advertised),
                            receive_frame_buffer: Some(f.raw.clone()),
                            receive_header_block_buffer: Some(f.block.clone()),
                            receive_header_field_pool: Some(f.fields.clone()),
                            receive_header_table_buffer: fixed.then(|| f.table.clone()),
                            ..Default::default()
                        };
                        assert!(
                            saved.lock().unwrap().replace(f).is_none(),
                            "one actual connection attempt"
                        );
                        Ok::<_, io::Error>(config)
                    });
                let input = Arc::new(Mutex::new(Some(io)));
                let connector = tower::service_fn(move |_: hyper::http::Uri| {
                    let io = input.lock().unwrap().take();
                    async move {
                        io.map(TokioIo::new)
                            .ok_or_else(|| io::Error::from(io::ErrorKind::ConnectionRefused))
                    }
                });
                let channel = endpoint.connect_with_connector(connector).await.unwrap();
                let funding = observed.lock().unwrap().take().unwrap();
                drop(endpoint);
                (Requests::Tonic(channel), None, funding)
            }
        };
        let original_budget = funding.budget.clone();
        let total = funding.total;
        let field_grant = funding.field_grant;
        Self {
            side,
            requests: Some(requests),
            io: wire,
            counts,
            task,
            executor,
            funding: Some(funding),
            original_budget,
            total,
            field_grant,
            preface_read: false,
            settings_read: false,
            next_stream: 1,
            ack_sent: false,
            advertised,
        }
    }
    async fn read_frame(&mut self) -> (u8, u8, u32, Vec<u8>) {
        let mut header = [0; 9];
        self.io.read_exact(&mut header).await.unwrap();
        let len =
            (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
        assert!(len <= MAX, "bounded raw peer output frame");
        let mut payload = vec![0; len];
        self.io.read_exact(&mut payload).await.unwrap();
        let stream = u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff;
        if header[3] == 4 && header[4] & 1 == 0 {
            let size = payload.chunks_exact(6).find_map(|setting| {
                (setting[..2] == [0, 1])
                    .then(|| u32::from_be_bytes(setting[2..].try_into().unwrap()))
            });
            assert_eq!(
                size,
                Some(self.advertised),
                "actual SETTINGS header table advertisement"
            );
            self.settings_read = true;
        }
        (header[3], header[4], stream, payload)
    }
    async fn published(&mut self, stream: u32) {
        tokio::time::timeout(DEADLINE, async {
            if !self.side.server() && !self.preface_read {
                let mut preface = [0; 24];
                self.io.read_exact(&mut preface).await.unwrap();
                assert_eq!(preface, PREFACE);
                self.preface_read = true;
            }
            let mut headers = false;
            for _ in 0..16 {
                if self.side.server() && self.settings_read {
                    return;
                }
                let (kind, flags, id, _) = self.read_frame().await;
                if kind == 1 && id == stream {
                    headers = true;
                }
                if !self.side.server()
                    && headers
                    && id == stream
                    && matches!(kind, 1 | 9)
                    && flags & 4 != 0
                {
                    return;
                }
            }
            panic!("actual request/SETTINGS publication gate did not complete");
        })
        .await
        .unwrap();
    }
    async fn request(&mut self, pieces: &[&[u8]], ack: bool) -> Result<HeaderMap, String> {
        use tower::ServiceExt;
        let stream_id = self.next_stream;
        self.next_stream += 2;
        let response: Option<JoinHandle<Result<HeaderMap, String>>> =
            match self.requests.as_mut().unwrap() {
                Requests::H2(sender) => {
                    let (response, stream) = sender
                        .send_request(
                            Request::builder()
                                .uri("http://localhost/")
                                .body(())
                                .unwrap(),
                            true,
                        )
                        .unwrap();
                    Some(tokio::spawn(async move {
                        let response = response.await.map_err(|e| format!("{e:?}"))?;
                        drop(stream);
                        let (parts, body) = response.into_parts();
                        drop(body);
                        Ok(parts.headers)
                    }))
                }
                Requests::Hyper(sender) => {
                    let response = sender.send_request(
                        Request::builder()
                            .uri("http://localhost/")
                            .body(Empty)
                            .unwrap(),
                    );
                    Some(tokio::spawn(async move {
                        let response = response.await.map_err(|e| format!("{e:?}"))?;
                        let (parts, body) = response.into_parts();
                        drop(body);
                        Ok(parts.headers)
                    }))
                }
                Requests::Tonic(channel) => {
                    let future = channel.clone().oneshot(
                        Request::builder()
                            .uri("http://localhost/table")
                            .body(tonic::body::empty_body())
                            .unwrap(),
                    );
                    Some(tokio::spawn(async move {
                        let response = future.await.map_err(|e| format!("{e:?}"))?;
                        let (parts, body) = response.into_parts();
                        drop(body);
                        Ok(parts.headers)
                    }))
                }
                Requests::Server(_) => None,
            };
        self.published(stream_id).await;
        let mut wire = Vec::new();
        if ack && !self.ack_sent {
            frame(&mut wire, 4, 1, 0, &[]);
            self.ack_sent = true;
        }
        for (index, piece) in pieces.iter().enumerate() {
            let first = index == 0;
            let last = index + 1 == pieces.len();
            frame(
                &mut wire,
                if first { 1 } else { 9 },
                u8::from(first) | if last { 4 } else { 0 },
                stream_id,
                piece,
            );
        }
        self.io.write_all(&wire).await.unwrap();
        if let Some(response) = response {
            tokio::time::timeout(DEADLINE, response)
                .await
                .unwrap()
                .unwrap()
        } else {
            let Requests::Server(rx) = self.requests.as_mut().unwrap() else {
                unreachable!()
            };
            tokio::time::timeout(DEADLINE, rx.recv())
                .await
                .unwrap()
                .ok_or_else(|| "connection refused header publication".into())
        }
    }
    async fn compression_error(&mut self) {
        let task = self.task.take().expect("direct h2 connection result");
        let error = tokio::time::timeout(DEADLINE, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            error.contains("COMPRESSION_ERROR"),
            "actual connection error: {error}"
        );
    }
    fn drop_public_owners(&mut self) {
        drop(self.funding.take());
        // All public raw/block/field/table handles and builders have exited.
        // A live real connection must retain all original grants. Omitting
        // table forwarding releases its independent grant and fails here.
        assert!(matches!(
            self.original_budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    async fn finish(mut self, aliases: Vec<HeaderMap>) {
        drop(self.requests.take());
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = tokio::time::timeout(DEADLINE, task).await.unwrap();
        }
        self.executor.stop_and_join().await;
        assert_eq!(self.counts.exits.load(Ordering::SeqCst), 1);
        // This also drops any handles retained for the independent bind test.
        drop(self.funding.take());
        let budget = self.original_budget;
        let total = self.total;
        let field_grant = self.field_grant;
        if aliases.is_empty() {
            drop(reserve(&budget, total));
        } else {
            let table_and_input = reserve(&budget, total - field_grant);
            assert!(matches!(
                budget.try_reserve_process(1).unwrap(),
                ResultWriteAdmission::Blocked
            ));
            drop(aliases);
            drop(reserve(&budget, field_grant));
            drop(table_and_input);
            drop(reserve(&budget, total));
        }
    }
}
fn pseudo(server: bool) -> Vec<u8> {
    if server {
        b"\x82\x86\x84\x01\x09localhost".to_vec()
    } else {
        vec![0x88]
    }
}
fn indexed_literal(block: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    let offset = block.len();
    literal(block, name, value);
    block[offset] = 0x40;
}
async fn dynamic_wrap(side: Side, fixed: bool, inserts: usize) {
    let mut peer = Peer::new(side, TABLE as u32, fixed).await;
    let mut first = pseudo(side.server());
    indexed_literal(&mut first, b"x-one", b"alpha");
    indexed_literal(&mut first, b"x-two", b"beta");
    let first = peer.request(&[&first], true).await.unwrap();
    let mut indexed = pseudo(side.server());
    indexed.extend_from_slice(&[0xbe, 0xbf]); // Most recent x-two, then x-one.
    let second = peer.request(&[&indexed], true).await.unwrap();
    assert_eq!(second["x-one"], "alpha");
    assert_eq!(second["x-two"], "beta");
    assert_eq!(
        first["x-one"].as_bytes().as_ptr(),
        second["x-one"].as_bytes().as_ptr()
    );
    drop(second);
    if fixed {
        peer.drop_public_owners();
    }
    for index in 0..inserts {
        let name = format!("x-{index:04}");
        let mut block = pseudo(side.server());
        indexed_literal(&mut block, name.as_bytes(), b"z");
        let received = peer.request(&[&block], true).await.unwrap();
        assert_eq!(received[name.as_str()], "z");
    }
    if inserts >= 2 {
        let mut latest = pseudo(side.server());
        latest.extend_from_slice(&[0xbe, 0xbf]);
        let received = peer.request(&[&latest], true).await.unwrap();
        assert_eq!(received[format!("x-{:04}", inserts - 1).as_str()], "z");
        assert_eq!(received[format!("x-{:04}", inserts - 2).as_str()], "z");
    }
    assert_eq!(first["x-one"], "alpha", "old aliases survive eviction/wrap");
    assert_eq!(first["x-two"], "beta");
    peer.finish(vec![first]).await;
}
#[tokio::test]
async fn h2_client_typed_ring_index_eviction_wrap_and_default_none_preserve_field_aliases() {
    dynamic_wrap(Side::H2Client, true, 130).await;
    dynamic_wrap(Side::H2Client, false, 2).await;
}
#[tokio::test]
async fn h2_server_typed_ring_index_eviction_wrap_and_default_none_preserve_field_aliases() {
    dynamic_wrap(Side::H2Server, true, 130).await;
    dynamic_wrap(Side::H2Server, false, 2).await;
}
#[tokio::test]
async fn hyper_both_directions_and_tonic_fresh_factory_use_actual_typed_table() {
    for side in [Side::HyperClient, Side::HyperServer, Side::Tonic] {
        dynamic_wrap(side, true, 2).await;
    }
}
#[tokio::test]
async fn ack_zero_requires_size_update_while_pre_ack_preserves_default_stock() {
    for side in [Side::H2Client, Side::H2Server] {
        let mut pre_ack = Peer::new(side, 0, true).await;
        let mut stock = pseudo(side.server());
        indexed_literal(&mut stock, b"x-stock", b"legal before ACK");
        let headers = pre_ack.request(&[&stock], false).await.unwrap();
        assert_eq!(headers["x-stock"], "legal before ACK");
        pre_ack.finish(vec![headers]).await;

        let mut valid = Peer::new(side, 0, true).await;
        let mut resized = vec![0x20];
        resized.extend_from_slice(&pseudo(side.server()));
        literal(&mut resized, b"x-zero", b"valid after ACK");
        let headers = valid.request(&[&resized], true).await.unwrap();
        assert_eq!(headers["x-zero"], "valid after ACK");
        valid.finish(vec![headers]).await;

        let mut invalid = Peer::new(side, 0, true).await;
        assert!(
            invalid
                .request(&[&pseudo(side.server())], true)
                .await
                .is_err()
        );
        invalid.compression_error().await;
        invalid.finish(Vec::new()).await;
    }
}
#[tokio::test]
async fn fragmented_size_integer_is_legal_but_size_update_after_completed_field_is_not() {
    for side in [Side::H2Client, Side::H2Server] {
        let mut valid = Peer::new(side, TABLE as u32, true).await;
        let mut tail = vec![0x1f];
        tail.extend_from_slice(&pseudo(side.server()));
        literal(&mut tail, b"x-fragment", b"integer accepted");
        let headers = valid
            .request(&[&[0x3f], &[0xe1], &tail], true)
            .await
            .unwrap();
        assert_eq!(headers["x-fragment"], "integer accepted");
        valid.finish(vec![headers]).await;

        let mut invalid = Peer::new(side, TABLE as u32, true).await;
        let mut first = pseudo(side.server());
        literal(&mut first, b"x-complete", b"already decoded");
        assert!(invalid.request(&[&first, &[0x20]], true).await.is_err());
        invalid.compression_error().await;
        invalid.finish(Vec::new()).await;
    }
}

#[tokio::test]
async fn complete_malformed_nonfinal_headers_preserve_continuation_table_for_next_stream() {
    for side in [Side::H2Client, Side::H2Server] {
        for fixed in [false, true] {
            let mut peer = Peer::new(side, TABLE as u32, fixed).await;
            let responses = if side == Side::H2Client {
                let Requests::H2(sender) = peer.requests.as_mut().unwrap() else {
                    unreachable!()
                };
                let first = sender
                    .send_request(
                        Request::builder()
                            .uri("http://localhost/")
                            .body(())
                            .unwrap(),
                        true,
                    )
                    .unwrap();
                let next = sender
                    .send_request(
                        Request::builder()
                            .uri("http://localhost/")
                            .body(())
                            .unwrap(),
                        true,
                    )
                    .unwrap();
                Some((first, next))
            } else {
                None
            };
            peer.published(1).await;
            if side == Side::H2Client {
                peer.published(3).await;
            }
            let mut bad = pseudo(side.server());
            // This first fragment is complete and malformed, not NeedMore.
            // Its error must remain pending until END_HEADERS, while HPACK
            // still consumes the continuation and commits its indexed field.
            indexed_literal(&mut bad, b"connection", b"close");
            let mut continuation = Vec::new();
            indexed_literal(&mut continuation, b"x-ok", b"abc");
            let mut next = pseudo(side.server());
            next.push(0xbe); // CONT's x-ok is exactly dynamic index 62.
            literal(&mut next, b"x-next", b"stream3");
            let mut wire = Vec::new();
            frame(&mut wire, 4, 1, 0, &[]);
            frame(&mut wire, 1, 1, 1, &bad);
            frame(&mut wire, 9, 4, 1, &continuation);
            frame(&mut wire, 1, 5, 3, &next);
            peer.io.write_all(&wire).await.unwrap();
            let headers = if let Some(((first, first_stream), (next, next_stream))) = responses {
                let error = tokio::time::timeout(DEADLINE, first)
                    .await
                    .unwrap()
                    .unwrap_err();
                assert_eq!(error.reason(), Some(h2::Reason::PROTOCOL_ERROR));
                let response = tokio::time::timeout(DEADLINE, next).await.unwrap().unwrap();
                drop(first_stream);
                drop(next_stream);
                let (parts, body) = response.into_parts();
                drop(body);
                parts.headers
            } else {
                let Requests::Server(rx) = peer.requests.as_mut().unwrap() else {
                    unreachable!()
                };
                tokio::time::timeout(DEADLINE, rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
            };
            assert_eq!(headers["x-ok"], "abc");
            assert_eq!(
                headers["x-next"], "stream3",
                "malformed stream1 must never publish"
            );
            tokio::time::timeout(DEADLINE, async {
                for _ in 0..16 {
                    let (kind, _, stream, payload) = peer.read_frame().await;
                    if kind == 3 && stream == 1 {
                        assert_eq!(payload, u32::from(h2::Reason::PROTOCOL_ERROR).to_be_bytes());
                        return;
                    }
                }
                panic!("actual malformed stream1 reset was not written");
            })
            .await
            .unwrap();
            peer.finish(vec![headers]).await;
        }
    }
}

#[tokio::test]
async fn table_geometry_and_dependency_refuse_before_io_and_once_binding_is_independent() {
    assert!(ReceiveHeaderTableBuffer::allocation_capacity_bound(4095).is_err());
    for server in [false, true] {
        for oversized in [false, true] {
            let f = funded(budget());
            let (io, _peer, counts) = io_pair();
            let error = if server {
                let mut b = h2::server::Builder::new();
                b.receive_header_table_buffer(f.table.clone());
                if oversized {
                    server_config(&mut b, &f, 8192, false);
                }
                match b.handshake::<_, Bytes>(io).await {
                    Err(e) => e,
                    Ok(_) => panic!("invalid table geometry accepted"),
                }
            } else {
                let mut b = h2::client::Builder::new();
                b.receive_header_table_buffer(f.table.clone());
                if oversized {
                    client_config(&mut b, &f, 8192, false);
                }
                match b.handshake::<_, Bytes>(io).await {
                    Err(e) => e,
                    Ok(_) => panic!("invalid table geometry accepted"),
                }
            };
            assert!(error.is_io());
            assert_eq!(counts.reads.load(Ordering::SeqCst), 0);
            assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
            let original = f.budget.clone();
            let total = f.total;
            drop(f);
            drop(reserve(&original, total));
        }
    }
    // Hyper/Tonic forwarding is verified causally: after a real first
    // connection, only its table object is reused with all dependencies fresh.
    for side in [
        Side::H2Client,
        Side::H2Server,
        Side::HyperClient,
        Side::HyperServer,
        Side::Tonic,
    ] {
        let mut first = Peer::new(side, TABLE as u32, true).await;
        let received = first
            .request(&[&pseudo(side.server())], true)
            .await
            .unwrap();
        drop(received);
        let second = funded(budget());
        let (io, _peer, counts) = io_pair();
        let error = if side.server() {
            let mut b = h2::server::Builder::new();
            server_config(&mut b, &second, TABLE as u32, false);
            b.receive_header_table_buffer(first.funding.as_ref().unwrap().table.clone());
            match b.handshake::<_, Bytes>(io).await {
                Err(e) => e,
                Ok(_) => panic!("table buffer forwarding did not bind the first table"),
            }
        } else {
            let mut b = h2::client::Builder::new();
            client_config(&mut b, &second, TABLE as u32, false);
            b.receive_header_table_buffer(first.funding.as_ref().unwrap().table.clone());
            match b.handshake::<_, Bytes>(io).await {
                Err(e) => e,
                Ok(_) => panic!("table buffer forwarding did not bind the first table"),
            }
        };
        assert!(
            error
                .to_string()
                .contains("header table buffer already bound"),
            "{side:?}: {error:?}"
        );
        assert_eq!(counts.reads.load(Ordering::SeqCst), 0);
        assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
        let original = second.budget.clone();
        let total = second.total;
        drop(second);
        drop(reserve(&original, total));
        first.finish(Vec::new()).await;
    }
}

#[tokio::test]
async fn tonic_typed_table_dependencies_and_advertisement_are_checked_before_dial() {
    for missing_field in [true, false] {
        let original = budget();
        let factory_budget = original.clone();
        let endpoint = tonic::transport::Endpoint::from_static("http://localhost")
            .http2_connection_factory(move || {
                let f = funded(factory_budget.clone());
                Ok::<_, io::Error>(tonic::transport::Http2ConnectionConfig {
                    max_header_list_size: Some(MAX as u32),
                    max_receive_header_block_size: Some(MAX),
                    header_table_size: Some(if missing_field { 4096 } else { 8192 }),
                    receive_frame_buffer: Some(f.raw),
                    receive_header_block_buffer: Some(f.block),
                    receive_header_field_pool: if missing_field { None } else { Some(f.fields) },
                    receive_header_table_buffer: Some(f.table),
                    ..Default::default()
                })
            });
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let connector = tower::service_fn(move |_: hyper::http::Uri| {
            observed.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<TokioIo<DuplexStream>, _>(io::Error::from(io::ErrorKind::ConnectionRefused))
            }
        });
        assert!(endpoint.connect_with_connector(connector).await.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "invalid typed table configuration must refuse before creating a dial future"
        );
        drop(endpoint);
        drop(reserve(&original, sizes().iter().sum()));
    }
}
