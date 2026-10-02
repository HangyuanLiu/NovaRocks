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

//! Actual incoming fixed HeaderMap storage through wire and physical task exit.
//! Raw/block/field/map owners and their Bytes carriers use original Worker
//! grants. Default HPACK table, pseudo-header conversions, URI, peer IO and
//! executor/test scaffolds remain separate; this is not a connection bound.

use bytes::Bytes;
use h2::{ReceiveFrameBuffer, ReceiveHeaderBlockBuffer, ReceiveHeaderFieldPool};
use hyper::body::{Body, Frame, Incoming};
use hyper::http::header::HeaderMapAllocationPool;
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
#[derive(Copy, Clone)]
struct Limits {
    maps: usize,
    keys: usize,
    extra: usize,
}
const NORMAL: Limits = Limits {
    maps: 16,
    keys: 16,
    extra: 8,
};
struct Funding {
    raw: ReceiveFrameBuffer,
    block: ReceiveHeaderBlockBuffer,
    fields: ReceiveHeaderFieldPool,
    maps: HeaderMapAllocationPool,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
    field_grant: usize,
    map_grant: usize,
}
fn sizes(limits: Limits) -> [usize; 4] {
    let carrier = Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    [
        ReceiveFrameBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
        ReceiveHeaderBlockBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
        ReceiveHeaderFieldPool::allocation_capacity_bound(ARENA, POSITIONS, MAX).unwrap() + carrier,
        HeaderMapAllocationPool::allocation_capacity_bound(limits.maps, limits.keys, limits.extra)
            .unwrap()
            + carrier,
    ]
}
fn funded(budget: Arc<ResultRetainedBudget>, limits: Limits) -> Funding {
    let amounts = sizes(limits);
    let mut owners = amounts.map(|bytes| {
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap()
        else {
            panic!("complete original raw/block/field/map pregrant");
        };
        Bytes::from_owner_with_exit_guard(Bytes::new(), credit)
    });
    Funding {
        raw: ReceiveFrameBuffer::new(MAX, std::mem::take(&mut owners[0])).unwrap(),
        block: ReceiveHeaderBlockBuffer::new(MAX, std::mem::take(&mut owners[1])).unwrap(),
        fields: ReceiveHeaderFieldPool::new(ARENA, POSITIONS, MAX, std::mem::take(&mut owners[2]))
            .unwrap(),
        maps: HeaderMapAllocationPool::new(
            limits.maps,
            limits.keys,
            limits.extra,
            std::mem::take(&mut owners[3]),
        )
        .unwrap(),
        budget,
        total: amounts.iter().sum(),
        field_grant: amounts[2],
        map_grant: amounts[3],
    }
}
fn budget(limits: Limits) -> Arc<ResultRetainedBudget> {
    ResultRetainedBudget::new(NonZeroUsize::new(sizes(limits).iter().sum()).unwrap())
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
        builder.receive_header_map_pool(f.maps.clone());
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
        builder.receive_header_map_pool(f.maps.clone());
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
    map_grant: usize,
    preface_read: bool,
    settings_read: bool,
    next_stream: u32,
    ack_sent: bool,
    advertised: u32,
}
impl Peer {
    async fn new(side: Side, limits: Limits, fixed: bool) -> Self {
        let advertised = TABLE as u32;
        let original = budget(limits);
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
                let f = funded(original, limits);
                let mut builder = h2::client::Builder::new();
                client_config(&mut builder, &f, advertised, fixed);
                let (sender, connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
                drop(builder);
                let task =
                    tokio::spawn(async move { connection.await.map_err(|e| format!("{e:?}")) });
                (Requests::H2(sender), Some(task), f)
            }
            Side::H2Server => {
                let f = funded(original, limits);
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
                let f = funded(original, limits);
                let mut builder = hyper::client::conn::http2::Builder::new(executor.clone());
                builder
                    .max_receive_header_block_size(MAX)
                    .receive_frame_buffer(f.raw.clone())
                    .receive_header_block_buffer(f.block.clone())
                    .receive_header_field_pool(f.fields.clone())
                    .header_table_size(advertised);
                if fixed {
                    builder.receive_header_map_pool(f.maps.clone());
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
                let f = funded(original, limits);
                let mut builder = hyper::server::conn::http2::Builder::new(executor.clone());
                builder
                    .max_receive_header_block_size(MAX)
                    .receive_frame_buffer(f.raw.clone())
                    .receive_header_block_buffer(f.block.clone())
                    .receive_header_field_pool(f.fields.clone())
                    .header_table_size(advertised);
                if fixed {
                    builder.receive_header_map_pool(f.maps.clone());
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
                        let f = funded(factory_budget.clone(), limits);
                        let config = tonic::transport::Http2ConnectionConfig {
                            max_header_list_size: Some(MAX as u32),
                            max_receive_header_block_size: Some(MAX),
                            header_table_size: Some(advertised),
                            receive_frame_buffer: Some(f.raw.clone()),
                            receive_header_block_buffer: Some(f.block.clone()),
                            receive_header_field_pool: Some(f.fields.clone()),
                            receive_header_map_pool: fixed.then(|| f.maps.clone()),
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
        let map_grant = funding.map_grant;
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
            map_grant,
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
        self.request_fragments(pieces, ack, true).await
    }
    async fn request_fragments(
        &mut self,
        pieces: &[&[u8]],
        ack: bool,
        end_headers: bool,
    ) -> Result<HeaderMap, String> {
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
                u8::from(first) | if last && end_headers { 4 } else { 0 },
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
    async fn internal_error(&mut self) {
        self.connection_error("INTERNAL_ERROR").await;
    }
    async fn connection_error(&mut self, expected: &str) {
        let task = self.task.take().expect("direct h2 connection result");
        let error = tokio::time::timeout(DEADLINE, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.contains(expected), "actual connection error: {error}");
    }
    fn drop_public_owners(&mut self) {
        drop(self.funding.take());
        // All public raw/block/field/map handles and builders have exited.
        // A live real connection must retain all original grants. Omitting
        // map forwarding releases its independent grant and fails here.
        assert!(matches!(
            self.original_budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    async fn exit(mut self) -> Exited {
        drop(self.requests.take());
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = tokio::time::timeout(DEADLINE, task).await.unwrap();
        }
        self.executor.stop_and_join().await;
        assert_eq!(self.counts.exits.load(Ordering::SeqCst), 1);
        drop(self.funding.take());
        Exited {
            budget: self.original_budget,
            total: self.total,
            fields: self.field_grant,
            maps: self.map_grant,
        }
    }
    async fn finish(self, aliases: Vec<HeaderMap>) {
        let exited = self.exit().await;
        if aliases.is_empty() {
            drop(reserve(&exited.budget, exited.total));
        } else {
            let transport = reserve(&exited.budget, exited.total - exited.fields - exited.maps);
            assert!(matches!(
                exited.budget.try_reserve_process(1).unwrap(),
                ResultWriteAdmission::Blocked
            ));
            drop(aliases);
            drop(reserve(&exited.budget, exited.fields + exited.maps));
            drop(transport);
            drop(reserve(&exited.budget, exited.total));
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

struct Exited {
    budget: Arc<ResultRetainedBudget>,
    total: usize,
    fields: usize,
    maps: usize,
}
fn duplicates(server: bool) -> Vec<u8> {
    let mut block = pseudo(server);
    literal(&mut block, b"x-owned", b"alpha");
    literal(&mut block, b"x-owned", b"beta");
    literal(&mut block, b"x-other", b"gamma");
    block
}
fn assert_duplicates(map: &HeaderMap) {
    assert_eq!(map["x-other"], "gamma");
    assert!(
        map.get_all("x-owned")
            .iter()
            .map(|v| v.as_bytes())
            .eq([b"alpha".as_slice(), b"beta".as_slice()])
    );
}

#[tokio::test]
async fn h2_both_directions_maps_clones_iterators_and_field_aliases_outlive_io() {
    for side in [Side::H2Client, Side::H2Server] {
        let mut peer = Peer::new(side, NORMAL, true).await;
        let received = peer
            .request(&[&duplicates(side.server())], true)
            .await
            .unwrap();
        assert_duplicates(&received);
        let cloned = received.try_clone().unwrap();
        assert_ne!(
            received.get("x-owned").unwrap() as *const _,
            cloned.get("x-owned").unwrap() as *const _
        );
        let mut iterator = cloned.into_iter();
        let field_alias = iterator.next().unwrap();
        peer.drop_public_owners();
        let exited = peer.exit().await;
        let transport = reserve(&exited.budget, exited.total - exited.fields - exited.maps);
        assert!(matches!(
            exited.budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        assert_duplicates(&received);
        drop(received);
        assert!(matches!(
            exited.budget.try_reserve_process(exited.maps).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        drop(iterator);
        let maps = reserve(&exited.budget, exited.maps);
        assert!(matches!(
            exited.budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        assert_eq!(field_alias.1, "alpha");
        drop(field_alias);
        drop(reserve(&exited.budget, exited.fields));
        drop((maps, transport));
        drop(reserve(&exited.budget, exited.total));
    }
}

#[tokio::test]
async fn hyper_both_directions_and_tonic_factory_forward_original_map_pool() {
    for side in [Side::HyperClient, Side::HyperServer, Side::Tonic] {
        let mut peer = Peer::new(side, NORMAL, true).await;
        let received = peer
            .request(&[&duplicates(side.server())], true)
            .await
            .unwrap();
        assert_duplicates(&received);
        let copied = received.try_clone().unwrap();
        peer.drop_public_owners();
        peer.finish(vec![received, copied]).await;
    }
}

#[tokio::test]
async fn fragmented_headers_keep_one_funded_map_and_default_none_still_works() {
    for side in [Side::H2Client, Side::H2Server] {
        let limits = Limits { maps: 1, ..NORMAL };
        let mut peer = Peer::new(side, limits, true).await;
        let block = duplicates(side.server());
        // The first literal is complete before CONT; later duplicates belong
        // to the same logical block and must not claim a second map.
        let split = pseudo(side.server()).len() + 1 + 1 + 7 + 1 + 5;
        let received = peer
            .request(&[&block[..split], &block[split..]], true)
            .await
            .unwrap();
        assert_duplicates(&received);
        assert_eq!(peer.funding.as_ref().unwrap().maps.available_maps(), 0);
        assert!(received.try_clone().is_err());
        peer.finish(vec![received]).await;
        let mut default_peer = Peer::new(side, NORMAL, false).await;
        let received = default_peer.request(&[&block], true).await.unwrap();
        assert_duplicates(&received);
        drop(received);
        default_peer.finish(Vec::new()).await;
    }
}

#[tokio::test]
async fn key_and_extra_shortage_are_actual_internal_connection_errors() {
    for side in [Side::H2Client, Side::H2Server] {
        for limits in [Limits { keys: 1, ..NORMAL }, Limits { extra: 0, ..NORMAL }] {
            let mut peer = Peer::new(side, limits, true).await;
            assert!(
                peer.request(&[&duplicates(side.server())], true)
                    .await
                    .is_err()
            );
            peer.internal_error().await;
            peer.finish(Vec::new()).await;
        }
    }
}

#[tokio::test]
async fn escaped_first_map_exhausts_aggregate_positions_without_waiting_or_heap_fallback() {
    for side in [Side::H2Client, Side::H2Server] {
        let mut peer = Peer::new(side, Limits { maps: 1, ..NORMAL }, true).await;
        let first = peer
            .request(&[&duplicates(side.server())], true)
            .await
            .unwrap();
        assert_eq!(peer.funding.as_ref().unwrap().maps.available_maps(), 0);
        assert!(
            peer.request(&[&duplicates(side.server())], true)
                .await
                .is_err()
        );
        peer.internal_error().await;
        assert_duplicates(&first);
        peer.finish(vec![first]).await;
    }
}

#[tokio::test]
async fn map_pool_dependency_and_independent_once_bind_refuse_before_io() {
    for server in [false, true] {
        let original = budget(NORMAL);
        let f = funded(original.clone(), NORMAL);
        let (io, _, counts) = io_pair();
        if server {
            let mut b = h2::server::Builder::new();
            b.receive_header_map_pool(f.maps.clone());
            assert!(b.handshake::<_, Bytes>(io).await.is_err());
        } else {
            let mut b = h2::client::Builder::new();
            b.receive_header_map_pool(f.maps.clone());
            assert!(b.handshake::<_, Bytes>(io).await.is_err());
        }
        assert_eq!(counts.reads.load(Ordering::SeqCst), 0);
        assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
        drop(f);
        drop(reserve(&original, sizes(NORMAL).iter().sum()));
    }
    for side in [
        Side::H2Client,
        Side::H2Server,
        Side::HyperClient,
        Side::HyperServer,
        Side::Tonic,
    ] {
        let mut first = Peer::new(side, NORMAL, true).await;
        drop(
            first
                .request(&[&duplicates(side.server())], true)
                .await
                .unwrap(),
        );
        let used_pool = first.funding.as_ref().unwrap().maps.clone();
        let original = budget(NORMAL);
        let second = funded(original.clone(), NORMAL);
        let (io, _, counts) = io_pair();
        let error = if side.server() {
            let mut b = h2::server::Builder::new();
            server_config(&mut b, &second, TABLE as u32, false);
            b.receive_header_map_pool(used_pool);
            match b.handshake::<_, Bytes>(io).await {
                Err(e) => e,
                Ok(_) => panic!("map pool forwarding did not bind first connection"),
            }
        } else {
            let mut b = h2::client::Builder::new();
            client_config(&mut b, &second, TABLE as u32, false);
            b.receive_header_map_pool(used_pool);
            match b.handshake::<_, Bytes>(io).await {
                Err(e) => e,
                Ok(_) => panic!("map pool forwarding did not bind first connection"),
            }
        };
        assert!(
            error.to_string().contains("already bound"),
            "{side:?}: {error:?}"
        );
        assert_eq!(counts.reads.load(Ordering::SeqCst), 0);
        assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
        drop(second);
        drop(reserve(&original, sizes(NORMAL).iter().sum()));
        first.finish(Vec::new()).await;
    }
}

#[tokio::test]
async fn tonic_map_pool_missing_field_dependency_is_rejected_before_dial() {
    let original = budget(NORMAL);
    let factory_budget = original.clone();
    let endpoint = tonic::transport::Endpoint::from_static("http://localhost")
        .http2_connection_factory(move || {
            let f = funded(factory_budget.clone(), NORMAL);
            Ok::<_, io::Error>(tonic::transport::Http2ConnectionConfig {
                max_header_list_size: Some(MAX as u32),
                max_receive_header_block_size: Some(MAX),
                receive_frame_buffer: Some(f.raw),
                receive_header_block_buffer: Some(f.block),
                receive_header_map_pool: Some(f.maps),
                ..Default::default()
            })
        });
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let connector = tower::service_fn(move |_: hyper::http::Uri| {
        observed.fetch_add(1, Ordering::SeqCst);
        async { Err::<TokioIo<DuplexStream>, _>(io::Error::from(io::ErrorKind::ConnectionRefused)) }
    });
    assert!(endpoint.connect_with_connector(connector).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(endpoint);
    drop(reserve(&original, sizes(NORMAL).iter().sum()));
}

#[tokio::test]
async fn real_hpack_errors_take_precedence_but_exhausted_nonfinal_need_more_fails_immediately() {
    for side in [Side::H2Client, Side::H2Server] {
        for (suffix, expected, end_headers) in [
            (0x20, "COMPRESSION_ERROR", true),
            (0x80, "PROTOCOL_ERROR", true),
            (0x00, "INTERNAL_ERROR", false),
        ] {
            let limits = Limits { keys: 1, ..NORMAL };
            let mut peer = Peer::new(side, limits, true).await;
            let mut block = duplicates(side.server());
            // Two distinct keys exceed the fixed map before this suffix.
            // 0x20 is an illegal mid-block table size update; 0x80 is
            // indexed zero. 0x00 starts an unfinished literal, and !END
            // deliberately leaves the peer silent without any CONT frame.
            block.push(suffix);
            assert!(
                peer.request_fragments(&[&block], true, end_headers)
                    .await
                    .is_err()
            );
            peer.connection_error(expected).await;
            peer.finish(Vec::new()).await;
        }
    }
}

#[tokio::test]
async fn invalid_frame_geometry_does_not_burn_any_original_buffer_or_map_binding() {
    for side in [Side::H2Client, Side::H2Server] {
        let original = budget(NORMAL);
        let f = funded(original.clone(), NORMAL);
        let (bad_io, _, bad_counts) = io_pair();
        if side.server() {
            let mut builder = h2::server::Builder::new();
            server_config(&mut builder, &f, TABLE as u32, true);
            builder.max_frame_size(32768);
            assert!(builder.handshake::<_, Bytes>(bad_io).await.is_err());
        } else {
            let mut builder = h2::client::Builder::new();
            client_config(&mut builder, &f, TABLE as u32, true);
            builder.max_frame_size(32768);
            assert!(builder.handshake::<_, Bytes>(bad_io).await.is_err());
        }
        assert_eq!(bad_counts.reads.load(Ordering::SeqCst), 0);
        assert_eq!(bad_counts.writes.load(Ordering::SeqCst), 0);
        assert_eq!(bad_counts.exits.load(Ordering::SeqCst), 1);
        let (io, mut wire, counts) = io_pair();
        let mut initial = if side.server() {
            PREFACE.to_vec()
        } else {
            Vec::new()
        };
        frame(&mut initial, 4, 0, 0, &[]);
        wire.write_all(&initial).await.unwrap();
        // Reuse every exact original capability, not fresh replacements:
        // a failed geometry check must precede all once-binding operations.
        let (requests, task) = if side.server() {
            let mut builder = h2::server::Builder::new();
            server_config(&mut builder, &f, TABLE as u32, true);
            builder.max_frame_size(MAX as u32);
            let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
            drop(builder);
            let (tx, rx) = mpsc::channel(1);
            let task = tokio::spawn(async move {
                while let Some(result) = connection.accept().await {
                    let (request, response) = result.map_err(|e| format!("{e:?}"))?;
                    let (parts, body) = request.into_parts();
                    drop((body, response));
                    tx.send(parts.headers).await.map_err(|e| e.to_string())?;
                }
                Ok(())
            });
            (Requests::Server(rx), task)
        } else {
            let mut builder = h2::client::Builder::new();
            client_config(&mut builder, &f, TABLE as u32, true);
            builder.max_frame_size(MAX as u32);
            let (sender, connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
            drop(builder);
            let task = tokio::spawn(async move { connection.await.map_err(|e| format!("{e:?}")) });
            (Requests::H2(sender), task)
        };
        let mut peer = Peer {
            side,
            requests: Some(requests),
            io: wire,
            counts,
            task: Some(task),
            executor: JoinedExecutor::default(),
            original_budget: original,
            total: f.total,
            field_grant: f.field_grant,
            map_grant: f.map_grant,
            funding: Some(f),
            preface_read: false,
            settings_read: false,
            next_stream: 1,
            ack_sent: false,
            advertised: TABLE as u32,
        };
        let received = peer
            .request(&[&duplicates(side.server())], true)
            .await
            .unwrap();
        assert_duplicates(&received);
        peer.finish(vec![received]).await;
    }
}
