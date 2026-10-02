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

//! Actual Hyper generated Date/Content-Length payloads in an attached original
//! field arena. Raw/block/field/map storage and carriers use the real Worker
//! budget. Static pseudo-headers avoid consuming field extents before generation.
//! Peer-decoded HeaderValues are independent wire copies, not original aliases.
//! Public wire proof covers retained Date arena positions, both generators
//! refusing exhausted fields, dispatch survival and actual IO/task exit. Original generated HeaderValue
//! alias/allocator primitives are verified separately by private source probes.
//! HPACK, task/socket, TLS/test scaffolds and the complete connection are separate.

use bytes::Bytes;
use h2::{ReceiveFrameBuffer, ReceiveHeaderBlockBuffer, ReceiveHeaderFieldPool};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::http::header::{CONTENT_LENGTH, DATE, HeaderMapAllocationPool};
use hyper::http::{HeaderValue, Method, Request, Response};
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
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const MAX: usize = 16384;
const DEADLINE: Duration = Duration::from_secs(5);
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
                if let Err(error) = tokio::time::timeout(DEADLINE, task).await.unwrap() {
                    assert!(!error.is_panic(), "Hyper executor task panicked: {error}");
                }
            }
        }
    }
}

#[derive(Default)]
struct TestBody(Option<Bytes>);
impl Body for TestBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.take().map(|data| Ok(Frame::data(data))))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
    fn size_hint(&self) -> SizeHint {
        let mut hint = SizeHint::new();
        hint.set_exact(self.0.as_ref().map_or(0, |data| data.len() as u64));
        hint
    }
}
struct Funding {
    raw: ReceiveFrameBuffer,
    block: ReceiveHeaderBlockBuffer,
    fields: ReceiveHeaderFieldPool,
    maps: HeaderMapAllocationPool,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
}
impl Funding {
    fn new(mode: Mode) -> Self {
        let (capacity, positions) = mode.geometry();
        let carrier = Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
        let amounts = [
            ReceiveFrameBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
            ReceiveHeaderBlockBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
            ReceiveHeaderFieldPool::allocation_capacity_bound(capacity, positions, 512).unwrap()
                + carrier,
            HeaderMapAllocationPool::allocation_capacity_bound(8, 4, 4).unwrap() + carrier,
        ];
        let total = amounts.iter().sum();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let mut owners = amounts.map(|amount| {
            let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(amount).unwrap()
            else {
                panic!("complete original pregrant");
            };
            Bytes::from_owner_with_exit_guard(Bytes::new(), credit)
        });
        Self {
            raw: ReceiveFrameBuffer::new(MAX, std::mem::take(&mut owners[0])).unwrap(),
            block: ReceiveHeaderBlockBuffer::new(MAX, std::mem::take(&mut owners[1])).unwrap(),
            fields: ReceiveHeaderFieldPool::new(
                capacity,
                positions,
                512,
                std::mem::take(&mut owners[2]),
            )
            .unwrap(),
            maps: HeaderMapAllocationPool::new(8, 4, 4, std::mem::take(&mut owners[3])).unwrap(),
            budget,
            total,
        }
    }
    fn exited(self) {
        let Self {
            raw,
            block,
            fields,
            maps,
            budget,
            total,
        } = self;
        drop((raw, block, fields, maps));
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap()
        else {
            panic!("all actual storage aliases must have exited");
        };
        drop(credit);
    }
}
async fn stop<T>(task: JoinHandle<T>) {
    task.abort();
    if let Err(error) = tokio::time::timeout(DEADLINE, task).await.unwrap() {
        assert!(!error.is_panic(), "connection task panicked: {error}");
    }
}
#[derive(Copy, Clone)]
enum Mode {
    Free,
    Positions,
    Aggregate,
    Default,
}
impl Mode {
    fn geometry(self) -> (usize, usize) {
        match self {
            Self::Positions => (1024, 1),
            _ => (512, 8),
        }
    }
    fn fixed(self) -> bool {
        !matches!(self, Self::Default)
    }
    fn exhausted(self) -> bool {
        matches!(self, Self::Positions | Self::Aggregate)
    }
    fn block(self, fields: &ReceiveHeaderFieldPool) -> Option<Bytes> {
        if !self.exhausted() {
            return None;
        }
        let len = if matches!(self, Self::Aggregate) {
            512
        } else {
            1
        };
        let blocker = fields
            .allocation_pool()
            .try_fill::<Infallible>(len, |bytes| {
                bytes.fill(b'x');
                Ok(())
            })
            .unwrap();
        assert_eq!(
            fields.allocation_pool().available_positions(),
            fields.allocation_pool().field_positions() - 1
        );
        // Positions mode leaves 960 actual arena bytes free but zero positions;
        // aggregate mode leaves seven positions free but no rounded extent.
        Some(blocker)
    }
}
#[derive(Copy, Clone)]
enum Generated {
    Date,
    Length,
}
const EXISTING_DATE: &str = "Mon, 01 Jan 2024 00:00:00 GMT";
fn date_bytes(bytes: &[u8]) {
    assert_eq!(bytes.len(), 29);
    assert_eq!(&bytes[3..5], b", ");
    assert_eq!(&bytes[25..], b" GMT");
    for at in [5, 6, 12, 13, 14, 15, 17, 18, 20, 21, 23, 24] {
        assert!(bytes[at].is_ascii_digit());
    }
}
async fn consume(response: Response<h2::RecvStream>, field: Generated, present: bool) {
    match field {
        Generated::Date => {
            let date = response.headers().get(DATE).unwrap();
            date_bytes(date.as_bytes());
            if present {
                assert_eq!(date, EXISTING_DATE);
            }
        }
        Generated::Length => assert_eq!(response.headers()[CONTENT_LENGTH], "1"),
    }
    let mut body = response.into_body();
    let mut received = 0;
    while let Some(chunk) = tokio::time::timeout(DEADLINE, body.data()).await.unwrap() {
        let chunk = chunk.unwrap();
        if matches!(field, Generated::Length) {
            assert_eq!(chunk.as_ref(), b"x");
        }
        received += chunk.len();
    }
    assert_eq!(received, usize::from(matches!(field, Generated::Length)));
}
async fn server_generated(field: Generated, mode: Mode, present: bool) {
    let f = Funding::new(mode);
    let mut blocker = mode.block(&f.fields);
    let executor = JoinedExecutor::default();
    let (io, peer, counts) = io_pair();
    let served = Arc::new(AtomicUsize::new(0));
    let called = served.clone();
    let service = service_fn(move |request: Request<Incoming>| {
        let called = called.clone();
        async move {
            called.fetch_add(1, Ordering::SeqCst);
            assert!(
                request.headers().is_empty(),
                "static pseudo-headers consume no field extents"
            );
            let (parts, body) = request.into_parts();
            drop(body);
            let response_body = if matches!(field, Generated::Length) {
                TestBody(Some(Bytes::from_static(b"x")))
            } else {
                TestBody::default()
            };
            let mut response = Response::new(response_body);
            *response.headers_mut() = parts.headers;
            if present {
                match field {
                    Generated::Date => {
                        response
                            .headers_mut()
                            .try_insert(DATE, HeaderValue::from_static(EXISTING_DATE))
                            .unwrap();
                    }
                    Generated::Length => {
                        response
                            .headers_mut()
                            .try_insert(CONTENT_LENGTH, HeaderValue::from_static("1"))
                            .unwrap();
                    }
                }
            }
            Ok::<_, Infallible>(response)
        }
    });
    let mut builder = hyper::server::conn::http2::Builder::new(executor.clone());
    builder.auto_date_header(matches!(field, Generated::Date));
    if mode.fixed() {
        builder
            .max_header_list_size(512)
            .max_receive_header_block_size(MAX)
            .receive_frame_buffer(f.raw.clone())
            .receive_header_block_buffer(f.block.clone())
            .receive_header_field_pool(f.fields.clone())
            .receive_header_map_pool(f.maps.clone());
    }
    let connection = builder.serve_connection(TokioIo::new(io), service);
    drop(builder);
    let server = tokio::spawn(connection);
    let (mut sender, connection) = h2::client::handshake(peer).await.unwrap();
    let client = tokio::spawn(connection);
    // HTTP/1.1 origin-form is a supported H2 forwarding case. Its wire pseudo
    // fields GET/http/"/" are all complete static indexes, with no :authority.
    let (response, stream) = sender
        .send_request(Request::builder().uri("/").body(()).unwrap(), true)
        .unwrap();
    let response = tokio::time::timeout(DEADLINE, response).await.unwrap();
    let reject = mode.exhausted() && !present;
    if reject {
        assert_eq!(
            response.unwrap_err().reason(),
            Some(h2::Reason::INTERNAL_ERROR)
        );
        assert_eq!(
            served.load(Ordering::SeqCst),
            1,
            "field rejection occurs after accepted service response"
        );
        drop(blocker.take());
        // Original refusal resets one stream; actual stream3 succeeds after
        // the original blocker physically exits. No new connection is created.
        let (response, next_stream) = sender
            .send_request(Request::builder().uri("/").body(()).unwrap(), true)
            .unwrap();
        assert_eq!(next_stream.stream_id().as_u32(), 3);
        consume(
            tokio::time::timeout(DEADLINE, response)
                .await
                .unwrap()
                .unwrap(),
            field,
            false,
        )
        .await;
        drop(next_stream);
        assert_eq!(served.load(Ordering::SeqCst), 2);
    } else {
        consume(response.unwrap(), field, present).await;
        assert_eq!(served.load(Ordering::SeqCst), 1);
    }
    let baseline = f.fields.allocation_pool().field_positions();
    let retained_date = mode.fixed() && !present && matches!(field, Generated::Date);
    let blocked = usize::from(blocker.is_some());
    // Header::skip_value_index excludes Content-Length (h2 header.rs:212), so
    // its transient original extent has exited once the peer receives headers.
    // Date is indexed and retains one real original position until connection
    // exit. This exact distinction does not permit either producer to bypass
    // the explicit positions/extent refusal checked above. Original independent
    // CL aliases/copy rejection are covered by the public HTTP producer probe.
    assert_eq!(
        f.fields.allocation_pool().available_positions(),
        baseline - blocked - usize::from(retained_date)
    );
    drop((stream, sender));
    stop(client).await;
    stop(server).await;
    executor.stop_and_join().await;
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.fields.allocation_pool().available_positions(),
        baseline - blocked
    );
    drop(blocker);
    assert_eq!(f.fields.allocation_pool().available_positions(), baseline);
    f.exited();
}
#[tokio::test(flavor = "current_thread")]
async fn generated_server_date_uses_original_arena_and_refuses_positions_and_extent_exhaustion() {
    for mode in [Mode::Free, Mode::Positions, Mode::Aggregate, Mode::Default] {
        server_generated(Generated::Date, mode, false).await;
    }
    for mode in [Mode::Positions, Mode::Aggregate] {
        server_generated(Generated::Date, mode, true).await;
    }
}
#[tokio::test(flavor = "current_thread")]
async fn generated_server_content_length_uses_original_arena_and_preserves_present_length() {
    for mode in [Mode::Free, Mode::Positions, Mode::Aggregate, Mode::Default] {
        server_generated(Generated::Length, mode, false).await;
    }
    for mode in [Mode::Positions, Mode::Aggregate] {
        server_generated(Generated::Length, mode, true).await;
    }
}
async fn client_generated(mode: Mode, present: bool) {
    let f = Funding::new(mode);
    let blocker = mode.block(&f.fields);
    let executor = JoinedExecutor::default();
    let (io, peer, counts) = io_pair();
    let mut builder = hyper::client::conn::http2::Builder::new(executor.clone());
    if mode.fixed() {
        builder
            .max_header_list_size(512)
            .max_receive_header_block_size(MAX)
            .receive_frame_buffer(f.raw.clone())
            .receive_header_block_buffer(f.block.clone())
            .receive_header_field_pool(f.fields.clone())
            .receive_header_map_pool(f.maps.clone());
    }
    let (mut sender, connection) = builder
        .handshake::<_, TestBody>(TokioIo::new(io))
        .await
        .unwrap();
    drop(builder);
    let client = tokio::spawn(connection);
    let (observed_tx, observed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut connection = h2::server::handshake(peer).await.unwrap();
        let mut observed_tx = Some(observed_tx);
        let mut accepted = 0;
        while let Some(result) = connection.accept().await {
            let (request, mut response) = result.unwrap();
            accepted += 1;
            if accepted == 1 {
                assert_eq!(request.method(), Method::GET);
                assert!(request.headers().is_empty());
            } else {
                assert_eq!(
                    request.body().stream_id().as_u32(),
                    3,
                    "failed generation did not open a stream or kill dispatch"
                );
                observed_tx
                    .take()
                    .unwrap()
                    .send((
                        request.method().clone(),
                        request.headers().get(CONTENT_LENGTH).cloned(),
                    ))
                    .unwrap();
            }
            response.send_response(Response::new(()), true).unwrap();
            drop(request);
        }
    });
    let first = sender.send_request(
        Request::builder()
            .uri("/")
            .body(TestBody::default())
            .unwrap(),
    );
    let first = tokio::time::timeout(DEADLINE, first)
        .await
        .unwrap()
        .unwrap();
    let (parts, body) = first.into_parts();
    drop(body);
    assert!(parts.headers.is_empty());
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/")
        .body(TestBody::default())
        .unwrap();
    *request.headers_mut() = parts.headers;
    if present {
        request
            .headers_mut()
            .try_insert(CONTENT_LENGTH, HeaderValue::from_static("0"))
            .unwrap();
    }
    let second = tokio::time::timeout(DEADLINE, sender.send_request(request))
        .await
        .unwrap();
    let reject = mode.exhausted() && !present;
    if reject {
        let error = second.unwrap_err();
        assert!(
            format!("{error:?}").contains("INTERNAL_ERROR"),
            "actual callback rejection: {error:?}"
        );
        // Keep the arena blocker live: a bodyless GET requires no generation.
        // Actual stream3 acceptance proves the same dispatch remains usable.
        let next = sender.send_request(
            Request::builder()
                .uri("/")
                .body(TestBody::default())
                .unwrap(),
        );
        drop(tokio::time::timeout(DEADLINE, next).await.unwrap().unwrap());
    } else {
        drop(second.unwrap());
    }
    let (method, length) = tokio::time::timeout(DEADLINE, observed_rx)
        .await
        .unwrap()
        .unwrap();
    if reject {
        assert_eq!(method, Method::GET);
        assert!(length.is_none());
    } else {
        assert_eq!(method, Method::POST);
        assert_eq!(length.unwrap(), "0");
    }
    let baseline = f.fields.allocation_pool().field_positions();
    let blocked = usize::from(blocker.is_some());
    // Content-Length is explicitly excluded from dynamic value indexing.
    // The original generated wrapper must have physically retired after this
    // actual accepted wire request; only the separately prepared blocker lives.
    // The refusal cases, not a broadened lifetime allowance, detect a bypass of
    // the original field capability. The HTTP alias probe covers copy mutation.
    assert_eq!(
        f.fields.allocation_pool().available_positions(),
        baseline - blocked
    );
    drop(sender);
    stop(client).await;
    stop(server).await;
    executor.stop_and_join().await;
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.fields.allocation_pool().available_positions(),
        baseline - blocked
    );
    drop(blocker);
    assert_eq!(f.fields.allocation_pool().available_positions(), baseline);
    f.exited();
}
#[tokio::test(flavor = "current_thread")]
async fn generated_client_content_length_refusal_keeps_dispatch_alive_and_present_value_skips_fill()
{
    for mode in [Mode::Free, Mode::Positions, Mode::Aggregate, Mode::Default] {
        client_generated(mode, false).await;
    }
    for mode in [Mode::Positions, Mode::Aggregate] {
        client_generated(mode, true).await;
    }
}
