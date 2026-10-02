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

//! Actual Hyper augmentation of a received, originally funded HeaderMap.
//! Raw/block/field/map storage and carriers use the real Worker budget.
//! Generated Date/Content-Length payloads, HPACK, task/socket and peer storage
//! remain separate. These tests do not claim a whole connection envelope.

use bytes::Bytes;
use h2::{ReceiveFrameBuffer, ReceiveHeaderBlockBuffer, ReceiveHeaderFieldPool};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::http::header::{CONTENT_LENGTH, DATE, HeaderMapAllocationPool};
use hyper::http::{HeaderName, HeaderValue, Method, Request, Response};
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
    fn new(keys: usize) -> Self {
        let carrier = Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
        let amounts = [
            ReceiveFrameBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
            ReceiveHeaderBlockBuffer::allocation_capacity_bound(MAX).unwrap() + carrier,
            ReceiveHeaderFieldPool::allocation_capacity_bound(65536, 1024, MAX).unwrap() + carrier,
            HeaderMapAllocationPool::allocation_capacity_bound(8, keys, 4).unwrap() + carrier,
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
            fields: ReceiveHeaderFieldPool::new(65536, 1024, MAX, std::mem::take(&mut owners[2]))
                .unwrap(),
            maps: HeaderMapAllocationPool::new(8, keys, 4, std::mem::take(&mut owners[3])).unwrap(),
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
enum ServerCase {
    MissingDate,
    PresentDate,
    MissingLength,
    PresentLength,
}
async fn server_echo(case: ServerCase, keys: usize, fixed: bool, reject: bool) {
    let f = Funding::new(keys);
    let executor = JoinedExecutor::default();
    let (io, peer, counts) = io_pair();
    let served = Arc::new(AtomicUsize::new(0));
    let called = served.clone();
    let service = service_fn(move |request: Request<Incoming>| {
        let called = called.clone();
        async move {
            called.fetch_add(1, Ordering::SeqCst);
            let (parts, body) = request.into_parts();
            drop(body);
            let response_body = match case {
                ServerCase::MissingLength | ServerCase::PresentLength => {
                    TestBody(Some(Bytes::from_static(b"x")))
                }
                _ => TestBody::default(),
            };
            let mut response = Response::new(response_body);
            // Move the actual received fixed map into the outgoing response;
            // no replacement map or eager copy can hide the augmentation.
            *response.headers_mut() = parts.headers;
            Ok::<_, Infallible>(response)
        }
    });
    let mut builder = hyper::server::conn::http2::Builder::new(executor.clone());
    builder.auto_date_header(matches!(
        case,
        ServerCase::MissingDate | ServerCase::PresentDate
    ));
    if fixed {
        builder
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
    let mut request = Request::builder()
        .uri("http://localhost/")
        .body(())
        .unwrap();
    match case {
        ServerCase::PresentDate => {
            request.headers_mut().insert(
                DATE,
                HeaderValue::from_static("Mon, 01 Jan 2024 00:00:00 GMT"),
            );
        }
        ServerCase::PresentLength => {
            // A non-EOS request carries exactly one byte, matching the
            // preexisting length that the echoed response must preserve.
            request
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from_static("1"));
        }
        _ => {
            request.headers_mut().insert(
                HeaderName::from_static("x-owned"),
                HeaderValue::from_static("value"),
            );
        }
    }
    let has_request_body = matches!(case, ServerCase::PresentLength);
    let (response, mut stream) = sender.send_request(request, !has_request_body).unwrap();
    if has_request_body {
        stream.send_data(Bytes::from_static(b"x"), true).unwrap();
    }
    let response = tokio::time::timeout(DEADLINE, response).await.unwrap();
    assert_eq!(
        served.load(Ordering::SeqCst),
        1,
        "actual received map reached the service"
    );
    if reject {
        assert_eq!(
            response.unwrap_err().reason(),
            Some(h2::Reason::INTERNAL_ERROR)
        );
    } else {
        let response = response.unwrap();
        match case {
            ServerCase::MissingDate => {
                assert!(response.headers().contains_key(DATE));
                assert_eq!(response.headers()["x-owned"], "value");
            }
            ServerCase::PresentDate => {
                assert_eq!(response.headers()[DATE], "Mon, 01 Jan 2024 00:00:00 GMT");
            }
            ServerCase::MissingLength => {
                assert_eq!(response.headers()[CONTENT_LENGTH], "1");
                assert_eq!(response.headers()["x-owned"], "value");
            }
            ServerCase::PresentLength => {
                assert_eq!(response.headers()[CONTENT_LENGTH], "1");
            }
        }
        let mut body = response.into_body();
        while let Some(chunk) = tokio::time::timeout(DEADLINE, body.data()).await.unwrap() {
            drop(chunk.unwrap());
        }
    }
    drop((stream, sender));
    stop(client).await;
    stop(server).await;
    executor.stop_and_join().await;
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    f.exited();
}
#[tokio::test]
async fn echoed_received_map_date_insertion_rejects_full_capacity_and_preserves_existing_date() {
    server_echo(ServerCase::MissingDate, 1, true, true).await;
    server_echo(ServerCase::MissingDate, 2, true, false).await;
    server_echo(ServerCase::PresentDate, 1, true, false).await;
    server_echo(ServerCase::MissingDate, 1, false, false).await;
}
#[tokio::test]
async fn echoed_received_map_content_length_rejects_full_capacity_and_preserves_existing_length() {
    server_echo(ServerCase::MissingLength, 1, true, true).await;
    server_echo(ServerCase::MissingLength, 2, true, false).await;
    server_echo(ServerCase::PresentLength, 1, true, false).await;
    server_echo(ServerCase::MissingLength, 1, false, false).await;
}
async fn client_forward(keys: usize, fixed: bool, present: bool, reject: bool) {
    let f = Funding::new(keys);
    let executor = JoinedExecutor::default();
    let (io, peer, counts) = io_pair();
    let mut builder = hyper::client::conn::http2::Builder::new(executor.clone());
    if fixed {
        builder
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
                let mut head = Response::new(());
                if present {
                    head.headers_mut()
                        .insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
                } else {
                    head.headers_mut().insert(
                        HeaderName::from_static("x-owned"),
                        HeaderValue::from_static("value"),
                    );
                }
                response.send_response(head, true).unwrap();
            } else {
                assert_eq!(
                    request.body().stream_id().as_u32(),
                    3,
                    "rejected augmentation must not open a stream"
                );
                observed_tx
                    .take()
                    .unwrap()
                    .send((
                        request.method().clone(),
                        request.headers().get(CONTENT_LENGTH).cloned(),
                    ))
                    .unwrap();
                response.send_response(Response::new(()), true).unwrap();
            }
            drop(request);
        }
    });
    let first = sender.send_request(
        Request::builder()
            .uri("http://localhost/")
            .body(TestBody::default())
            .unwrap(),
    );
    let first = tokio::time::timeout(DEADLINE, first)
        .await
        .unwrap()
        .unwrap();
    let (parts, body) = first.into_parts();
    drop(body);
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("http://localhost/")
        .body(TestBody::default())
        .unwrap();
    *request.headers_mut() = parts.headers;
    let second = tokio::time::timeout(DEADLINE, sender.send_request(request))
        .await
        .unwrap();
    if reject {
        let error = second.unwrap_err();
        assert!(
            format!("{error:?}").contains("INTERNAL_ERROR"),
            "actual callback error: {error:?}"
        );
        // Prove the dispatch task survived and no refused stream was opened.
        let next = sender.send_request(
            Request::builder()
                .uri("http://localhost/")
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
    drop(sender);
    stop(client).await;
    stop(server).await;
    executor.stop_and_join().await;
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    f.exited();
}
#[tokio::test]
async fn received_response_map_forwarded_to_post_rejects_full_capacity_without_killing_dispatch() {
    client_forward(1, true, false, true).await;
    client_forward(2, true, false, false).await;
    client_forward(1, true, true, false).await;
    client_forward(1, false, false, false).await;
}
