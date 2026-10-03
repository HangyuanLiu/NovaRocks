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

//! Actual Native bootstrap gates with the production two-second deadline.
//! Plaintext TCP and deterministic duplex peers do not prove TLS or the
//! outstanding connection/task/stream allocation envelope. Fixture buffers
//! and service allocations are independent of the original transport stock.

use super::serve_native_listener;
use crate::native_transport_capacity::{NativeTransportCapacityFactory, TransportClass};
use bytes::Bytes;
use hyper::http::{Request, Response, Uri};
use hyper_util::rt::{TokioExecutor, TokioIo};
use novarocks_execution::runtime::fragment::io::ResultWriteAdmission;
use novarocks_native_trust::NativeIncomingAdapter;
use novarocks_types::NativeEndpoint;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::{Notify, oneshot, watch};
use tonic::body::{BoxBody, boxed};
use tower::{Service, service_fn};

const WATCHDOG: Duration = Duration::from_secs(8);
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(WATCHDOG, future)
        .await
        .expect("bootstrap fixture exceeded its outer failure watchdog")
}

fn stock() -> (
    NativeTransportCapacityFactory,
    Arc<ResultRetainedBudget>,
    usize,
) {
    let bytes = NativeTransportCapacityFactory::allocation_capacity_bound().unwrap();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
    let factory = NativeTransportCapacityFactory::try_new(budget.clone()).unwrap();
    (factory, budget, bytes)
}

async fn returned(factory: &NativeTransportCapacityFactory, class: TransportClass) {
    bounded(async {
        while factory.available_positions(class) != factory.positions(class) {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

async fn released(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    bounded(async {
        loop {
            match budget.try_reserve_process(bytes).unwrap() {
                ResultWriteAdmission::Granted(credit) => {
                    drop(credit);
                    break;
                }
                ResultWriteAdmission::Blocked => tokio::task::yield_now().await,
            }
        }
    })
    .await;
}

async fn initial_settings<R: AsyncRead + Unpin>(peer: &mut R) {
    let mut header = [0_u8; 9];
    bounded(peer.read_exact(&mut header)).await.unwrap();
    let len =
        (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    assert_eq!(header[3], 4, "actual first output must be SETTINGS");
    assert_eq!(header[4], 0, "initial SETTINGS is not an ACK");
    assert_eq!(&header[5..], &[0, 0, 0, 0]);
    assert!(
        len <= 256,
        "fixture only drains the real small SETTINGS payload"
    );
    let mut payload = vec![0; len];
    bounded(peer.read_exact(&mut payload)).await.unwrap();
}

#[derive(Clone)]
struct HeldService {
    calls: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    exited: Arc<AtomicUsize>,
}

struct RequestExit(Arc<AtomicUsize>);

impl Drop for RequestExit {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

impl Service<Request<axum::body::Body>> for HeldService {
    type Response = Response<BoxBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<axum::body::Body>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::AcqRel);
        assert!(request.headers().allocation_pool().is_some());
        assert!(request.headers().field_allocation_pool().is_some());
        let entered = self.entered.clone();
        let release = self.release.clone();
        let exited = RequestExit(self.exited.clone());
        Box::pin(async move {
            entered.notify_one();
            release.notified().await;
            drop(request);
            drop(exited);
            Ok(Response::new(boxed(axum::body::Body::empty())))
        })
    }
}

fn service() -> HeldService {
    HeldService {
        calls: Arc::new(AtomicUsize::new(0)),
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        exited: Arc::new(AtomicUsize::new(0)),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_listener_partial_bootstraps_expire_without_dispatch_and_return_original_slots() {
    // Every case keeps the production 2s setting. Delaying the preface in the
    // final cases distinguishes an accept-origin deadline from a fresh 2s
    // allowance incorrectly started when the old server handshake returns.
    for case in ["zero", "23-byte-preface", "no-settings", "partial-settings"] {
        let (factory, budget, bytes) = stock();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = service();
        let failures = Arc::new(AtomicUsize::new(0));
        let observed = failures.clone();
        let (shutdown, stopping) = watch::channel(false);
        let serving = tokio::spawn(serve_native_listener(
            listener,
            service.clone(),
            NativeIncomingAdapter::plaintext(),
            stopping,
            Arc::new(move || {
                observed.fetch_add(1, Ordering::AcqRel);
            }),
            "initial-settings-test",
            Some((factory.clone(), TransportClass::Data)),
        ));
        let started = Instant::now();
        let mut peer = bounded(tokio::net::TcpStream::connect(address))
            .await
            .unwrap();
        initial_settings(&mut peer).await;
        assert_eq!(
            factory.available_positions(TransportClass::Data),
            factory.positions(TransportClass::Data) - 1
        );
        match case {
            "zero" => {}
            "23-byte-preface" => peer.write_all(&PREFACE[..23]).await.unwrap(),
            "no-settings" | "partial-settings" => {
                // This delay supplies a controlled late phase; it is not an
                // exit oracle. Actual EOF, listener join and stock return are.
                tokio::time::sleep_until(tokio::time::Instant::from_std(
                    started + Duration::from_millis(1100),
                ))
                .await;
                peer.write_all(PREFACE).await.unwrap();
                if case == "partial-settings" {
                    peer.write_all(&[0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 3])
                        .await
                        .unwrap();
                }
            }
            _ => unreachable!(),
        }
        let mut remaining = Vec::new();
        let ended = bounded(peer.read_to_end(&mut remaining)).await;
        if let Err(error) = ended {
            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(1800),
            "{case}: the product two-second allowance expired prematurely: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(2800),
            "{case}: acquisition restarted its absolute deadline: {elapsed:?}"
        );
        assert_eq!(service.calls.load(Ordering::Acquire), 0, "{case}");
        assert_eq!(failures.load(Ordering::Acquire), 1, "{case}");
        drop(peer);
        shutdown.send(true).unwrap();
        assert_eq!(bounded(serving).await.unwrap(), Ok(()));
        returned(&factory, TransportClass::Data).await;
        drop(factory);
        released(&budget, bytes).await;
    }
}

struct ExitingIo {
    io: Option<DuplexStream>,
    exit: Option<oneshot::Sender<()>>,
}

impl Drop for ExitingIo {
    fn drop(&mut self) {
        drop(self.io.take());
        if let Some(exit) = self.exit.take() {
            let _ = exit.send(());
        }
    }
}

impl AsyncRead for ExitingIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io.as_mut().unwrap()).poll_read(cx, buf)
    }
}

impl AsyncWrite for ExitingIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().io.as_mut().unwrap()).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io.as_mut().unwrap()).poll_shutdown(cx)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_capacity_endpoint_waits_for_peer_initial_settings_before_returning_channel() {
    let (factory, budget, bytes) = stock();
    let runtime = crate::BackendDataRuntime::new(
        tokio::runtime::Handle::current(),
        crate::backend_test_support::test_backend_native_trust(),
        crate::BackendNativeTransport::Plaintext,
    )
    .with_transport_capacity(factory.clone())
    .unwrap();
    let address = NativeEndpoint::from_host_port("localhost", 9070).unwrap();
    let endpoint =
        crate::native_client::capacity_endpoint(&runtime, &address, TransportClass::Data).unwrap();
    let (client, mut peer) = tokio::io::duplex(65536);
    let (exit, exited) = oneshot::channel();
    let input = Arc::new(Mutex::new(Some(ExitingIo {
        io: Some(client),
        exit: Some(exit),
    })));
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let observed = factory.clone();
    let connector = service_fn(move |_: Uri| {
        assert_eq!(
            observed.available_positions(TransportClass::Data),
            observed.positions(TransportClass::Data) - 1
        );
        counted.fetch_add(1, Ordering::AcqRel);
        let io = input.lock().unwrap().take().unwrap();
        async move { Ok::<_, io::Error>(TokioIo::new(io)) }
    });
    let connect = tokio::spawn(async move { endpoint.connect_with_connector(connector).await });
    let mut preface = [0; 24];
    bounded(peer.read_exact(&mut preface)).await.unwrap();
    assert_eq!(&preface, PREFACE);
    initial_settings(&mut peer).await;
    // The peer deliberately stays open without replying to SETTINGS. A
    // successful Channel would expose the old send-preface-only handshake.
    let result = bounded(connect).await.unwrap();
    assert!(
        result.is_err(),
        "a Channel cannot escape before peer initial SETTINGS"
    );
    assert_eq!(calls.load(Ordering::Acquire), 1);
    bounded(exited).await.unwrap();
    drop(peer);
    drop(runtime);
    returned(&factory, TransportClass::Data).await;
    drop(factory);
    released(&budget, bytes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_native_bootstrap_does_not_expire_a_long_application_stream() {
    let (factory, budget, bytes) = stock();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = service();
    let failures = Arc::new(AtomicUsize::new(0));
    let observed = failures.clone();
    let (shutdown, stopping) = watch::channel(false);
    let serving = tokio::spawn(serve_native_listener(
        listener,
        service.clone(),
        NativeIncomingAdapter::plaintext(),
        stopping,
        Arc::new(move || {
            observed.fetch_add(1, Ordering::AcqRel);
        }),
        "long-initial-settings-test",
        Some((factory.clone(), TransportClass::Data)),
    ));
    let stream = bounded(tokio::net::TcpStream::connect(address))
        .await
        .unwrap();
    let (mut sender, connection) = bounded(h2::client::handshake(stream)).await.unwrap();
    let driver = tokio::spawn(async move { connection.await });
    let request = Request::builder()
        .method("POST")
        .uri("http://localhost/long-stream")
        .body(())
        .unwrap();
    let (response, body) = sender.send_request(request, true).unwrap();
    drop(body);
    bounded(service.entered.notified()).await;
    assert_eq!(service.calls.load(Ordering::Acquire), 1);
    // This is the actual application work interval, not a reclamation guess.
    tokio::time::sleep(Duration::from_millis(2200)).await;
    service.release.notify_one();
    let response = bounded(response).await.unwrap();
    assert_eq!(response.status(), 200);
    drop(response);
    assert_eq!(service.exited.load(Ordering::Acquire), 1);
    assert_eq!(failures.load(Ordering::Acquire), 0);
    shutdown.send(true).unwrap();
    assert_eq!(bounded(serving).await.unwrap(), Ok(()));
    drop(sender);
    let _ = bounded(driver).await.unwrap();
    returned(&factory, TransportClass::Data).await;
    drop(factory);
    released(&budget, bytes).await;
}

struct ReadyIo {
    input: &'static [u8],
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
}

impl AsyncRead for ReadyIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.reads.fetch_add(1, Ordering::AcqRel);
        let n = this.input.len().min(buf.remaining());
        buf.put_slice(&this.input[..n]);
        this.input = &this.input[n..];
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ReadyIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.writes.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.writes.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.writes.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(()))
    }
}

fn ready_io() -> (ReadyIo, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let reads = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    (
        ReadyIo {
            input: PREFACE,
            reads: reads.clone(),
            writes: writes.clone(),
        },
        reads,
        writes,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_first_poll_refuses_ready_io_through_both_h2_and_hyper_builders() {
    // These component checks use an already-expired absolute deadline. They
    // do not replace or shorten the product listener/endpoint two-second cap.
    let deadline = Instant::now() - Duration::from_millis(1);
    let (io, reads, writes) = ready_io();
    let mut client = h2::client::Builder::new();
    client.initial_settings_deadline(deadline);
    assert!(bounded(client.handshake::<_, Bytes>(io)).await.is_err());
    assert_eq!(reads.load(Ordering::Acquire), 0);
    assert_eq!(writes.load(Ordering::Acquire), 0);

    let (io, reads, writes) = ready_io();
    let mut server = h2::server::Builder::new();
    server.initial_settings_deadline(deadline);
    assert!(bounded(server.handshake::<_, Bytes>(io)).await.is_err());
    assert_eq!(reads.load(Ordering::Acquire), 0);
    assert_eq!(writes.load(Ordering::Acquire), 0);

    let (io, reads, writes) = ready_io();
    let mut client = hyper::client::conn::http2::Builder::new(TokioExecutor::new());
    client.initial_settings_deadline(deadline);
    assert!(
        bounded(client.handshake::<_, axum::body::Body>(TokioIo::new(io)))
            .await
            .is_err()
    );
    assert_eq!(reads.load(Ordering::Acquire), 0);
    assert_eq!(writes.load(Ordering::Acquire), 0);

    let (io, reads, writes) = ready_io();
    let mut server = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
    server.initial_settings_deadline(deadline);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let connection = server.serve_connection(
        TokioIo::new(io),
        hyper::service::service_fn(move |_: Request<hyper::body::Incoming>| {
            observed.fetch_add(1, Ordering::AcqRel);
            async { Ok::<_, Infallible>(Response::new(axum::body::Body::empty())) }
        }),
    );
    assert!(!connection.initial_settings_complete());
    assert!(bounded(connection).await.is_err());
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert_eq!(reads.load(Ordering::Acquire), 0);
    assert_eq!(writes.load(Ordering::Acquire), 0);
}
