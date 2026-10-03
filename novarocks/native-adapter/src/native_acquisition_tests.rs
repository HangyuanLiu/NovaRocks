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

//! Actual accepted/dial acquisition gates backed by the original BE stock.
//! Unpolled funded configs deliberately occupy the other finite positions;
//! this is not a claim of 32 TCP clients, per-lane gates or production Control
//! routing. Plaintext fixtures do not establish TLS or whole task metadata.

use super::serve_native_listener;
use crate::native_transport_capacity::{NativeTransportCapacityFactory, TransportClass};
use bytes::Bytes;
use hyper::http::{HeaderValue, Request, Response, Uri};
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::ResultWriteAdmission;
use novarocks_native_trust::NativeIncomingAdapter;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
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
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::{Notify, oneshot, watch};
use tonic::body::{BoxBody, boxed};
use tower::{Service, service_fn};

const WATCHDOG: Duration = Duration::from_secs(5);
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const ORIGINAL: &str = "acquisition-independent-original-alias";
async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(WATCHDOG, future)
        .await
        .expect("acquisition fixture failed to progress")
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
async fn stock_at(
    factory: &NativeTransportCapacityFactory,
    class: TransportClass,
    positions: usize,
) {
    bounded(async {
        while factory.available_positions(class) != positions {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
async fn acquisition_at(
    factory: &NativeTransportCapacityFactory,
    class: TransportClass,
    positions: usize,
) {
    bounded(async {
        while factory.available_acquisitions(class) != positions {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
async fn credit_returned(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
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
    let mut header = [0; 9];
    bounded(peer.read_exact(&mut header)).await.unwrap();
    assert_eq!(header[3], 4);
    assert_eq!(header[4], 0);
    assert_eq!(&header[5..], &[0, 0, 0, 0]);
    let len =
        (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    assert!(len <= 256);
    let mut payload = vec![0; len];
    bounded(peer.read_exact(&mut payload)).await.unwrap();
}
#[derive(Clone)]
struct HeldService {
    calls: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    alias: Arc<Mutex<Option<HeaderValue>>>,
}
impl HeldService {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            alias: Arc::new(Mutex::new(None)),
        }
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
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(request.headers().allocation_pool().is_some());
        assert!(request.headers().field_allocation_pool().is_some());
        let value = request.headers().get("x-native-original").unwrap();
        assert_eq!(value.as_bytes(), ORIGINAL.as_bytes());
        let alias = value.clone();
        assert_eq!(alias.as_bytes().as_ptr(), value.as_bytes().as_ptr());
        *self.alias.lock().unwrap() = Some(alias);
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            entered.notify_one();
            release.notified().await;
            drop(request);
            Ok(Response::new(boxed(axum::body::Body::empty())))
        })
    }
}
struct Listener {
    address: std::net::SocketAddr,
    service: HeldService,
    method: NativeRpcMethod,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), String>>,
}
impl Listener {
    async fn start(factory: &NativeTransportCapacityFactory, class: TransportClass) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = HeldService::new();
        let (shutdown, stopping) = watch::channel(false);
        let task = tokio::spawn(serve_native_listener(
            listener,
            service.clone(),
            NativeIncomingAdapter::plaintext(),
            stopping,
            Arc::new(|| {}),
            "acquisition-test",
            Some((factory.clone(), class)),
        ));
        Self {
            address,
            service,
            method: match class {
                TransportClass::Data => NativeRpcMethod::ApplyTaskOperations,
                TransportClass::Control => NativeRpcMethod::Heartbeat,
            },
            shutdown,
            task,
        }
    }
    async fn stop(self) {
        self.shutdown.send(true).unwrap();
        assert_eq!(bounded(self.task).await.unwrap(), Ok(()));
    }
    async fn half_open(&self) -> tokio::net::TcpStream {
        let mut peer = bounded(tokio::net::TcpStream::connect(self.address))
            .await
            .unwrap();
        // Actual output proves acceptance and construction before the peer
        // deliberately withholds its entire H2 preface and initial SETTINGS.
        initial_settings(&mut peer).await;
        peer
    }
    async fn assert_refused_without_preface_output(&self) {
        let mut peer = bounded(tokio::net::TcpStream::connect(self.address))
            .await
            .unwrap();
        let mut wire = Vec::new();
        let result = bounded(peer.read_to_end(&mut wire)).await;
        if let Err(error) = result {
            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        }
        assert!(
            wire.is_empty(),
            "full acquisition gate must refuse before initial SETTINGS I/O"
        );
        assert_eq!(self.service.calls.load(Ordering::SeqCst), 0);
    }
}
struct LiveRequest {
    sender: h2::client::SendRequest<Bytes>,
    response: h2::client::ResponseFuture,
    task: tokio::task::JoinHandle<Result<(), h2::Error>>,
}
async fn application(listener: &Listener) -> LiveRequest {
    let stream = bounded(tokio::net::TcpStream::connect(listener.address))
        .await
        .unwrap();
    let (mut sender, connection) = bounded(h2::client::handshake(stream)).await.unwrap();
    let task = tokio::spawn(connection);
    let (response, body) = sender
        .send_request(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "http://localhost{}",
                    listener.method.contract().path
                ))
                .header("x-native-original", ORIGINAL)
                .body(())
                .unwrap(),
            true,
        )
        .unwrap();
    drop(body);
    bounded(listener.service.entered.notified()).await;
    LiveRequest {
        sender,
        response,
        task,
    }
}
async fn finish_application(listener: Listener, request: LiveRequest) {
    listener.service.release.notify_one();
    let response = bounded(request.response).await.unwrap();
    assert_eq!(response.status(), 200);
    drop(response);
    listener.stop().await;
    drop(request.sender);
    let _ = bounded(request.task).await.unwrap();
}
async fn limit_case(class: TransportClass, opposite: TransportClass, expected: usize) {
    let (factory, budget, bytes) = stock();
    assert_eq!(factory.acquisition_positions(class), expected);
    let held = (0..expected - 1)
        .map(|_| factory.try_config(class).unwrap())
        .collect::<Vec<_>>();
    // Only one pending TCP bootstrap is real; the other positions are genuine
    // original-funded unpolled configuration owners, with no fictional clients.
    let blocked_listener = Listener::start(&factory, class).await;
    let other_listener = Listener::start(&factory, opposite).await;
    let other_alias = other_listener.service.alias.clone();
    let live = application(&other_listener).await;
    acquisition_at(&factory, opposite, factory.acquisition_positions(opposite)).await;
    let peer = blocked_listener.half_open().await;
    assert_eq!(factory.available_acquisitions(class), 0);
    blocked_listener
        .assert_refused_without_preface_output()
        .await;
    assert_eq!(factory.available_acquisitions(class), 0);
    assert_eq!(
        other_listener.service.calls.load(Ordering::SeqCst),
        1,
        "the independent class must keep application progress"
    );
    drop(peer);
    blocked_listener.stop().await;
    drop(held);
    acquisition_at(&factory, class, expected).await;
    finish_application(other_listener, live).await;
    drop(other_alias.lock().unwrap().take());
    stock_at(&factory, class, factory.positions(class)).await;
    stock_at(&factory, opposite, factory.positions(opposite)).await;
    drop(factory);
    credit_returned(&budget, bytes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_32_refuses_the_next_accepted_socket_while_control_application_progresses() {
    limit_case(TransportClass::Data, TransportClass::Control, 32).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_8_refuses_the_next_accepted_socket_without_borrowing_data() {
    limit_case(TransportClass::Control, TransportClass::Data, 8).await;
}

struct ExitingIo {
    io: Option<DuplexStream>,
    exit: Option<oneshot::Sender<()>>,
    factory: NativeTransportCapacityFactory,
}
impl Drop for ExitingIo {
    fn drop(&mut self) {
        drop(self.io.take());
        assert_eq!(
            self.factory.available_acquisitions(TransportClass::Data),
            0,
            "actual socket must exit before its acquisition position returns"
        );
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
async fn accepted_and_real_endpoint_dial_share_data_32_and_cancel_drops_io_before_gate() {
    let (factory, budget, bytes) = stock();
    let held = (0..30)
        .map(|_| factory.try_config(TransportClass::Data).unwrap())
        .collect::<Vec<_>>();
    let listener = Listener::start(&factory, TransportClass::Data).await;
    let inbound = listener.half_open().await;
    assert_eq!(factory.available_acquisitions(TransportClass::Data), 1);
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
        factory: factory.clone(),
    })));
    let connector_calls = Arc::new(AtomicUsize::new(0));
    let calls = connector_calls.clone();
    let connector = service_fn(move |_: Uri| {
        calls.fetch_add(1, Ordering::SeqCst);
        let io = input.lock().unwrap().take().unwrap();
        async move { Ok::<_, io::Error>(TokioIo::new(io)) }
    });
    let connecting = tokio::spawn(async move { endpoint.connect_with_connector(connector).await });
    let mut preface = [0; 24];
    bounded(peer.read_exact(&mut preface)).await.unwrap();
    assert_eq!(&preface, PREFACE);
    initial_settings(&mut peer).await;
    assert_eq!(connector_calls.load(Ordering::SeqCst), 1);
    assert_eq!(factory.available_acquisitions(TransportClass::Data), 0);
    let refused_connector_calls = Arc::new(AtomicUsize::new(0));
    let observed = refused_connector_calls.clone();
    let refused = service_fn(move |_: Uri| {
        observed.fetch_add(1, Ordering::SeqCst);
        async { Err::<TokioIo<DuplexStream>, _>(io::Error::other("unexpected connector call")) }
    });
    let next =
        crate::native_client::capacity_endpoint(&runtime, &address, TransportClass::Data).unwrap();
    assert!(bounded(next.connect_with_connector(refused)).await.is_err());
    assert_eq!(
        refused_connector_calls.load(Ordering::SeqCst),
        0,
        "incoming and outgoing acquisition gates cannot each permit 32"
    );
    connecting.abort();
    assert!(bounded(connecting).await.unwrap_err().is_cancelled());
    bounded(exited).await.unwrap();
    acquisition_at(&factory, TransportClass::Data, 1).await;
    stock_at(
        &factory,
        TransportClass::Data,
        factory.positions(TransportClass::Data) - 31,
    )
    .await;
    drop(peer);
    drop(inbound);
    listener.stop().await;
    drop(runtime);
    // Endpoint construction retains the original factory for reconnects even
    // after this refused attempt exits. Retire that real issuer alias too.
    drop(next);
    drop(held);
    acquisition_at(&factory, TransportClass::Data, 32).await;
    stock_at(
        &factory,
        TransportClass::Data,
        factory.positions(TransportClass::Data),
    )
    .await;
    drop(factory);
    credit_returned(&budget, bytes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn applied_settings_returns_acquisition_immediately_but_last_header_alias_keeps_original_stock()
 {
    let (factory, budget, bytes) = stock();
    let listener = Listener::start(&factory, TransportClass::Data).await;
    let alias = listener.service.alias.clone();
    let live = application(&listener).await;
    // The counter itself is the completion oracle. This failure watchdog is
    // shorter than Native's 2s timer, so waiting until D cannot pass as success.
    tokio::time::timeout(
        Duration::from_secs(1),
        acquisition_at(&factory, TransportClass::Data, 32),
    )
    .await
    .expect("completed SETTINGS must return acquisition before the 2s timer");
    assert_eq!(
        factory.available_positions(TransportClass::Data),
        factory.positions(TransportClass::Data) - 1
    );
    assert!(matches!(
        budget.try_reserve_process(bytes).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    finish_application(listener, live).await;
    stock_at(
        &factory,
        TransportClass::Data,
        factory.positions(TransportClass::Data) - 1,
    )
    .await;
    assert_eq!(factory.available_acquisitions(TransportClass::Data), 32);
    assert_eq!(
        alias.lock().unwrap().as_ref().unwrap().as_bytes(),
        ORIGINAL.as_bytes()
    );
    drop(alias.lock().unwrap().take());
    stock_at(
        &factory,
        TransportClass::Data,
        factory.positions(TransportClass::Data),
    )
    .await;
    drop(factory);
    credit_returned(&budget, bytes).await;
}
