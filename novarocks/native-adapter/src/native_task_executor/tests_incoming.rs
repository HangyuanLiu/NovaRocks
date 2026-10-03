// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Actual incoming TCP/H2 head admission using original Native transport stock.
//! The lifecycle becomes Live only after real peer SETTINGS and ACK flush.
//! Ordinary TCP registration, outer task/fixture allocations and complete auth
//! scratch are separate; this is not a whole listener or topology proof.

use super::*;
use crate::native_transport_capacity::{
    NativeTransportCapacityFactory, TransportClass, configure_server,
};
use hyper::body::Incoming;
use hyper::http::{HeaderValue, Request, Response, header::AUTHORIZATION};
use hyper::service::Service;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::ResultWriteAdmission;
use novarocks_native_trust::{
    DeploymentId, ManualClock, NativeCallerSubject, NativeProcessIdentity, NativeTransportMode,
    NativeTrust, TOKEN_LIFETIME_SECONDS, ValidatedSharedSecret,
};
use novarocks_secret::SecretValue;
use novarocks_types::{BackendProcessId, FrontendProcessId};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::body::BoxBody;

const WATCHDOG: Duration = Duration::from_secs(5);
const OWNER_HEADER: &str = "x-incoming-original";
const OWNER_VALUE: &str = "actual-incoming-original-field";
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static PREPARATIONS: AtomicUsize = AtomicUsize::new(0);

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(WATCHDOG, future)
        .await
        .expect("incoming TCP/H2 fixture must progress")
}
fn peer(frontend: bool, suffix: u8) -> NativeProcessIdentity {
    let mut bytes = [0; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[15] = suffix;
    if frontend {
        NativeProcessIdentity::Frontend(FrontendProcessId::try_from_bytes(bytes).unwrap())
    } else {
        NativeProcessIdentity::Backend(BackendProcessId::try_from_bytes(bytes).unwrap())
    }
}
fn trust(clock: &Arc<ManualClock>, identity: Option<NativeProcessIdentity>) -> NativeTrust {
    let trust = NativeTrust::new_with_clock(
        DeploymentId::parse("incoming-head-proof").unwrap(),
        ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef")).unwrap(),
        // Deliberately identical and misleading across FE/BE issuers. Subject
        // and socket endpoint must never substitute for the signed process.
        NativeCallerSubject::parse("fe@diagnostic-same-subject:9080").unwrap(),
        NativeTransportMode::Disabled,
        clock.clone(),
    );
    if let Some(identity) = identity {
        trust.bind_process_identity(identity).unwrap();
    }
    trust
}
fn authorization(trust: &NativeTrust) -> HeaderValue {
    let mut metadata = tonic::metadata::MetadataMap::new();
    trust.apply_client_authorization(&mut metadata).unwrap();
    HeaderValue::from_bytes(metadata.get("authorization").unwrap().as_encoded_bytes()).unwrap()
}
struct Stock {
    factory: NativeTransportCapacityFactory,
    budget: Arc<ResultRetainedBudget>,
    bytes: usize,
}
impl Stock {
    fn new() -> Self {
        let bytes = NativeTransportCapacityFactory::allocation_capacity_bound().unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
        let factory = NativeTransportCapacityFactory::try_new(budget.clone()).unwrap();
        Self {
            factory,
            budget,
            bytes,
        }
    }
    fn held(&self) {
        assert!(matches!(
            self.budget.try_reserve_process(self.bytes).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    async fn positions(&self, expected: usize) {
        bounded(async {
            while self.factory.available_positions(TransportClass::Data) != expected {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }
    async fn finish(self) {
        self.positions(self.factory.positions(TransportClass::Data))
            .await;
        let Self {
            factory,
            budget,
            bytes,
        } = self;
        drop(factory);
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
}
#[derive(Clone)]
struct CountExecutor(NativeTaskExecutor);
impl<F> hyper::rt::Executor<F> for CountExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn task_allocation_capacity_bound() -> io::Result<usize> {
        <NativeTaskExecutor as hyper::rt::Executor<F>>::task_allocation_capacity_bound()
    }
    fn admit_request_head(&self, uri: &http::Uri, headers: &http::HeaderMap) -> io::Result<()> {
        <NativeTaskExecutor as hyper::rt::Executor<F>>::admit_request_head(&self.0, uri, headers)
    }
    fn try_prepare_task(&self) -> io::Result<Option<Self>> {
        PREPARATIONS.fetch_add(1, Ordering::SeqCst);
        <NativeTaskExecutor as hyper::rt::Executor<F>>::try_prepare_task(&self.0)
            .map(|executor| executor.map(Self))
    }
    fn execute(&self, future: F) {
        self.0.execute(future);
    }
}
#[derive(Default)]
struct Observed {
    calls: AtomicUsize,
    polls: AtomicUsize,
    field: Mutex<Option<HeaderValue>>,
    waker: Mutex<Option<Waker>>,
}
struct Handler(Arc<Observed>);
struct ReplyFuture(Arc<Observed>);
impl Future for ReplyFuture {
    type Output = Result<Response<BoxBody>, Infallible>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.polls.fetch_add(1, Ordering::SeqCst);
        *self.0.waker.lock().unwrap() = Some(cx.waker().clone());
        Poll::Ready(Ok(Response::new(tonic::body::empty_body())))
    }
}
impl Service<Request<Incoming>> for Handler {
    type Response = Response<BoxBody>;
    type Error = Infallible;
    type Future = ReplyFuture;
    fn call(&self, request: Request<Incoming>) -> ReplyFuture {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        assert!(request.headers().allocation_pool().is_some());
        assert!(request.headers().field_allocation_pool().is_some());
        let source = request.headers().get(OWNER_HEADER).unwrap();
        assert_eq!(source.as_bytes(), OWNER_VALUE.as_bytes());
        let alias = source.clone();
        assert_eq!(alias.as_bytes().as_ptr(), source.as_bytes().as_ptr());
        *self.0.field.lock().unwrap() = Some(alias);
        drop(request); // No fixture handler consumes the request body.
        ReplyFuture(self.0.clone())
    }
}
struct ObservedIo {
    stream: Option<TcpStream>,
    original: Option<Bytes>,
    exited: Arc<AtomicUsize>,
}
impl Drop for ObservedIo {
    fn drop(&mut self) {
        drop(self.stream.take());
        self.exited.fetch_add(1, Ordering::SeqCst);
        drop(self.original.take());
    }
}
impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().unwrap()).poll_read(cx, buffer)
    }
}
impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.stream.as_mut().unwrap()).poll_write(cx, data)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().unwrap()).poll_shutdown(cx)
    }
}
struct Session {
    client: Option<h2::client::SendRequest<Bytes>>,
    server: JoinHandle<Result<(), hyper::Error>>,
    peer_task: JoinHandle<Result<(), h2::Error>>,
    observed: Arc<Observed>,
    server_exit: Arc<AtomicUsize>,
    peer_exit: Arc<AtomicUsize>,
    io_alias: Option<Bytes>,
}
impl Session {
    async fn new(stock: &Stock, verifier: &NativeTrust, keep_io_alias: bool) -> Self {
        let (mut config, binding) = stock
            .factory
            .try_incoming_config(TransportClass::Data)
            .unwrap();
        let original = config.io_owner.as_ref().unwrap().clone();
        let io_alias = keep_io_alias.then(|| original.clone());
        let native = stock
            .factory
            .server_stream_executor(original.clone())
            .unwrap()
            .with_incoming_head(
                binding,
                verifier.server_admission(),
                NativeEndpointDomain::BackendData,
            );
        let actual = hyper::server::conn::http2::Builder::<CountExecutor>::stream_task_allocation_capacity_bound::<Handler>().unwrap();
        // The wrapper is inline and the real future fits the production pregrant.
        // A larger generic test must not silently exceed that original receipt.
        stock.factory.validate_stream_task_capacity(actual).unwrap();
        let mut builder = hyper::server::conn::http2::Builder::new(CountExecutor(native));
        configure_server(&mut builder, &config).unwrap();
        builder
            .auto_date_header(false)
            .reject_connect_for_preallocated_tasks(true)
            .initial_settings_deadline(Instant::now() + Duration::from_secs(2));
        let lifecycle = config.connection_lifecycle.as_ref().unwrap().clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = TcpStream::connect(address).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        drop(listener);
        let observed = Arc::new(Observed::default());
        let server_exit = Arc::new(AtomicUsize::new(0));
        let peer_exit = Arc::new(AtomicUsize::new(0));
        let (ready_tx, ready_rx) = oneshot::channel();
        let mut connection = builder.serve_connection(
            TokioIo::new(ObservedIo {
                stream: Some(stream),
                original: Some(original),
                exited: server_exit.clone(),
            }),
            Handler(observed.clone()),
        );
        drop(builder);
        let server = tokio::spawn(async move {
            std::future::poll_fn(|cx| {
                let result = Pin::new(&mut connection).poll(cx);
                if connection.initial_settings_complete() {
                    Poll::Ready(Ok(()))
                } else {
                    match result {
                        Poll::Ready(result) => Poll::Ready(result),
                        Poll::Pending => Poll::Pending,
                    }
                }
            })
            .await?;
            // Initial SETTINGS and the final acquisition are distinct real
            // events. No service is dispatched in the initial phase yield.
            lifecycle.on_acquisition_complete().unwrap();
            lifecycle.release_acquisition_owner().unwrap();
            drop(config.acquisition_owner.take());
            drop(config);
            drop(lifecycle);
            ready_tx.send(()).unwrap();
            connection.await
        });
        let (client, peer_connection) = h2::client::handshake(ObservedIo {
            stream: Some(peer),
            original: None,
            exited: peer_exit.clone(),
        })
        .await
        .unwrap();
        let peer_task = tokio::spawn(peer_connection);
        bounded(ready_rx).await.unwrap();
        Self {
            client: Some(client),
            server,
            peer_task,
            observed,
            server_exit,
            peer_exit,
            io_alias,
        }
    }
    async fn request(
        &mut self,
        path: &str,
        authorization: Option<HeaderValue>,
    ) -> Result<Response<h2::RecvStream>, h2::Error> {
        let client = self.client.as_mut().unwrap();
        std::future::poll_fn(|cx| client.poll_ready(cx)).await?;
        let mut request = Request::builder()
            .uri(format!("http://incoming.test{path}"))
            .body(())
            .unwrap();
        request
            .headers_mut()
            .insert(OWNER_HEADER, HeaderValue::from_static(OWNER_VALUE));
        if let Some(authorization) = authorization {
            request.headers_mut().insert(AUTHORIZATION, authorization);
        }
        let (response, _) = client.send_request(request, true)?;
        bounded(response).await
    }
    async fn allowed(&mut self, method: NativeRpcMethod, caller: &NativeTrust) {
        let response = self
            .request(method.contract().path, Some(authorization(caller)))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        drop(response);
    }
    async fn close(self, natural_error: bool) -> (Option<HeaderValue>, Option<Bytes>) {
        let Self {
            mut client,
            server,
            peer_task,
            observed,
            server_exit,
            peer_exit,
            io_alias,
        } = self;
        if natural_error {
            let result = bounded(server).await.unwrap();
            assert!(
                result.is_err(),
                "head mismatch must fail the actual Hyper connection, not merely reset one stream"
            );
            drop(result);
        } else {
            server.abort();
            let result = bounded(server).await;
            assert!(result.is_ok() || result.as_ref().is_err_and(|error| error.is_cancelled()));
            drop(result);
        }
        assert_eq!(
            server_exit.load(Ordering::SeqCst),
            1,
            "actual TCP/IO must have been destroyed"
        );
        drop(client.take());
        // No abort on the peer: the actual connection EOF/error drives its
        // natural termination and closes its socket before the assertion.
        drop(bounded(peer_task).await.unwrap());
        assert_eq!(peer_exit.load(Ordering::SeqCst), 1);
        drop(observed.waker.lock().unwrap().take());
        let field = observed.field.lock().unwrap().take();
        (field, io_alias)
    }
}

#[tokio::test]
async fn actual_same_lane_methods_and_renewed_signed_process_share_one_live_connection() {
    let _serial = SERIAL.lock().await;
    PREPARATIONS.store(0, Ordering::SeqCst);
    let stock = Stock::new();
    let clock = Arc::new(ManualClock::new(1_700_000_000));
    let verifier = trust(&clock, Some(peer(false, 99)));
    let caller = trust(&clock, Some(peer(true, 1)));
    let first = authorization(&caller);
    let mut session = Session::new(&stock, &verifier, false).await;
    session
        .allowed(NativeRpcMethod::FetchTaskDynamicFilters, &caller)
        .await;
    session
        .allowed(NativeRpcMethod::GetFinalTaskInfo, &caller)
        .await;
    clock.set_unix_seconds(1_700_000_000 + TOKEN_LIFETIME_SECONDS + 1);
    let renewed = authorization(&caller);
    assert_ne!(first, renewed);
    session
        .allowed(NativeRpcMethod::SubscribeTaskStatus, &caller)
        .await;
    assert_eq!(session.observed.calls.load(Ordering::SeqCst), 3);
    assert_eq!(PREPARATIONS.load(Ordering::SeqCst), 3);
    let (alias, io) = session.close(false).await;
    assert!(io.is_none());
    stock.held();
    assert_eq!(
        stock.factory.available_positions(TransportClass::Data),
        stock.factory.positions(TransportClass::Data) - 1
    );
    drop(alias);
    stock.finish().await;
}

#[tokio::test]
async fn actual_cross_process_or_lane_fails_connection_before_prepare_or_service() {
    let _serial = SERIAL.lock().await;
    let stock = Stock::new();
    let clock = Arc::new(ManualClock::new(1_700_000_000));
    let verifier = trust(&clock, Some(peer(false, 99)));
    let first = trust(&clock, Some(peer(true, 1)));
    let other = trust(&clock, Some(peer(true, 2)));
    for (caller, method) in [
        (&other, NativeRpcMethod::GetFinalTaskInfo),
        (&first, NativeRpcMethod::FetchTaskResult),
    ] {
        PREPARATIONS.store(0, Ordering::SeqCst);
        let mut session = Session::new(&stock, &verifier, false).await;
        session
            .allowed(NativeRpcMethod::FetchTaskDynamicFilters, &first)
            .await;
        let failure = session
            .request(method.contract().path, Some(authorization(caller)))
            .await;
        assert!(failure.is_err());
        drop(failure);
        assert_eq!(session.observed.calls.load(Ordering::SeqCst), 1);
        assert_eq!(PREPARATIONS.load(Ordering::SeqCst), 1);
        let aliases = session.close(true).await;
        stock.held();
        drop(aliases);
        stock
            .positions(stock.factory.positions(TransportClass::Data))
            .await;
    }
    stock.finish().await;
}

#[tokio::test]
async fn actual_authenticated_wrong_role_and_legacy_missing_process_close_before_dispatch() {
    let _serial = SERIAL.lock().await;
    let stock = Stock::new();
    let clock = Arc::new(ManualClock::new(1_700_000_000));
    let verifier = trust(&clock, Some(peer(false, 99)));
    let backend = trust(&clock, Some(peer(false, 1)));
    let legacy = trust(&clock, None);
    for caller in [&backend, &legacy] {
        let token = authorization(caller);
        let mut headers = http::HeaderMap::new();
        headers.insert(AUTHORIZATION, token.clone());
        assert!(
            verifier.server_admission().admit_headers(&headers).is_ok(),
            "this is an authenticated deployment token, not an auth failure"
        );
        PREPARATIONS.store(0, Ordering::SeqCst);
        let mut session = Session::new(&stock, &verifier, false).await;
        let failure = session
            .request(
                NativeRpcMethod::FetchTaskDynamicFilters.contract().path,
                Some(token),
            )
            .await;
        assert!(failure.is_err());
        drop(failure);
        assert_eq!(session.observed.calls.load(Ordering::SeqCst), 0);
        assert_eq!(PREPARATIONS.load(Ordering::SeqCst), 0);
        let aliases = session.close(true).await;
        assert!(aliases.0.is_none());
        drop(aliases);
        stock
            .positions(stock.factory.positions(TransportClass::Data))
            .await;
    }
    stock.finish().await;
}

#[tokio::test]
async fn missing_bad_auth_unknown_and_wrong_domain_preserve_fallback_without_sealing() {
    let _serial = SERIAL.lock().await;
    PREPARATIONS.store(0, Ordering::SeqCst);
    let stock = Stock::new();
    let clock = Arc::new(ManualClock::new(1_700_000_000));
    let verifier = trust(&clock, Some(peer(false, 99)));
    let frontend = trust(&clock, Some(peer(true, 1)));
    let backend = trust(&clock, Some(peer(false, 2)));
    let mut session = Session::new(&stock, &verifier, false).await;
    // This fixture's service stands for existing auth/route error handling.
    // The borrowed head hook must not reinterpret those errors as a lane seal.
    for (path, token) in [
        (
            NativeRpcMethod::FetchTaskDynamicFilters.contract().path,
            None,
        ),
        (
            NativeRpcMethod::FetchTaskDynamicFilters.contract().path,
            Some(HeaderValue::from_static("Bearer invalid")),
        ),
        ("/not/a/native/method", Some(authorization(&backend))),
        (
            NativeRpcMethod::Heartbeat.contract().path,
            Some(authorization(&frontend)),
        ),
    ] {
        let response = session.request(path, token).await.unwrap();
        assert_eq!(response.status(), 200);
        drop(response);
    }
    // A different role/lane is still legal because none of the above sealed.
    session
        .allowed(NativeRpcMethod::TransmitRuntimeFilterEnvelope, &backend)
        .await;
    assert_eq!(session.observed.calls.load(Ordering::SeqCst), 5);
    assert_eq!(PREPARATIONS.load(Ordering::SeqCst), 5);
    let other = trust(&clock, Some(peer(false, 3)));
    let failure = session
        .request(
            NativeRpcMethod::TransmitRuntimeFilterEnvelope
                .contract()
                .path,
            Some(authorization(&other)),
        )
        .await;
    assert!(failure.is_err());
    drop(failure);
    assert_eq!(session.observed.calls.load(Ordering::SeqCst), 5);
    assert_eq!(PREPARATIONS.load(Ordering::SeqCst), 5);
    drop(session.close(true).await);
    stock.finish().await;
}

#[tokio::test]
async fn actual_field_and_io_aliases_hold_closing_generations_until_last_original_exit() {
    let _serial = SERIAL.lock().await;
    PREPARATIONS.store(0, Ordering::SeqCst);
    let stock = Stock::new();
    let clock = Arc::new(ManualClock::new(1_700_000_000));
    let verifier = trust(&clock, Some(peer(false, 99)));
    let caller = trust(&clock, Some(peer(false, 1)));
    let method = NativeRpcMethod::TransmitRuntimeFilterEnvelope;
    let positions = stock.factory.positions(TransportClass::Data);
    let mut first = Session::new(&stock, &verifier, false).await;
    first.allowed(method, &caller).await;
    let (field_first, io_first) = first.close(false).await;
    assert!(io_first.is_none());
    assert!(field_first.is_some());
    stock.positions(positions - 1).await;
    // First generation occupies the sole Closing row via a real field alias.
    let mut second = Session::new(&stock, &verifier, true).await;
    second.allowed(method, &caller).await;
    let (field_second, io_second) = second.close(false).await;
    drop(field_second);
    assert!(io_second.is_some());
    stock.positions(positions - 2).await;
    // Closing is already occupied, so this second retired generation retains
    // its Live charge. A new real connection cannot seal the same filter lane.
    let before = PREPARATIONS.load(Ordering::SeqCst);
    let mut third = Session::new(&stock, &verifier, false).await;
    let failure = third
        .request(method.contract().path, Some(authorization(&caller)))
        .await;
    assert!(failure.is_err());
    drop(failure);
    assert_eq!(third.observed.calls.load(Ordering::SeqCst), 0);
    assert_eq!(PREPARATIONS.load(Ordering::SeqCst), before);
    drop(third.close(true).await);
    stock.positions(positions - 2).await;
    stock.held();
    // Dropping the IO capability of exactly the second physical generation
    // releases its RetiringLive row, while the first Closing field still lives.
    drop(io_second);
    stock.positions(positions - 1).await;
    let mut recovered = Session::new(&stock, &verifier, false).await;
    recovered.allowed(method, &caller).await;
    assert_eq!(recovered.observed.calls.load(Ordering::SeqCst), 1);
    drop(recovered.close(false).await);
    stock.positions(positions - 1).await;
    drop(field_first);
    stock.finish().await;
}
