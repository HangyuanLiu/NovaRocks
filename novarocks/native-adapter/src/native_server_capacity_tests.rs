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

//! Actual listener/endpoint installation and independent decoded-field exits.
//! Plaintext TCP/duplex fixtures do not prove TLS, complete stream/task/body
//! ownership, allocator caches, a whole connection envelope, or Native V1.

use super::serve_native_listener;
use crate::native_transport_capacity::{NativeTransportCapacityFactory, TransportClass};
use bytes::Bytes;
use hyper::http::{HeaderValue, Request, Response, Uri};
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::ResultWriteAdmission;
use novarocks_native_trust::NativeIncomingAdapter;
use novarocks_types::NativeEndpoint;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::collections::VecDeque;
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
use tokio::sync::{Notify, oneshot, watch};
use tonic::body::{BoxBody, boxed};
use tower::{Service, ServiceExt, service_fn};

const WATCHDOG: Duration = Duration::from_secs(5);
const ORIGINAL_VALUE: &str = "independent-original-wire-field";

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(WATCHDOG, future)
        .await
        .expect("actual transport fixture exceeded its failure watchdog")
}

fn original_stock() -> (
    NativeTransportCapacityFactory,
    Arc<ResultRetainedBudget>,
    usize,
) {
    let bytes = NativeTransportCapacityFactory::allocation_capacity_bound().unwrap();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
    let factory = NativeTransportCapacityFactory::try_new(budget.clone()).unwrap();
    (factory, budget, bytes)
}

fn held(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    assert!(matches!(
        budget.try_reserve_process(bytes).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}

fn released(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("all concrete original stock owners must have physically exited");
    };
    drop(credit);
}

async fn returned(
    factory: &NativeTransportCapacityFactory,
    class: TransportClass,
    positions: usize,
) {
    bounded(async {
        while factory.available_positions(class) != positions {
            // Observe the actual exit condition, rather than assuming a delay
            // means the connection/task/owner has already disappeared.
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[derive(Clone)]
struct CaptureService {
    expects_original: bool,
    alias: Arc<Mutex<Option<HeaderValue>>>,
    entered: Arc<Notify>,
    release: Option<Arc<Notify>>,
}

impl Service<Request<axum::body::Body>> for CaptureService {
    type Response = Response<BoxBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<axum::body::Body>) -> Self::Future {
        assert_eq!(
            request.headers().allocation_pool().is_some(),
            self.expects_original
        );
        assert_eq!(
            request.headers().field_allocation_pool().is_some(),
            self.expects_original
        );
        if self.expects_original {
            let maps = request.headers().allocation_pool().unwrap();
            let fields = request.headers().field_allocation_pool().unwrap();
            assert!(maps.field_allocation_pool().unwrap().same_pool(fields));
        }
        let original = request.headers().get("x-native-original").unwrap();
        assert_eq!(original.as_bytes(), ORIGINAL_VALUE.as_bytes());
        let alias = original.clone();
        if self.expects_original {
            assert_eq!(alias.as_bytes().as_ptr(), original.as_bytes().as_ptr());
        }
        *self.alias.lock().unwrap() = Some(alias);
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            // Retain the actual incoming map/body until this request finishes.
            // Only the captured HeaderValue escapes, not another pool handle.
            entered.notify_one();
            if let Some(release) = release {
                release.notified().await;
            }
            drop(request);
            Ok(Response::new(boxed(axum::body::Body::empty())))
        })
    }
}

async fn listener_request(
    address: std::net::SocketAddr,
) -> (
    h2::client::SendRequest<Bytes>,
    h2::client::ResponseFuture,
    tokio::task::JoinHandle<()>,
) {
    let stream = bounded(tokio::net::TcpStream::connect(address))
        .await
        .unwrap();
    let (mut sender, connection) = bounded(h2::client::handshake(stream)).await.unwrap();
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let request = Request::builder()
        .method("POST")
        .uri("http://localhost/original")
        .header("x-native-original", ORIGINAL_VALUE)
        .body(())
        .unwrap();
    let (response, body) = sender.send_request(request, true).unwrap();
    drop(body);
    (sender, response, driver)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_tcp_listener_decoded_alias_survives_actual_connection_exit() {
    let (factory, budget, bytes) = original_stock();
    let total_positions = factory.positions(TransportClass::Data);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let alias = Arc::new(Mutex::new(None));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (shutdown, shutting_down) = watch::channel(false);
    let serving = tokio::spawn(serve_native_listener(
        listener,
        CaptureService {
            expects_original: true,
            alias: alias.clone(),
            entered: entered.clone(),
            release: Some(release.clone()),
        },
        NativeIncomingAdapter::plaintext(),
        shutting_down,
        Arc::new(|| {}),
        "capacity-test",
        Some((factory.clone(), TransportClass::Data)),
    ));
    let (sender, response, driver) = listener_request(address).await;
    bounded(entered.notified()).await;
    assert_eq!(
        factory.available_positions(TransportClass::Data),
        total_positions - 1
    );
    shutdown.send(true).unwrap();
    release.notify_one();
    let response = bounded(response).await.unwrap();
    assert_eq!(response.status(), 200);
    drop(response);
    assert_eq!(bounded(serving).await.unwrap(), Ok(()));
    drop(sender);
    bounded(driver).await.unwrap();
    // The listener and actual peer connection tasks have returned. The one
    // independent HeaderValue is the intentionally retained physical owner.
    assert_eq!(
        factory.available_positions(TransportClass::Data),
        total_positions - 1
    );
    let value = alias.lock().unwrap().take().unwrap();
    assert_eq!(value.as_bytes(), ORIGINAL_VALUE.as_bytes());
    held(&budget, bytes);
    drop(value);
    returned(&factory, TransportClass::Data, total_positions).await;
    drop(factory);
    released(&budget, bytes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_tcp_listener_keeps_default_decoded_headers_without_capability() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let alias = Arc::new(Mutex::new(None));
    let entered = Arc::new(Notify::new());
    let (shutdown, shutting_down) = watch::channel(false);
    let serving = tokio::spawn(serve_native_listener(
        listener,
        CaptureService {
            expects_original: false,
            alias: alias.clone(),
            entered: entered.clone(),
            release: None,
        },
        NativeIncomingAdapter::plaintext(),
        shutting_down,
        Arc::new(|| {}),
        "ordinary-capacity-test",
        None,
    ));
    let (sender, response, driver) = listener_request(address).await;
    bounded(entered.notified()).await;
    let response = bounded(response).await.unwrap();
    assert_eq!(response.status(), 200);
    drop(response);
    shutdown.send(true).unwrap();
    assert_eq!(bounded(serving).await.unwrap(), Ok(()));
    drop(sender);
    bounded(driver).await.unwrap();
    assert_eq!(
        alias.lock().unwrap().take().unwrap().as_bytes(),
        ORIGINAL_VALUE.as_bytes()
    );
}

struct ExitingDuplex {
    io: DuplexStream,
    exited: Option<oneshot::Sender<()>>,
}

impl Drop for ExitingDuplex {
    fn drop(&mut self) {
        if let Some(exited) = self.exited.take() {
            let _ = exited.send(());
        }
    }
}

impl AsyncRead for ExitingDuplex {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl AsyncWrite for ExitingDuplex {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

fn endpoint_peer(
    value: &'static str,
) -> (
    ExitingDuplex,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let (client, peer) = tokio::io::duplex(65536);
    let (exiting, exited) = oneshot::channel();
    let (close, closing) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut connection = h2::server::handshake(peer).await.unwrap();
        let (request, mut sender) = connection.accept().await.unwrap().unwrap();
        assert_eq!(request.uri().path(), "/capacity-endpoint");
        drop(request);
        let response = Response::builder()
            .header("x-peer-original", value)
            .body(())
            .unwrap();
        drop(sender.send_response(response, true).unwrap());
        // Polling accept also flushes the actual response. The independent
        // fixture cancellation then drops this concrete peer connection.
        tokio::select! {
            _ = closing => {},
            request = connection.accept() => assert!(request.is_none()),
        }
        drop(connection);
    });
    (
        ExitingDuplex {
            io: client,
            exited: Some(exiting),
        },
        exited,
        close,
        task,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_capacity_endpoint_clones_install_two_fresh_original_configurations() {
    let (factory, budget, bytes) = original_stock();
    let runtime = crate::BackendDataRuntime::new(
        tokio::runtime::Handle::current(),
        crate::backend_test_support::test_backend_native_trust(),
        crate::BackendNativeTransport::Plaintext,
    )
    .with_transport_capacity(factory.clone())
    .unwrap();
    let address = NativeEndpoint::from_host_port("localhost", 9070).unwrap();
    let endpoint =
        crate::native_client::capacity_endpoint(&runtime, &address, TransportClass::Control)
            .unwrap();
    let (first_io, first_exit, first_close, first_join) =
        endpoint_peer("first-original-response-field");
    let (second_io, second_exit, second_close, second_join) =
        endpoint_peer("second-original-response-field");
    let inputs = Arc::new(Mutex::new(VecDeque::from([first_io, second_io])));
    let calls = Arc::new(AtomicUsize::new(0));
    let connector = || {
        let inputs = inputs.clone();
        let calls = calls.clone();
        let factory = factory.clone();
        service_fn(move |_: Uri| {
            // The real factory claim must precede actual connector I/O.
            assert!(
                factory.available_positions(TransportClass::Control)
                    < factory.positions(TransportClass::Control)
            );
            calls.fetch_add(1, Ordering::AcqRel);
            let io = inputs.lock().unwrap().pop_front();
            async move {
                io.map(TokioIo::new)
                    .ok_or_else(|| io::Error::from(io::ErrorKind::ConnectionRefused))
            }
        })
    };
    let first = bounded(endpoint.clone().connect_with_connector(connector()))
        .await
        .unwrap();
    let second = bounded(endpoint.clone().connect_with_connector(connector()))
        .await
        .unwrap();
    let first_response = bounded(
        first.clone().oneshot(
            Request::builder()
                .uri("http://localhost/capacity-endpoint")
                .body(boxed(axum::body::Body::empty()))
                .unwrap(),
        ),
    )
    .await
    .unwrap();
    let second_response = bounded(
        second.clone().oneshot(
            Request::builder()
                .uri("http://localhost/capacity-endpoint")
                .body(boxed(axum::body::Body::empty()))
                .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::Acquire), 2);
    let first_maps = first_response.headers().allocation_pool().unwrap();
    let second_maps = second_response.headers().allocation_pool().unwrap();
    assert!(
        !first_maps.same_pool(second_maps),
        "each actual attempt must install a fresh map family"
    );
    let first_fields = first_response.headers().field_allocation_pool().unwrap();
    let second_fields = second_response.headers().field_allocation_pool().unwrap();
    assert!(!first_fields.same_pool(second_fields));
    assert!(
        first_maps
            .field_allocation_pool()
            .unwrap()
            .same_pool(first_fields)
    );
    assert!(
        second_maps
            .field_allocation_pool()
            .unwrap()
            .same_pool(second_fields)
    );
    let first_value = first_response
        .headers()
        .get("x-peer-original")
        .unwrap()
        .clone();
    let second_value = second_response
        .headers()
        .get("x-peer-original")
        .unwrap()
        .clone();
    assert_eq!(first_value.as_bytes(), b"first-original-response-field");
    assert_eq!(second_value.as_bytes(), b"second-original-response-field");
    drop(first_response);
    drop(second_response);
    assert_eq!(factory.available_positions(TransportClass::Control), 18);
    assert_eq!(
        factory.available_positions(TransportClass::Data),
        factory.positions(TransportClass::Data)
    );
    first_close.send(()).unwrap();
    second_close.send(()).unwrap();
    bounded(first_join).await.unwrap();
    bounded(second_join).await.unwrap();
    bounded(first_exit).await.unwrap();
    bounded(second_exit).await.unwrap();
    drop(first);
    drop(second);
    drop(endpoint);
    drop(runtime);
    assert_eq!(factory.available_positions(TransportClass::Control), 18);
    held(&budget, bytes);
    drop(first_value);
    returned(&factory, TransportClass::Control, 19).await;
    drop(second_value);
    returned(&factory, TransportClass::Control, 20).await;
    drop(factory);
    // Channel's buffer worker may retire its connector handle on a subsequent
    // task turn. Require the real original reservation to return, rather than
    // treating IO Drop as proof that all asynchronous caller handles exited.
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
