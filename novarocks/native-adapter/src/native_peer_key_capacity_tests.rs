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

//! Actual per-key factory admission, including the same Channel's reconnect.
//! The Worker budget funds the production transport stock. Direct config
//! blockers are explicitly kernel positions, not alleged TCP/H2 connections.
//! Socket/task/executor fixtures and complete enclosing memory remain separate.

use super::{NativeChannelKey, capacity_endpoint_for_key};
use crate::backend_test_support::test_backend_data_runtime;
use crate::native_transport_capacity::{NativeTransportCapacityFactory, TransportClass};
use bytes::Bytes;
use hyper::body::Body;
use hyper::http::{Request, Response, Uri};
use hyper::rt::Executor;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::ResultWriteAdmission;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_types::{BackendProcessId, NativeEndpoint};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tower::{ServiceExt, service_fn};

const WATCHDOG: Duration = Duration::from_secs(5);
type Task = Pin<Box<dyn Future<Output = ()> + Send>>;
#[derive(Clone, Default)]
struct ManualExecutor {
    tasks: Arc<Mutex<Vec<Task>>>,
    completed: Arc<AtomicUsize>,
}
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for ManualExecutor {
    fn execute(&self, future: F) {
        self.tasks.lock().unwrap().push(Box::pin(future));
    }
}
impl ManualExecutor {
    fn poll_once(&self, cx: &mut Context<'_>) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for mut task in tasks {
            if task.as_mut().poll(cx).is_pending() {
                self.tasks.lock().unwrap().push(task);
            } else {
                drop(task);
                self.completed.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}
async fn drive<F: Future>(executor: &ManualExecutor, future: F) -> F::Output {
    let mut future = Box::pin(future);
    tokio::time::timeout(
        WATCHDOG,
        std::future::poll_fn(|cx| {
            executor.poll_once(cx);
            match future.as_mut().poll(cx) {
                Poll::Ready(value) => Poll::Ready(value),
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }),
    )
    .await
    .expect("actual connection fixture must make bounded protocol progress")
}
async fn drain(executor: &ManualExecutor) {
    drive(
        executor,
        std::future::poll_fn(|_| {
            if executor.tasks.lock().unwrap().is_empty() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await;
}
struct ExitIo {
    socket: Option<TcpStream>,
    exit: Option<oneshot::Sender<()>>,
}
impl AsyncRead for ExitIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.socket.as_mut().unwrap()).poll_read(cx, buf)
    }
}
impl AsyncWrite for ExitIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.socket.as_mut().unwrap()).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.socket.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.socket.as_mut().unwrap()).poll_shutdown(cx)
    }
}
impl Drop for ExitIo {
    fn drop(&mut self) {
        drop(self.socket.take());
        if let Some(exit) = self.exit.take() {
            let _ = exit.send(());
        }
    }
}
struct Peer {
    close: oneshot::Sender<()>,
    join: JoinHandle<()>,
}
struct Server {
    endpoint: NativeEndpoint,
    accepted: Arc<AtomicUsize>,
    peers: mpsc::Receiver<Peer>,
    listener: JoinHandle<()>,
}
impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = accepted.clone();
        let (publish, peers) = mpsc::channel(2);
        let listener = tokio::spawn(async move {
            for number in 1..=2 {
                let (socket, _) = listener.accept().await.unwrap();
                counted.fetch_add(1, Ordering::SeqCst);
                let (close, mut closing) = oneshot::channel();
                let join = tokio::spawn(async move {
                    let mut connection = h2::server::handshake(socket).await.unwrap();
                    let (request, mut response) = connection.accept().await.unwrap().unwrap();
                    drop(request);
                    let mut stream = response.send_response(Response::new(()), false).unwrap();
                    let payload = if number == 1 {
                        b"first original DATA".as_slice()
                    } else {
                        b"second original DATA".as_slice()
                    };
                    stream.send_data(Bytes::from_static(payload), true).unwrap();
                    tokio::select! {
                        closed = &mut closing => closed.unwrap(),
                        request = connection.accept() => assert!(request.is_none(), "one actual request per connection"),
                    }
                    drop(stream);
                    drop(connection);
                });
                publish.send(Peer { close, join }).await.unwrap();
            }
        });
        Self {
            endpoint,
            accepted,
            peers,
            listener,
        }
    }
    async fn next_peer(&mut self) -> Peer {
        tokio::time::timeout(WATCHDOG, self.peers.recv())
            .await
            .unwrap()
            .unwrap()
    }
}
fn stock() -> (
    crate::BackendDataRuntime,
    NativeTransportCapacityFactory,
    Arc<ResultRetainedBudget>,
    usize,
) {
    let bytes = NativeTransportCapacityFactory::allocation_capacity_bound().unwrap();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
    let factory = NativeTransportCapacityFactory::try_new(budget.clone()).unwrap();
    let runtime = test_backend_data_runtime()
        .with_transport_capacity(factory.clone())
        .unwrap();
    (runtime, factory, budget, bytes)
}
fn held(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    assert!(matches!(
        budget.try_reserve_process(bytes).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn returned(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("all original stock owners must physically exit");
    };
    drop(credit);
}
fn request(method: NativeRpcMethod) -> Request<tonic::body::BoxBody> {
    Request::builder()
        .uri(method.contract().path)
        .body(tonic::body::empty_body())
        .unwrap()
}
async fn data(executor: &ManualExecutor, channel: &Channel, method: NativeRpcMethod) -> Bytes {
    let mut response = drive(executor, channel.clone().oneshot(request(method)))
        .await
        .unwrap();
    let frame = drive(
        executor,
        std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)),
    )
    .await
    .unwrap()
    .unwrap();
    frame.into_data().unwrap()
}
fn io_kind(mut cause: &(dyn std::error::Error + 'static)) -> io::ErrorKind {
    loop {
        if let Some(error) = cause.downcast_ref::<io::Error>() {
            return error.kind();
        }
        cause = cause
            .source()
            .expect("actual admission error must preserve its IO cause");
    }
}

#[tokio::test]
async fn actual_same_channel_internal_reconnect_refuses_connecting_gate_before_new_tcp_dial() {
    let (runtime, factory, budget, bytes) = stock();
    let mut server = Server::start().await;
    let method = NativeRpcMethod::ExchangeUnary;
    let key = NativeChannelKey::new(
        Some(BackendProcessId::new_v7()),
        server.endpoint.clone(),
        method,
    )
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let exits = Arc::new(Mutex::new(VecDeque::new()));
    // Endpoint's real URI is used by TcpStream; no fake Channel is constructed.
    let endpoint = capacity_endpoint_for_key(&runtime, &key, TransportClass::Data).unwrap();
    let uri: Uri = endpoint.uri().clone();
    let host = uri.host().unwrap().to_owned();
    let port = uri.port_u16().unwrap();
    let counted = calls.clone();
    let traced = exits.clone();
    let connector = service_fn(move |_: Uri| {
        counted.fetch_add(1, Ordering::SeqCst);
        let (exit, observed) = oneshot::channel();
        traced.lock().unwrap().push_back(observed);
        let host = host.clone();
        async move {
            let socket = TcpStream::connect((host.as_str(), port)).await?;
            Ok::<_, io::Error>(TokioIo::new(ExitIo {
                socket: Some(socket),
                exit: Some(exit),
            }))
        }
    });
    let executor = ManualExecutor::default();
    let endpoint = endpoint.executor(executor.clone());
    let channel = endpoint.connect_with_connector(connector).await.unwrap();
    let first_peer = server.next_peer().await;
    let first = data(&executor, &channel, method).await;
    assert_eq!(first.as_ref(), b"first original DATA");
    let alias = first.clone();
    drop(first);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
    // Explicit direct-lifecycle kernel preparation: no socket or H2 proof is
    // claimed for this unbound configuration occupying Connecting=1.
    let blocker = factory
        .try_config_for_key(TransportClass::Data, key.inline_identity().unwrap())
        .unwrap();
    first_peer.close.send(()).unwrap();
    tokio::time::timeout(WATCHDOG, first_peer.join)
        .await
        .unwrap()
        .unwrap();
    let first_exit = exits.lock().unwrap().pop_front().unwrap();
    drive(&executor, first_exit).await.unwrap();
    // Conn/Pipe/Send now use their original typed dispatcher. Only the still
    // live Channel buffer worker belongs to this selected legacy executor.
    assert_eq!(executor.completed.load(Ordering::SeqCst), 0);
    assert_eq!(executor.tasks.lock().unwrap().len(), 1);
    let refused = drive(&executor, channel.clone().oneshot(request(method)))
        .await
        .unwrap_err();
    assert_eq!(io_kind(&refused), io::ErrorKind::WouldBlock);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "same Channel's internal factory must refuse before connector.call"
    );
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(alias.as_ref(), b"first original DATA");
    held(&budget, bytes);
    drop(blocker);
    // Retry on the SAME Channel, with no cache lookup/miss between requests.
    let second = data(&executor, &channel, method).await;
    let second_peer = server.next_peer().await;
    assert_eq!(second.as_ref(), b"second original DATA");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(server.accepted.load(Ordering::SeqCst), 2);
    drop(alias);
    second_peer.close.send(()).unwrap();
    tokio::time::timeout(WATCHDOG, second_peer.join)
        .await
        .unwrap()
        .unwrap();
    let second_exit = exits.lock().unwrap().pop_front().unwrap();
    drive(&executor, second_exit).await.unwrap();
    drop(second);
    drop(channel);
    drop(endpoint);
    drain(&executor).await;
    tokio::time::timeout(WATCHDOG, server.listener)
        .await
        .unwrap()
        .unwrap();
    drop(runtime);
    drop(factory);
    // IO exit precedes the separately scheduled live driver's final Cell exit.
    // Observe actual original-credit reuse, not the manual protocol queue alone.
    tokio::time::timeout(WATCHDOG, async {
        loop {
            if let ResultWriteAdmission::Granted(credit) =
                budget.try_reserve_process(bytes).unwrap()
            {
                drop(credit);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual driver and original stock must physically exit");
    returned(&budget, bytes);
}

#[tokio::test]
async fn exact_method_connecting_kernel_position_refuses_before_actual_connector_call() {
    let (runtime, factory, budget, bytes) = stock();
    let process = BackendProcessId::new_v7();
    let endpoint = NativeEndpoint::from_host_port("127.0.0.1", 1).unwrap();
    for (peer, method) in [
        (Some(process), NativeRpcMethod::ExchangeUnary),
        (
            Some(process),
            NativeRpcMethod::TransmitRuntimeFilterEnvelope,
        ),
        (None, NativeRpcMethod::AnnounceBackend),
    ] {
        let key = NativeChannelKey::new(peer, endpoint.clone(), method).unwrap();
        // Kernel-only preparation, deliberately not an actual IO connection.
        let blocker = factory
            .try_config_for_key(TransportClass::Data, key.inline_identity().unwrap())
            .unwrap();
        let capacity = capacity_endpoint_for_key(&runtime, &key, TransportClass::Data).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let connector = service_fn(move |_: Uri| {
            counted.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<TokioIo<TcpStream>, _>(io::Error::from(io::ErrorKind::ConnectionRefused))
            }
        });
        let error = capacity
            .connect_with_connector(connector)
            .await
            .unwrap_err();
        assert_eq!(io_kind(&error), io::ErrorKind::WouldBlock);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(blocker);
        drop(capacity);
    }
    held(&budget, bytes);
    drop(runtime);
    drop(factory);
    returned(&budget, bytes);
}

#[tokio::test]
async fn production_cache_miss_uses_same_key_gate_before_tcp_and_recovers_failed_leader() {
    let (runtime, factory, budget, bytes) = stock();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
    let method = NativeRpcMethod::ExchangeUnary;
    let key = NativeChannelKey::new(Some(BackendProcessId::new_v7()), endpoint, method).unwrap();
    // Direct original config is only a kernel Connecting position. All IO in
    // this test occurs through production get_or_create_channel, not this config.
    let blocker = factory
        .try_config_for_key(TransportClass::Data, key.inline_identity().unwrap())
        .unwrap();
    let refused = tokio::time::timeout(
        WATCHDOG,
        super::get_or_create_channel(&runtime, key.clone()),
    )
    .await
    .unwrap()
    .unwrap_err();
    // Native's public String retains this transport stage but not the typed
    // nested IO cause. The other two cases assert actual WouldBlock directly.
    assert!(
        refused.starts_with("connect exchange endpoint failed:"),
        "must pass cache election and fail in its keyed transport factory: {refused}"
    );
    assert!(
        listener
            .poll_accept(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending(),
        "a refused production cache miss must not reach the real TCP listener"
    );
    drop(blocker);
    let accepted = Arc::new(AtomicUsize::new(0));
    let counted = accepted.clone();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        counted.fetch_add(1, Ordering::SeqCst);
        let mut connection = h2::server::handshake(socket).await.unwrap();
        let (request, mut response) = connection.accept().await.unwrap().unwrap();
        drop(request);
        let mut stream = response.send_response(Response::new(()), false).unwrap();
        stream
            .send_data(Bytes::from_static(b"production keyed original DATA"), true)
            .unwrap();
        drop(stream);
        // Drive the actual response and wait for the client to naturally close
        // after cache removal/last Channel Drop. No abort or sleep is an exit.
        assert!(connection.accept().await.is_none());
        drop(connection);
        drop(listener);
    });
    let channel = tokio::time::timeout(
        WATCHDOG,
        super::get_or_create_channel(&runtime, key.clone()),
    )
    .await
    .unwrap()
    .unwrap();
    let mut response = tokio::time::timeout(WATCHDOG, channel.clone().oneshot(request(method)))
        .await
        .unwrap()
        .unwrap();
    let frame = tokio::time::timeout(
        WATCHDOG,
        std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let alias = frame.into_data().unwrap();
    assert_eq!(alias.as_ref(), b"production keyed original DATA");
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    drop(response);
    drop(runtime.channels().remove(&key));
    drop(channel);
    tokio::time::timeout(WATCHDOG, peer)
        .await
        .expect("actual peer must exit naturally after client channel closes")
        .unwrap();
    drop(runtime);
    drop(factory);
    held(&budget, bytes);
    assert_eq!(alias.as_ref(), b"production keyed original DATA");
    drop(alias);
    tokio::time::timeout(WATCHDOG, async {
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
    .await
    .expect("original stock must return after the real final field/pool/task exits");
}
