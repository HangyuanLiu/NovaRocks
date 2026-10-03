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

//! Actual cache-key routing through TCP, Hyper H2 and a raw Tonic Channel.
//! Ordinary fixtures preserve the no-capacity path; funded fixtures install
//! the real original transport factory and its bounded cache. They do not prove
//! authentication, per-lane admission limits or a whole-connection envelope.

use super::{NativeChannelKey, get_or_create_channel};
use crate::backend_test_support::test_backend_data_runtime;
use crate::native_transport_capacity::NativeTransportCapacityFactory;
use hyper::http::{Request, Response};
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::ResultWriteAdmission;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_types::{BackendProcessId, NativeEndpoint};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::convert::Infallible;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::{Barrier, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tower::ServiceExt;

const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
struct JoinedExecutor(Arc<Mutex<Vec<JoinHandle<()>>>>);
impl<F> Executor<F> for JoinedExecutor
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, future: F) {
        self.0.lock().unwrap().push(tokio::spawn(async move {
            let _ = future.await;
        }));
    }
}
impl JoinedExecutor {
    async fn join_naturally(&self) {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        assert!(
            !tasks.is_empty(),
            "actual paused socket task must be present"
        );
        for task in tasks {
            tokio::time::timeout(WATCHDOG, task)
                .await
                .expect("cancelled dial must close its real peer socket")
                .expect("peer socket task must exit without abort");
        }
    }
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
                let _ = tokio::time::timeout(WATCHDOG, task).await.unwrap();
            }
        }
    }
}

struct FirstBootstrapPause {
    ready: oneshot::Sender<()>,
    exited: oneshot::Sender<()>,
}

struct Server {
    endpoint: NativeEndpoint,
    accepted: Arc<AtomicUsize>,
    stop: CancellationToken,
    accept_task: JoinHandle<()>,
    executor: JoinedExecutor,
}
impl Server {
    async fn start(first_id: usize) -> Self {
        Self::start_inner(first_id, None).await
    }

    async fn start_paused(first_id: usize) -> (Self, oneshot::Receiver<()>, oneshot::Receiver<()>) {
        let (ready, seen) = oneshot::channel();
        let (exited, closed) = oneshot::channel();
        (
            Self::start_inner(first_id, Some(FirstBootstrapPause { ready, exited })).await,
            seen,
            closed,
        )
    }

    async fn start_inner(first_id: usize, mut pause: Option<FirstBootstrapPause>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
        let accepted = Arc::new(AtomicUsize::new(0));
        let observed = accepted.clone();
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let executor = JoinedExecutor::default();
        let serving = executor.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                let (io, _) = tokio::select! {
                    biased;
                    _ = stopping.cancelled() => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                let id = first_id + observed.fetch_add(1, Ordering::SeqCst);
                if let Some(pause) = pause.take() {
                    serving.execute(async move {
                        let mut io = io;
                        let mut preface = [0; 24];
                        io.read_exact(&mut preface).await.unwrap();
                        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
                        let mut header = [0; 9];
                        io.read_exact(&mut header).await.unwrap();
                        assert_eq!((header[3], header[4]), (4, 0));
                        let size =
                            u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
                        let mut payload = vec![0; size];
                        io.read_exact(&mut payload).await.unwrap();
                        pause.ready.send(()).unwrap();
                        // Do not send SETTINGS. Observe actual TCP EOF after
                        // cancellation, then physically drop this peer socket.
                        let mut remaining = [0; 1024];
                        while io.read(&mut remaining).await.unwrap() != 0 {}
                        drop(io);
                        pause.exited.send(()).unwrap();
                    });
                    continue;
                }
                let builder = hyper::server::conn::http2::Builder::new(serving.clone());
                let service = service_fn(move |_: Request<hyper::body::Incoming>| async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .header("x-connection-id", id.to_string())
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    )
                });
                serving.execute(async move {
                    let _ = builder.serve_connection(TokioIo::new(io), service).await;
                });
            }
        });
        Self {
            endpoint,
            accepted,
            stop,
            accept_task,
            executor,
        }
    }

    async fn shutdown(self) {
        self.stop.cancel();
        tokio::time::timeout(WATCHDOG, self.accept_task)
            .await
            .unwrap()
            .unwrap();
        // Includes the actual connection future and Hyper's executor children.
        self.executor.stop_and_join().await;
    }
}

async fn connection_id(channel: Channel, method: NativeRpcMethod) -> usize {
    let request = Request::builder()
        .uri(method.contract().path)
        .body(tonic::body::empty_body())
        .unwrap();
    let response = tokio::time::timeout(WATCHDOG, channel.oneshot(request))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    response.headers()["x-connection-id"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn actual_cached_channels_separate_process_endpoint_and_manifest_method() {
    let runtime = test_backend_data_runtime();
    let first = Server::start(1).await;
    let other = Server::start(100).await;
    let old = BackendProcessId::new_v7();
    let replacement = BackendProcessId::new_v7();
    let exchange = NativeRpcMethod::ExchangeUnary;
    let filter = NativeRpcMethod::TransmitRuntimeFilterEnvelope;
    let old_exchange = NativeChannelKey::new(Some(old), first.endpoint.clone(), exchange).unwrap();
    let new_exchange =
        NativeChannelKey::new(Some(replacement), first.endpoint.clone(), exchange).unwrap();
    let old_filter = NativeChannelKey::new(Some(old), first.endpoint.clone(), filter).unwrap();
    let other_endpoint =
        NativeChannelKey::new(Some(old), other.endpoint.clone(), exchange).unwrap();

    let original = get_or_create_channel(&runtime, old_exchange.clone())
        .await
        .unwrap();
    let original_id = connection_id(original.clone(), exchange).await;
    let replaced = get_or_create_channel(&runtime, new_exchange.clone())
        .await
        .unwrap();
    let replacement_id = connection_id(replaced.clone(), exchange).await;
    assert_ne!(original_id, replacement_id);
    let filtering = get_or_create_channel(&runtime, old_filter.clone())
        .await
        .unwrap();
    let filter_id = connection_id(filtering.clone(), filter).await;
    assert_ne!(original_id, filter_id);
    assert_ne!(replacement_id, filter_id);

    let repeated = get_or_create_channel(&runtime, old_exchange.clone())
        .await
        .unwrap();
    assert_eq!(connection_id(repeated.clone(), exchange).await, original_id);
    assert_eq!(first.accepted.load(Ordering::SeqCst), 3);

    let elsewhere = get_or_create_channel(&runtime, other_endpoint.clone())
        .await
        .unwrap();
    assert_eq!(connection_id(elsewhere.clone(), exchange).await, 100);
    assert_eq!(other.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(connection_id(original.clone(), exchange).await, original_id);

    // Remove only this fixture's keys from the shared ordinary test runtime.
    // No global clear can invalidate another test's unrelated channel.
    for key in [old_exchange, new_exchange, old_filter, other_endpoint] {
        runtime.channels().remove(&key);
    }
    drop(original);
    drop(replaced);
    drop(filtering);
    drop(repeated);
    drop(elsewhere);
    first.shutdown().await;
    other.shutdown().await;
}

#[test]
fn channel_key_refuses_missing_or_foreign_peer_domains_and_frontend_methods() {
    let endpoint = NativeEndpoint::from_host_port("127.0.0.1", 9010).unwrap();
    let process = BackendProcessId::new_v7();
    assert!(NativeChannelKey::new(None, endpoint.clone(), NativeRpcMethod::ExchangeUnary).is_err());
    assert!(
        NativeChannelKey::new(
            None,
            endpoint.clone(),
            NativeRpcMethod::TransmitRuntimeFilterEnvelope,
        )
        .is_err()
    );
    assert!(
        NativeChannelKey::new(
            Some(process),
            endpoint.clone(),
            NativeRpcMethod::AnnounceBackend,
        )
        .is_err()
    );
    for method in [
        NativeRpcMethod::FetchTaskResult,
        NativeRpcMethod::ApplyTaskOperations,
        NativeRpcMethod::Heartbeat,
        NativeRpcMethod::RetiredExchange,
    ] {
        assert!(NativeChannelKey::new(Some(process), endpoint.clone(), method).is_err());
        assert!(NativeChannelKey::new(None, endpoint.clone(), method).is_err());
    }
    assert!(NativeChannelKey::new(None, endpoint, NativeRpcMethod::AnnounceBackend).is_ok());
}

struct FundedRuntime {
    runtime: crate::BackendDataRuntime,
    factory: NativeTransportCapacityFactory,
    budget: Arc<ResultRetainedBudget>,
    bytes: usize,
}
impl FundedRuntime {
    fn new() -> Self {
        let bytes = NativeTransportCapacityFactory::allocation_capacity_bound().unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
        let factory = NativeTransportCapacityFactory::try_new(budget.clone()).unwrap();
        let runtime = test_backend_data_runtime()
            .with_transport_capacity(factory.clone())
            .unwrap();
        Self {
            runtime,
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
    async fn finish(self) {
        self.finish_with_alias(None).await;
    }
    async fn finish_with_alias(self, escaped: Option<Channel>) {
        self.held();
        let Self {
            runtime,
            factory,
            budget,
            bytes,
        } = self;
        drop(runtime);
        drop(factory);
        if escaped.is_some() {
            assert!(
                matches!(
                    budget.try_reserve_process(bytes).unwrap(),
                    ResultWriteAdmission::Blocked
                ),
                "escaped real Channel must retain its original stock after all public runtime/factory handles exit"
            );
        }
        drop(escaped);
        // Observe the original release, not logical cache eviction or a delay.
        // Client background tasks exit naturally when their last Channel/IO
        // aliases exit; the stock credit's physical owner is the oracle here.
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
        .expect("actual stock and all original transport aliases must exit");
    }
}

#[tokio::test]
async fn funded_singleflight_nine_real_callers_share_one_dial_and_exact_peer_key() {
    let funded = FundedRuntime::new();
    let first = Server::start(1).await;
    let other = Server::start(100).await;
    let old = BackendProcessId::new_v7();
    let replacement = BackendProcessId::new_v7();
    let exchange = NativeRpcMethod::ExchangeUnary;
    let filter = NativeRpcMethod::TransmitRuntimeFilterEnvelope;
    let key = NativeChannelKey::new(Some(old), first.endpoint.clone(), exchange).unwrap();
    let gate = Arc::new(Barrier::new(10));
    let tasks: Vec<_> = (0..9)
        .map(|_| {
            let runtime = funded.runtime.clone();
            let key = key.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                gate.wait().await;
                let channel = get_or_create_channel(&runtime, key).await.unwrap();
                let id = connection_id(channel.clone(), exchange).await;
                (channel, id)
            })
        })
        .collect();
    gate.wait().await;
    let mut aliases = Vec::new();
    for task in tasks {
        let (channel, id) = tokio::time::timeout(WATCHDOG, task).await.unwrap().unwrap();
        assert_eq!(id, 1);
        aliases.push(channel);
    }
    assert_eq!(first.accepted.load(Ordering::SeqCst), 1);
    let replacement_key =
        NativeChannelKey::new(Some(replacement), first.endpoint.clone(), exchange).unwrap();
    let filter_key = NativeChannelKey::new(Some(old), first.endpoint.clone(), filter).unwrap();
    let elsewhere_key = NativeChannelKey::new(Some(old), other.endpoint.clone(), exchange).unwrap();
    let replaced = get_or_create_channel(&funded.runtime, replacement_key.clone())
        .await
        .unwrap();
    let filtering = get_or_create_channel(&funded.runtime, filter_key.clone())
        .await
        .unwrap();
    let elsewhere = get_or_create_channel(&funded.runtime, elsewhere_key.clone())
        .await
        .unwrap();
    assert_eq!(connection_id(replaced.clone(), exchange).await, 2);
    assert_eq!(connection_id(filtering.clone(), filter).await, 3);
    assert_eq!(connection_id(elsewhere.clone(), exchange).await, 100);
    let repeated = get_or_create_channel(&funded.runtime, key.clone())
        .await
        .unwrap();
    assert_eq!(connection_id(repeated.clone(), exchange).await, 1);
    assert_eq!(first.accepted.load(Ordering::SeqCst), 3);
    assert_eq!(other.accepted.load(Ordering::SeqCst), 1);
    for key in [key, replacement_key, filter_key, elsewhere_key] {
        drop(funded.runtime.channels().remove(&key));
    }
    // An escaped Channel survives cache removal and still has the original
    // startup family. There is no independent per-key/per-lane budget here.
    funded.held();
    let escaped = aliases.pop().unwrap();
    drop(aliases);
    drop(replaced);
    drop(filtering);
    drop(elsewhere);
    drop(repeated);
    first.shutdown().await;
    other.shutdown().await;
    funded.finish_with_alias(Some(escaped)).await;
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);
impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn funded_singleflight_eight_joiners_ninth_refusal_and_leader_cancel_recover_real_io() {
    let funded = FundedRuntime::new();
    let (server, ready, closed) = Server::start_paused(1).await;
    let exchange = NativeRpcMethod::ExchangeUnary;
    let key = NativeChannelKey::new(
        Some(BackendProcessId::new_v7()),
        server.endpoint.clone(),
        exchange,
    )
    .unwrap();
    let mut leader = Box::pin(get_or_create_channel(&funded.runtime, key.clone()));
    tokio::time::timeout(WATCHDOG, async {
        tokio::select! {
            outcome = leader.as_mut() => panic!("paused real initial SETTINGS dial must remain pending: {outcome:?}"),
            seen = ready => seen.unwrap(),
        }
    }).await.unwrap();
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
    let counters: Vec<_> = (0..8).map(|_| Arc::new(WakeCounter::default())).collect();
    let mut joiners: Vec<_> = (0..8)
        .map(|_| Box::pin(get_or_create_channel(&funded.runtime, key.clone())))
        .collect();
    for (joiner, counter) in joiners.iter_mut().zip(&counters) {
        let waker = Waker::from(counter.clone());
        assert!(
            joiner
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    }
    let refused = get_or_create_channel(&funded.runtime, key.clone())
        .await
        .unwrap_err();
    assert!(refused.starts_with("Native channel election refused:"));
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
    drop(leader);
    for (joiner, counter) in joiners.iter_mut().zip(&counters) {
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "leader cancellation must wake each exact registered caller"
        );
        let waker = Waker::from(counter.clone());
        match joiner.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(Err(error)) => {
                assert!(error.starts_with("Native channel election refused:"))
            }
            other => panic!("registered joiner must observe cancellation, got {other:?}"),
        }
    }
    drop(joiners);
    tokio::time::timeout(WATCHDOG, closed)
        .await
        .unwrap()
        .unwrap();
    server.executor.join_naturally().await;
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
    funded.held();
    let recovered = get_or_create_channel(&funded.runtime, key.clone())
        .await
        .unwrap();
    assert_eq!(connection_id(recovered.clone(), exchange).await, 2);
    assert_eq!(server.accepted.load(Ordering::SeqCst), 2);
    let repeated = get_or_create_channel(&funded.runtime, key.clone())
        .await
        .unwrap();
    assert_eq!(connection_id(repeated.clone(), exchange).await, 2);
    assert_eq!(server.accepted.load(Ordering::SeqCst), 2);
    drop(funded.runtime.channels().remove(&key));
    drop(repeated);
    drop(recovered);
    server.shutdown().await;
    funded.finish().await;
}
