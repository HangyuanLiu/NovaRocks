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
//! This fixture uses the ordinary no-capacity Backend test runtime. It does not
//! prove authentication, original funding or per-lane admission limits.

use super::{NativeChannelKey, get_or_create_channel};
use crate::backend_test_support::test_backend_data_runtime;
use hyper::http::{Request, Response};
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_types::{BackendProcessId, NativeEndpoint};
use std::convert::Infallible;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
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

struct Server {
    endpoint: NativeEndpoint,
    accepted: Arc<AtomicUsize>,
    stop: CancellationToken,
    accept_task: JoinHandle<()>,
    executor: JoinedExecutor,
}
impl Server {
    async fn start(first_id: usize) -> Self {
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
        runtime.channels().lock().unwrap().remove(&key);
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
