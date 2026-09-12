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

//! Native inbound RPC listener over a Server-resolved transport capability.

use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use axum::Router;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use novarocks_native_trust::{NativeIncomingAdapter, NativeServerAdmission, NativeTrust};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tonic::body::boxed;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tower::ServiceExt;

use crate::generated::nova_rocks_grpc_server::{NovaRocksGrpc, NovaRocksGrpcServer};

const GRPC_MAX_MESSAGE_BYTES: usize =
    novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES;

/// Instance-owned native gRPC listener lifecycle.
///
/// The role provides its generated service and owns every domain handler. This
/// adapter owns only the generic socket, authenticated transport, h2 serving,
/// shutdown, and supervision mechanics.
pub struct NativeRpcServerHandle {
    bound_addr: SocketAddr,
    shutdown_tx: Option<watch::Sender<bool>>,
    failure_rx: mpsc::Receiver<String>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Arc<AtomicBool>,
}

impl NativeRpcServerHandle {
    #[expect(
        clippy::too_many_arguments,
        reason = "role identity and its authentication metric remain explicit composition inputs"
    )]
    pub fn start<S, F, H>(
        host: &str,
        port: u16,
        service: S,
        native_trust: Arc<NativeTrust>,
        incoming_adapter: NativeIncomingAdapter,
        role_label: &'static str,
        thread_name: &'static str,
        on_authentication_failure: F,
        on_transport_handshake_failure: H,
    ) -> Result<Self, String>
    where
        S: NovaRocksGrpc + Clone + Send + Sync + 'static,
        F: Fn() + Send + Sync + 'static,
        H: Fn() + Send + Sync + 'static,
    {
        let address = (host, port)
            .to_socket_addrs()
            .map_err(|error| {
                format!("resolve native {role_label} gRPC address {host}:{port}: {error}")
            })?
            .next()
            .ok_or_else(|| {
                format!("resolve native {role_label} gRPC address {host}:{port}: no address")
            })?;
        let listener = TcpListener::bind(address)
            .map_err(|error| format!("bind native {role_label} gRPC address {address}: {error}"))?;
        listener.set_nonblocking(true).map_err(|error| {
            format!("set native {role_label} gRPC listener nonblocking: {error}")
        })?;
        let bound_addr = listener
            .local_addr()
            .map_err(|error| format!("read native {role_label} gRPC bound address: {error}"))?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let thread_stop_requested = Arc::clone(&stop_requested);
        let authentication_failure = Arc::new(on_authentication_failure);
        let transport_handshake_failure = Arc::new(on_transport_handshake_failure);
        let join_handle = std::thread::Builder::new()
            .name(thread_name.to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .worker_threads(8)
                        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
                        .build()
                        .map_err(|error| {
                            format!("build native {role_label} gRPC runtime: {error}")
                        })?;
                    runtime.block_on(async move {
                        let listener = TokioTcpListener::from_std(listener).map_err(|error| {
                            format!("create Tokio native {role_label} gRPC listener: {error}")
                        })?;
                        let service = NovaRocksGrpcServer::new(service)
                            .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                            .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                        let grpc_path = format!(
                            "/{}/*rest",
                            <NovaRocksGrpcServer<S> as NamedService>::NAME
                        );
                        let app = tower::ServiceExt::<axum::http::Request<axum::body::Body>>::map_response(
                            Router::new()
                                .route_service(&grpc_path, AxumGrpcService::new(service))
                                .fallback(grpc_unimplemented_fallback),
                            |response: axum::http::Response<axum::body::Body>| response.map(boxed),
                        );
                        let app = NativeListenerAuthService::new(
                            app,
                            native_trust.server_admission(),
                            authentication_failure,
                        );
                        serve_native_listener(
                            listener,
                            app,
                            incoming_adapter,
                            shutdown_rx,
                            transport_handshake_failure,
                        )
                        .await
                    })
                }));
                if thread_stop_requested.load(Ordering::Acquire) {
                    return;
                }
                let error = match outcome {
                    Ok(Ok(())) => format!("native {role_label} gRPC server exited unexpectedly"),
                    Ok(Err(error)) => error,
                    Err(payload) => payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|value| (*value).to_string()))
                        .unwrap_or_else(|| format!("native {role_label} gRPC server panicked")),
                };
                let _ = failure_tx.send(error);
            })
            .map_err(|error| format!("spawn native {role_label} gRPC server: {error}"))?;
        Ok(Self {
            bound_addr,
            shutdown_tx: Some(shutdown_tx),
            failure_rx,
            join_handle: Some(join_handle),
            stop_requested,
        })
    }

    pub const fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub fn poll_failure(&mut self) -> Result<Option<String>, String> {
        match self.failure_rx.try_recv() {
            Ok(error) => Ok(Some(error)),
            Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn stop(&mut self) -> Result<(), String> {
        self.stop_requested.store(true, Ordering::Release);
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .map_err(|_| "native gRPC server thread panicked".to_string())?;
        }
        Ok(())
    }
}

impl Drop for NativeRpcServerHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

async fn serve_native_listener<S>(
    listener: TokioTcpListener,
    app: S,
    incoming: NativeIncomingAdapter,
    mut shutdown_rx: watch::Receiver<bool>,
    on_transport_handshake_failure: Arc<dyn Fn() + Send + Sync>,
) -> Result<(), String>
where
    S: Service<
            axum::http::Request<axum::body::Body>,
            Response = axum::http::Response<tonic::body::BoxBody>,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|error| format!("accept native gRPC connection: {error}"))?;
                let app = app.clone();
                let incoming = incoming.clone();
                let on_transport_handshake_failure = Arc::clone(&on_transport_handshake_failure);
                tokio::spawn(async move {
                    let stream = match incoming.accept(stream).await {
                        Ok(stream) => stream,
                        Err(_) => {
                            on_transport_handshake_failure();
                            return;
                        }
                    };
                    let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                        let app = app.clone();
                        async move {
                            let response = app
                                .oneshot(request.map(axum::body::Body::new))
                                .await
                                .expect("Native route service is infallible");
                            Ok::<_, std::convert::Infallible>(response)
                        }
                    });
                    let _ = http2::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        }
    }
}

#[derive(Clone)]
struct NativeListenerAuthService<S> {
    admission: NativeServerAdmission,
    inner: S,
    on_authentication_failure: Arc<dyn Fn() + Send + Sync>,
}

impl<S> NativeListenerAuthService<S> {
    fn new(
        inner: S,
        admission: NativeServerAdmission,
        on_authentication_failure: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            admission,
            inner,
            on_authentication_failure,
        }
    }
}

impl<S, Body> Service<axum::http::Request<Body>> for NativeListenerAuthService<S>
where
    S: Service<axum::http::Request<Body>, Response = axum::http::Response<tonic::body::BoxBody>>
        + Send,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = axum::http::Response<tonic::body::BoxBody>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: axum::http::Request<Body>) -> Self::Future {
        if self.admission.admit_headers(request.headers()).is_err() {
            (self.on_authentication_failure)();
            return Box::pin(async {
                Ok(
                    tonic::Status::unauthenticated("native caller authentication failed")
                        .into_http(),
                )
            });
        }
        Box::pin(self.inner.call(request))
    }
}

async fn grpc_unimplemented_fallback() -> impl IntoResponse {
    (
        StatusCode::OK,
        [
            (tonic::Status::GRPC_STATUS, HeaderValue::from_static("12")),
            (
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/grpc"),
            ),
        ],
    )
}

#[derive(Clone)]
struct AxumGrpcService<S> {
    inner: S,
}

impl<S> AxumGrpcService<S> {
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<axum::http::Request<axum::body::Body>> for AxumGrpcService<S>
where
    S: Service<
            axum::http::Request<tonic::body::BoxBody>,
            Response = axum::http::Response<tonic::body::BoxBody>,
            Error = std::convert::Infallible,
        > + Clone,
{
    type Response = axum::http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: axum::http::Request<axum::body::Body>) -> Self::Future {
        self.inner.call(request.map(boxed))
    }
}
