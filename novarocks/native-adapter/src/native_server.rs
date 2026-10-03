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

use std::future::Future;
use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::Duration;

#[cfg(test)]
#[path = "native_server_capacity_tests.rs"]
mod capacity_tests;

#[cfg(test)]
#[path = "native_initial_settings_tests.rs"]
mod initial_settings_tests;

#[cfg(test)]
#[path = "native_acquisition_tests.rs"]
mod acquisition_tests;

use axum::Router;
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
use crate::native_ingress::NativeIngressService;
use crate::native_response::respond_from_request;
use crate::native_transport_capacity::{
    NativeTransportCapacityFactory, TransportClass, configure_server,
};

/// How long a stopping listener lets its already-accepted connections finish.
///
/// This covers answering a request that was already dispatched -- the root
/// result poll waits at most 200ms, and task operations answer immediately.
/// It deliberately does not cover draining a long-lived stream: `Exchange` is
/// a bidirectional data pipe that GOAWAY does not end, and cutting it is what
/// cancelling it means. Anything still open when this expires is cancelled,
/// so the bound is what keeps one open stream from holding a stopping process.
const SHUTDOWN_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// How long to wait after a failed `accept` before trying again, so a full
/// descriptor table cannot turn this loop into a spin.
const ACCEPT_ERROR_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

/// How many `accept` calls may fail in a row before the listener is reported
/// as broken. One transient refusal resets the count; a listener that can no
/// longer accept anything still reaches supervision.
const MAX_CONSECUTIVE_ACCEPT_ERRORS: u32 = 64;

const GRPC_MAX_MESSAGE_BYTES: usize =
    novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES;
pub const NATIVE_MAX_MESSAGE_BYTES: usize = GRPC_MAX_MESSAGE_BYTES;

/// Server-validated limits for one Native listener. The listener owns only
/// local transport and scheduling capacity, not FE query admission or Worker
/// context reservations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeIngressConfig {
    pub worker_threads: usize,
    pub max_blocking_threads: usize,
    pub ordinary_running: usize,
    pub ordinary_waiting: usize,
    pub control_worker_threads: usize,
    pub control_running: usize,
    pub control_waiting: usize,
    pub ordinary_request_max_bytes: usize,
    pub ordinary_response_max_bytes: usize,
    pub control_request_max_bytes: usize,
    pub control_response_max_bytes: usize,
}

impl Default for NativeIngressConfig {
    fn default() -> Self {
        Self {
            worker_threads: 8,
            max_blocking_threads: 64,
            ordinary_running: 8,
            ordinary_waiting: 8,
            control_worker_threads: 4,
            control_running: 4,
            control_waiting: 4,
            ordinary_request_max_bytes: GRPC_MAX_MESSAGE_BYTES,
            ordinary_response_max_bytes: GRPC_MAX_MESSAGE_BYTES,
            control_request_max_bytes: 1024 * 1024,
            control_response_max_bytes: GRPC_MAX_MESSAGE_BYTES,
        }
    }
}

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
        ingress_config: NativeIngressConfig,
    ) -> Result<Self, String>
    where
        S: NovaRocksGrpc + Clone + Send + Sync + 'static,
        F: Fn() + Send + Sync + 'static,
        H: Fn() + Send + Sync + 'static,
    {
        Self::start_inner(
            host,
            port,
            service,
            native_trust,
            incoming_adapter,
            role_label,
            thread_name,
            on_authentication_failure,
            on_transport_handshake_failure,
            ingress_config,
            None,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "role identity and its authentication metric remain explicit composition inputs"
    )]
    pub(crate) fn start_with_transport_capacity<S, F, H>(
        host: &str,
        port: u16,
        service: S,
        native_trust: Arc<NativeTrust>,
        incoming_adapter: NativeIncomingAdapter,
        role_label: &'static str,
        thread_name: &'static str,
        on_authentication_failure: F,
        on_transport_handshake_failure: H,
        ingress_config: NativeIngressConfig,
        transport_capacity: NativeTransportCapacityFactory,
        transport_class: TransportClass,
    ) -> Result<Self, String>
    where
        S: NovaRocksGrpc + Clone + Send + Sync + 'static,
        F: Fn() + Send + Sync + 'static,
        H: Fn() + Send + Sync + 'static,
    {
        Self::start_inner(
            host,
            port,
            service,
            native_trust,
            incoming_adapter,
            role_label,
            thread_name,
            on_authentication_failure,
            on_transport_handshake_failure,
            ingress_config,
            Some((transport_capacity, transport_class)),
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "role identity and its authentication metric remain explicit composition inputs"
    )]
    fn start_inner<S, F, H>(
        host: &str,
        port: u16,
        service: S,
        native_trust: Arc<NativeTrust>,
        incoming_adapter: NativeIncomingAdapter,
        role_label: &'static str,
        thread_name: &'static str,
        on_authentication_failure: F,
        on_transport_handshake_failure: H,
        ingress_config: NativeIngressConfig,
        transport_capacity: Option<(NativeTransportCapacityFactory, TransportClass)>,
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
                        .worker_threads(ingress_config.worker_threads)
                        .max_blocking_threads(ingress_config.max_blocking_threads)
                        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
                        .build()
                        .map_err(|error| {
                            format!("build native {role_label} gRPC runtime: {error}")
                        })?;
                    let outcome = runtime.block_on(async move {
                        let listener = TokioTcpListener::from_std(listener).map_err(|error| {
                            format!("create Tokio native {role_label} gRPC listener: {error}")
                        })?;
                        // FE consumes listener runtime sizing only. Its report
                        // service keeps the native baseline message limits;
                        // BE method limits belong solely to task ingress.
                        let (ordinary_request_limit, ordinary_response_limit, control_request_limit, control_response_limit) =
                            if role_label == "backend" {
                                (
                                    ingress_config.ordinary_request_max_bytes,
                                    ingress_config.ordinary_response_max_bytes,
                                    ingress_config.control_request_max_bytes,
                                    ingress_config.control_response_max_bytes,
                                )
                            } else {
                                (GRPC_MAX_MESSAGE_BYTES, GRPC_MAX_MESSAGE_BYTES, GRPC_MAX_MESSAGE_BYTES, GRPC_MAX_MESSAGE_BYTES)
                            };
                        let ordinary_service = NovaRocksGrpcServer::new(service.clone())
                            .max_decoding_message_size(ordinary_request_limit)
                            .max_encoding_message_size(ordinary_response_limit);
                        let control_service = NovaRocksGrpcServer::new(service)
                            .max_decoding_message_size(control_request_limit)
                            .max_encoding_message_size(control_response_limit);
                        let grpc_path = format!(
                            "/{}/*rest",
                            <NovaRocksGrpcServer<S> as NamedService>::NAME
                        );
                        let control_path = format!(
                            "/{}/ApplyTaskControlOperations",
                            <NovaRocksGrpcServer<S> as NamedService>::NAME
                        );
                        let app = tower::ServiceExt::<axum::http::Request<axum::body::Body>>::map_response(
                            Router::new()
                                .route_service(&control_path, AxumGrpcService::new(control_service))
                                .route_service(&grpc_path, AxumGrpcService::new(ordinary_service))
                                .fallback(grpc_unimplemented_fallback),
                            |response: axum::http::Response<axum::body::Body>| response.map(boxed),
                        );
                        let app = NativeIngressService::new(
                            app,
                            ingress_config,
                            <NovaRocksGrpcServer<S> as NamedService>::NAME,
                            role_label == "backend",
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
                            role_label,
                            transport_capacity,
                        )
                        .await
                    });
                    // Dropping this runtime cancels every connection task it
                    // still owns. Each one reports itself on the way out.
                    drop(runtime);
                    tracing::debug!(
                        role = role_label,
                        "native listener runtime dropped"
                    );
                    outcome
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

#[expect(
    clippy::large_enum_variant,
    reason = "The existing connection task retains acquisition output inline without another backing allocation."
)]
enum AcquiredConnection<C> {
    Installed(C, Option<tonic::transport::Http2ConnectionConfig>),
    Finished(Result<(), hyper::Error>, bool),
}

async fn serve_native_listener<S>(
    listener: TokioTcpListener,
    app: S,
    incoming: NativeIncomingAdapter,
    mut shutdown_rx: watch::Receiver<bool>,
    on_transport_handshake_failure: Arc<dyn Fn() + Send + Sync>,
    role_label: &'static str,
    transport_capacity: Option<(NativeTransportCapacityFactory, TransportClass)>,
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
    // Per listener rather than per process: one all-in-one process hosts both
    // role listeners, and a shared counter would report the other one's work.
    let live_connections = Arc::new(AtomicU64::new(0));
    let next_connection_id = Arc::new(AtomicU64::new(1));
    // Every connection holds a receiver for as long as it is serving, so
    // sending on this channel is how the listener asks them all to wind down
    // and `closed()` is how it learns that they have.
    let (drain_tx, drain_rx) = watch::channel(());
    let mut consecutive_accept_errors = 0_u32;
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Err(error) => {
                        // One refused connection is not a broken listener. The
                        // previous behaviour returned here, which dropped this
                        // thread's runtime and cancelled every other live
                        // connection on this process -- their peers saw a
                        // socket close with no GOAWAY. A listener that is
                        // really gone still surfaces, through the consecutive
                        // count below, so supervision can act on it.
                        consecutive_accept_errors += 1;
                        tracing::warn!(
                            role = role_label,
                            %error,
                            consecutive_accept_errors,
                            "native listener could not accept a connection"
                        );
                        if consecutive_accept_errors >= MAX_CONSECUTIVE_ACCEPT_ERRORS {
                            return Err(format!(
                                "accept native gRPC connection failed                                  {consecutive_accept_errors} times in a row: {error}"
                            ));
                        }
                        // Descriptor exhaustion returns immediately and would
                        // otherwise spin this loop against a full table.
                        tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                        continue;
                    }
                    Ok(accepted) => accepted,
                };
                let accepted_at = std::time::Instant::now();
                consecutive_accept_errors = 0;
                let mut builder = http2::Builder::new(TokioExecutor::new());
                let initial_settings_deadline = transport_capacity.as_ref().map(|_| {
                    accepted_at + Duration::from_millis(
                        novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1.transport_handshake_deadline_ms,
                    )
                });
                // Claim the shared process acquisition position before cloning
                // application owners, creating tasks or performing TLS/H2 IO.
                let mut capacity = match &transport_capacity {
                    Some((factory, class)) => match factory.try_config(*class) {
                        Ok(config) => {
                            if configure_server(&mut builder, &config).is_err() {
                                drop(stream);
                                continue;
                            }
                            builder.initial_settings_deadline(initial_settings_deadline.expect("funded listener has an absolute bootstrap deadline"));
                            Some(config)
                        }
                        Err(_) => {
                            // Refusal has no pending queue and no protocol IO.
                            drop(stream);
                            continue;
                        }
                    },
                    None => None,
                };
                let acquisition_owner = capacity.as_mut().and_then(|config| config.acquisition_owner.take());
                let app = app.clone();
                let incoming = incoming.clone();
                let on_transport_handshake_failure = Arc::clone(&on_transport_handshake_failure);
                let mut served = ServedConnection::open(
                    role_label, peer, &next_connection_id, &live_connections,
                );
                let mut drain = drain_rx.clone();
                // Construct the ordered scope before spawn: an unpolled task
                // cancellation must drop its captured socket/future before
                // returning the original acquisition position too.
                let acquisition = tonic::transport::ConnectionAcquisition::new(async move {
                    let bootstrap = async move {
                        if initial_settings_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                            return Err("Native acquisition deadline elapsed before first poll".to_owned());
                        }
                        let stream = incoming.accept(stream).await.map_err(|error| format!("{error:?}"))?;
                        if initial_settings_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                            drop(stream);
                            return Err("Native acquisition deadline elapsed during TLS".to_owned());
                        }
                        let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                            let app = app.clone();
                            async move {
                                let response = app.oneshot(request.map(axum::body::Body::new)).await
                                    .expect("Native route service is infallible");
                                Ok::<_, std::convert::Infallible>(response)
                            }
                        });
                        let mut connection = builder.serve_connection(TokioIo::new(stream), service);
                        if initial_settings_deadline.is_none() {
                            return Ok(AcquiredConnection::Installed(connection, capacity));
                        }
                        let terminal = std::future::poll_fn(|cx| {
                            let outcome = std::pin::Pin::new(&mut connection).poll(cx);
                            let complete = connection.initial_settings_complete();
                            match outcome {
                                Poll::Ready(outcome) => Poll::Ready(Some((outcome, complete))),
                                Poll::Pending if complete => Poll::Ready(None),
                                Poll::Pending => Poll::Pending,
                            }
                        }).await;
                        if let Some((outcome, complete)) = terminal {
                            // Failure/EOF cannot return a still-live IO as an
                            // output while releasing the acquisition capability.
                            drop(connection);
                            drop(capacity);
                            return Ok(AcquiredConnection::Finished(outcome, complete));
                        }
                        if initial_settings_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                            drop(connection);
                            drop(capacity);
                            return Err("Native acquisition deadline elapsed at initial settings completion".to_owned());
                        }
                        Ok(AcquiredConnection::Installed(connection, capacity))
                    };
                    match initial_settings_deadline {
                        Some(deadline) => tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), bootstrap)
                            .await.map_err(|_| "Native TCP/TLS/HTTP2 initial settings deadline elapsed".to_owned())?,
                        None => bootstrap.await,
                    }
                }, acquisition_owner);
                tokio::spawn(async move {
                    let (connection, capacity) = match acquisition.await {
                        Ok(AcquiredConnection::Installed(connection, capacity)) => (connection, capacity),
                        Ok(AcquiredConnection::Finished(outcome, complete)) => {
                            if outcome.is_err() && !complete && initial_settings_deadline.is_some() {
                                on_transport_handshake_failure();
                            }
                            served.close("http2", &match outcome {
                                Ok(()) => "ok".to_owned(),
                                Err(error) => format!("{error:?}"),
                            });
                            return;
                        }
                        Err(error) => {
                            served.close("transport_handshake", &error);
                            on_transport_handshake_failure();
                            return;
                        }
                    };
                    // The acquisition scope has exited. Application streams
                    // retain independent pool/IO owners without the 2s timer.
                    let mut connection = std::pin::pin!(connection);
                    let mut winding_down = false;
                    let outcome = loop {
                        tokio::select! {
                            outcome = &mut connection => break outcome,
                            _ = drain.changed(), if !winding_down => {
                                winding_down = true;
                                connection.as_mut().graceful_shutdown();
                            }
                        }
                    };
                    drop(capacity);
                    served.close("http2", &match outcome {
                        Ok(()) => "ok".to_owned(),
                        Err(error) => format!("{error:?}"),
                    });
                });
            }
        }
    }

    // Stop accepting first, then let what is already in flight answer. The
    // listener's own receiver has to go before `closed()` can ever resolve.
    drop(drain_rx);
    let _ = drain_tx.send(());
    let live = live_connections.load(Ordering::Acquire);
    if live == 0 {
        tracing::debug!(role = role_label, "native listener stopped");
        return Ok(());
    }
    tracing::debug!(
        role = role_label,
        live_connections = live,
        "native listener is draining its connections"
    );
    if tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, drain_tx.closed())
        .await
        .is_err()
    {
        // Bounded on purpose: this runs inside process shutdown, and a peer
        // that never finishes its stream must not be able to hold the process
        // open. What is left is about to be cancelled, and each one says so.
        tracing::warn!(
            role = role_label,
            live_connections = live_connections.load(Ordering::Acquire),
            drain_timeout_ms = SHUTDOWN_DRAIN_TIMEOUT.as_millis(),
            "native listener stopped before every connection finished draining"
        );
    } else {
        tracing::debug!(role = role_label, "native listener drained and stopped");
    }
    Ok(())
}

/// One inbound connection this listener is serving, and the record of how it
/// ended.
///
/// A connection that reaches [`ServedConnection::close`] ended because its own
/// serving future resolved, and the reason is whatever hyper reported. A
/// connection whose guard drops first was ended by an owner outside it, which
/// means its peer saw the socket close with no HTTP/2 GOAWAY and, under TLS,
/// no `close_notify`. Without this record that difference is invisible from
/// both ends of the connection.
struct ServedConnection {
    role: &'static str,
    id: u64,
    peer: SocketAddr,
    live: Arc<AtomicU64>,
    closed: bool,
}

impl ServedConnection {
    fn open(
        role: &'static str,
        peer: SocketAddr,
        next_id: &Arc<AtomicU64>,
        live: &Arc<AtomicU64>,
    ) -> Self {
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        let live_now = live.fetch_add(1, Ordering::AcqRel) + 1;
        tracing::debug!(
            role,
            connection_id = id,
            %peer,
            live_connections = live_now,
            "native listener accepted a connection"
        );
        Self {
            role,
            id,
            peer,
            live: Arc::clone(live),
            closed: false,
        }
    }

    fn close(&mut self, stage: &str, detail: &str) {
        self.closed = true;
        tracing::debug!(
            role = self.role,
            connection_id = self.id,
            peer = %self.peer,
            stage,
            detail,
            "native listener connection ended"
        );
    }
}

impl Drop for ServedConnection {
    fn drop(&mut self) {
        let live_now = self.live.fetch_sub(1, Ordering::AcqRel).saturating_sub(1);
        if self.closed {
            return;
        }
        // Unwinding means a handler panicked through the serving future; a
        // quiet drop means an owner outside the connection cancelled it, which
        // today is this listener's runtime being dropped.
        let cause = if std::thread::panicking() {
            "panic"
        } else {
            "cancelled"
        };
        tracing::warn!(
            role = self.role,
            connection_id = self.id,
            peer = %self.peer,
            cause,
            live_connections = live_now,
            "native listener connection was ended without finishing; its peer sees an abrupt close"
        );
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
            let response = respond_from_request(
                request,
                tonic::Status::from_static(
                    tonic::Code::Unauthenticated,
                    "native caller authentication failed",
                ),
            );
            return Box::pin(async move { Ok(response) });
        }
        Box::pin(self.inner.call(request))
    }
}

async fn grpc_unimplemented_fallback(
    request: axum::http::Request<axum::body::Body>,
) -> axum::http::Response<axum::body::Body> {
    respond_from_request(
        request,
        tonic::Status::from_static(tonic::Code::Unimplemented, ""),
    )
    .map(axum::body::Body::new)
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    struct FundedHeaders {
        maps: axum::http::header::HeaderMapAllocationPool,
        fields: axum::http::header::HeaderFieldAllocationPool,
        budget: Arc<novarocks_worker::result_buffer::ResultRetainedBudget>,
        bytes: usize,
    }

    impl FundedHeaders {
        fn new() -> Self {
            use axum::http::header::{HeaderFieldAllocationPool, HeaderMapAllocationPool};
            use novarocks_execution::runtime::fragment::io::{
                ResultWriteAdmission, ResultWriteCredit,
            };
            use novarocks_worker::result_buffer::ResultRetainedBudget;
            let carrier = bytes::Bytes::owner_with_exit_guard_metadata_size::<
                bytes::Bytes,
                ResultWriteCredit,
            >();
            let map_bytes =
                HeaderMapAllocationPool::allocation_capacity_bound(1, 8, 2).unwrap() + carrier;
            let field_bytes = HeaderFieldAllocationPool::allocation_capacity_bound(1024, 8, 256)
                .unwrap()
                + carrier;
            let bytes = map_bytes + field_bytes;
            let budget = ResultRetainedBudget::new(std::num::NonZeroUsize::new(bytes).unwrap());
            let owner = |capacity| {
                let ResultWriteAdmission::Granted(credit) =
                    budget.try_reserve_process(capacity).unwrap()
                else {
                    panic!("complete original pregrant required");
                };
                bytes::Bytes::from_owner_with_exit_guard(bytes::Bytes::new(), credit)
            };
            let fields = HeaderFieldAllocationPool::new(1024, 8, 256, owner(field_bytes)).unwrap();
            let maps = HeaderMapAllocationPool::new(1, 8, 2, owner(map_bytes)).unwrap();
            fields.try_bind_once().unwrap();
            maps.try_bind_connection_with_fields(&fields).unwrap();
            Self {
                maps,
                fields,
                budget,
                bytes,
            }
        }

        fn request(&self) -> axum::http::Request<axum::body::Body> {
            let mut request = axum::http::Request::new(axum::body::Body::empty());
            *request.headers_mut() =
                axum::http::HeaderMap::try_from_allocation_pool(&self.maps).unwrap();
            request.headers_mut().insert(
                "private-request",
                axum::http::HeaderValue::from_static("secret"),
            );
            request
        }

        fn original(&self, headers: &axum::http::HeaderMap) {
            assert!(headers.allocation_pool().unwrap().same_pool(&self.maps));
            assert!(
                headers
                    .field_allocation_pool()
                    .unwrap()
                    .same_pool(&self.fields)
            );
            assert!(headers.get("private-request").is_none());
        }

        fn retire(self) {
            use novarocks_execution::runtime::fragment::io::ResultWriteAdmission;
            let Self {
                maps,
                fields,
                budget,
                bytes,
            } = self;
            assert_eq!(maps.available_maps(), 1);
            assert!(matches!(
                budget.try_reserve_process(bytes).unwrap(),
                ResultWriteAdmission::Blocked
            ));
            drop(maps);
            drop(fields);
            assert!(matches!(
                budget.try_reserve_process(bytes).unwrap(),
                ResultWriteAdmission::Granted(_)
            ));
        }
    }

    #[tokio::test]
    async fn authentication_refusal_reuses_input_capacity_before_polling_its_future() {
        use std::sync::atomic::AtomicUsize;
        let fixture = FundedHeaders::new();
        let entered = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));
        let observed = entered.clone();
        let inner = tower::service_fn(move |_request: axum::http::Request<axum::body::Body>| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, std::convert::Infallible>(tonic::Status::ok("").into_http()) }
        });
        let rejected = failures.clone();
        let mut service = NativeListenerAuthService::new(
            inner,
            crate::backend_test_support::test_backend_native_trust().server_admission(),
            Arc::new(move || {
                rejected.fetch_add(1, Ordering::SeqCst);
            }),
        );
        let future = service.call(fixture.request());
        assert_eq!(failures.load(Ordering::SeqCst), 1);
        assert_eq!(entered.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.maps.available_maps(), 0);
        let response = future.await.unwrap();
        fixture.original(response.headers());
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["grpc-status"], "16");
        assert_eq!(
            response.headers()["grpc-message"],
            "native%20caller%20authentication%20failed"
        );
        drop(response);
        let cancelled = service.call(fixture.request());
        assert_eq!(fixture.maps.available_maps(), 0);
        drop(cancelled);
        fixture.retire();
    }

    #[tokio::test]
    async fn unknown_route_refusal_consumes_the_actual_original_request_map() {
        let fixture = FundedHeaders::new();
        let response = grpc_unimplemented_fallback(fixture.request()).await;
        fixture.original(response.headers());
        assert_eq!(fixture.maps.available_maps(), 0);
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["grpc-status"], "12");
        assert_eq!(response.headers()["content-type"], "application/grpc");
        drop(response);
        fixture.retire();
    }

    #[tokio::test]
    async fn unknown_route_without_original_fields_preserves_legacy_response() {
        let response =
            grpc_unimplemented_fallback(axum::http::Request::new(axum::body::Body::empty())).await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["grpc-status"], "12");
        assert!(response.headers().allocation_pool().is_none());
        assert!(response.headers().field_allocation_pool().is_none());
    }

    /// A service that reports when it has been entered and answers only when
    /// it is released, so a test can hold one request in flight.
    #[derive(Clone)]
    struct HeldService {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl Service<axum::http::Request<axum::body::Body>> for HeldService {
        type Response = axum::http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
        >;

        fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: axum::http::Request<axum::body::Body>) -> Self::Future {
            let entered = Arc::clone(&self.entered);
            let release = Arc::clone(&self.release);
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Ok(axum::http::Response::builder()
                    .status(200)
                    .body(boxed(axum::body::Body::empty()))
                    .expect("held response"))
            })
        }
    }

    /// Stopping the listener must not cut a request that is already being
    /// served.
    ///
    /// The listener's runtime is dropped as soon as this function returns, and
    /// dropping it cancels every connection task it still owns. So "does not
    /// return while a request is in flight" is the whole of what keeps a
    /// stopping backend from closing its caller's socket with no GOAWAY.
    #[tokio::test(flavor = "multi_thread")]
    async fn stopping_waits_for_a_request_that_is_already_being_served() {
        let listener = TokioTcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("test listener address");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut serving = tokio::spawn(serve_native_listener(
            listener,
            HeldService {
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            },
            NativeIncomingAdapter::plaintext(),
            shutdown_rx,
            Arc::new(|| {}) as Arc<dyn Fn() + Send + Sync>,
            "test",
            None,
        ));

        let stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect to the test listener");
        let (mut sender, connection) = h2::client::handshake(stream)
            .await
            .expect("client handshake");
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/held")
            .body(())
            .expect("held request");
        let (response, _body) = sender.send_request(request, true).expect("send request");
        entered.notified().await;

        shutdown_tx.send(true).expect("request shutdown");
        assert!(
            tokio::time::timeout(Duration::from_millis(250), &mut serving)
                .await
                .is_err(),
            "the listener returned while a request was still being served"
        );

        release.notify_one();
        assert_eq!(
            response.await.expect("held response arrives").status(),
            200,
            "a request already in flight must still be answered"
        );
        let served = tokio::time::timeout(Duration::from_secs(5), serving)
            .await
            .expect("the listener returns once its connections drained")
            .expect("listener task");
        assert_eq!(served, Ok(()));

        drop(sender);
        let _ = driver.await;
    }
}
