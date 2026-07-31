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

//! Production native-BE gRPC service and its instance-owned listener.
//!
//! The generated service is intentionally owned by `novarocks-backend`.  The
//! core service remains the compatibility-neutral implementation while the
//! closeout migrates individual execution adapters behind this backend entry
//! point; no process-global listener state is used here.

use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;

use novarocks::query_execution::lifecycle::{QueryLifecycleIngress, QueryTerminalIngress};
use novarocks::query_execution::report::NativeReportHandler;
use novarocks::service::grpc_server::GrpcService;
use novarocks::service::native_fragment_ingress::NativeFragmentIngress;
use novarocks_protocol::{filter, novarocks as proto};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;

use super::transport::nova_rocks_grpc_server::{NovaRocksGrpc, NovaRocksGrpcServer};

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Backend-owned Tonic facade.  It fixes the production server type at the
/// backend boundary while the remaining compatibility-neutral adapters are
/// incrementally narrowed in core.
#[derive(Clone, Debug)]
pub(crate) struct NativeBackendGrpcService {
    inner: GrpcService,
}

impl NativeBackendGrpcService {
    pub(crate) fn new(
        native_fragment_ingress: Arc<dyn NativeFragmentIngress>,
        query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
        report_handler: Arc<dyn NativeReportHandler>,
        terminal_ingress: Option<Arc<dyn QueryTerminalIngress>>,
    ) -> Self {
        let mut inner = GrpcService::with_fragment_execution(
            native_fragment_ingress,
            query_lifecycle_ingress,
            report_handler,
        );
        if let Some(terminal_ingress) = terminal_ingress {
            inner = inner.with_terminal_ingress(terminal_ingress);
        }
        Self {
            inner,
        }
    }

    fn with_query_control_shutdown(mut self, shutdown: watch::Receiver<bool>) -> Self {
        self.inner = self.inner.with_query_control_shutdown(shutdown);
        self
    }
}

#[tonic::async_trait]
impl NovaRocksGrpc for NativeBackendGrpcService {
    type ExchangeStream = <GrpcService as novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc>::ExchangeStream;
    type QueryControlStreamStream = <GrpcService as novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc>::QueryControlStreamStream;

    async fn exchange(
        &self,
        request: tonic::Request<tonic::Streaming<proto::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::exchange(
            &self.inner,
            request,
        )
        .await
    }

    async fn exchange_unary(
        &self,
        request: tonic::Request<proto::ExchangeRequest>,
    ) -> Result<tonic::Response<proto::ExchangeResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::exchange_unary(
            &self.inner,
            request,
        )
        .await
    }

    async fn transmit_runtime_filter_envelope(
        &self,
        request: tonic::Request<filter::RuntimeFilterEnvelope>,
    ) -> Result<tonic::Response<filter::RuntimeFilterEnvelopeResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::transmit_runtime_filter_envelope(&self.inner, request).await
    }

    async fn lookup(
        &self,
        request: tonic::Request<filter::LookupRequest>,
    ) -> Result<tonic::Response<filter::LookupResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::lookup(
            &self.inner,
            request,
        )
        .await
    }

    async fn fetch_result(
        &self,
        request: tonic::Request<proto::FetchResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::fetch_result(
            &self.inner,
            request,
        )
        .await
    }

    async fn cancel_fragment(
        &self,
        request: tonic::Request<proto::CancelFragmentRequest>,
    ) -> Result<tonic::Response<proto::CancelFragmentResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::cancel_fragment(
            &self.inner,
            request,
        )
        .await
    }

    async fn ensure_connector_execution_binding(
        &self,
        request: tonic::Request<proto::EnsureConnectorExecutionBindingRequest>,
    ) -> Result<tonic::Response<proto::EnsureConnectorExecutionBindingResponse>, tonic::Status>
    {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::ensure_connector_execution_binding(&self.inner, request).await
    }

    async fn retire_connector_execution_binding(
        &self,
        request: tonic::Request<proto::RetireConnectorExecutionBindingRequest>,
    ) -> Result<tonic::Response<proto::RetireConnectorExecutionBindingResponse>, tonic::Status>
    {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::retire_connector_execution_binding(&self.inner, request).await
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<proto::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::HeartbeatResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::heartbeat(
            &self.inner,
            request,
        )
        .await
    }

    async fn init_query(
        &self,
        request: tonic::Request<proto::InitQueryRequest>,
    ) -> Result<tonic::Response<proto::InitQueryResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::init_query(
            &self.inner,
            request,
        )
        .await
    }

    async fn stage_fragments(
        &self,
        request: tonic::Request<proto::StageFragmentsRequest>,
    ) -> Result<tonic::Response<proto::StageFragmentsResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::stage_fragments(
            &self.inner,
            request,
        )
        .await
    }

    async fn start_prepared_query(
        &self,
        request: tonic::Request<proto::StartPreparedQueryRequest>,
    ) -> Result<tonic::Response<proto::StartPreparedQueryResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::start_prepared_query(
            &self.inner,
            request,
        )
        .await
    }

    async fn abort_query(
        &self,
        request: tonic::Request<proto::AbortQueryRequest>,
    ) -> Result<tonic::Response<proto::AbortQueryResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::abort_query(
            &self.inner,
            request,
        )
        .await
    }

    async fn query_control_stream(
        &self,
        request: tonic::Request<tonic::Streaming<proto::QueryControlRequest>>,
    ) -> Result<tonic::Response<Self::QueryControlStreamStream>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::query_control_stream(
            &self.inner,
            request,
        )
        .await
    }

    async fn report_exec_status(
        &self,
        request: tonic::Request<proto::ReportExecStatusRequest>,
    ) -> Result<tonic::Response<proto::ReportExecStatusResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::report_exec_status(
            &self.inner,
            request,
        )
        .await
    }

    async fn batch_report_exec_status(
        &self,
        request: tonic::Request<proto::BatchReportExecStatusRequest>,
    ) -> Result<tonic::Response<proto::BatchReportExecStatusResponse>, tonic::Status> {
        novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc::batch_report_exec_status(&self.inner, request).await
    }
}

/// A backend application owns exactly one native listener.  Unlike the legacy
/// core listener, this handle has no global reservation or shutdown state.
pub(crate) struct NativeGrpcServerHandle {
    bound_addr: SocketAddr,
    shutdown_tx: Option<watch::Sender<bool>>,
    failure_rx: mpsc::Receiver<String>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Arc<AtomicBool>,
}

impl NativeGrpcServerHandle {
    pub(crate) fn start(
        host: &str,
        port: u16,
        service: NativeBackendGrpcService,
    ) -> Result<Self, String> {
        let address = (host, port)
            .to_socket_addrs()
            .map_err(|error| format!("resolve native backend gRPC address {host}:{port}: {error}"))?
            .next()
            .ok_or_else(|| {
                format!("resolve native backend gRPC address {host}:{port}: no address")
            })?;
        let listener = TcpListener::bind(address)
            .map_err(|error| format!("bind native backend gRPC address {address}: {error}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("set native backend gRPC listener nonblocking: {error}"))?;
        let bound_addr = listener
            .local_addr()
            .map_err(|error| format!("read native backend gRPC bound address: {error}"))?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let thread_stop_requested = Arc::clone(&stop_requested);
        let join_handle = std::thread::Builder::new()
            .name("native-backend-grpc".to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .worker_threads(8)
                        .thread_stack_size(
                            novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES,
                        )
                        .build()
                        .map_err(|error| format!("build native backend gRPC runtime: {error}"))?;
                    runtime.block_on(async move {
                        let listener = TokioTcpListener::from_std(listener).map_err(|error| {
                            format!("create Tokio native backend gRPC listener: {error}")
                        })?;
                        let service = NovaRocksGrpcServer::new(
                            service.with_query_control_shutdown(shutdown_rx.clone()),
                        )
                        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                        let mut shutdown_rx = shutdown_rx;
                        tonic::transport::Server::builder()
                            .add_service(service)
                            .serve_with_incoming_shutdown(
                                TcpListenerStream::new(listener),
                                async move {
                                    while !*shutdown_rx.borrow() {
                                        if shutdown_rx.changed().await.is_err() {
                                            break;
                                        }
                                    }
                                },
                            )
                            .await
                            .map_err(|error| {
                                format!("native backend gRPC serve future failed: {error}")
                            })
                    })
                }));
                if thread_stop_requested.load(Ordering::Acquire) {
                    return;
                }
                let error = match outcome {
                    Ok(Ok(())) => "native backend gRPC server exited unexpectedly".to_string(),
                    Ok(Err(error)) => error,
                    Err(payload) => payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| {
                            payload
                                .downcast_ref::<&str>()
                                .map(|value| (*value).to_string())
                        })
                        .unwrap_or_else(|| "native backend gRPC server panicked".to_string()),
                };
                let _ = failure_tx.send(error);
            })
            .map_err(|error| format!("spawn native backend gRPC server: {error}"))?;
        Ok(Self {
            bound_addr,
            shutdown_tx: Some(shutdown_tx),
            failure_rx,
            join_handle: Some(join_handle),
            stop_requested,
        })
    }

    pub(crate) const fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub(crate) fn poll_failure(&mut self) -> Result<Option<String>, String> {
        match self.failure_rx.try_recv() {
            Ok(error) => Ok(Some(error)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub(crate) fn stop(&mut self) -> Result<(), String> {
        self.stop_requested.store(true, Ordering::Release);
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .map_err(|_| "native backend gRPC server thread panicked".to_string())?;
        }
        Ok(())
    }
}

impl Drop for NativeGrpcServerHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
