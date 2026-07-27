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
#[cfg(test)]
use std::collections::BTreeSet;
#[cfg(feature = "compat")]
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use axum::Router;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
#[cfg(feature = "compat")]
use axum::routing::{post, put};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::boxed;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::service::Routes;
#[cfg(any(test, feature = "compat"))]
use tonic::transport::Server;

#[cfg(feature = "compat")]
use crate::common::config::grpc_port;
use crate::common::config::http_port;
#[cfg(feature = "compat")]
use crate::common::config::starlet_port;
use crate::common::engine_error::EngineError;
use crate::common::types::format_uuid;
#[cfg(feature = "compat")]
use crate::connector::starrocks::starmgr;
use crate::coordinator::ports::CoordinatorReportHandler;
use crate::coordinator::report::CoordinatorExecStatusReportHandler;
use crate::novarocks_logging::info;
#[cfg(feature = "compat")]
use crate::novarocks_logging::warn;
#[cfg(feature = "compat")]
use crate::runtime::starlet_shard_registry;
use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeIngress;
use crate::service::grpc_runtime_filter_adapter::handle_runtime_filter_envelope;
use crate::service::grpc_runtime_filter_install_adapter::{
    RuntimeFilterDeploymentIngress, query_scoped_runtime_filter_deployment_ingress,
};
use crate::service::internal_rpc;
use crate::service::runtime_filter_envelope_ingress::query_scoped_runtime_filter_envelope_ingress;
#[cfg(feature = "compat")]
use crate::service::stream_load_http;
use crate::service::{load_tracking_http, metrics_http};

pub(crate) use crate::common::engine_error::{
    REPORT_EXEC_STATUS_OK, REPORT_EXEC_STATUS_QUERY_GONE,
};
pub use crate::proto;

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const CANCEL_FRAGMENT_OK: i32 = 0;
const CANCEL_FRAGMENT_IGNORED_STALE_EPOCH: i32 = 2;
#[cfg(test)]
const CANCEL_FRAGMENT_NOT_OWNED: i32 = 3;
static SUBMIT_FRAGMENT_CALLS: AtomicUsize = AtomicUsize::new(0);
static FETCH_RESULT_CALLS: AtomicUsize = AtomicUsize::new(0);
static CANCEL_FRAGMENT_CALLS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static PAUSE_STANDALONE_GRPC_STARTUP_AFTER_RESERVATION: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static STANDALONE_GRPC_STARTUP_RESERVATIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct GrpcServerState {
    started: bool,
    starting: bool,
    bound_port: Option<u16>,
    shutdown_tx: Option<watch::Sender<bool>>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Option<Arc<AtomicBool>>,
    failure_rx: Option<mpsc::Receiver<String>>,
}

fn grpc_server_state() -> &'static Mutex<GrpcServerState> {
    static STATE: OnceLock<Mutex<GrpcServerState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(GrpcServerState::default()))
}

#[cfg(test)]
fn pause_standalone_grpc_startup_after_reservation() {
    if !PAUSE_STANDALONE_GRPC_STARTUP_AFTER_RESERVATION.load(Ordering::Acquire) {
        return;
    }
    STANDALONE_GRPC_STARTUP_RESERVATIONS.fetch_add(1, Ordering::AcqRel);
    while PAUSE_STANDALONE_GRPC_STARTUP_AFTER_RESERVATION.load(Ordering::Acquire) {
        std::thread::yield_now();
    }
}

#[derive(Clone)]
pub struct GrpcService {
    allow_local_execution: bool,
    report_handler: Arc<dyn CoordinatorReportHandler>,
    runtime_filter_envelope_ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
    runtime_filter_deployment_ingress: Arc<dyn RuntimeFilterDeploymentIngress>,
    #[cfg(test)]
    execution_query_manager: Option<Arc<crate::runtime::query_context::QueryContextManager>>,
    #[cfg(test)]
    execution_owned_finsts: Option<Arc<Mutex<BTreeSet<crate::common::types::UniqueId>>>>,
    #[cfg(test)]
    submit_fragment_entry_probe: Option<Arc<AtomicUsize>>,
}

impl std::fmt::Debug for GrpcService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcService")
            .field("allow_local_execution", &self.allow_local_execution)
            .finish_non_exhaustive()
    }
}

impl Default for GrpcService {
    fn default() -> Self {
        Self::full_execution()
    }
}

impl GrpcService {
    pub fn full_execution() -> Self {
        Self::full_execution_with_report_handler(Arc::new(CoordinatorExecStatusReportHandler))
    }

    pub(crate) fn full_execution_with_report_handler(
        report_handler: Arc<dyn CoordinatorReportHandler>,
    ) -> Self {
        Self::with_handlers(
            true,
            report_handler,
            query_scoped_runtime_filter_envelope_ingress(),
            query_scoped_runtime_filter_deployment_ingress(),
        )
    }

    pub fn report_only() -> Self {
        Self::report_only_with_report_handler(Arc::new(CoordinatorExecStatusReportHandler))
    }

    pub(crate) fn report_only_with_report_handler(
        report_handler: Arc<dyn CoordinatorReportHandler>,
    ) -> Self {
        Self::with_handlers(
            false,
            report_handler,
            query_scoped_runtime_filter_envelope_ingress(),
            query_scoped_runtime_filter_deployment_ingress(),
        )
    }

    fn with_handlers(
        allow_local_execution: bool,
        report_handler: Arc<dyn CoordinatorReportHandler>,
        runtime_filter_envelope_ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
        runtime_filter_deployment_ingress: Arc<dyn RuntimeFilterDeploymentIngress>,
    ) -> Self {
        Self {
            allow_local_execution,
            report_handler,
            runtime_filter_envelope_ingress,
            runtime_filter_deployment_ingress,
            #[cfg(test)]
            execution_query_manager: None,
            #[cfg(test)]
            execution_owned_finsts: None,
            #[cfg(test)]
            submit_fragment_entry_probe: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn full_execution_with_handlers(
        report_handler: Arc<dyn CoordinatorReportHandler>,
        runtime_filter_envelope_ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
    ) -> Self {
        Self::with_handlers(
            true,
            report_handler,
            runtime_filter_envelope_ingress,
            query_scoped_runtime_filter_deployment_ingress(),
        )
    }

    #[cfg(test)]
    pub(crate) fn report_only_with_handlers(
        report_handler: Arc<dyn CoordinatorReportHandler>,
        runtime_filter_envelope_ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
    ) -> Self {
        Self::with_handlers(
            false,
            report_handler,
            runtime_filter_envelope_ingress,
            query_scoped_runtime_filter_deployment_ingress(),
        )
    }

    #[cfg(test)]
    pub(crate) fn full_execution_with_runtime_filter_manager(
        report_handler: Arc<dyn CoordinatorReportHandler>,
        manager: Arc<crate::runtime::query_context::QueryContextManager>,
    ) -> Self {
        let mut service = Self::with_handlers(
            true,
            report_handler,
            crate::service::runtime_filter_envelope_ingress::query_scoped_runtime_filter_envelope_ingress_with_manager(
                manager.clone(),
            ),
            crate::service::grpc_runtime_filter_install_adapter::query_scoped_runtime_filter_deployment_ingress_with_manager(
                manager.clone(),
            ),
        );
        service.execution_query_manager = Some(manager);
        service.execution_owned_finsts = Some(Arc::new(Mutex::new(BTreeSet::new())));
        service
    }

    #[cfg(test)]
    fn with_submit_fragment_entry_probe(mut self, probe: Arc<AtomicUsize>) -> Self {
        self.submit_fragment_entry_probe = Some(probe);
        self
    }

    fn require_local_execution(&self, rpc_name: &str) -> Result<(), tonic::Status> {
        if self.allow_local_execution {
            Ok(())
        } else {
            Err(tonic::Status::failed_precondition(format!(
                "report-only NovaRocksGrpc endpoint rejects local execution RPC: {rpc_name}"
            )))
        }
    }
}

#[cfg(test)]
pub(crate) struct IndependentGrpcRuntimeFilterNode {
    endpoint: SocketAddr,
    resources: IndependentGrpcRuntimeFilterResources,
}

#[cfg(test)]
struct IndependentGrpcRuntimeFilterResources {
    manager: Arc<crate::runtime::query_context::QueryContextManager>,
    submit_fragment_entry_probe: Arc<AtomicUsize>,
    shutdown_tx: Option<watch::Sender<bool>>,
    server_handle: Option<JoinHandle<()>>,
    clean_handle: Option<JoinHandle<()>>,
}

#[cfg(test)]
impl IndependentGrpcRuntimeFilterResources {
    fn shutdown(&mut self, wait: std::time::Duration) -> Result<(), String> {
        self.manager.stop_clean_loop_for_test();
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        let failures = join_independent_runtime_filter_threads(
            self.server_handle.take(),
            self.clean_handle.take(),
            wait,
        );
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}

#[cfg(test)]
impl Drop for IndependentGrpcRuntimeFilterResources {
    fn drop(&mut self) {
        let _ = self.shutdown(IndependentGrpcRuntimeFilterNode::WAIT);
    }
}

#[cfg(test)]
struct IndependentGrpcRuntimeFilterStartupGuard {
    resources: Option<IndependentGrpcRuntimeFilterResources>,
}

#[cfg(test)]
impl IndependentGrpcRuntimeFilterStartupGuard {
    fn new(resources: IndependentGrpcRuntimeFilterResources) -> Self {
        Self {
            resources: Some(resources),
        }
    }

    fn resources(&self) -> &IndependentGrpcRuntimeFilterResources {
        self.resources
            .as_ref()
            .expect("startup resources remain armed until readiness")
    }

    fn resources_mut(&mut self) -> &mut IndependentGrpcRuntimeFilterResources {
        self.resources
            .as_mut()
            .expect("startup resources remain armed until readiness")
    }

    fn disarm(mut self) -> IndependentGrpcRuntimeFilterResources {
        self.resources
            .take()
            .expect("successful startup transfers its resources to the live node")
    }
}

#[cfg(test)]
impl Drop for IndependentGrpcRuntimeFilterStartupGuard {
    fn drop(&mut self) {
        if let Some(resources) = &mut self.resources {
            let _ = resources.shutdown(IndependentGrpcRuntimeFilterNode::WAIT);
        }
    }
}

#[cfg(test)]
struct IndependentGrpcStartupProbe {
    manager: mpsc::SyncSender<std::sync::Weak<crate::runtime::query_context::QueryContextManager>>,
    server_exited: mpsc::SyncSender<()>,
    clean_exited: mpsc::SyncSender<()>,
    panic_before_ready: bool,
}

#[cfg(test)]
struct IndependentGrpcServerExitSignal(Option<mpsc::SyncSender<()>>);

#[cfg(test)]
impl Drop for IndependentGrpcServerExitSignal {
    fn drop(&mut self) {
        if let Some(exited) = self.0.take() {
            let _ = exited.send(());
        }
    }
}

#[cfg(test)]
impl IndependentGrpcRuntimeFilterNode {
    const WAIT: std::time::Duration = std::time::Duration::from_secs(5);

    pub(crate) fn start() -> Result<Self, String> {
        Self::start_with_probe(None)
    }

    fn start_with_probe(probe: Option<IndependentGrpcStartupProbe>) -> Result<Self, String> {
        let listener = bind_tcp_listener("127.0.0.1", 0, "independent runtime-filter gRPC")?;
        let endpoint = listener
            .local_addr()
            .map_err(|error| format!("read independent runtime-filter address failed: {error}"))?;
        let (manager, clean_handle) = if let Some(probe) = &probe {
            crate::runtime::query_context::QueryContextManager::new_for_live_test_with_exit_signal(
                Some(probe.clean_exited.clone()),
            )
        } else {
            crate::runtime::query_context::QueryContextManager::new_for_live_test()
        };
        if let Some(probe) = &probe {
            let _ = probe.manager.send(Arc::downgrade(&manager));
        }
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let resources = IndependentGrpcRuntimeFilterResources {
            manager,
            submit_fragment_entry_probe: Arc::new(AtomicUsize::new(0)),
            shutdown_tx: Some(shutdown_tx),
            server_handle: None,
            clean_handle: Some(clean_handle),
        };
        let mut startup = IndependentGrpcRuntimeFilterStartupGuard::new(resources);
        let service_manager = Arc::clone(&startup.resources().manager);
        let submit_fragment_entry_probe =
            Arc::clone(&startup.resources().submit_fragment_entry_probe);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let server_handle = std::thread::spawn(move || {
            let _exit = IndependentGrpcServerExitSignal(
                probe.as_ref().map(|probe| probe.server_exited.clone()),
            );
            if probe.as_ref().is_some_and(|probe| probe.panic_before_ready) {
                panic!("injected independent runtime-filter pre-ready server panic");
            }
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .thread_stack_size(crate::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
                .build()
                .expect("build independent runtime-filter gRPC runtime");
            runtime.block_on(async move {
                let listener = TokioTcpListener::from_std(listener)
                    .expect("create independent runtime-filter Tokio listener");
                let service = GrpcService::full_execution_with_runtime_filter_manager(
                    Arc::new(CoordinatorExecStatusReportHandler),
                    service_manager,
                )
                .with_submit_fragment_entry_probe(submit_fragment_entry_probe);
                let service =
                    proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(service)
                        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                let app = build_novarocks_http_app(Routes::new(service));
                let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                    while !*shutdown_rx.borrow() {
                        if shutdown_rx.changed().await.is_err() {
                            break;
                        }
                    }
                });
                let _ = ready_tx.send(());
                server
                    .await
                    .expect("independent runtime-filter gRPC server failed");
            });
        });
        startup.resources_mut().server_handle = Some(server_handle);
        ready_rx.recv_timeout(Self::WAIT).map_err(|error| {
            format!("independent runtime-filter gRPC server did not become ready: {error}")
        })?;
        Ok(Self {
            endpoint,
            resources: startup.disarm(),
        })
    }

    pub(crate) const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub(crate) fn manager(&self) -> &Arc<crate::runtime::query_context::QueryContextManager> {
        &self.resources.manager
    }

    pub(crate) fn submit_fragment_handler_calls(&self) -> usize {
        self.resources
            .submit_fragment_entry_probe
            .load(Ordering::SeqCst)
    }

    pub(crate) fn shutdown(&mut self) -> Result<(), String> {
        self.resources.shutdown(Self::WAIT)
    }
}

#[cfg(test)]
impl Drop for IndependentGrpcRuntimeFilterNode {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
fn join_independent_runtime_filter_thread(
    handle: JoinHandle<()>,
    label: &'static str,
    timeout: std::time::Duration,
) -> Result<(), String> {
    let (joined_tx, joined_rx) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _ = joined_tx.send(handle.join());
    });
    match joined_rx.recv_timeout(timeout) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(payload)) => {
            let detail = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload
                        .downcast_ref::<&str>()
                        .map(|value| (*value).to_string())
                })
                .unwrap_or_else(|| "unknown panic payload".to_string());
            Err(format!(
                "independent runtime-filter {label} panicked: {detail}"
            ))
        }
        Err(error) => Err(format!(
            "independent runtime-filter {label} did not stop within {timeout:?}: {error}"
        )),
    }
}

#[cfg(test)]
fn join_independent_runtime_filter_threads(
    server_handle: Option<JoinHandle<()>>,
    clean_handle: Option<JoinHandle<()>>,
    wait: std::time::Duration,
) -> Vec<String> {
    let deadline_at = std::time::Instant::now() + wait;
    let mut failures = Vec::new();
    if let Some(handle) = server_handle
        && let Err(error) = join_independent_runtime_filter_thread(
            handle,
            "gRPC server",
            deadline_at.saturating_duration_since(std::time::Instant::now()),
        )
    {
        failures.push(error);
    }
    if let Some(handle) = clean_handle
        && let Err(error) = join_independent_runtime_filter_thread(
            handle,
            "manager clean loop",
            deadline_at.saturating_duration_since(std::time::Instant::now()),
        )
    {
        failures.push(error);
    }
    failures
}

#[tonic::async_trait]
impl proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc for GrpcService {
    type ExchangeStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<proto::novarocks::ExchangeResponse, tonic::Status>,
                > + Send
                + 'static,
        >,
    >;

    async fn exchange(
        &self,
        request: tonic::Request<tonic::Streaming<proto::novarocks::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        use crate::novarocks_logging::debug;

        self.require_local_execution("Exchange")?;
        let mut inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel::<
            Result<proto::novarocks::ExchangeResponse, tonic::Status>,
        >(4096);

        tokio::spawn(async move {
            loop {
                let req = match inbound.message().await {
                    Ok(Some(v)) => v,
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!(
                                "exchange recv failed: {e}"
                            ))))
                            .await;
                        break;
                    }
                };

                let finst_id_hi = req.finst_id_hi;
                let finst_id_lo = req.finst_id_lo;
                let node_id = req.node_id;
                let sender_id = req.sender_id;
                let be_number = req.be_number;
                let eos = req.eos;
                let sequence = req.sequence;
                // handle_transmit_chunk includes Arrow IPC decoding which is CPU-intensive.
                // Offload to the blocking thread pool so async worker threads stay free for I/O.
                let result = match tokio::task::spawn_blocking(move || {
                    internal_rpc::handle_transmit_chunk(req)
                })
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!(
                                "exchange handler panicked: {e}"
                            ))))
                            .await;
                        break;
                    }
                };
                let ack = result;
                let handler_failed = ack.status.as_ref().is_some_and(|status| status.code != 0);
                debug!(
                    "exchange ack SEND: finst={} node_id={} sender_id={} be_number={} eos={} seq={}",
                    format_uuid(finst_id_hi, finst_id_lo),
                    node_id,
                    sender_id,
                    be_number,
                    eos,
                    sequence
                );

                if tx.send(Ok(ack)).await.is_err() {
                    break;
                }
                if handler_failed {
                    break;
                }
                debug!(
                    "exchange ack SENT: finst={} node_id={} sender_id={} be_number={} eos={} seq={}",
                    format_uuid(finst_id_hi, finst_id_lo),
                    node_id,
                    sender_id,
                    be_number,
                    eos,
                    sequence
                );
            }
        });

        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn exchange_unary(
        &self,
        request: tonic::Request<proto::novarocks::ExchangeRequest>,
    ) -> Result<tonic::Response<proto::novarocks::ExchangeResponse>, tonic::Status> {
        self.require_local_execution("ExchangeUnary")?;
        let req = request.into_inner();
        let result = tokio::task::spawn_blocking(move || internal_rpc::handle_transmit_chunk(req))
            .await
            .map_err(|e| {
                tonic::Status::internal(format!("exchange_unary handler panicked: {e}"))
            })?;
        Ok(tonic::Response::new(result))
    }

    async fn transmit_runtime_filter_envelope(
        &self,
        request: tonic::Request<proto::filter::RuntimeFilterEnvelope>,
    ) -> Result<tonic::Response<proto::filter::RuntimeFilterEnvelopeResponse>, tonic::Status> {
        self.require_local_execution("TransmitRuntimeFilterEnvelope")?;
        let ingress = self.runtime_filter_envelope_ingress.clone();
        let request = request.into_inner();
        let response =
            tokio::task::spawn_blocking(move || handle_runtime_filter_envelope(ingress, request))
                .await
                .map_err(|error| {
                    tonic::Status::internal(format!(
                        "transmit_runtime_filter_envelope handler panicked: {error}"
                    ))
                })??;
        Ok(tonic::Response::new(response))
    }

    async fn install_runtime_filter_deployment(
        &self,
        request: tonic::Request<proto::filter::InstallRuntimeFilterDeploymentRequest>,
    ) -> Result<tonic::Response<proto::filter::InstallRuntimeFilterDeploymentResponse>, tonic::Status>
    {
        self.require_local_execution("InstallRuntimeFilterDeployment")?;
        let ingress = self.runtime_filter_deployment_ingress.clone();
        let request = request.into_inner();
        let response = tokio::task::spawn_blocking(move || ingress.install(request))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!(
                    "install_runtime_filter_deployment handler panicked: {error}"
                ))
            })?;
        Ok(tonic::Response::new(response))
    }

    async fn abort_runtime_filter_deployment(
        &self,
        request: tonic::Request<proto::filter::AbortRuntimeFilterDeploymentRequest>,
    ) -> Result<tonic::Response<proto::filter::AbortRuntimeFilterDeploymentResponse>, tonic::Status>
    {
        self.require_local_execution("AbortRuntimeFilterDeployment")?;
        let ingress = self.runtime_filter_deployment_ingress.clone();
        let request = request.into_inner();
        let response = tokio::task::spawn_blocking(move || ingress.abort(request))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!(
                    "abort_runtime_filter_deployment handler panicked: {error}"
                ))
            })?;
        Ok(tonic::Response::new(response))
    }

    async fn lookup(
        &self,
        request: tonic::Request<proto::filter::LookupRequest>,
    ) -> Result<tonic::Response<proto::filter::LookupResponse>, tonic::Status> {
        self.require_local_execution("Lookup")?;
        Ok(tonic::Response::new(internal_rpc::handle_lookup(
            request.into_inner(),
        )))
    }

    async fn submit_fragment(
        &self,
        request: tonic::Request<proto::novarocks::SubmitFragmentRequest>,
    ) -> Result<tonic::Response<proto::novarocks::SubmitFragmentResponse>, tonic::Status> {
        self.require_local_execution("SubmitFragment")?;
        #[cfg(test)]
        if let Some(probe) = self.submit_fragment_entry_probe.as_ref() {
            probe.fetch_add(1, Ordering::SeqCst);
        }
        let call_index = SUBMIT_FRAGMENT_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        if crate::common::config::debug_emit_grpc_fragment_marker() {
            println!("NOVAROCKS_GRPC_SUBMIT call={call_index}");
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
        if crate::common::config::debug_fault_inject_submit_fail_after()
            .is_some_and(|successes| call_index > successes)
        {
            return Err(tonic::Status::unavailable(format!(
                "debug submit fault injected on call {call_index}"
            )));
        }
        let proto::novarocks::SubmitFragmentRequest {
            plan,
            instance_params,
        } = request.into_inner();
        #[cfg(test)]
        let owned_finst = instance_params
            .as_ref()
            .and_then(|params| params.fragment_instance_id.as_ref())
            .map(|id| crate::common::types::UniqueId {
                hi: id.hi,
                lo: id.lo,
            });
        let result = match (plan, instance_params) {
            (Some(plan), Some(instance_params)) => {
                #[cfg(test)]
                let execution_query_manager = self.execution_query_manager.clone();
                tokio::task::spawn_blocking(move || {
                    #[cfg(test)]
                    if let Some(manager) = execution_query_manager {
                        return crate::service::native_fragment_service::submit_exec_plan_fragment_native_with_manager(
                            plan,
                            instance_params,
                            manager,
                        );
                    }
                    crate::service::native_fragment_service::submit_exec_plan_fragment_native(
                        plan,
                        instance_params,
                    )
                })
            .await
            .map_err(|e| {
                tonic::Status::internal(format!("submit_fragment handler panicked: {e}"))
            })?
            }
            _ => Err("SubmitFragmentRequest requires native plan and instance_params".to_string()),
        };
        #[cfg(test)]
        if result.is_ok()
            && let (Some(owned), Some(finst_id)) =
                (self.execution_owned_finsts.as_ref(), owned_finst)
        {
            owned
                .lock()
                .expect("gRPC execution ownership lock")
                .insert(finst_id);
        }
        match result {
            Ok(()) => Ok(tonic::Response::new(
                proto::novarocks::SubmitFragmentResponse {
                    status_code: 0,
                    message: String::new(),
                },
            )),
            Err(e) => Ok(tonic::Response::new(
                proto::novarocks::SubmitFragmentResponse {
                    status_code: 1,
                    message: e,
                },
            )),
        }
    }

    async fn fetch_result(
        &self,
        request: tonic::Request<proto::novarocks::FetchResultRequest>,
    ) -> Result<tonic::Response<proto::novarocks::FetchResultResponse>, tonic::Status> {
        use proto::novarocks::fetch_result_response::Status as FetchStatus;

        self.require_local_execution("FetchResult")?;
        let req = request.into_inner();
        let finst_id = match req.finst_id {
            Some(id) => crate::UniqueId {
                hi: id.hi,
                lo: id.lo,
            },
            None => {
                return Ok(tonic::Response::new(
                    proto::novarocks::FetchResultResponse {
                        status: FetchStatus::Error as i32,
                        message: "missing finst_id in FetchResultRequest".to_string(),
                        packet_seq: 0,
                        eos: false,
                        result_arrow_ipc: vec![],
                    },
                ));
            }
        };
        let call_index = FETCH_RESULT_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        #[cfg(test)]
        if let Some(owned) = self.execution_owned_finsts.as_ref()
            && !owned
                .lock()
                .expect("gRPC execution ownership lock")
                .contains(&finst_id)
        {
            return Ok(tonic::Response::new(
                proto::novarocks::FetchResultResponse {
                    status: FetchStatus::Error as i32,
                    message: format!("fragment instance is not owned by this endpoint: {finst_id}"),
                    packet_seq: 0,
                    eos: false,
                    result_arrow_ipc: vec![],
                },
            ));
        }
        if crate::common::config::debug_fault_inject_fetch_not_ready_count()
            .is_some_and(|limit| call_index <= limit)
        {
            return Ok(tonic::Response::new(
                proto::novarocks::FetchResultResponse {
                    status: FetchStatus::NotReady as i32,
                    message: String::new(),
                    packet_seq: 0,
                    eos: false,
                    result_arrow_ipc: vec![],
                },
            ));
        }

        // wait_fetch_typed uses std::sync::Condvar::wait_timeout, which blocks
        // the OS thread for up to max_wait_ms. Offload to the blocking thread
        // pool so tonic worker threads remain free for I/O.
        use crate::runtime::result_buffer::{TryFetchTypedResult, wait_fetch_typed};
        let max_wait_ms = req.max_wait_ms;
        let fetch_result =
            tokio::task::spawn_blocking(move || wait_fetch_typed(finst_id, max_wait_ms))
                .await
                .map_err(|e| {
                    tonic::Status::internal(format!("fetch_result handler panicked: {e}"))
                })?;
        match fetch_result {
            TryFetchTypedResult::Ready(result) => {
                emit_grpc_typed_fetch_marker(FetchStatus::Ready as i32);
                Ok(tonic::Response::new(
                    proto::novarocks::FetchResultResponse {
                        status: FetchStatus::Ready as i32,
                        message: String::new(),
                        packet_seq: result.packet_seq,
                        eos: result.eos,
                        result_arrow_ipc: result.payload,
                    },
                ))
            }
            TryFetchTypedResult::NotReady => {
                emit_grpc_typed_fetch_marker(FetchStatus::NotReady as i32);
                Ok(tonic::Response::new(
                    proto::novarocks::FetchResultResponse {
                        status: FetchStatus::NotReady as i32,
                        message: String::new(),
                        packet_seq: 0,
                        eos: false,
                        result_arrow_ipc: vec![],
                    },
                ))
            }
            TryFetchTypedResult::Error(err) => {
                emit_grpc_typed_fetch_marker(FetchStatus::Error as i32);
                Ok(tonic::Response::new(
                    proto::novarocks::FetchResultResponse {
                        status: FetchStatus::Error as i32,
                        message: err.message,
                        packet_seq: 0,
                        eos: false,
                        result_arrow_ipc: vec![],
                    },
                ))
            }
        }
    }

    async fn cancel_fragment(
        &self,
        request: tonic::Request<proto::novarocks::CancelFragmentRequest>,
    ) -> Result<tonic::Response<proto::novarocks::CancelFragmentResponse>, tonic::Status> {
        self.require_local_execution("CancelFragment")?;
        let req = request.into_inner();
        if req.start_epoch != 0 && req.start_epoch != crate::runtime::start_epoch::start_epoch() {
            return Ok(tonic::Response::new(
                proto::novarocks::CancelFragmentResponse {
                    status_code: CANCEL_FRAGMENT_IGNORED_STALE_EPOCH,
                },
            ));
        }
        #[cfg(test)]
        if let Some(owned) = self.execution_owned_finsts.as_ref()
            && req.finst_ids.iter().any(|id| {
                !owned
                    .lock()
                    .expect("gRPC execution ownership lock")
                    .contains(&crate::common::types::UniqueId {
                        hi: id.hi,
                        lo: id.lo,
                    })
            })
        {
            return Ok(tonic::Response::new(
                proto::novarocks::CancelFragmentResponse {
                    status_code: CANCEL_FRAGMENT_NOT_OWNED,
                },
            ));
        }
        for id in &req.finst_ids {
            let finst_id = crate::UniqueId {
                hi: id.hi,
                lo: id.lo,
            };
            #[cfg(test)]
            if let Some(manager) = self.execution_query_manager.clone() {
                crate::service::fragment_control::cancel_with_manager(finst_id, manager);
                continue;
            }
            crate::cancel(finst_id);
        }
        if crate::common::config::debug_emit_cancel_marker() {
            let count = CANCEL_FRAGMENT_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
            println!(
                "NOVAROCKS_CANCEL count={} finsts={} reason={}",
                count,
                req.finst_ids.len(),
                req.reason
            );
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
        Ok(tonic::Response::new(
            proto::novarocks::CancelFragmentResponse {
                status_code: CANCEL_FRAGMENT_OK,
            },
        ))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<proto::novarocks::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::novarocks::HeartbeatResponse>, tonic::Status> {
        let _req = request.into_inner();
        let num_cores = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1);
        Ok(tonic::Response::new(proto::novarocks::HeartbeatResponse {
            start_epoch: crate::runtime::start_epoch::start_epoch(),
            version: crate::version::short_version().to_string(),
            num_cores,
            status_code: 0,
        }))
    }

    async fn report_exec_status(
        &self,
        request: tonic::Request<proto::novarocks::ReportExecStatusRequest>,
    ) -> Result<tonic::Response<proto::novarocks::ReportExecStatusResponse>, tonic::Status> {
        let report = request.into_inner().report;
        let report_handler = Arc::clone(&self.report_handler);
        let result = tokio::task::spawn_blocking(move || {
            let report = report.ok_or_else(|| {
                EngineError::protocol_decode("ReportExecStatusRequest missing report")
            })?;
            report_handler.handle_exec_status_report(report)?;
            Ok::<(), EngineError>(())
        })
        .await
        .map_err(|e| {
            tonic::Status::internal(format!("report_exec_status handler panicked: {e}"))
        })?;

        match result {
            Ok(()) => Ok(tonic::Response::new(
                proto::novarocks::ReportExecStatusResponse {
                    status_code: REPORT_EXEC_STATUS_OK,
                    message: String::new(),
                    error_code: String::new(),
                },
            )),
            Err(e) => Ok(tonic::Response::new(
                proto::novarocks::ReportExecStatusResponse {
                    status_code: e.to_report_status_code(),
                    message: e.to_user_message(),
                    error_code: e.to_report_error_code().to_string(),
                },
            )),
        }
    }

    async fn batch_report_exec_status(
        &self,
        request: tonic::Request<proto::novarocks::BatchReportExecStatusRequest>,
    ) -> Result<tonic::Response<proto::novarocks::BatchReportExecStatusResponse>, tonic::Status>
    {
        let reports = request.into_inner().reports;
        let report_handler = Arc::clone(&self.report_handler);
        let result = tokio::task::spawn_blocking(move || {
            for report in reports {
                report_handler.handle_exec_status_report(report)?;
            }
            Ok::<(), EngineError>(())
        })
        .await
        .map_err(|e| {
            tonic::Status::internal(format!("batch_report_exec_status handler panicked: {e}"))
        })?;

        match result {
            Ok(()) => Ok(tonic::Response::new(
                proto::novarocks::BatchReportExecStatusResponse {
                    status_code: REPORT_EXEC_STATUS_OK,
                    message: String::new(),
                    error_code: String::new(),
                },
            )),
            Err(e) => Ok(tonic::Response::new(
                proto::novarocks::BatchReportExecStatusResponse {
                    status_code: e.to_report_status_code(),
                    message: e.to_user_message(),
                    error_code: e.to_report_error_code().to_string(),
                },
            )),
        }
    }
}

fn emit_grpc_typed_fetch_marker(status: i32) {
    if crate::common::config::debug_emit_grpc_fragment_marker() {
        println!("NOVAROCKS_GRPC_FETCH_TYPED status={status}");
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

#[cfg(feature = "compat")]
#[derive(Default)]
pub struct StarletGrpcService;

#[cfg(feature = "compat")]
fn staros_ok_status() -> proto::staros::StarStatus {
    proto::staros::StarStatus {
        status_code: proto::staros::StatusCode::Ok as i32,
        error_msg: String::new(),
        extra_info: Vec::new(),
    }
}

#[cfg(feature = "compat")]
fn parse_add_shard_s3_config(
    path_info: &proto::staros::FilePathInfo,
) -> Result<Option<starlet_shard_registry::S3StoreConfig>, String> {
    starmgr::parse_s3_config_from_file_path_info(path_info)
}

#[cfg(feature = "compat")]
fn summarize_top_counts(counts: &HashMap<String, usize>, top_n: usize) -> String {
    if counts.is_empty() {
        return "-".to_string();
    }
    let mut entries = counts
        .iter()
        .map(|(key, count)| (key.clone(), *count))
        .collect::<Vec<_>>();
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    entries
        .into_iter()
        .take(top_n.max(1))
        .map(|(key, count)| format!("{key}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
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

    fn call(&mut self, req: axum::http::Request<axum::body::Body>) -> Self::Future {
        self.inner.call(req.map(boxed))
    }
}

#[cfg(feature = "compat")]
fn build_novarocks_http_app(grpc_routes: Routes) -> Router {
    grpc_routes
        .into_axum_router()
        .route(
            "/api/:db/:table/_stream_load",
            put(stream_load_http::handle_stream_load),
        )
        .route(
            "/api/transaction/load",
            put(stream_load_http::handle_transaction_load),
        )
        .route(
            "/api/transaction/:txn_op",
            post(stream_load_http::handle_transaction_op)
                .put(stream_load_http::handle_transaction_op),
        )
        .route(
            "/api/_load_tracking/:hi/:lo",
            get(load_tracking_http::handle_load_tracking_log),
        )
        .route("/metrics", get(metrics_http::handle_metrics))
}

#[cfg(not(feature = "compat"))]
fn build_novarocks_http_app(grpc_routes: Routes) -> Router {
    grpc_routes
        .into_axum_router()
        .route(
            "/api/_load_tracking/:hi/:lo",
            get(load_tracking_http::handle_load_tracking_log),
        )
        .route("/metrics", get(metrics_http::handle_metrics))
}

#[cfg(feature = "compat")]
#[tonic::async_trait]
impl proto::staros::starlet_server::Starlet for StarletGrpcService {
    async fn add_shard(
        &self,
        request: tonic::Request<proto::staros::AddShardRequest>,
    ) -> Result<tonic::Response<proto::staros::AddShardResponse>, tonic::Status> {
        let req = request.into_inner();
        starmgr::observe_starlet_service(&req.service_id);
        let worker_id = req.worker_id;
        let shard_count = req.shard_info.len();
        let shard_infos = req.shard_info;

        // AddShard may carry very large batches. Process in background so
        // heartbeat RPCs are not blocked by shard registry updates.
        tokio::task::spawn_blocking(move || {
            let mut updates = Vec::with_capacity(shard_infos.len());
            let mut invalid_shard_id = 0usize;
            let mut missing_full_path = 0usize;
            let mut invalid_s3_config = 0usize;
            let mut s3_config_count = 0usize;
            let mut s3_endpoint_counts: HashMap<String, usize> = HashMap::new();
            let mut s3_bucket_counts: HashMap<String, usize> = HashMap::new();
            for shard in &shard_infos {
                let Ok(shard_id) = i64::try_from(shard.shard_id) else {
                    invalid_shard_id += 1;
                    continue;
                };
                let Some(path_info) = shard.file_path_info.as_ref() else {
                    missing_full_path += 1;
                    continue;
                };
                if path_info.full_path.trim().is_empty() {
                    missing_full_path += 1;
                    continue;
                }
                let s3 = match parse_add_shard_s3_config(path_info) {
                    Ok(v) => v,
                    Err(err) => {
                        invalid_s3_config += 1;
                        warn!(
                            target: "novarocks::grpc",
                            shard_id,
                            error = %err,
                            "skip invalid AddShard S3 fs_info; only full_path is cached"
                        );
                        None
                    }
                };
                if let Some(cfg) = s3.as_ref() {
                    s3_config_count = s3_config_count.saturating_add(1);
                    *s3_endpoint_counts.entry(cfg.endpoint.clone()).or_insert(0) += 1;
                    *s3_bucket_counts.entry(cfg.bucket.clone()).or_insert(0) += 1;
                }
                updates.push((
                    shard_id,
                    starlet_shard_registry::StarletShardInfo {
                        full_path: path_info.full_path.clone(),
                        s3,
                    },
                ));
            }
            let upserted = starlet_shard_registry::upsert_many_infos(updates);
            info!(
                target: "novarocks::grpc",
                worker_id,
                shard_count,
                upserted,
                invalid_shard_id,
                missing_full_path,
                invalid_s3_config,
                s3_config_count,
                s3_endpoint_summary = %summarize_top_counts(&s3_endpoint_counts, 3),
                s3_bucket_summary = %summarize_top_counts(&s3_bucket_counts, 3),
                "processed starlet AddShard"
            );
        });

        info!(
            target: "novarocks::grpc",
            worker_id,
            shard_count,
            "accepted starlet AddShard"
        );
        Ok(tonic::Response::new(proto::staros::AddShardResponse {
            status: Some(staros_ok_status()),
        }))
    }

    async fn remove_shard(
        &self,
        request: tonic::Request<proto::staros::RemoveShardRequest>,
    ) -> Result<tonic::Response<proto::staros::RemoveShardResponse>, tonic::Status> {
        let req = request.into_inner();
        starmgr::observe_starlet_service(&req.service_id);
        let tablet_ids = req
            .shard_ids
            .iter()
            .filter_map(|id| i64::try_from(*id).ok())
            .collect::<Vec<_>>();
        let removed = starlet_shard_registry::remove_many(tablet_ids);
        info!(
            target: "novarocks::grpc",
            worker_id = req.worker_id,
            service_id = req.service_id,
            shard_count = req.shard_ids.len(),
            removed,
            "received starlet RemoveShard"
        );
        Ok(tonic::Response::new(proto::staros::RemoveShardResponse {
            status: Some(staros_ok_status()),
        }))
    }

    async fn starlet_heartbeat(
        &self,
        request: tonic::Request<proto::staros::StarletHeartbeatRequest>,
    ) -> Result<tonic::Response<proto::staros::StarletHeartbeatResponse>, tonic::Status> {
        let req = request.into_inner();
        starmgr::observe_starlet_heartbeat(
            &req.star_mgr_leader,
            &req.service_id,
            req.worker_group_id,
            req.worker_id,
        );
        info!(
            target: "novarocks::grpc",
            worker_id = req.worker_id,
            worker_group_id = req.worker_group_id,
            service_id = req.service_id,
            star_mgr_leader = req.star_mgr_leader,
            "received starlet StarletHeartbeat"
        );
        Ok(tonic::Response::new(
            proto::staros::StarletHeartbeatResponse {
                status: Some(staros_ok_status()),
            },
        ))
    }

    async fn write_cache(
        &self,
        request: tonic::Request<proto::staros::WriteCacheRequest>,
    ) -> Result<tonic::Response<proto::staros::WriteCacheResponse>, tonic::Status> {
        let req = request.into_inner();
        info!(
            target: "novarocks::grpc",
            shard_id = req.shard_id,
            payload_bytes = req.data.len(),
            "received starlet WriteCache"
        );
        Ok(tonic::Response::new(proto::staros::WriteCacheResponse {
            status: Some(staros_ok_status()),
        }))
    }
}

pub fn start_grpc_server(host: &str) -> Result<(), String> {
    #[cfg(feature = "compat")]
    {
        return start_grpc_server_on_ports(host, http_port(), grpc_port(), starlet_port());
    }
    #[cfg(not(feature = "compat"))]
    {
        start_grpc_http_server(host, http_port())
    }
}

#[cfg(not(feature = "compat"))]
fn start_grpc_http_server(host: &str, grpc_http_port: u16) -> Result<(), String> {
    {
        let state = grpc_server_state()
            .lock()
            .map_err(|_| "lock grpc server state failed".to_string())?;
        if state.started {
            return Ok(());
        }
    }

    let host = host.to_string();
    let std_listener = bind_tcp_listener(&host, grpc_http_port, "novarocks grpc/http")?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (failure_tx, failure_rx) = mpsc::channel();
    let stop_requested = Arc::new(AtomicBool::new(false));
    let stop_requested_for_thread = Arc::clone(&stop_requested);

    let join_handle = std::thread::spawn(move || {
        supervise_grpc_server_thread(stop_requested_for_thread, failure_tx, move || {
            info!(
                target: "novarocks::grpc",
                host = %host,
                http_port = grpc_http_port,
                "starting grpc server"
            );
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(8)
                .thread_stack_size(crate::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
                .build()
                .map_err(|error| format!("build grpc server runtime failed: {error}"))?;

            rt.block_on(async move {
                let listener = TokioTcpListener::from_std(std_listener)
                    .map_err(|error| format!("create grpc/http tokio listener failed: {error}"))?;
                let mut http_shutdown = shutdown_rx.clone();

                let svc = GrpcService::full_execution();
                let svc = proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(svc)
                    .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                let app = build_novarocks_http_app(Routes::new(svc));
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        while !*http_shutdown.borrow() {
                            if http_shutdown.changed().await.is_err() {
                                break;
                            }
                        }
                    })
                    .await
                    .map_err(|error| format!("grpc/http serve future failed: {error}"))
            })
        });
    });

    let mut state = grpc_server_state()
        .lock()
        .map_err(|_| "lock grpc server state failed".to_string())?;
    if state.started {
        return Ok(());
    }
    state.started = true;
    state.bound_port = Some(grpc_http_port);
    state.shutdown_tx = Some(shutdown_tx);
    state.join_handle = Some(join_handle);
    state.stop_requested = Some(stop_requested);
    state.failure_rx = Some(failure_rx);
    Ok(())
}

fn supervise_grpc_server_thread<F>(
    stop_requested: Arc<AtomicBool>,
    failure_tx: mpsc::Sender<String>,
    run: F,
) where
    F: FnOnce() -> Result<(), String>,
{
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
    if stop_requested.load(Ordering::Acquire) {
        return;
    }
    let detail = match outcome {
        Ok(Ok(())) => "serve future ended unexpectedly after readiness".to_string(),
        Ok(Err(error)) => format!("serve future failed after readiness: {error}"),
        Err(payload) => format!(
            "server thread panicked after readiness: {}",
            panic_payload_message(payload)
        ),
    };
    let _ = failure_tx.send(format!("grpc server {detail}"));
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|value| (*value).to_string())
        })
        .unwrap_or_else(|| "unknown panic payload".to_string())
}

#[cfg(feature = "compat")]
async fn supervise_compat_serve_futures<H, G, S>(
    http_server: H,
    grpc_server: G,
    starlet_server: S,
    stop_requested: Arc<AtomicBool>,
    failure_tx: mpsc::Sender<String>,
) where
    H: std::future::Future<Output = Result<(), String>>,
    G: std::future::Future<Output = Result<(), String>>,
    S: std::future::Future<Output = Result<(), String>>,
{
    let (service, result) = tokio::select! {
        result = http_server => ("http", result),
        result = grpc_server => ("grpc", result),
        result = starlet_server => ("starlet", result),
    };
    if stop_requested.load(Ordering::Acquire) {
        return;
    }
    let detail = match result {
        Ok(()) => "serve future ended unexpectedly after readiness".to_string(),
        Err(error) => format!("serve future failed after readiness: {error}"),
    };
    let _ = failure_tx.send(format!("grpc server {service} {detail}"));
}

#[cfg(feature = "compat")]
fn start_grpc_server_on_ports(
    host: &str,
    http_port: u16,
    grpc_port: u16,
    starlet_port: u16,
) -> Result<(), String> {
    {
        let state = grpc_server_state()
            .lock()
            .map_err(|_| "lock grpc server state failed".to_string())?;
        if state.started {
            return Ok(());
        }
    }
    validate_compat_grpc_ports(http_port, grpc_port, starlet_port)?;
    let http_listener = bind_tcp_listener(host, http_port, "novarocks http")?;
    let grpc_listener = bind_tcp_listener(host, grpc_port, "novarocks grpc")?;
    let starlet_listener = bind_tcp_listener(host, starlet_port, "starlet grpc")?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(8)
        .thread_stack_size(crate::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| format!("build compat grpc server runtime failed: {error}"))?;
    let host = host.to_string();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);
    let (failure_tx, failure_rx) = mpsc::channel();
    let stop_requested = Arc::new(AtomicBool::new(false));
    let stop_requested_for_thread = Arc::clone(&stop_requested);

    let join_handle = std::thread::Builder::new()
        .name("compat-grpc-server".to_string())
        .spawn(move || {
            runtime.block_on(async move {
                let http_listener = match TokioTcpListener::from_std(http_listener) {
                    Ok(listener) => listener,
                    Err(error) => {
                        let _ = ready_tx.send(Err(format!(
                            "create novarocks http tokio listener failed: {error}"
                        )));
                        return;
                    }
                };
                let grpc_listener = match TokioTcpListener::from_std(grpc_listener) {
                    Ok(listener) => listener,
                    Err(error) => {
                        let _ = ready_tx.send(Err(format!(
                            "create novarocks grpc tokio listener failed: {error}"
                        )));
                        return;
                    }
                };
                let starlet_listener = match TokioTcpListener::from_std(starlet_listener) {
                    Ok(listener) => listener,
                    Err(error) => {
                        let _ = ready_tx.send(Err(format!(
                            "create starlet grpc tokio listener failed: {error}"
                        )));
                        return;
                    }
                };
                info!(
                    target: "novarocks::grpc",
                    host = %host,
                    http_port,
                    grpc_port,
                    starlet_port,
                    "starting compat http and grpc servers"
                );

                let http_svc = proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(
                    GrpcService::full_execution(),
                )
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                let http_app = build_novarocks_http_app(Routes::new(http_svc));
                let grpc_svc = proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(
                    GrpcService::full_execution(),
                )
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                let starlet_svc =
                    proto::staros::starlet_server::StarletServer::new(StarletGrpcService)
                        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);

                let mut http_shutdown = shutdown_rx.clone();
                let mut grpc_shutdown = shutdown_rx.clone();
                let mut starlet_shutdown = shutdown_rx.clone();
                let http_server =
                    axum::serve(http_listener, http_app).with_graceful_shutdown(async move {
                        while !*http_shutdown.borrow() {
                            if http_shutdown.changed().await.is_err() {
                                break;
                            }
                        }
                    });
                let grpc_server = Server::builder()
                    .add_service(grpc_svc)
                    .serve_with_incoming_shutdown(
                        futures::stream::unfold(grpc_listener, |listener| async move {
                            let accepted = listener.accept().await.map(|(stream, _)| stream);
                            Some((accepted, listener))
                        }),
                        async move {
                            while !*grpc_shutdown.borrow() {
                                if grpc_shutdown.changed().await.is_err() {
                                    break;
                                }
                            }
                        },
                    );
                let starlet_server = Server::builder()
                    .add_service(starlet_svc)
                    .serve_with_incoming_shutdown(
                        futures::stream::unfold(starlet_listener, |listener| async move {
                            let accepted = listener.accept().await.map(|(stream, _)| stream);
                            Some((accepted, listener))
                        }),
                        async move {
                            while !*starlet_shutdown.borrow() {
                                if starlet_shutdown.changed().await.is_err() {
                                    break;
                                }
                            }
                        },
                    );
                if ready_tx.send(Ok(())).is_err() {
                    return;
                }
                let http_server =
                    async move { http_server.await.map_err(|error| error.to_string()) };
                let grpc_server =
                    async move { grpc_server.await.map_err(|error| error.to_string()) };
                let starlet_server =
                    async move { starlet_server.await.map_err(|error| error.to_string()) };
                supervise_compat_serve_futures(
                    http_server,
                    grpc_server,
                    starlet_server,
                    stop_requested_for_thread,
                    failure_tx,
                )
                .await;
            });
        })
        .map_err(|error| format!("spawn compat grpc server thread failed: {error}"))?;

    match ready_rx.recv() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let _ = join_handle.join();
            return Err(error);
        }
        Err(error) => {
            let _ = join_handle.join();
            return Err(format!("compat grpc readiness channel closed: {error}"));
        }
    }
    let mut state = grpc_server_state()
        .lock()
        .map_err(|_| "lock grpc server state failed".to_string())?;
    if state.started {
        let _ = shutdown_tx.send(true);
        let _ = join_handle.join();
        return Ok(());
    }
    state.started = true;
    state.bound_port = Some(grpc_port);
    state.shutdown_tx = Some(shutdown_tx);
    state.join_handle = Some(join_handle);
    state.stop_requested = Some(stop_requested);
    state.failure_rx = Some(failure_rx);
    Ok(())
}

pub fn grpc_server_bound_port() -> Result<u16, String> {
    if let Some(failure) = poll_grpc_server_failure()? {
        return Err(failure);
    }
    let state = grpc_server_state()
        .lock()
        .map_err(|_| "lock grpc server state failed".to_string())?;
    if !state.started {
        return Err("grpc server not started".to_string());
    }
    state
        .bound_port
        .ok_or_else(|| "grpc server bound port unavailable".to_string())
}

pub fn poll_grpc_server_failure() -> Result<Option<String>, String> {
    let mut state = grpc_server_state()
        .lock()
        .map_err(|_| "lock grpc server state failed".to_string())?;
    let Some(failure_rx) = state.failure_rx.as_ref() else {
        return Ok(None);
    };
    match failure_rx.try_recv() {
        Ok(failure) => {
            state.started = false;
            state.bound_port = None;
            state.failure_rx = None;
            Ok(Some(failure))
        }
        Err(mpsc::TryRecvError::Empty) => Ok(None),
        Err(mpsc::TryRecvError::Disconnected) => {
            let expected = state
                .stop_requested
                .as_ref()
                .is_some_and(|stop| stop.load(Ordering::Acquire));
            state.failure_rx = None;
            if expected {
                Ok(None)
            } else {
                state.started = false;
                state.bound_port = None;
                Ok(Some(
                    "grpc server supervisor exited unexpectedly after readiness".to_string(),
                ))
            }
        }
    }
}

fn join_grpc_server_thread(handle: JoinHandle<()>) -> Result<(), String> {
    handle.join().map_err(|payload| {
        let detail = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|value| (*value).to_string())
            })
            .unwrap_or_else(|| "unknown panic payload".to_string());
        format!("grpc server thread panicked: {detail}")
    })
}

pub fn stop_grpc_server() -> Result<(), String> {
    let (shutdown_tx, join_handle, stop_requested, failure_rx) = {
        let mut state = grpc_server_state()
            .lock()
            .map_err(|_| "lock grpc server state failed".to_string())?;
        if !state.started
            && state.shutdown_tx.is_none()
            && state.join_handle.is_none()
            && state.failure_rx.is_none()
        {
            return Ok(());
        }
        state.started = false;
        state.bound_port = None;
        (
            state.shutdown_tx.take(),
            state.join_handle.take(),
            state.stop_requested.take(),
            state.failure_rx.take(),
        )
    };

    if let Some(stop_requested) = stop_requested {
        stop_requested.store(true, Ordering::Release);
    }
    if let Some(tx) = shutdown_tx {
        let _ = tx.send(true);
    }
    let join_result = match join_handle {
        Some(handle) => join_grpc_server_thread(handle),
        None => Ok(()),
    };

    let mut failures = Vec::new();
    if let Some(receiver) = failure_rx {
        failures.extend(receiver.try_iter());
    }
    if let Err(error) = join_result {
        failures.push(error);
    }
    if !failures.is_empty() {
        return Err(failures.join("; "));
    }
    Ok(())
}

fn validate_grpc_ports(http_port: u16, starlet_port: u16) -> Result<(), String> {
    if http_port == starlet_port {
        return Err(format!(
            "invalid config: server.http_port ({http_port}) and server.starlet_port ({starlet_port}) must be different"
        ));
    }
    Ok(())
}

#[cfg(feature = "compat")]
fn validate_compat_grpc_ports(
    http_port: u16,
    grpc_port: u16,
    starlet_port: u16,
) -> Result<(), String> {
    validate_grpc_ports(http_port, starlet_port)?;
    if grpc_port == http_port || grpc_port == starlet_port {
        return Err(format!(
            "invalid config: server.grpc_port ({grpc_port}) must differ from server.http_port ({http_port}) and server.starlet_port ({starlet_port})"
        ));
    }
    Ok(())
}

/// Parse a gRPC bind address from a host string and port.
///
/// Handles bare IPv6 addresses (`::`, `::1`), bracketed IPv6 (`[::]`, `[::1]`),
/// and IPv4/hostname strings.  Bare and bracketed IPv6 forms are parsed via
/// `IpAddr` to avoid the `:::PORT` ambiguity that arises from naive
/// `format!("{host}:{port}")` string concatenation.
/// Build both gRPC server bind addresses from a single host string and two ports.
///
/// Uses [`parse_grpc_bind_addr`] for each port so bare IPv6 addresses like `::` and
/// `::1` are handled correctly, avoiding the `:::PORT` ambiguity produced by naive
/// `format!("{host}:{port}")` string concatenation.
pub(crate) fn grpc_server_bind_addrs(
    host: &str,
    http_port: u16,
    starlet_port: u16,
) -> Result<(SocketAddr, SocketAddr), String> {
    let http_addr = parse_grpc_bind_addr(host, http_port)
        .map_err(|e| format!("parse grpc/http bind addr failed: {e}"))?;
    let starlet_addr = parse_grpc_bind_addr(host, starlet_port)
        .map_err(|e| format!("parse starlet bind addr failed: {e}"))?;
    Ok((http_addr, starlet_addr))
}

pub(crate) fn parse_grpc_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    // Strip brackets from bracketed IPv6 literals, e.g. `[::1]` -> `::1`.
    let bare = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        host
    };

    // If the bare string is a valid IP literal, build SocketAddr directly.
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }

    // Fallback for hostnames: use bracketed form for any host containing `:`.
    let formatted = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    formatted
        .parse::<SocketAddr>()
        .map_err(|e| format!("parse gRPC bind addr '{formatted}' failed: {e}"))
}

fn ensure_bindable(host: &str, port: u16, role: &str) -> Result<(), String> {
    drop(bind_tcp_listener(host, port, role)?);
    Ok(())
}

fn bind_tcp_listener(host: &str, port: u16, role: &str) -> Result<TcpListener, String> {
    let addr = parse_grpc_bind_addr(host, port)
        .map_err(|e| format!("parse {role} bind addr failed: {e}"))?;
    let listener = TcpListener::bind(addr)
        .map_err(|e| format!("failed to bind {role} listener on {addr}: {e}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("failed to configure {role} listener on {addr}: {e}"))?;
    Ok(listener)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StandaloneGrpcMode {
    FullExecution,
    ReportOnly,
}

impl StandaloneGrpcMode {
    fn service(self) -> GrpcService {
        match self {
            StandaloneGrpcMode::FullExecution => GrpcService::full_execution(),
            StandaloneGrpcMode::ReportOnly => GrpcService::report_only(),
        }
    }

    fn label(self) -> &'static str {
        match self {
            StandaloneGrpcMode::FullExecution => "standalone grpc report/exchange",
            StandaloneGrpcMode::ReportOnly => "standalone grpc report-only",
        }
    }
}

/// Start a lightweight gRPC exchange/report server on a specific port.
///
/// Unlike [`start_grpc_server`] this does not require global config to be
/// initialised — the caller supplies the bind address directly.
pub fn start_grpc_exchange_server(host: &str, port: u16) -> Result<(), String> {
    start_standalone_grpc_server(host, port, StandaloneGrpcMode::FullExecution)
}

/// Start a report-only standalone NovaRocksGrpc endpoint on a specific port.
pub fn start_grpc_report_server(host: &str, port: u16) -> Result<(), String> {
    start_standalone_grpc_server(host, port, StandaloneGrpcMode::ReportOnly)
}

fn start_standalone_grpc_server(
    host: &str,
    port: u16,
    mode: StandaloneGrpcMode,
) -> Result<(), String> {
    {
        let mut state = grpc_server_state()
            .lock()
            .map_err(|_| "lock grpc server state failed".to_string())?;
        if state.started {
            return Ok(());
        }
        if state.starting {
            return Err("grpc server startup already in progress".to_string());
        }
        state.starting = true;
    }
    #[cfg(test)]
    pause_standalone_grpc_startup_after_reservation();

    let host = host.to_string();
    let std_listener = match bind_tcp_listener(&host, port, mode.label()) {
        Ok(listener) => listener,
        Err(error) => {
            clear_grpc_server_startup_reservation();
            return Err(error);
        }
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (failure_tx, failure_rx) = mpsc::channel();
    let stop_requested = Arc::new(AtomicBool::new(false));
    let stop_requested_for_thread = Arc::clone(&stop_requested);

    let join_handle = std::thread::spawn(move || {
        supervise_grpc_server_thread(stop_requested_for_thread, failure_tx, move || {
            info!(
                target: "novarocks::grpc",
                host = %host,
                port = port,
                mode = ?mode,
                "starting standalone grpc server"
            );
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(8)
                .thread_stack_size(crate::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
                .build()
                .map_err(|error| format!("build standalone grpc server runtime failed: {error}"))?;

            rt.block_on(async move {
                let listener = TokioTcpListener::from_std(std_listener).map_err(|error| {
                    format!("create standalone grpc/http tokio listener failed: {error}")
                })?;
                let mut shutdown = shutdown_rx.clone();

                let svc = mode.service();
                let svc = proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(svc)
                    .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                let grpc_path = format!(
                    "/{}/*rest",
                    <proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer<GrpcService> as NamedService>::NAME
                );
                let grpc_service = AxumGrpcService::new(svc);
                let app = Router::new()
                    .route_service(&grpc_path, grpc_service)
                    .route(
                        "/api/_load_tracking/:hi/:lo",
                        get(load_tracking_http::handle_load_tracking_log),
                    )
                    .route("/metrics", get(metrics_http::handle_metrics))
                    .fallback(grpc_unimplemented_fallback);
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        while !*shutdown.borrow() {
                            if shutdown.changed().await.is_err() {
                                break;
                            }
                        }
                    })
                    .await
                    .map_err(|error| format!("standalone grpc serve future failed: {error}"))
            })
        });
    });

    let mut state = match grpc_server_state().lock() {
        Ok(state) => state,
        Err(_) => {
            stop_requested.store(true, Ordering::Release);
            let _ = shutdown_tx.send(true);
            let _ = join_grpc_server_thread(join_handle);
            clear_grpc_server_startup_reservation();
            return Err("lock grpc server state failed".to_string());
        }
    };
    debug_assert!(state.starting, "standalone grpc start lost its reservation");
    state.starting = false;
    state.started = true;
    state.bound_port = Some(port);
    state.shutdown_tx = Some(shutdown_tx);
    state.join_handle = Some(join_handle);
    state.stop_requested = Some(stop_requested);
    state.failure_rx = Some(failure_rx);
    Ok(())
}

fn clear_grpc_server_startup_reservation() {
    if let Ok(mut state) = grpc_server_state().lock() {
        state.starting = false;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IndependentGrpcRuntimeFilterNode, IndependentGrpcStartupProbe, ensure_bindable,
        parse_grpc_bind_addr, validate_grpc_ports,
    };
    use crate::runtime::query_context::QueryId;
    use prost::Message;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, TcpListener};
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;

    #[derive(Clone, PartialEq, Message)]
    struct RawSubmitFragmentRequest {
        #[prost(bytes = "vec", tag = "2")]
        instance_params: Vec<u8>,
        #[prost(bytes = "vec", tag = "7")]
        outer_unknown_seven: Vec<u8>,
    }

    fn instance_params_wire(include_legacy_tag: bool) -> Vec<u8> {
        let mut bytes = vec![
            0x0a, 0x04, // query_id, length-delimited
            0x08, 42, // query_id.hi
            0x10, 43, // query_id.lo
        ];
        if include_legacy_tag {
            bytes.extend_from_slice(&[0x3a, 0x01, 0xff]);
        }
        bytes
    }

    async fn send_raw_submit(
        endpoint: std::net::SocketAddr,
        request: RawSubmitFragmentRequest,
    ) -> Result<tonic::Response<crate::proto::novarocks::SubmitFragmentResponse>, tonic::Status>
    {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{endpoint}"))
            .expect("raw submit endpoint")
            .connect()
            .await
            .expect("connect raw submit client");
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.expect("raw submit client ready");
        client
            .unary(
                tonic::Request::new(request),
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/novarocks.NovaRocksGrpc/SubmitFragment",
                ),
                tonic::codec::ProstCodec::<
                    RawSubmitFragmentRequest,
                    crate::proto::novarocks::SubmitFragmentResponse,
                >::default(),
            )
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generated_server_rejects_legacy_instance_tag_before_handler_or_query_state() {
        let node = IndependentGrpcRuntimeFilterNode::start().expect("start generated gRPC server");
        let query_id = QueryId { hi: 42, lo: 43 };

        let error = send_raw_submit(
            node.endpoint(),
            RawSubmitFragmentRequest {
                instance_params: instance_params_wire(true),
                outer_unknown_seven: Vec::new(),
            },
        )
        .await
        .expect_err("legacy direct InstanceParams tag 7 must fail in the generated codec");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("tag 7"), "{error}");
        assert_eq!(node.submit_fragment_handler_calls(), 0);
        assert_eq!(node.manager().fragment_counts_for_test(query_id), None);
        let service = node.manager().runtime_filter_service_for_ingress(query_id);
        let pending = service
            .as_ref()
            .map(|service| service.transport_pending_len_for_test())
            .unwrap_or(0);
        assert_eq!(
            pending, 0,
            "codec rejection must not enqueue transport work"
        );
        assert!(
            service.is_none(),
            "codec rejection must not create a query-owned Service"
        );
        let response = send_raw_submit(
            node.endpoint(),
            RawSubmitFragmentRequest {
                instance_params: instance_params_wire(false),
                outer_unknown_seven: Vec::new(),
            },
        )
        .await
        .expect("current request must reach the handler")
        .into_inner();
        assert_ne!(response.status_code, 0, "missing plan is a business error");
        assert_eq!(node.submit_fragment_handler_calls(), 1);

        let response = send_raw_submit(
            node.endpoint(),
            RawSubmitFragmentRequest {
                instance_params: instance_params_wire(false),
                outer_unknown_seven: vec![1],
            },
        )
        .await
        .expect("outer request tag 7 must remain an ordinary unknown field")
        .into_inner();
        assert_ne!(response.status_code, 0, "missing plan is a business error");
        assert_eq!(node.submit_fragment_handler_calls(), 2);
    }

    #[test]
    fn independent_runtime_filter_start_failure_stops_all_started_threads() {
        let (manager_tx, manager_rx) = mpsc::sync_channel(1);
        let (server_exited_tx, server_exited_rx) = mpsc::sync_channel(1);
        let (clean_exited_tx, clean_exited_rx) = mpsc::sync_channel(1);
        let result =
            IndependentGrpcRuntimeFilterNode::start_with_probe(Some(IndependentGrpcStartupProbe {
                manager: manager_tx,
                server_exited: server_exited_tx,
                clean_exited: clean_exited_tx,
                panic_before_ready: true,
            }));
        assert!(result.is_err(), "injected pre-ready failure must surface");
        let manager = manager_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("startup publishes the manager weak reference");
        server_exited_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("server thread exits after injected startup failure");
        let clean_exited_before_cleanup = clean_exited_rx
            .recv_timeout(Duration::from_millis(100))
            .is_ok();
        let leaked_manager = manager.upgrade();
        if let Some(manager) = &leaked_manager {
            manager.stop_clean_loop_for_test();
            let _ = clean_exited_rx.recv_timeout(Duration::from_secs(1));
        }
        assert!(
            clean_exited_before_cleanup,
            "start returning Err must stop the manager clean loop"
        );
        assert!(
            leaked_manager.is_none(),
            "start returning Err must release the manager"
        );
    }

    #[test]
    fn independent_runtime_filter_shutdown_shares_one_deadline_across_threads() {
        let server = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(100));
        });
        let clean = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(1));
        });
        let wait = Duration::from_millis(150);
        let started_at = std::time::Instant::now();

        let failures =
            super::join_independent_runtime_filter_threads(Some(server), Some(clean), wait);
        let elapsed = started_at.elapsed();

        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("manager clean loop"), "{failures:?}");
        assert!(
            elapsed < Duration::from_millis(225),
            "server and clean loop each received a fresh deadline: elapsed={elapsed:?}"
        );
    }

    #[test]
    fn grpc_stop_join_propagates_server_thread_panic() {
        let handle = std::thread::spawn(|| panic!("injected grpc server panic"));
        let error = super::join_grpc_server_thread(handle)
            .expect_err("grpc server thread panic must reach stop caller");
        assert!(error.contains("panicked"), "{error}");
        assert!(error.contains("injected grpc server panic"), "{error}");
    }

    #[test]
    fn grpc_supervisor_reports_post_ready_serve_exit() {
        let (failure_tx, failure_rx) = mpsc::channel();
        super::supervise_grpc_server_thread(
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            failure_tx,
            || Ok(()),
        );

        let failure = failure_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("unexpected post-ready serve exit must be reported");
        assert!(failure.contains("grpc server"), "{failure}");
        assert!(
            failure.contains("ended unexpectedly after readiness"),
            "{failure}"
        );
    }

    #[test]
    fn grpc_supervisor_reports_post_ready_panic() {
        let (failure_tx, failure_rx) = mpsc::channel();
        super::supervise_grpc_server_thread(
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            failure_tx,
            || -> Result<(), String> { panic!("injected post-ready grpc panic") },
        );

        let failure = failure_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("post-ready panic must be reported");
        assert!(failure.contains("panicked after readiness"), "{failure}");
        assert!(
            failure.contains("injected post-ready grpc panic"),
            "{failure}"
        );
    }

    #[test]
    fn grpc_poll_reports_post_ready_supervisor_error() {
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let mut state = super::grpc_server_state()
                .lock()
                .expect("lock grpc server state");
            *state = super::GrpcServerState {
                started: true,
                starting: false,
                bound_port: Some(19080),
                shutdown_tx: None,
                join_handle: None,
                stop_requested: Some(std::sync::Arc::clone(&stop_requested)),
                failure_rx: Some(failure_rx),
            };
        }

        super::supervise_grpc_server_thread(stop_requested, failure_tx, || {
            Err("injected runtime/serve failure".to_string())
        });

        let failure = super::poll_grpc_server_failure()
            .expect("poll grpc server failure")
            .expect("post-ready supervisor error must be observable");
        assert!(
            failure.contains("injected runtime/serve failure"),
            "{failure}"
        );

        *super::grpc_server_state()
            .lock()
            .expect("reset grpc server state") = super::GrpcServerState::default();
    }

    #[test]
    fn grpc_poll_reports_unexpected_post_ready_supervisor_exit() {
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let mut state = super::grpc_server_state()
                .lock()
                .expect("lock grpc server state");
            *state = super::GrpcServerState {
                started: true,
                starting: false,
                bound_port: Some(19080),
                shutdown_tx: None,
                join_handle: None,
                stop_requested: Some(stop_requested),
                failure_rx: Some(failure_rx),
            };
        }
        drop(failure_tx);

        let failure = super::poll_grpc_server_failure()
            .expect("poll grpc server failure")
            .expect("unexpected post-ready exit must be observable");
        assert!(failure.contains("grpc server supervisor"), "{failure}");
        assert!(
            failure.contains("unexpectedly after readiness"),
            "{failure}"
        );

        *super::grpc_server_state()
            .lock()
            .expect("reset grpc server state") = super::GrpcServerState::default();
    }

    #[test]
    fn grpc_poll_ignores_requested_stop_after_supervisor_exit() {
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        {
            let mut state = super::grpc_server_state()
                .lock()
                .expect("lock grpc server state");
            *state = super::GrpcServerState {
                started: false,
                starting: false,
                bound_port: None,
                shutdown_tx: None,
                join_handle: None,
                stop_requested: Some(stop_requested),
                failure_rx: Some(failure_rx),
            };
        }
        drop(failure_tx);

        assert_eq!(
            super::poll_grpc_server_failure().expect("poll grpc server failure"),
            None,
            "requested stop must not be reported as a supervisor failure"
        );

        *super::grpc_server_state()
            .lock()
            .expect("reset grpc server state") = super::GrpcServerState::default();
    }

    #[test]
    fn concurrent_standalone_grpc_start_releases_the_losing_listener() {
        fn unused_port() -> u16 {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
            let port = listener.local_addr().expect("read ephemeral port").port();
            drop(listener);
            port
        }

        let first_port = unused_port();
        let second_port = unused_port();
        super::STANDALONE_GRPC_STARTUP_RESERVATIONS.store(0, Ordering::Release);
        super::PAUSE_STANDALONE_GRPC_STARTUP_AFTER_RESERVATION.store(true, Ordering::Release);

        let first =
            std::thread::spawn(move || super::start_grpc_exchange_server("127.0.0.1", first_port));
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while super::STANDALONE_GRPC_STARTUP_RESERVATIONS.load(Ordering::Acquire) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "first startup did not reserve gRPC lifecycle ownership"
            );
            std::thread::yield_now();
        }

        let second =
            std::thread::spawn(move || super::start_grpc_exchange_server("127.0.0.1", second_port));
        let second_deadline = std::time::Instant::now() + Duration::from_millis(100);
        while super::STANDALONE_GRPC_STARTUP_RESERVATIONS.load(Ordering::Acquire) < 2
            && std::time::Instant::now() < second_deadline
        {
            std::thread::yield_now();
        }
        super::PAUSE_STANDALONE_GRPC_STARTUP_AFTER_RESERVATION.store(false, Ordering::Release);

        let first_result = first.join().expect("first startup thread panicked");
        let second_result = second.join().expect("second startup thread panicked");
        let successful_starts =
            usize::from(first_result.is_ok()) + usize::from(second_result.is_ok());
        let failed_starts =
            usize::from(first_result.is_err()) + usize::from(second_result.is_err());
        let rejected_start = first_result
            .as_ref()
            .err()
            .or_else(|| second_result.as_ref().err());
        let stop_result = super::stop_grpc_server();
        let first_released = TcpListener::bind(("127.0.0.1", first_port)).is_ok();
        let second_released = TcpListener::bind(("127.0.0.1", second_port)).is_ok();

        assert_eq!(successful_starts, 1, "only the owner may start a listener");
        assert_eq!(
            failed_starts, 1,
            "the concurrent non-owner must be rejected"
        );
        assert!(
            rejected_start
                .expect("concurrent non-owner must return an error")
                .contains("startup already in progress")
        );
        assert!(
            stop_result.is_ok(),
            "owner shutdown failed: {stop_result:?}"
        );
        assert!(first_released, "first listener was not released");
        assert!(second_released, "second listener was not released");
    }

    #[test]
    fn test_validate_grpc_ports_accept_distinct_ports() {
        assert!(validate_grpc_ports(8040, 9070).is_ok());
    }

    #[test]
    fn test_validate_grpc_ports_reject_same_port() {
        let err = validate_grpc_ports(8040, 8040).expect_err("expected same-port validation error");
        assert!(err.contains("must be different"));
    }

    #[test]
    fn test_ensure_bindable_fails_for_occupied_port() {
        let occupied = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral test port");
        let occupied_port = occupied.local_addr().expect("get local addr").port();
        let err = ensure_bindable("127.0.0.1", occupied_port, "unit-test")
            .expect_err("expected bind failure");
        assert!(err.contains("failed to bind"));
        drop(occupied);
    }

    #[test]
    fn parse_grpc_bind_addr_bare_ipv6_wildcard() {
        let addr = parse_grpc_bind_addr("::", 9070).expect("parse :: wildcard");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_bracketed_ipv6_wildcard() {
        let addr = parse_grpc_bind_addr("[::]", 9070).expect("parse [::] wildcard");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_bracketed_ipv6_loopback() {
        let addr = parse_grpc_bind_addr("[::1]", 9070).expect("parse [::1]");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_bare_ipv6_loopback() {
        let addr = parse_grpc_bind_addr("::1", 9070).expect("parse ::1");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_ipv4() {
        let addr = parse_grpc_bind_addr("127.0.0.1", 9070).expect("parse 127.0.0.1");
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_ipv4_wildcard() {
        let addr = parse_grpc_bind_addr("0.0.0.0", 9070).expect("parse 0.0.0.0");
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 9070);
    }

    // --- PR-4 regression: grpc_server_bind_addrs must use safe addr construction ---

    #[test]
    fn grpc_server_bind_addrs_bare_ipv6_wildcard_two_ports() {
        let (http, starlet) =
            super::grpc_server_bind_addrs("::", 8040, 9070).expect("bare :: two ports");
        assert_eq!(http.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(http.port(), 8040);
        assert_eq!(starlet.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(starlet.port(), 9070);
    }

    #[test]
    fn grpc_server_bind_addrs_bare_ipv6_loopback_two_ports() {
        let (http, starlet) =
            super::grpc_server_bind_addrs("::1", 8040, 9070).expect("bare ::1 two ports");
        assert_eq!(http.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(http.port(), 8040);
        assert_eq!(starlet.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(starlet.port(), 9070);
    }

    #[test]
    fn grpc_server_bind_addrs_ipv4_two_ports() {
        let (http, starlet) =
            super::grpc_server_bind_addrs("127.0.0.1", 8040, 9070).expect("ipv4 two ports");
        assert_eq!(http.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(http.port(), 8040);
        assert_eq!(starlet.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(starlet.port(), 9070);
    }
}

#[cfg(test)]
mod pr3_tests {
    use super::GrpcService;
    use super::proto;
    use super::proto::common::{Status as ProtoStatus, UniqueId as ProtoUniqueId};
    use super::proto::novarocks::fetch_result_response::Status as FetchStatus;
    use super::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc as _;
    use super::proto::novarocks::{
        BatchReportExecStatusRequest, CancelFragmentRequest, ExchangeRequest, ExecStatusReport,
        FetchResultRequest, HeartbeatRequest, IcebergCommitInfo, IcebergDataFile,
        IcebergFileContent, ReportExecStatusRequest, SubmitFragmentRequest,
    };
    use super::proto::{novarocks, plan};
    use crate::common::engine_error::EngineError;
    use crate::common::types::UniqueId;
    use crate::coordinator::ports::CoordinatorReportHandler;
    use crate::protocol::native::{
        RuntimeFilterQueryLifecycleOptions, encode_abort_runtime_filter_deployment,
        encode_participant_install,
    };
    use crate::runtime::query_context::runtime_filter_service_lifecycle_tests::participant_install;
    use crate::runtime::query_context::{QueryContextManager, QueryId};
    use crate::runtime_filter::port::transport::{
        RuntimeFilterEnvelope, RuntimeFilterEnvelopeIngress, RuntimeFilterIngressResult,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tonic::Request;

    struct CapturingReportHandler {
        reports: Mutex<Vec<ExecStatusReport>>,
        fail_on_call: Option<(usize, EngineError)>,
    }

    impl CapturingReportHandler {
        fn accepting() -> Self {
            Self {
                reports: Mutex::new(Vec::new()),
                fail_on_call: None,
            }
        }

        fn failing(error: EngineError) -> Self {
            Self::failing_on_call(1, error)
        }

        fn failing_on_call(call: usize, error: EngineError) -> Self {
            Self {
                reports: Mutex::new(Vec::new()),
                fail_on_call: Some((call, error)),
            }
        }
    }

    impl CoordinatorReportHandler for CapturingReportHandler {
        fn handle_exec_status_report(&self, report: ExecStatusReport) -> Result<(), EngineError> {
            let mut reports = self.reports.lock().expect("capture reports");
            reports.push(report);
            match &self.fail_on_call {
                Some((call, error)) if reports.len() == *call => Err(error.clone()),
                _ => Ok(()),
            }
        }
    }

    struct RecordingEnvelopeIngress {
        calls: AtomicUsize,
        result: RuntimeFilterIngressResult,
    }

    impl RecordingEnvelopeIngress {
        fn accepting() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                result: RuntimeFilterIngressResult::accepted(),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl RuntimeFilterEnvelopeIngress for RecordingEnvelopeIngress {
        fn accept(&self, _envelope: RuntimeFilterEnvelope) -> RuntimeFilterIngressResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.result.clone()
        }
    }

    fn valid_runtime_filter_envelope() -> proto::filter::RuntimeFilterEnvelope {
        proto::filter::RuntimeFilterEnvelope {
            kind: proto::filter::RuntimeFilterEnvelopeKind::Contribution as i32,
            query_id: Some(ProtoUniqueId { hi: 11, lo: 12 }),
            channel_id: 13,
            deployment_epoch: 14,
            route_identity: Some(proto::filter::RuntimeFilterRouteIdentity {
                value: Some(
                    proto::filter::runtime_filter_route_identity::Value::Contribution(
                        proto::filter::RuntimeFilterContributionRouteIdentity {
                            producer_binding_id: 15,
                            fragment_instance_id: Some(ProtoUniqueId { hi: 16, lo: 17 }),
                            partition_id: 18,
                            sequence: 19,
                        },
                    ),
                ),
            }),
            schema_digest: vec![20; 32],
            payload: b"contribution".to_vec(),
            producer_open: Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                local_partition_count: 21,
            }),
        }
    }

    fn id(hi: i64, lo: i64) -> UniqueId {
        UniqueId { hi, lo }
    }

    fn empty_values_result_fragment(fragment_id: u32, root_node_id: i32) -> plan::PlanFragment {
        plan::PlanFragment {
            fragment_id,
            root: Some(plan::DistributedNode {
                node_id: root_node_id,
                fragment_id,
                limit: -1,
                payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                    output_columns: Vec::new(),
                    kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                        rows: Vec::new(),
                        columns: Vec::new(),
                    })),
                })),
                ..Default::default()
            }),
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::Result(true)),
            }),
            output_columns: Vec::new(),
            runtime_filter_bindings: Some(plan::RuntimeFilterBindingTable {
                fragment_id,
                bindings: Vec::new(),
            }),
            ..Default::default()
        }
    }

    fn ok_report(query: UniqueId, finst: UniqueId) -> ExecStatusReport {
        ExecStatusReport {
            query_id: Some(ProtoUniqueId {
                hi: query.hi,
                lo: query.lo,
            }),
            fragment_instance_id: Some(ProtoUniqueId {
                hi: finst.hi,
                lo: finst.lo,
            }),
            backend_num: 0,
            status: Some(ProtoStatus {
                code: 0,
                message: String::new(),
            }),
            done: true,
            iceberg_commits: Vec::new(),
            loaded_rows: 0,
            sink_load_bytes: 0,
            filtered_rows: 0,
            profile: None,
        }
    }

    fn write_report(query: UniqueId, finst: UniqueId) -> ExecStatusReport {
        let mut report = ok_report(query, finst);
        report.iceberg_commits = vec![IcebergCommitInfo {
            iceberg_data_file: Some(IcebergDataFile {
                path: Some("s3://w/grpc-query-gone.parquet".to_string()),
                format: Some("parquet".to_string()),
                record_count: Some(1),
                file_size_in_bytes: Some(1),
                partition_spec_id: Some(0),
                file_content: IcebergFileContent::Data as i32,
                ..Default::default()
            }),
            is_overwrite: None,
            is_rewrite: None,
        }];
        report
    }

    fn error_report(query: UniqueId, finst: UniqueId, message: &str) -> ExecStatusReport {
        let mut report = ok_report(query, finst);
        report.status = Some(ProtoStatus {
            code: 1,
            message: message.to_string(),
        });
        report
    }

    #[tokio::test]
    async fn runtime_filter_envelope_full_execution_invokes_ingress_and_accepts() {
        let ingress = Arc::new(RecordingEnvelopeIngress::accepting());
        let svc = GrpcService::full_execution_with_handlers(
            Arc::new(CapturingReportHandler::accepting()),
            ingress.clone(),
        );

        let response = svc
            .transmit_runtime_filter_envelope(Request::new(valid_runtime_filter_envelope()))
            .await
            .expect("valid envelope must be handled")
            .into_inner();

        assert_eq!(
            response.accept_status,
            proto::filter::RuntimeFilterAcceptStatus::Accepted as i32
        );
        assert_eq!(ingress.calls(), 1);
    }

    #[tokio::test]
    async fn runtime_filter_envelope_report_only_rejects_before_ingress() {
        let ingress = Arc::new(RecordingEnvelopeIngress::accepting());
        let svc = GrpcService::report_only_with_handlers(
            Arc::new(CapturingReportHandler::accepting()),
            ingress.clone(),
        );

        let error = svc
            .transmit_runtime_filter_envelope(Request::new(valid_runtime_filter_envelope()))
            .await
            .expect_err("report-only service must reject local envelope ingress");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(ingress.calls(), 0);
    }

    #[tokio::test]
    async fn runtime_filter_envelope_report_only_gate_precedes_wire_validation() {
        let ingress = Arc::new(RecordingEnvelopeIngress::accepting());
        let svc = GrpcService::report_only_with_handlers(
            Arc::new(CapturingReportHandler::accepting()),
            ingress.clone(),
        );
        let mut request = valid_runtime_filter_envelope();
        request.channel_id = 0;
        request.route_identity = None;

        let error = svc
            .transmit_runtime_filter_envelope(Request::new(request))
            .await
            .expect_err("report-only gate must run before envelope decoding");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(ingress.calls(), 0);
    }

    #[tokio::test]
    async fn runtime_filter_envelope_default_full_execution_rejects_unknown_query() {
        // The default full-execution constructor now installs the query-scoped
        // producer ingress. An envelope for a query the global manager does not
        // know is a normal query-unavailable rejection, no longer the
        // "ingress is not configured" placeholder.
        let response = GrpcService::full_execution()
            .transmit_runtime_filter_envelope(Request::new(valid_runtime_filter_envelope()))
            .await
            .expect("query-unavailable rejection is a normal response")
            .into_inner();

        assert_eq!(
            response.accept_status,
            proto::filter::RuntimeFilterAcceptStatus::Rejected as i32
        );
        assert_ne!(
            response.rejection_reason, "runtime filter envelope ingress is not configured",
            "default ingress must no longer be the unconfigured placeholder"
        );
        assert!(
            response.rejection_reason.contains("[query-unavailable]"),
            "unknown query must surface the query-unavailable prefix: {}",
            response.rejection_reason
        );
    }

    #[tokio::test]
    async fn runtime_filter_envelope_default_report_only_rejects_before_query_scoped_ingress() {
        // report-only rejects at the local-execution gate before the query-scoped
        // ingress is consulted: the response is a gRPC error, not a normal
        // query-unavailable rejection response.
        let error = GrpcService::report_only()
            .transmit_runtime_filter_envelope(Request::new(valid_runtime_filter_envelope()))
            .await
            .expect_err("report-only endpoint must reject local envelope ingress");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn deployment_handlers_publish_installed_and_aborted_on_independent_manager() {
        let manager = QueryContextManager::new_for_test();
        let svc = GrpcService::full_execution_with_runtime_filter_manager(
            Arc::new(CapturingReportHandler::accepting()),
            manager.clone(),
        );
        let query = QueryId {
            hi: 92_201,
            lo: 92_202,
        };
        let install = participant_install();
        let lifecycle = RuntimeFilterQueryLifecycleOptions {
            delivery_expire: std::time::Duration::from_secs(11),
            query_expire: std::time::Duration::from_secs(29),
            transport_retry_interval: std::time::Duration::from_millis(200),
            transport_max_attempts: 3,
            transport_deadline: std::time::Duration::from_secs(5),
            transport_max_pending_entries: 128,
            transport_max_pending_bytes: 1024 * 1024,
        };
        let wire_query = UniqueId {
            hi: query.hi,
            lo: query.lo,
        };

        let install_response = svc
            .install_runtime_filter_deployment(Request::new(
                encode_participant_install(wire_query, lifecycle, &install)
                    .expect("encode install"),
            ))
            .await
            .expect("install handler")
            .into_inner();
        assert_eq!(
            install_response.status,
            proto::filter::RuntimeFilterDeploymentResponseStatus::Applied as i32
        );
        assert!(manager.runtime_filter_deployment_is_installed_for_test(query));

        let abort_response = svc
            .abort_runtime_filter_deployment(Request::new(
                encode_abort_runtime_filter_deployment(wire_query, install.epoch())
                    .expect("encode abort"),
            ))
            .await
            .expect("abort handler")
            .into_inner();
        assert_eq!(
            abort_response.status,
            proto::filter::RuntimeFilterDeploymentResponseStatus::Applied as i32
        );
        assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));
    }

    #[tokio::test]
    async fn runtime_filter_envelope_malformed_request_never_reaches_ingress() {
        let ingress = Arc::new(RecordingEnvelopeIngress::accepting());
        let svc = GrpcService::full_execution_with_handlers(
            Arc::new(CapturingReportHandler::accepting()),
            ingress.clone(),
        );
        let mut request = valid_runtime_filter_envelope();
        request.channel_id = 0;

        let error = svc
            .transmit_runtime_filter_envelope(Request::new(request))
            .await
            .expect_err("malformed envelope must fail validation");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(ingress.calls(), 0);
    }

    #[tokio::test]
    async fn exchange_unary_decode_error_returns_native_status_not_rpc_error() {
        let svc = GrpcService::default();
        let resp = svc
            .exchange_unary(Request::new(ExchangeRequest {
                finst_id_hi: 11,
                finst_id_lo: 22,
                node_id: 7,
                sender_id: 3,
                be_number: 9,
                eos: false,
                sequence: 42,
                payload: vec![1, 2, 3],
            }))
            .await
            .expect("handler status must not become tonic error");
        let body = resp.into_inner();
        assert_eq!(body.ack_sequence, 42);
        let status = body.status.expect("exchange response status");
        assert_ne!(status.code, 0);
        assert!(
            status.message.contains("exchange decode failed"),
            "unexpected status message: {}",
            status.message
        );
    }

    #[tokio::test]
    async fn submit_fragment_missing_native_payload_returns_business_error() {
        let svc = GrpcService::default();
        let req = Request::new(SubmitFragmentRequest {
            plan: None,
            instance_params: None,
        });
        let resp = svc.submit_fragment(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_ne!(body.status_code, 0);
        assert!(
            body.message
                .contains("requires native plan and instance_params"),
            "{}",
            body.message
        );
    }

    #[tokio::test]
    async fn submit_fragment_native_payload_validates_instance_params() {
        let svc = GrpcService::default();
        let req = Request::new(SubmitFragmentRequest {
            plan: Some(plan::PlanFragment::default()),
            instance_params: Some(novarocks::InstanceParams::default()),
        });
        let resp = svc.submit_fragment(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_ne!(body.status_code, 0);
        assert!(
            body.message.contains("query_id"),
            "native path should validate InstanceParams, got: {}",
            body.message
        );
    }

    #[tokio::test]
    async fn submit_fragment_rejects_partial_native_payload() {
        let svc = GrpcService::default();
        let req = Request::new(SubmitFragmentRequest {
            plan: Some(plan::PlanFragment::default()),
            instance_params: None,
        });
        let resp = svc.submit_fragment(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_ne!(body.status_code, 0);
        assert!(
            body.message
                .contains("requires native plan and instance_params"),
            "partial native sidecar should be rejected directly, got: {}",
            body.message
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_fragment_native_result_sink_precreates_fetch_buffer() {
        use crate::common::types::UniqueId;
        use crate::runtime::fragment::native_execution::install_test_result_buffer_creation_gate;
        use crate::runtime::result_buffer::{self, FetchErrorKind, TryFetchResult};

        let finst = ProtoUniqueId { hi: 7101, lo: 7102 };
        let finst_id = UniqueId {
            hi: finst.hi,
            lo: finst.lo,
        };
        let creation_gate = install_test_result_buffer_creation_gate(finst_id);
        let svc = GrpcService::default();
        let req = Request::new(SubmitFragmentRequest {
            plan: Some(empty_values_result_fragment(7, 41)),
            instance_params: Some(novarocks::InstanceParams {
                query_id: Some(ProtoUniqueId { hi: 7001, lo: 7002 }),
                fragment_instance_id: Some(finst.clone()),
                backend_num: 3,
                query_options: Some(novarocks::QueryOptions {
                    batch_size: 1024,
                    pipeline_dop: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        });
        let mut submit = tokio::spawn(async move { svc.submit_fragment(req).await });
        creation_gate.wait_until_worker_enters();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), &mut submit)
                .await
                .is_err(),
            "submit RPC must wait until the runtime-owned result buffer is registered"
        );
        creation_gate.release();
        let resp = submit
            .await
            .expect("submit task")
            .expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(body.status_code, 0, "{}", body.message);

        match result_buffer::try_fetch(finst_id) {
            TryFetchResult::Error(err) if matches!(err.kind, FetchErrorKind::NotFound) => {
                panic!(
                    "successful native result submit must register its fetch buffer before returning"
                )
            }
            _ => {}
        }
        crate::runtime::query_context::query_context_manager().unregister_finst(finst_id);
    }

    #[tokio::test]
    async fn report_only_submit_fragment_is_rejected_before_payload_handling() {
        let svc = GrpcService::report_only();
        let req = Request::new(SubmitFragmentRequest {
            plan: None,
            instance_params: None,
        });
        let err = svc
            .submit_fragment(req)
            .await
            .expect_err("report-only endpoint must reject local execution RPCs");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("report-only"));
    }

    #[tokio::test]
    async fn cancel_fragment_is_idempotent() {
        let svc = GrpcService::default();
        let req = Request::new(CancelFragmentRequest {
            finst_ids: vec![ProtoUniqueId { hi: 1, lo: 2 }],
            reason: "test".to_string(),
            start_epoch: 0,
        });
        let resp = svc.cancel_fragment(req).await.expect("RPC success");
        assert_eq!(resp.into_inner().status_code, super::CANCEL_FRAGMENT_OK);

        let req2 = Request::new(CancelFragmentRequest {
            finst_ids: vec![ProtoUniqueId { hi: 1, lo: 2 }],
            reason: "test-2".to_string(),
            start_epoch: 0,
        });
        let resp2 = svc.cancel_fragment(req2).await.expect("RPC success");
        assert_eq!(resp2.into_inner().status_code, super::CANCEL_FRAGMENT_OK);
    }

    mod cancel_epoch_tests {
        use super::super::proto::common::UniqueId as ProtoUniqueId;
        use super::super::proto::novarocks::CancelFragmentRequest;
        use super::super::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc as _;
        use super::super::{CANCEL_FRAGMENT_IGNORED_STALE_EPOCH, GrpcService};
        use crate::common::types::UniqueId;
        use crate::runtime::exchange::{
            self, ExchangeKey, set_expected_senders, snapshot_receiver_state,
        };
        use tonic::Request;

        struct ExchangeCleanup(UniqueId);

        impl Drop for ExchangeCleanup {
            fn drop(&mut self) {
                exchange::cancel_fragment(self.0.hi, self.0.lo);
            }
        }

        #[tokio::test]
        async fn cancel_with_mismatched_epoch_is_ignored() {
            let svc = GrpcService::default();
            let finst = ProtoUniqueId { hi: 6201, lo: 6202 };
            let key = ExchangeKey {
                finst_id_hi: finst.hi,
                finst_id_lo: finst.lo,
                node_id: 6203,
            };
            set_expected_senders(key, 1);
            let _cleanup = ExchangeCleanup(UniqueId {
                hi: finst.hi,
                lo: finst.lo,
            });
            assert!(snapshot_receiver_state(key).is_some());

            let mut stale_epoch = crate::runtime::start_epoch::start_epoch().wrapping_add(1);
            if stale_epoch == 0 {
                stale_epoch = stale_epoch.wrapping_add(1);
            }

            let resp = svc
                .cancel_fragment(Request::new(CancelFragmentRequest {
                    finst_ids: vec![finst],
                    reason: "stale epoch".to_string(),
                    start_epoch: stale_epoch,
                }))
                .await
                .expect("RPC success")
                .into_inner();

            assert_eq!(resp.status_code, CANCEL_FRAGMENT_IGNORED_STALE_EPOCH);
            assert!(snapshot_receiver_state(key).is_some());
        }
    }

    #[tokio::test]
    async fn heartbeat_returns_local_start_epoch_and_capacity() {
        let svc = GrpcService::default();
        let resp = svc
            .heartbeat(tonic::Request::new(HeartbeatRequest {
                assigned_be_id: 7,
                fe_epoch: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.start_epoch, crate::runtime::start_epoch::start_epoch());
        assert!(resp.num_cores >= 1);
        assert_eq!(resp.status_code, 0);
    }

    #[tokio::test]
    async fn report_exec_status_missing_report_returns_business_error() {
        let svc = GrpcService::default();
        let req = Request::new(ReportExecStatusRequest { report: None });
        let resp = svc
            .report_exec_status(req)
            .await
            .expect("RPC level success");
        let body = resp.into_inner();
        assert_ne!(body.status_code, 0);
        assert_eq!(body.error_code, "ProtocolDecodeError");
        assert!(body.message.contains("missing report"), "{}", body.message);
    }

    #[tokio::test]
    async fn report_exec_status_forwards_complete_report_to_injected_handler() {
        let report = write_report(id(901, 902), id(903, 904));
        let handler = Arc::new(CapturingReportHandler::accepting());
        let svc = GrpcService::full_execution_with_report_handler(handler.clone());

        let body = svc
            .report_exec_status(Request::new(ReportExecStatusRequest {
                report: Some(report.clone()),
            }))
            .await
            .expect("RPC level success")
            .into_inner();

        assert_eq!(body.status_code, super::REPORT_EXEC_STATUS_OK);
        let captured = handler.reports.lock().expect("captured reports");
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], report);
    }

    #[tokio::test]
    async fn batch_report_exec_status_preserves_handler_order() {
        let first = ok_report(id(911, 912), id(913, 914));
        let second = write_report(id(921, 922), id(923, 924));
        let handler = Arc::new(CapturingReportHandler::accepting());
        let svc = GrpcService::report_only_with_report_handler(handler.clone());

        let body = svc
            .batch_report_exec_status(Request::new(BatchReportExecStatusRequest {
                reports: vec![first.clone(), second.clone()],
            }))
            .await
            .expect("RPC level success")
            .into_inner();

        assert_eq!(body.status_code, super::REPORT_EXEC_STATUS_OK);
        let captured = handler.reports.lock().expect("captured reports");
        assert_eq!(captured.as_slice(), &[first, second]);
    }

    #[tokio::test]
    async fn batch_report_exec_status_stops_after_first_handler_error() {
        let first = ok_report(id(951, 952), id(953, 954));
        let second = ok_report(id(961, 962), id(963, 964));
        let third = ok_report(id(971, 972), id(973, 974));
        let expected = EngineError::write_coordinator_gone(id(961, 962));
        let handler = Arc::new(CapturingReportHandler::failing_on_call(2, expected.clone()));
        let svc = GrpcService::report_only_with_report_handler(handler.clone());

        let body = svc
            .batch_report_exec_status(Request::new(BatchReportExecStatusRequest {
                reports: vec![first.clone(), second.clone(), third],
            }))
            .await
            .expect("RPC level success")
            .into_inner();

        assert_eq!(body.status_code, expected.to_report_status_code());
        assert_eq!(body.message, expected.to_user_message());
        assert_eq!(body.error_code, expected.to_report_error_code());
        let captured = handler.reports.lock().expect("captured reports");
        assert_eq!(captured.as_slice(), &[first, second]);
    }

    #[tokio::test]
    async fn report_exec_status_maps_injected_engine_error() {
        let query_id = id(931, 932);
        let expected = EngineError::write_coordinator_gone(query_id);
        let handler = Arc::new(CapturingReportHandler::failing(expected.clone()));
        let svc = GrpcService::full_execution_with_report_handler(handler.clone());
        let report = ok_report(query_id, id(933, 934));

        let body = svc
            .report_exec_status(Request::new(ReportExecStatusRequest {
                report: Some(report.clone()),
            }))
            .await
            .expect("RPC level success")
            .into_inner();

        assert_eq!(body.status_code, expected.to_report_status_code());
        assert_eq!(body.message, expected.to_user_message());
        assert_eq!(body.error_code, expected.to_report_error_code());
        let captured = handler.reports.lock().expect("captured reports");
        assert_eq!(captured.as_slice(), &[report]);
    }

    #[tokio::test]
    async fn report_only_report_exec_status_missing_report_reaches_report_handler() {
        let svc = GrpcService::report_only();
        let req = Request::new(ReportExecStatusRequest { report: None });
        let resp = svc
            .report_exec_status(req)
            .await
            .expect("report-only endpoint must allow report RPCs");
        let body = resp.into_inner();
        assert_ne!(body.status_code, 0);
        assert_eq!(body.error_code, "ProtocolDecodeError");
        assert!(body.message.contains("missing report"), "{}", body.message);
    }

    #[tokio::test]
    async fn report_exec_status_updates_registered_write_coordinator() {
        let mut guard = crate::coordinator::write::write_registry_test_guard();
        let query = id(701, 801);
        let finst = id(702, 802);
        guard
            .register_query(
                query,
                vec![crate::coordinator::write::report::WriterKey {
                    query_id: query,
                    fragment_instance_id: finst,
                    backend_num: 0,
                }],
            )
            .expect("register write coordinator");
        let report = ok_report(query, finst);
        let svc = GrpcService::default();
        let req = Request::new(ReportExecStatusRequest {
            report: Some(report),
        });
        let resp = svc
            .report_exec_status(req)
            .await
            .expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(body.status_code, 0, "{}", body.message);
        assert_eq!(body.error_code, "");
    }

    #[tokio::test]
    async fn report_exec_status_ignores_non_writer_ok_for_registered_write_query() {
        let mut guard = crate::coordinator::write::write_registry_test_guard();
        let query = id(711, 811);
        let writer_finst = id(712, 812);
        let ordinary_finst = id(713, 813);
        let coord = guard
            .register_query(
                query,
                vec![crate::coordinator::write::report::WriterKey {
                    query_id: query,
                    fragment_instance_id: writer_finst,
                    backend_num: 0,
                }],
            )
            .expect("register write coordinator");
        let report = ok_report(query.clone(), ordinary_finst);
        let svc = GrpcService::default();
        let req = Request::new(ReportExecStatusRequest {
            report: Some(report),
        });
        let resp = svc
            .report_exec_status(req)
            .await
            .expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(body.status_code, 0, "{}", body.message);
        assert_eq!(body.error_code, "");
        assert!(
            !coord.lock().expect("write coordinator lock").has_failed(),
            "ordinary OK fragment reports must not fail the write coordinator"
        );

        let req = Request::new(ReportExecStatusRequest {
            report: Some(ok_report(query, writer_finst)),
        });
        let resp = svc
            .report_exec_status(req)
            .await
            .expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(body.status_code, 0, "{}", body.message);
        assert_eq!(body.error_code, "");
        coord
            .lock()
            .expect("write coordinator lock")
            .commit_input()
            .expect("writer report should still commit");
    }

    #[tokio::test]
    async fn report_exec_status_rejects_unknown_writer_with_write_metadata() {
        let mut guard = crate::coordinator::write::write_registry_test_guard();
        let query = id(714, 814);
        let writer_finst = id(715, 815);
        let unknown_writer_finst = id(716, 816);
        let coord = guard
            .register_query(
                query,
                vec![crate::coordinator::write::report::WriterKey {
                    query_id: query,
                    fragment_instance_id: writer_finst,
                    backend_num: 0,
                }],
            )
            .expect("register write coordinator");
        let report = write_report(query, unknown_writer_finst);
        let svc = GrpcService::default();
        let req = Request::new(ReportExecStatusRequest {
            report: Some(report),
        });
        let resp = svc
            .report_exec_status(req)
            .await
            .expect("RPC level success");
        let body = resp.into_inner();
        assert_ne!(body.status_code, 0);
        assert_eq!(body.error_code, "DistributedWriteOutputMismatch");
        assert!(
            body.message.contains("unknown writer"),
            "unexpected message: {}",
            body.message
        );
        assert!(
            coord.lock().expect("write coordinator lock").has_failed(),
            "unknown writer commit metadata must fail the registered write query"
        );
    }

    #[tokio::test]
    async fn report_exec_status_non_writer_error_fails_registered_write_query() {
        let mut guard = crate::coordinator::write::write_registry_test_guard();
        let query = id(721, 821);
        let writer_finst = id(722, 822);
        let ordinary_finst = id(723, 823);
        let coord = guard
            .register_query(
                query,
                vec![crate::coordinator::write::report::WriterKey {
                    query_id: query,
                    fragment_instance_id: writer_finst,
                    backend_num: 0,
                }],
            )
            .expect("register write coordinator");
        let message = "remote non-writer fragment failed";
        let report = error_report(query, ordinary_finst, message);
        let svc = GrpcService::default();
        let req = Request::new(ReportExecStatusRequest {
            report: Some(report),
        });
        let resp = svc
            .report_exec_status(req)
            .await
            .expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(body.status_code, 0, "{}", body.message);
        assert_eq!(body.error_code, "");
        let abort = coord
            .lock()
            .expect("write coordinator lock")
            .abort_input()
            .expect("non-writer failure should abort the write query");
        assert!(abort.reason.contains(message), "{}", abort.reason);
    }

    #[tokio::test]
    async fn report_exec_status_query_gone_returns_terminal_code() {
        let _guard = crate::coordinator::write::write_registry_test_guard();
        let query = id(801, 901);
        let finst = id(802, 902);
        let report = write_report(query, finst);
        let svc = GrpcService::default();
        let req = Request::new(ReportExecStatusRequest {
            report: Some(report),
        });

        let resp = svc
            .report_exec_status(req)
            .await
            .expect("RPC level success");
        let body = resp.into_inner();

        assert_eq!(
            body.status_code,
            crate::service::grpc_server::REPORT_EXEC_STATUS_QUERY_GONE,
            "{}",
            body.message
        );
        assert_eq!(body.error_code, "WriteCoordinatorGone");
        assert!(body.message.contains("not found"), "{}", body.message);
    }

    #[tokio::test]
    async fn report_exec_status_error_without_write_coordinator_marks_query_failed() {
        use crate::common::types::UniqueId;
        use crate::runtime::query_context::{QueryId, query_context_manager};
        use crate::runtime::result_buffer::{self, FetchErrorKind, TryFetchResult};

        let _guard = crate::coordinator::write::write_registry_test_guard();
        let query = id(811, 911);
        let finst = id(812, 912);
        let query_id = QueryId {
            hi: query.hi,
            lo: query.lo,
        };
        let finst_id = UniqueId {
            hi: finst.hi,
            lo: finst.lo,
        };
        let message = "remote fragment failed before exchange eos";

        result_buffer::create_sender(finst_id);
        query_context_manager().register_finst(finst_id, query_id);

        let report = error_report(query, finst, message);
        let svc = GrpcService::default();
        let req = Request::new(ReportExecStatusRequest {
            report: Some(report),
        });
        let resp = svc
            .report_exec_status(req)
            .await
            .expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(body.status_code, 0, "{}", body.message);
        assert_eq!(body.error_code, "");

        let TryFetchResult::Error(err) = result_buffer::try_fetch(finst_id) else {
            panic!("remote fragment error must close the root result buffer");
        };
        assert!(matches!(err.kind, FetchErrorKind::Failed));
        assert!(err.message.contains(message), "{}", err.message);

        query_context_manager().unregister_finst(finst_id);
    }

    #[tokio::test]
    async fn fetch_result_missing_finst_id_returns_error_status() {
        let svc = GrpcService::default();
        let req = Request::new(FetchResultRequest {
            finst_id: None,
            max_wait_ms: 0,
        });
        let resp = svc.fetch_result(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(
            body.status,
            FetchStatus::Error as i32,
            "missing finst_id must return ERROR status"
        );
        assert!(!body.message.is_empty(), "error message must be non-empty");
        assert_eq!(body.packet_seq, 0);
        assert!(!body.eos);
        assert!(
            body.result_arrow_ipc.is_empty(),
            "payload must be empty on error"
        );
    }

    #[tokio::test]
    async fn fetch_result_empty_open_buffer_returns_not_ready_without_wait() {
        use crate::common::types::UniqueId;
        use crate::runtime::result_buffer::create_typed_sender;

        let finst_id = UniqueId { hi: 8801, lo: 8802 };
        create_typed_sender(finst_id);

        let svc = GrpcService::default();
        let req = Request::new(FetchResultRequest {
            finst_id: Some(ProtoUniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            max_wait_ms: 0,
        });
        let resp = svc.fetch_result(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(
            body.status,
            FetchStatus::NotReady as i32,
            "empty open buffer with max_wait_ms=0 must return NOT_READY"
        );
        assert_eq!(body.packet_seq, 0);
        assert!(!body.eos);
        assert!(body.result_arrow_ipc.is_empty());
    }

    #[tokio::test]
    async fn fetch_result_waits_for_ready_arrow_ipc_result() {
        use crate::common::types::UniqueId;
        use crate::runtime::result_buffer::{create_typed_sender, insert_typed};

        let finst_id = UniqueId { hi: 8803, lo: 8804 };
        create_typed_sender(finst_id);

        // Insert a result from a background thread after 20 ms.
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            insert_typed(finst_id, vec![1, 2, 3, 4]).expect("insert typed payload");
        });

        let svc = GrpcService::default();
        let req = Request::new(FetchResultRequest {
            finst_id: Some(ProtoUniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            max_wait_ms: 1000,
        });
        let resp = svc.fetch_result(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(
            body.status,
            FetchStatus::Ready as i32,
            "should return READY after delayed insert with max_wait_ms=1000"
        );
        assert_eq!(body.packet_seq, 0);
        assert!(!body.eos);
        assert_eq!(body.result_arrow_ipc, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn fetch_result_typed_request_returns_arrow_ipc_payload() {
        use crate::common::types::UniqueId;
        use crate::runtime::result_buffer::{create_typed_sender, insert_typed};

        let finst_id = UniqueId { hi: 8811, lo: 8812 };
        create_typed_sender(finst_id);
        insert_typed(finst_id, vec![1, 2, 3, 4]).expect("insert typed payload");

        let svc = GrpcService::default();
        let req = Request::new(FetchResultRequest {
            finst_id: Some(ProtoUniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            max_wait_ms: 0,
        });
        let resp = svc.fetch_result(req).await.expect("RPC level success");
        let body = resp.into_inner();

        assert_eq!(body.status, FetchStatus::Ready as i32);
        assert_eq!(body.packet_seq, 0);
        assert!(!body.eos);
        assert_eq!(body.result_arrow_ipc, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn fetch_result_buffer_error_returns_error_status() {
        use crate::common::types::UniqueId;
        use crate::runtime::result_buffer::{close_error, create_typed_sender};

        let finst_id = UniqueId { hi: 8807, lo: 8808 };
        create_typed_sender(finst_id);
        close_error(finst_id, "boom".to_string());

        let svc = GrpcService::default();
        let req = Request::new(FetchResultRequest {
            finst_id: Some(ProtoUniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            max_wait_ms: 0,
        });
        let resp = svc.fetch_result(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(
            body.status,
            FetchStatus::Error as i32,
            "close_error buffer must return ERROR status"
        );
        assert_eq!(body.message, "boom", "error message must match");
        assert_eq!(body.packet_seq, 0);
        assert!(!body.eos);
        assert!(
            body.result_arrow_ipc.is_empty(),
            "payload must be empty on error"
        );
    }

    #[tokio::test]
    async fn fetch_result_closed_buffer_returns_ready_eos() {
        use crate::common::types::UniqueId;
        use crate::runtime::result_buffer::{close_ok, create_typed_sender};

        let finst_id = UniqueId { hi: 8805, lo: 8806 };
        create_typed_sender(finst_id);
        close_ok(finst_id);

        let svc = GrpcService::default();
        let req = Request::new(FetchResultRequest {
            finst_id: Some(ProtoUniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            max_wait_ms: 0,
        });
        let resp = svc.fetch_result(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(
            body.status,
            FetchStatus::Ready as i32,
            "closed buffer must return READY with eos=true"
        );
        assert_eq!(body.packet_seq, 0);
        assert!(body.eos);
        assert!(body.result_arrow_ipc.is_empty());
    }
}
