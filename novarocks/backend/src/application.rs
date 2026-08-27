use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use novarocks_execution::runtime::execution_runtime::{ExecutionRuntime, ExecutionRuntimeConfig};
use novarocks_native_trust::NativeTrust;
use novarocks_proto_codec::lifecycle::QueryControlEndpoint;
use novarocks_proto_codec::lifecycle::{
    QueryAbortRequest, QueryControlAttach, QueryInitAck, QueryInitRequest, QueryStageAck,
    QueryStageOutcome, QueryStageRequest, QueryStartAck, QueryStartRequest, QueryTerminationAck,
};
use novarocks_proto_codec::membership::BackendProcessDescriptor;
use novarocks_proto_codec::membership::{
    BackendAnnounceRequest, BackendAnnounceResult, BackendReportedState,
};
use novarocks_spi::connector::ConnectorExecutionInstaller;
use novarocks_types::{AdvertiseEndpoint, BackendProcessId, NativeEndpoint};

use crate::BackendDataRuntime;
use crate::connector::ConnectorRegistry;
use crate::exchange_receiver::BackendExchangeReceiverPort;
use crate::fragment::control::FragmentControlRegistry;
use crate::fragment::{
    NativeFragmentService, grpc_exchange_transmitter, grpc_fragment_lookup_client,
    native_result_writer,
};
use crate::metrics::{BackendMetricsRegistry, MetricsHttpServer};
use crate::query_lifecycle::{
    NativeQueryLifecycleLocalRuntime, QueryControlAttachment, QueryLifecycleError,
    QueryLifecycleIngress, QueryLifecycleRegistry, QueryLifecycleRegistryConfig,
};
use crate::rpc::client::BackendRpcClient;
use crate::rpc::runtime::BackendNativeTransport;
use crate::rpc::server::{BackendRpcServerHandle, BackendRpcService};
use crate::runtime_filter::rpc::BackendRuntimeFilterEnvelopeIngress;
use novarocks_execution::runtime::fragment::io::ExchangeReceiverPort;
use novarocks_spi::connector::WriteCommitEvidenceLimits;

const READINESS_TIMEOUT: Duration = Duration::from_secs(5);
const SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(50);
const ANNOUNCE_RPC_TIMEOUT: Duration = Duration::from_secs(3);

pub struct BackendServerConfig {
    pub bind_host: String,
    pub grpc_port: u16,
    pub metrics_http_port: u16,
    pub advertise_endpoint: AdvertiseEndpoint,
    /// Server-resolved Native caller authentication and transport material.
    /// Backend receives this immutable capability and never reads trust source
    /// configuration or credentials itself.
    pub native_trust: Arc<NativeTrust>,
    pub native_transport: BackendNativeTransport,
    /// Exact FE native ingress used exclusively for authenticated membership announce.
    pub frontend_endpoint: NativeEndpoint,
    pub announce_interval: Duration,
    pub announce_initial_backoff: Duration,
    pub announce_max_backoff: Duration,
    pub query_lifecycle_sweep_interval: Duration,
    pub query_lifecycle_config: QueryLifecycleRegistryConfig,
    /// Server-resolved per-fragment terminal write evidence budget.
    pub write_commit_evidence_limits: WriteCommitEvidenceLimits,
    pub execution_runtime_config: ExecutionRuntimeConfig,
    /// Provider-owned execution installers composed by the server role.
    ///
    /// Backend only owns registration and lifecycle of these contributions; it
    /// never constructs a provider-specific installer or catalog binding.
    pub execution_installers: Vec<Arc<dyn ConnectorExecutionInstaller>>,
    /// Provider-owned constructors for complete worker read bundles, one per
    /// provider kind. The Host installs factory and matching codec atomically
    /// for each exact admitted binding generation.
    pub read_execution_bundle_factories: Vec<(
        novarocks_spi::connector::ConnectorExecutionProviderKind,
        Arc<dyn novarocks_proto_codec::connector_read::ConnectorReadExecutionBundleFactory>,
    )>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendApplicationErrorKind {
    Configuration,
    Start,
    Readiness,
    Supervision,
    Shutdown,
    Signal,
}

#[derive(Debug)]
pub struct BackendApplicationError {
    kind: BackendApplicationErrorKind,
    message: String,
}

impl BackendApplicationError {
    fn new(kind: BackendApplicationErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
        }
    }

    fn with_cleanup_context(mut self, cleanup_error: impl fmt::Display) -> Self {
        self.message
            .push_str(&format!("; cleanup failed: {cleanup_error}"));
        self
    }

    pub const fn kind(&self) -> BackendApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for BackendApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for BackendApplicationError {}

pub struct BackendApplicationHost {
    ready_marker: String,
    grpc_server: BackendRpcServerHandle,
    _native_fragment_service: Arc<NativeFragmentService>,
    _query_lifecycle_registry: Arc<QueryLifecycleRegistry>,
    execution_host: Arc<crate::ConnectorExecutionHost>,
    _execution_runtime: Arc<ExecutionRuntime>,
    query_lifecycle_sweep: QueryLifecycleSweepTask,
    metrics_http_server: MetricsHttpServer,
    process_descriptor: BackendProcessDescriptor,
    announce_task: BackendAnnounceTask,
}

struct BackendAnnounceTask {
    stop: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    wake: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    join: Option<std::thread::JoinHandle<()>>,
    data_runtime: BackendDataRuntime,
    frontend_endpoint: NativeEndpoint,
    descriptor: BackendProcessDescriptor,
}

impl BackendAnnounceTask {
    fn start(
        data_runtime: BackendDataRuntime,
        frontend_endpoint: NativeEndpoint,
        descriptor: BackendProcessDescriptor,
        interval: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let draining = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let thread_stop = Arc::clone(&stop);
        let thread_draining = Arc::clone(&draining);
        let thread_wake = Arc::clone(&wake);
        let thread_runtime = data_runtime.clone();
        let thread_frontend_endpoint = frontend_endpoint.clone();
        let thread_descriptor = descriptor.clone();
        let join = std::thread::Builder::new()
            .name("backend-announce".to_string())
            .spawn(move || {
                let client = BackendRpcClient::new_native_endpoint(
                    thread_runtime,
                    thread_frontend_endpoint,
                );
                let initial_backoff = initial_backoff.max(Duration::from_millis(1));
                let max_backoff = max_backoff.max(initial_backoff);
                let mut retry_delay = initial_backoff;
                while !thread_stop.load(Ordering::Acquire) {
                    let reported_state = if thread_draining.load(Ordering::Acquire) {
                        BackendReportedState::Draining
                    } else {
                        BackendReportedState::Running
                    };
                    let request = BackendAnnounceRequest::new(
                        thread_descriptor.clone(),
                        reported_state,
                    )
                        .expect("backend process descriptor remains validated");
                    let next_delay = match client.blocking_announce_backend_with_timeout(
                        request.as_proto().clone(),
                        ANNOUNCE_RPC_TIMEOUT,
                    ) {
                        Ok(BackendAnnounceResult::Accepted { lease_ttl_ms }) => {
                            retry_delay = initial_backoff;
                            interval.min(Duration::from_millis(lease_ttl_ms.saturating_div(3).max(1)))
                        }
                        Ok(BackendAnnounceResult::Rejected { reason, safe_detail }) => {
                            tracing::error!(?reason, %safe_detail, "backend announce rejected by frontend");
                            let delay = retry_delay;
                            retry_delay = retry_delay.saturating_mul(2).min(max_backoff);
                            delay
                        }
                        Err(error) => {
                            tracing::warn!(%error, "backend announce attempt failed");
                            let delay = retry_delay;
                            retry_delay = retry_delay.saturating_mul(2).min(max_backoff);
                            delay
                        }
                    };
                    let (pending, signal) = &*thread_wake;
                    let mut pending = pending.lock().expect("backend announce wake lock");
                    if !*pending && !thread_stop.load(Ordering::Acquire) {
                        let (next, _) = signal
                            .wait_timeout(pending, next_delay)
                            .expect("backend announce wake wait");
                        pending = next;
                    }
                    *pending = false;
                }
            })
            .expect("spawn backend announce task");
        Self {
            stop,
            draining,
            wake,
            join: Some(join),
            data_runtime,
            frontend_endpoint,
            descriptor,
        }
    }

    fn begin_drain(&self) {
        self.draining.store(true, Ordering::Release);
        let client = BackendRpcClient::new_native_endpoint(
            self.data_runtime.clone(),
            self.frontend_endpoint.clone(),
        );
        let request =
            BackendAnnounceRequest::new(self.descriptor.clone(), BackendReportedState::Draining)
                .expect("backend process descriptor remains validated");
        match client.blocking_announce_backend_with_timeout(
            request.as_proto().clone(),
            ANNOUNCE_RPC_TIMEOUT,
        ) {
            Ok(BackendAnnounceResult::Accepted { .. }) => {}
            Ok(BackendAnnounceResult::Rejected {
                reason,
                safe_detail,
            }) => {
                tracing::error!(?reason, %safe_detail, "backend drain announce rejected by frontend");
            }
            Err(error) => {
                tracing::warn!(%error, "backend drain announce attempt failed");
            }
        }
        let (pending, signal) = &*self.wake;
        *pending.lock().expect("backend announce wake lock") = true;
        signal.notify_one();
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let (_, signal) = &*self.wake;
        signal.notify_one();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl fmt::Debug for BackendApplicationHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackendApplicationHost")
            .field("ready_marker", &self.ready_marker)
            .finish_non_exhaustive()
    }
}

struct BackendApplicationServices {
    native_fragment_service: Arc<NativeFragmentService>,
    query_lifecycle_registry: Arc<QueryLifecycleRegistry>,
    execution_host: Arc<crate::ConnectorExecutionHost>,
    execution_runtime: Arc<ExecutionRuntime>,
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
}

/// Backend composition root for the QLC-3 Stage/Start transaction.  The
/// registry owns lifecycle linearization while the fragment service owns
/// dormant local workers; neither exposes a direct production submit path.
struct BackendStageLifecycleIngress {
    registry: Arc<QueryLifecycleRegistry>,
    fragments: Arc<NativeFragmentService>,
}

impl QueryLifecycleIngress for BackendStageLifecycleIngress {
    fn backend_process_id(&self) -> BackendProcessId {
        self.registry.local_process_id()
    }

    fn is_draining(&self) -> bool {
        self.registry.is_draining()
    }

    fn init_query(&self, request: QueryInitRequest) -> QueryInitAck {
        self.registry.init_query(request)
    }

    fn stage_fragments(&self, request: QueryStageRequest) -> QueryStageAck {
        match self.registry.begin_stage(request.clone()) {
            crate::query_lifecycle::StageBuildDecision::Complete(ack) => ack,
            crate::query_lifecycle::StageBuildDecision::Build(permit) => {
                let execution_id = request.execution_id();
                let fragments = request.fragments();
                let stage_digest = permit.digest();
                let build = self
                    .fragments
                    .stage_fragments(execution_id, &fragments, permit.gate());
                match build {
                    Ok(()) => permit.commit(),
                    Err(error) => QueryStageAck::new(
                        request.execution_id(),
                        request.digest_version(),
                        stage_digest,
                        QueryStageOutcome::RejectedLocalFailure,
                        error.to_string(),
                    )
                    .expect("validated Stage request has a valid failure acknowledgement"),
                }
            }
        }
    }

    fn task_update(
        &self,
        request: crate::query_lifecycle::task_update::TaskUpdateRequest,
    ) -> crate::query_lifecycle::task_update::TaskUpdateAck {
        // Admission and delivery are deliberately separate: the lifecycle
        // decides whether this exact attempt may still receive work, and the
        // fragment runtime owns the queue the work lands in.
        if let Err(error) = self
            .registry
            .admit_task_update(request.execution_id(), request.fragment_instance_id())
        {
            return crate::query_lifecycle::task_update::rejection_from_lifecycle_error(&error);
        }
        self.fragments.deliver_split_assignments(
            request.execution_id(),
            request.fragment_instance_id(),
            request.assignments(),
        )
    }

    fn start_prepared_query(&self, request: QueryStartRequest) -> QueryStartAck {
        self.registry.start_prepared_query(request)
    }

    fn abort_query(
        &self,
        request: QueryAbortRequest,
    ) -> Result<QueryTerminationAck, QueryLifecycleError> {
        self.registry.abort_query(request)
    }

    fn attach_control(
        &self,
        attach: QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError> {
        self.registry.attach_control(attach)
    }
}

struct QueryLifecycleSweepTask {
    stop_tx: Option<std::sync::mpsc::Sender<()>>,
    failure_rx: mpsc::Receiver<String>,
    join_handle: Option<std::thread::JoinHandle<()>>,
    stop_requested: Arc<AtomicBool>,
}

impl QueryLifecycleSweepTask {
    fn start(registry: Arc<QueryLifecycleRegistry>, interval: Duration) -> Result<Self, String> {
        let (stop_tx, stop_rx) = std::sync::mpsc::channel();
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let thread_stop_requested = Arc::clone(&stop_requested);
        let join_handle = std::thread::Builder::new()
            .name("query-lifecycle-sweep".to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
                        stop_rx.recv_timeout(interval)
                    {
                        registry.sweep_expired(Instant::now());
                    }
                }));
                if thread_stop_requested.load(Ordering::Acquire) {
                    return;
                }
                let error = match outcome {
                    Ok(()) => "query lifecycle sweep task exited unexpectedly".to_string(),
                    Err(payload) => payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| {
                            payload
                                .downcast_ref::<&str>()
                                .map(|value| (*value).to_string())
                        })
                        .unwrap_or_else(|| "query lifecycle sweep task panicked".to_string()),
                };
                let _ = failure_tx.send(error);
            })
            .map_err(|error| format!("spawn query lifecycle sweep task: {error}"))?;
        Ok(Self {
            stop_tx: Some(stop_tx),
            failure_rx,
            join_handle: Some(join_handle),
            stop_requested,
        })
    }

    fn poll_failure(&mut self) -> Result<Option<String>, String> {
        match self.failure_rx.try_recv() {
            Ok(error) => Ok(Some(error)),
            Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => Ok(None),
        }
    }

    fn stop(&mut self) -> Result<(), String> {
        self.stop_requested.store(true, Ordering::Release);
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        let Some(join_handle) = self.join_handle.take() else {
            return Ok(());
        };
        join_handle
            .join()
            .map_err(|_| "query lifecycle sweep task panicked".to_string())
    }
}

impl Drop for QueryLifecycleSweepTask {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn compose_backend_application_services(
    data_runtime: BackendDataRuntime,
    execution_runtime_config: ExecutionRuntimeConfig,
    query_lifecycle_config: QueryLifecycleRegistryConfig,
    write_commit_evidence_limits: WriteCommitEvidenceLimits,
    execution_installers: &[Arc<dyn ConnectorExecutionInstaller>],
    read_execution_bundle_factories: &[(
        novarocks_spi::connector::ConnectorExecutionProviderKind,
        Arc<dyn novarocks_proto_codec::connector_read::ConnectorReadExecutionBundleFactory>,
    )],
) -> Result<BackendApplicationServices, BackendApplicationError> {
    let execution_runtime = Arc::new(ExecutionRuntime::new(execution_runtime_config).map_err(
        |error| BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error),
    )?);
    let controls = Arc::new(FragmentControlRegistry::default());
    let exchange_receiver_port: Arc<dyn ExchangeReceiverPort> = Arc::new(
        BackendExchangeReceiverPort::new(Arc::clone(&execution_runtime)),
    );
    // One registry per backend: the execution host writes it on ensure and the
    // fragment runtime reads it at decode, so both agree on which generation is
    // installed.
    let read_executions = Arc::new(crate::connector::InstalledReadExecutionRegistry::default());
    let execution_host = seal_connector_execution_host(
        execution_installers,
        read_execution_bundle_factories,
        Arc::clone(&read_executions),
    )?;
    let local_runtime = Arc::new(NativeQueryLifecycleLocalRuntime::new(
        Arc::clone(&controls),
        Arc::clone(&execution_host),
    ));
    let query_lifecycle_registry = QueryLifecycleRegistry::new_with_runtime(
        data_runtime.clone(),
        local_runtime,
        query_lifecycle_config,
    );
    let connector_registry = Arc::new(ConnectorRegistry::new());
    let native_fragment_service = Arc::new(
        NativeFragmentService::new_with_controls(
            grpc_exchange_transmitter(data_runtime.clone()),
            grpc_fragment_lookup_client(data_runtime),
            native_result_writer(),
            Arc::clone(&controls),
            Arc::clone(&query_lifecycle_registry),
            connector_registry,
            Arc::clone(&execution_host),
            Arc::clone(&execution_runtime),
        )
        .with_write_commit_evidence_limits(write_commit_evidence_limits)
        .with_exchange_receiver_port(Arc::clone(&exchange_receiver_port)),
    );
    controls.publish_resource_snapshot();
    execution_host.publish_resource_snapshot();
    crate::runtime::native_fragment_query::NativeFragmentQueryRuntime::global()
        .publish_resource_snapshot();
    let query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress> =
        Arc::new(BackendStageLifecycleIngress {
            registry: Arc::clone(&query_lifecycle_registry),
            fragments: Arc::clone(&native_fragment_service),
        });
    Ok(BackendApplicationServices {
        native_fragment_service,
        query_lifecycle_registry,
        execution_host,
        execution_runtime,
        exchange_receiver_port,
        query_lifecycle_ingress,
    })
}

fn seal_connector_execution_host(
    execution_installers: &[Arc<dyn ConnectorExecutionInstaller>],
    read_execution_bundle_factories: &[(
        novarocks_spi::connector::ConnectorExecutionProviderKind,
        Arc<dyn novarocks_proto_codec::connector_read::ConnectorReadExecutionBundleFactory>,
    )],
    read_executions: Arc<crate::connector::InstalledReadExecutionRegistry>,
) -> Result<Arc<crate::ConnectorExecutionHost>, BackendApplicationError> {
    #[cfg(test)]
    if execution_installers.is_empty() {
        // Application tests exercise lifecycle wiring without a provider
        // composition root. Production startup never takes this branch.
        return Ok(Arc::new(crate::ConnectorExecutionHost::empty_for_tests()));
    }
    crate::ConnectorExecutionHost::try_new(
        execution_installers.iter().cloned(),
        read_execution_bundle_factories.iter().cloned(),
        read_executions,
    )
    .map(Arc::new)
    .map_err(|error| {
        BackendApplicationError::new(
            BackendApplicationErrorKind::Configuration,
            format!("seal connector execution installer set: {error}"),
        )
    })
}

impl BackendApplicationHost {
    pub fn open(
        config: BackendServerConfig,
        data_runtime: BackendDataRuntime,
    ) -> Result<Self, BackendApplicationError> {
        Self::open_with_readiness_timeout(config, data_runtime, READINESS_TIMEOUT)
    }

    pub fn ready_marker(&self) -> &str {
        &self.ready_marker
    }

    /// Return the actual listener endpoint in a form a same-process frontend
    /// can dial.  A wildcard bind remains a listener concern; composition must
    /// use loopback rather than attempting to connect to `0.0.0.0` or `::`.
    pub fn connectable_native_endpoint(&self) -> SocketAddr {
        let bound = self.grpc_server.bound_addr();
        let ip = if bound.ip().is_unspecified() {
            match bound.ip() {
                std::net::IpAddr::V4(_) => std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                std::net::IpAddr::V6(_) => std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            }
        } else {
            bound.ip()
        };
        SocketAddr::new(ip, bound.port())
    }

    pub fn process_descriptor(&self) -> &BackendProcessDescriptor {
        &self.process_descriptor
    }

    /// SIGTERM makes this BE ineligible for new Init while existing admitted
    /// lifecycle entries remain reachable until their normal terminal state.
    pub fn begin_drain(&self) {
        self._query_lifecycle_registry.begin_drain();
        self.announce_task.begin_drain();
    }

    pub fn is_drained(&self) -> bool {
        self._query_lifecycle_registry.is_drained()
    }

    pub fn poll_failure(
        &mut self,
    ) -> Result<Option<BackendApplicationError>, BackendApplicationError> {
        for failure in [
            self.grpc_server.poll_failure(),
            self.metrics_http_server.poll_failure(),
            self.query_lifecycle_sweep.poll_failure(),
        ] {
            match failure {
                Ok(Some(error)) => {
                    return Ok(Some(BackendApplicationError::new(
                        BackendApplicationErrorKind::Supervision,
                        error,
                    )));
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(BackendApplicationError::new(
                        BackendApplicationErrorKind::Supervision,
                        error,
                    ));
                }
            }
        }
        Ok(None)
    }

    pub fn shutdown(mut self) -> Result<(), BackendApplicationError> {
        self.announce_task.stop();
        let listener_shutdown = self.grpc_server.stop();
        let execution_shutdown = self
            .execution_host
            .shutdown()
            .map_err(|error| error.to_string());
        let sweep_result = self.query_lifecycle_sweep.stop();
        let metrics_result = self.metrics_http_server.stop();
        combine_shutdown_results(listener_shutdown, sweep_result)
            .and(metrics_result)
            .and(execution_shutdown)
            .map_err(|error| {
                BackendApplicationError::new(BackendApplicationErrorKind::Shutdown, error)
            })
    }

    fn open_with_readiness_timeout(
        config: BackendServerConfig,
        data_runtime: BackendDataRuntime,
        readiness_timeout: Duration,
    ) -> Result<Self, BackendApplicationError> {
        let BackendServerConfig {
            bind_host,
            grpc_port,
            metrics_http_port,
            advertise_endpoint,
            native_trust,
            native_transport,
            frontend_endpoint,
            announce_interval,
            announce_initial_backoff,
            announce_max_backoff,
            query_lifecycle_sweep_interval,
            query_lifecycle_config,
            write_commit_evidence_limits,
            execution_runtime_config,
            execution_installers,
            read_execution_bundle_factories,
        } = config;
        let readiness_endpoint =
            NativeEndpoint::from_host_port(&advertise_endpoint.host, advertise_endpoint.port)
                .map_err(|error| {
                    BackendApplicationError::new(
                        BackendApplicationErrorKind::Configuration,
                        format!("invalid advertised Native readiness endpoint: {error}"),
                    )
                })?;
        let readiness_runtime = data_runtime.clone();
        let services = compose_backend_application_services(
            data_runtime,
            execution_runtime_config,
            query_lifecycle_config,
            write_commit_evidence_limits,
            &execution_installers,
            &read_execution_bundle_factories,
        )?;
        let process_descriptor = BackendProcessDescriptor::new(
            services.query_lifecycle_ingress.backend_process_id(),
            QueryControlEndpoint::new(advertise_endpoint.host.clone(), advertise_endpoint.port)
                .map_err(|error| {
                    BackendApplicationError::new(
                        BackendApplicationErrorKind::Configuration,
                        format!("resolve backend process endpoint: {error}"),
                    )
                })?,
            native_trust.deployment_id().as_str(),
            novarocks_version::native_build_identity(),
        )
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("construct backend process descriptor: {error}"),
            )
        })?;
        let metrics_registry = Arc::new(BackendMetricsRegistry::new().map_err(|error| {
            BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error)
        })?);
        let metrics_http_server =
            MetricsHttpServer::start(&bind_host, metrics_http_port, metrics_registry).map_err(
                |error| BackendApplicationError::new(BackendApplicationErrorKind::Start, error),
            )?;
        let native_fragment_service = Arc::clone(&services.native_fragment_service);
        let mut query_lifecycle_sweep = match QueryLifecycleSweepTask::start(
            Arc::clone(&services.query_lifecycle_registry),
            query_lifecycle_sweep_interval,
        ) {
            Ok(sweep) => sweep,
            Err(error) => {
                let metrics_result = metrics_http_server.stop();
                let primary =
                    BackendApplicationError::new(BackendApplicationErrorKind::Start, error);
                return Err(match metrics_result {
                    Ok(()) => primary,
                    Err(cleanup_error) => primary.with_cleanup_context(cleanup_error),
                });
            }
        };

        let runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress> =
            services.query_lifecycle_registry.clone();
        let mut grpc_server = match BackendRpcServerHandle::start(
            &bind_host,
            grpc_port,
            BackendRpcService::new(
                native_fragment_service.clone(),
                services.query_lifecycle_ingress.clone(),
                runtime_filter_ingress,
                Arc::clone(&services.exchange_receiver_port),
                process_descriptor.clone(),
            ),
            native_trust,
            native_transport,
        ) {
            Ok(server) => server,
            Err(error) => {
                let sweep_result = query_lifecycle_sweep.stop();
                let metrics_result = metrics_http_server.stop();
                let primary = BackendApplicationError::new(
                    BackendApplicationErrorKind::Start,
                    format!("start native backend gRPC server on {bind_host}:{grpc_port}: {error}"),
                );
                return Err(append_cleanup_results(
                    primary,
                    [sweep_result, metrics_result],
                ));
            }
        };

        if let Err(error) =
            wait_for_native_ready(&readiness_runtime, readiness_endpoint, readiness_timeout)
        {
            let listener_result = grpc_server.stop();
            let sweep_result = query_lifecycle_sweep.stop();
            let metrics_result = metrics_http_server.stop();
            let primary = BackendApplicationError::new(
                BackendApplicationErrorKind::Readiness,
                format!("advertised endpoint readiness failed: {error}"),
            );
            return Err(append_cleanup_results(
                primary,
                [listener_result, sweep_result, metrics_result],
            ));
        }

        let announce_task = BackendAnnounceTask::start(
            readiness_runtime,
            frontend_endpoint,
            process_descriptor.clone(),
            announce_interval.max(Duration::from_millis(100)),
            announce_initial_backoff,
            announce_max_backoff,
        );

        Ok(Self {
            ready_marker: format!(
                "NOVAROCKS_READY role=be grpc_port={grpc_port} advertise_host={} pid={}",
                advertise_endpoint.host,
                std::process::id()
            ),
            grpc_server,
            _native_fragment_service: native_fragment_service,
            _query_lifecycle_registry: services.query_lifecycle_registry,
            execution_host: services.execution_host,
            _execution_runtime: services.execution_runtime,
            query_lifecycle_sweep,
            metrics_http_server,
            process_descriptor,
            announce_task,
        })
    }
}

#[allow(
    dead_code,
    reason = "This library entrypoint is invoked by the backend server binary, not backend lib tests."
)]
pub fn run_backend_server(config: BackendServerConfig) -> Result<(), BackendApplicationError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Start,
                format!("build backend Tokio runtime failed: {error}"),
            )
        })?;
    let data_runtime = BackendDataRuntime::new(
        runtime.handle().clone(),
        Arc::clone(&config.native_trust),
        config.native_transport.clone(),
    );
    runtime.block_on(run_backend_server_until_signal(config, data_runtime))
}
pub async fn run_backend_server_until_shutdown<F>(
    config: BackendServerConfig,
    data_runtime: BackendDataRuntime,
    shutdown: F,
) -> Result<(), BackendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    run_backend_server_until(config, data_runtime, async move {
        shutdown.await;
        Ok(())
    })
    .await
}

pub async fn run_backend_server_until_signal(
    config: BackendServerConfig,
    data_runtime: BackendDataRuntime,
) -> Result<(), BackendApplicationError> {
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|error| {
        BackendApplicationError::new(
            BackendApplicationErrorKind::Signal,
            format!("install SIGINT listener failed: {error}"),
        )
    })?;

    run_backend_server_until(config, data_runtime, async {
        #[cfg(unix)]
        {
            // Register the OS handler before the host emits its ready marker.
            // A supervisor can otherwise deliver SIGINT in the narrow window
            // between readiness and the first poll of `tokio::signal::ctrl_c`.
            interrupt.recv().await;
            Ok(())
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await.map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Signal,
                format!("Ctrl-C listener failed: {error}"),
            )
        })
    })
    .await
}

async fn run_backend_server_until<F>(
    config: BackendServerConfig,
    data_runtime: BackendDataRuntime,
    shutdown: F,
) -> Result<(), BackendApplicationError>
where
    F: Future<Output = Result<(), BackendApplicationError>> + Send,
{
    let mut host = BackendApplicationHost::open(config, data_runtime)?;
    println!("{}", host.ready_marker());
    tokio::pin!(shutdown);

    let primary = loop {
        tokio::select! {
            signal_result = &mut shutdown => break signal_result,
            _ = tokio::time::sleep(SUPERVISION_POLL_INTERVAL) => match host.poll_failure() {
                Ok(Some(error)) | Err(error) => break Err(error),
                Ok(None) => {}
            },
        }
    };

    let primary = match primary {
        Ok(()) => match host.poll_failure() {
            Ok(Some(error)) | Err(error) => Err(error),
            Ok(None) => Ok(()),
        },
        Err(error) => Err(error),
    };
    host.begin_drain();
    combine_primary_and_shutdown(primary, host.shutdown())
}

fn combine_primary_and_shutdown(
    primary: Result<(), BackendApplicationError>,
    shutdown: Result<(), BackendApplicationError>,
) -> Result<(), BackendApplicationError> {
    match (primary, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(shutdown)) => Err(shutdown),
        (Err(primary), Err(shutdown)) => Err(primary.with_cleanup_context(shutdown)),
    }
}

fn append_cleanup_results(
    mut primary: BackendApplicationError,
    cleanup_results: impl IntoIterator<Item = Result<(), String>>,
) -> BackendApplicationError {
    for cleanup_result in cleanup_results {
        if let Err(cleanup_error) = cleanup_result {
            primary = primary.with_cleanup_context(cleanup_error);
        }
    }
    primary
}

fn combine_shutdown_results(
    listener: Result<(), String>,
    sweep: Result<(), String>,
) -> Result<(), String> {
    match (listener, sweep) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(sweep), Err(resources)) => Err(format!("{sweep}; {resources}")),
    }
}

fn wait_for_native_ready(
    runtime: &BackendDataRuntime,
    endpoint: NativeEndpoint,
    timeout: Duration,
) -> Result<(), String> {
    let connector = runtime.native_transport().connector_for(endpoint.clone())?;
    runtime.block_on(async move {
        tokio::time::timeout(timeout, connector.connect())
            .await
            .map_err(|_| {
                format!(
                    "advertised Native endpoint {endpoint} did not become ready within {}ms",
                    timeout.as_millis()
                )
            })?
            .map(|_| ())
            .map_err(|error| {
                format!("advertised Native endpoint {endpoint} readiness failed: {error}")
            })
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::Duration;

    use super::{
        BackendApplicationError, BackendApplicationErrorKind, BackendApplicationHost,
        BackendServerConfig, QueryLifecycleRegistryConfig, combine_primary_and_shutdown,
        compose_backend_application_services,
    };
    use crate::rpc::runtime::test_backend_native_trust;
    use crate::rpc::transport::nova_rocks_grpc_client::NovaRocksGrpcClient;
    use novarocks_execution::runtime::execution_runtime::{
        ExecutionRuntimeConfig, ExecutionSpillStorageConfig,
    };
    use novarocks_native_trust::NativeClientAuthInterceptor;
    use novarocks_proto_codec::lifecycle as protocol_lifecycle;
    use novarocks_proto_codec::lifecycle::{
        AttemptId, ParticipantBackendIdentity, ParticipantManifest, ParticipantManifestDigest,
        QueryAbortRequest, QueryControlEndpoint, QueryExecutionId, QueryInitRequest, QueryOptions,
        QueryTerminationReason,
    };
    use novarocks_proto_models::novarocks as protocol;
    use novarocks_proto_models::novarocks::{
        AbortQueryRequest as ProtoAbortQueryRequest, HeartbeatRequest, HeartbeatResponse,
        InitQueryRequest as ProtoInitQueryRequest, QueryControlAttach as ProtoQueryControlAttach,
        QueryControlRequest as ProtoQueryControlRequest,
    };
    use novarocks_spi::connector::WriteCommitEvidenceLimits;
    use novarocks_types::QueryId;
    use novarocks_types::{AdvertiseEndpoint, BackendProcessId, NativeEndpoint};
    use tokio_stream::wrappers::ReceiverStream;

    static LIVE_HOST_TEST: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn unused_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener
            .local_addr()
            .expect("read ephemeral address")
            .port();
        drop(listener);
        port
    }

    fn execution_runtime_config() -> ExecutionRuntimeConfig {
        ExecutionRuntimeConfig {
            driver_threads: 1,
            scan_threads: 1,
            scan_queue_capacity: 1,
            spill_io_threads: 1,
            spill_io_queue_capacity: 1,
            spill_storage: ExecutionSpillStorageConfig::default(),
            exchange_wait_ms: 1,
            exchange_io_threads: 1,
            exchange_io_max_inflight_bytes: 1,
            exchange_max_transmit_batched_bytes: 1,
            operator_buffer_chunks: 1,
            local_exchange_buffer_mem_limit_per_driver: 1,
            local_exchange_max_buffered_rows: -1,
            connector_io_tasks_per_scan_operator: 1,
            scan_submit_fail_max: 1,
            scan_submit_fail_timeout_ms: 1,
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            sink_io_worker_threads: 1,
            sink_io_max_blocking_threads: 1,
        }
    }

    fn test_data_runtime() -> crate::BackendDataRuntime {
        crate::rpc::runtime::test_backend_data_runtime()
    }

    fn http_get(port: u16, path: &str) -> std::io::Result<String> {
        let mut stream =
            std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect HTTP listener");
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set HTTP read timeout");
        write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
        )
        .expect("write HTTP request");
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    }

    fn query_lifecycle_registry_config(
        heartbeat_timeout: Duration,
    ) -> QueryLifecycleRegistryConfig {
        QueryLifecycleRegistryConfig::new(
            4_096,
            16_384,
            Duration::from_millis(120_000),
            heartbeat_timeout,
            Duration::from_millis(30_000),
            256,
            32,
            48 * 1024 * 1024,
            256 * 1024 * 1024,
            512,
            48 * 1024 * 1024,
            Duration::from_millis(30_000),
            Duration::from_millis(5_000),
            Duration::from_millis(5_000),
            5,
            Duration::from_millis(100),
            Duration::from_millis(1_000),
            Duration::from_millis(120_000),
            4_096,
            256 * 1024 * 1024,
        )
    }

    fn backend_config(grpc_port: u16, advertise_port: u16) -> BackendServerConfig {
        BackendServerConfig {
            bind_host: "127.0.0.1".to_string(),
            grpc_port,
            metrics_http_port: unused_port(),
            advertise_endpoint: AdvertiseEndpoint {
                host: "127.0.0.1".to_string(),
                port: advertise_port,
            },
            native_trust: crate::rpc::runtime::test_backend_native_trust(),
            native_transport: crate::rpc::runtime::BackendNativeTransport::Plaintext,
            frontend_endpoint: NativeEndpoint::from_host_port("127.0.0.1", unused_port())
                .expect("valid frontend endpoint"),
            announce_interval: Duration::from_secs(60),
            announce_initial_backoff: Duration::from_millis(100),
            announce_max_backoff: Duration::from_secs(2),
            query_lifecycle_sweep_interval: Duration::from_millis(1_000),
            query_lifecycle_config: query_lifecycle_registry_config(Duration::from_millis(5_000)),
            write_commit_evidence_limits: WriteCommitEvidenceLimits::default(),
            execution_runtime_config: execution_runtime_config(),
            execution_installers: Vec::new(),
            read_execution_bundle_factories: Vec::new(),
        }
    }

    fn live_query_init_request(process_id: BackendProcessId, query_low: i64) -> QueryInitRequest {
        let execution_id = QueryExecutionId::new(
            QueryId::new(0x514c_4302, query_low),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("valid execution id");
        QueryInitRequest::from_manifest(
            ParticipantManifest::new(
                execution_id,
                ParticipantBackendIdentity::new(
                    process_id,
                    QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid backend endpoint"),
                )
                .expect("valid backend identity"),
                [novarocks_proto_models::common::UniqueId {
                    hi: query_low,
                    lo: 1,
                }],
                QueryOptions::parse(protocol::QueryOptions::default())
                    .expect("valid default query options"),
                10_000,
                [],
                None,
                std::time::Duration::from_secs(30),
                QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
            )
            .expect("valid participant manifest"),
        )
    }

    fn protocol_init_request(request: &QueryInitRequest) -> protocol_lifecycle::QueryInitRequest {
        request.clone()
    }

    fn heartbeat_command(sequence: u64, sent_mono_ns: u64) -> ProtoQueryControlRequest {
        ProtoQueryControlRequest {
            command: Some(protocol::query_control_request::Command::Heartbeat(
                protocol::QueryControlHeartbeat {
                    sequence,
                    sent_mono_ns,
                },
            )),
        }
    }

    fn abort_command(reason: impl Into<String>) -> ProtoQueryControlRequest {
        ProtoQueryControlRequest {
            command: Some(protocol::query_control_request::Command::Abort(
                protocol::QueryControlAbort {
                    reason: reason.into(),
                },
            )),
        }
    }

    fn assert_event(
        event: protocol::QueryControlResponse,
        predicate: impl FnOnce(protocol::query_control_response::Event) -> bool,
    ) {
        assert!(predicate(event.event.expect("query control event")));
    }

    fn protocol_control_attach(
        init: &protocol_lifecycle::QueryInitRequest,
        frontend_owner_epoch: u64,
    ) -> ProtoQueryControlRequest {
        let manifest = init.manifest().expect("validated InitQuery has manifest");
        ProtoQueryControlRequest {
            command: Some(protocol::query_control_request::Command::Attach(
                ProtoQueryControlAttach {
                    execution_id: manifest.as_proto().execution_id,
                    init_digest: init
                        .manifest()
                        .expect("validated init manifest")
                        .digest()
                        .expect("validated InitQuery has digest")
                        .as_bytes()
                        .to_vec(),
                    frontend_owner_epoch,
                },
            )),
        }
    }

    fn protocol_abort_request(
        init: &protocol_lifecycle::QueryInitRequest,
        digest: &[u8],
        reason: impl Into<String>,
    ) -> ProtoAbortQueryRequest {
        let manifest = init.manifest().expect("validated InitQuery has manifest");
        ProtoAbortQueryRequest {
            execution_id: manifest.as_proto().execution_id,
            init_digest: digest.to_vec(),
            reason: reason.into(),
        }
    }

    async fn connect_live_client(
        grpc_port: u16,
    ) -> NovaRocksGrpcClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            NativeClientAuthInterceptor,
        >,
    > {
        let channel =
            tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
                .expect("construct native backend test endpoint")
                .connect()
                .await
                .expect("connect native backend gRPC");
        NovaRocksGrpcClient::with_interceptor(
            channel,
            test_backend_native_trust().client_interceptor(),
        )
        .max_encoding_message_size(64 * 1024 * 1024)
        .max_decoding_message_size(64 * 1024 * 1024)
    }

    async fn connect_live_channel(grpc_port: u16) -> tonic::transport::Channel {
        tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
            .expect("construct native backend test endpoint")
            .connect()
            .await
            .expect("connect native backend gRPC")
    }

    #[test]
    fn application_composition_owns_one_query_lifecycle_registry() {
        let services = compose_backend_application_services(
            test_data_runtime(),
            execution_runtime_config(),
            query_lifecycle_registry_config(Duration::from_millis(5_000)),
            WriteCommitEvidenceLimits::default(),
            &[],
            &[],
        )
        .expect("compose backend application services");

        assert_eq!(
            Arc::strong_count(&services.query_lifecycle_registry),
            3,
            "application, Stage ingress, and fragment service must share exactly one registry"
        );
    }

    #[test]
    fn readiness_failure_stops_and_joins_started_listener() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        config.advertise_endpoint.host = "127.0.0.2".to_string();
        let error = BackendApplicationHost::open_with_readiness_timeout(
            config,
            test_data_runtime(),
            std::time::Duration::from_millis(25),
        )
        .expect_err("unreachable advertised endpoint must fail readiness");

        assert_eq!(error.kind(), BackendApplicationErrorKind::Readiness);
        TcpListener::bind(("127.0.0.1", grpc_port))
            .expect("readiness cleanup must release the started listener");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_query_control_attachment_live_loopback_round_trip() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host =
            BackendApplicationHost::open(backend_config(grpc_port, grpc_port), test_data_runtime())
                .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;
        let process_id = host
            .process_descriptor()
            .process_id()
            .expect("host process identity");
        let init = live_query_init_request(process_id, 901);
        let protocol_init = protocol_init_request(&init);
        client
            .init_query(protocol_init.as_proto().clone())
            .await
            .expect("InitQuery succeeds");

        let (commands, command_rx) = tokio::sync::mpsc::channel(4);
        commands
            .send(protocol_control_attach(&protocol_init, 9))
            .await
            .expect("send Attach");
        let mut events = client
            .query_control_stream(ReceiverStream::new(command_rx))
            .await
            .expect("attach QueryControlStream")
            .into_inner();
        assert_event(
            events
                .message()
                .await
                .expect("read ControlReady")
                .expect("ControlReady"),
            |event| {
                matches!(
                    event,
                    protocol::query_control_response::Event::ControlReady(_)
                )
            },
        );
        commands
            .send(heartbeat_command(77, 123))
            .await
            .expect("send heartbeat");
        assert_event(
            events
                .message()
                .await
                .expect("read HeartbeatAck")
                .expect("HeartbeatAck"),
            |event| {
                matches!(
                    event,
                    protocol::query_control_response::Event::HeartbeatAck(
                        protocol::QueryControlHeartbeatAck { sequence: 77 }
                    )
                )
            },
        );
        commands
            .send(abort_command("live loopback cancellation"))
            .await
            .expect("send Abort");
        assert_event(
            events
                .message()
                .await
                .expect("read TerminationAccepted")
                .expect("TerminationAccepted"),
            |event| {
                matches!(
                    event,
                    protocol::query_control_response::Event::TerminationAccepted(
                        protocol::QueryControlTerminationAccepted { reason }
                    ) if reason == QueryTerminationReason::QueryTerminationCoordinatorAbort as i32
                )
            },
        );
        drop(events);
        drop(commands);
        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_query_control_heartbeat_timeout_fails_closed_with_open_socket() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        config.query_lifecycle_sweep_interval = Duration::from_millis(50);
        config.query_lifecycle_config = query_lifecycle_registry_config(Duration::from_millis(250));
        let host = BackendApplicationHost::open(config, test_data_runtime())
            .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;
        let process_id = host
            .process_descriptor()
            .process_id()
            .expect("host process identity");
        let init = live_query_init_request(process_id, 902);
        let protocol_init = protocol_init_request(&init);
        client
            .init_query(protocol_init.as_proto().clone())
            .await
            .expect("InitQuery succeeds");
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        commands
            .send(protocol_control_attach(&protocol_init, 9))
            .await
            .expect("send Attach");
        let mut events = client
            .query_control_stream(ReceiverStream::new(command_rx))
            .await
            .expect("attach QueryControlStream")
            .into_inner();
        let _ = events
            .message()
            .await
            .expect("read ControlReady")
            .expect("ControlReady");

        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert_event(
            tokio::time::timeout(std::time::Duration::from_secs(1), events.message())
                .await
                .expect("timeout termination event arrives")
                .expect("read timeout termination event")
                .expect("timeout TerminationAccepted"),
            |event| {
                matches!(
                    event,
                    protocol::query_control_response::Event::TerminationAccepted(
                        protocol::QueryControlTerminationAccepted { reason }
                    ) if reason == QueryTerminationReason::QueryTerminationCoordinatorHeartbeatTimeout as i32
                )
            },
        );
        let termination = client
            .abort_query(protocol_abort_request(
                &protocol_init,
                protocol_init
                    .manifest()
                    .expect("validated init manifest")
                    .digest()
                    .expect("validated InitQuery has digest")
                    .as_bytes(),
                "probe latched timeout",
            ))
            .await
            .expect("AbortQuery observes termination")
            .into_inner();
        assert_eq!(
            termination.accepted_reason,
            novarocks_proto_models::novarocks::QueryTerminationReason::
                QueryTerminationCoordinatorHeartbeatTimeout as i32
        );

        drop(events);
        drop(commands);
        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_shutdown_closes_live_query_control_stream_and_fails_closed() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host =
            BackendApplicationHost::open(backend_config(grpc_port, grpc_port), test_data_runtime())
                .expect("native backend host starts");
        let registry = Arc::clone(&host._query_lifecycle_registry);
        let mut client = connect_live_client(grpc_port).await;
        let process_id = host
            .process_descriptor()
            .process_id()
            .expect("host process identity");
        let init = live_query_init_request(process_id, 903);
        let protocol_init = protocol_init_request(&init);
        client
            .init_query(protocol_init.as_proto().clone())
            .await
            .expect("InitQuery succeeds");
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        commands
            .send(protocol_control_attach(&protocol_init, 9))
            .await
            .expect("send Attach");
        let mut events = client
            .query_control_stream(ReceiverStream::new(command_rx))
            .await
            .expect("attach QueryControlStream")
            .into_inner();
        let _ = events
            .message()
            .await
            .expect("read ControlReady")
            .expect("ControlReady");
        for sequence in 1..=17 {
            commands
                .send(heartbeat_command(sequence, sequence))
                .await
                .expect("send heartbeat without draining ACKs");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::sync_channel(1);
        let shutdown_thread = std::thread::spawn(move || {
            let _ = shutdown_tx.send(host.shutdown());
        });
        let early_shutdown = shutdown_rx.recv_timeout(std::time::Duration::from_millis(500));
        let returned_while_stream_live = early_shutdown.is_ok();

        // Always release the old implementation's graceful-shutdown wait so RED
        // leaves no global listener or detached thread behind.
        drop(events);
        drop(commands);
        let shutdown = match early_shutdown {
            Ok(result) => result,
            Err(_) => shutdown_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("shutdown completes after releasing the stream"),
        };
        shutdown_thread.join().expect("join shutdown thread");
        shutdown.expect("native backend shutdown");
        assert!(
            returned_while_stream_live,
            "host shutdown must not wait indefinitely for a live bidi stream"
        );

        let termination = registry
            .abort_query(QueryAbortRequest::new(
                init.manifest()
                    .expect("validated init manifest")
                    .execution_id()
                    .expect("validated manifest execution id"),
                ParticipantManifestDigest::new(
                    *protocol_init
                        .manifest()
                        .expect("validated init manifest")
                        .digest()
                        .expect("validated InitQuery has digest")
                        .as_bytes(),
                ),
                "observe fail-closed shutdown",
            ))
            .expect("observe latched shutdown termination");
        assert_eq!(
            termination
                .accepted_reason()
                .expect("validated termination reason"),
            QueryTerminationReason::QueryTerminationCoordinatorStreamLost
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_malformed_init_query_returns_invalid_argument() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host =
            BackendApplicationHost::open(backend_config(grpc_port, grpc_port), test_data_runtime())
                .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;

        let error = client
            .init_query(ProtoInitQueryRequest::default())
            .await
            .expect_err("malformed InitQuery must be a transport-visible error");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_authenticates_complete_native_route_set_before_domain_or_fallback() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let metrics_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        config.metrics_http_port = metrics_port;
        let host = BackendApplicationHost::open(config, test_data_runtime())
            .expect("native backend host starts");

        let mut missing_auth = NovaRocksGrpcClient::new(connect_live_channel(grpc_port).await);
        let error = missing_auth
            .heartbeat(HeartbeatRequest {
                expected_process_id: host.process_descriptor().as_proto().process_id.clone(),
            })
            .await
            .expect_err("Native RPC without JWT must fail before domain validation");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert_eq!(error.message(), "native caller authentication failed");
        let metrics = http_get(metrics_port, "/metrics").expect("read backend metrics");
        assert!(metrics.contains(
            "novarocks_native_authentication_failures_total{reason=\"authentication\"} 1"
        ));

        let channel = connect_live_channel(grpc_port).await;
        let service = tonic::service::interceptor::InterceptedService::new(
            channel,
            test_backend_native_trust().client_interceptor(),
        );
        let mut grpc = tonic::client::Grpc::new(service);
        grpc.ready()
            .await
            .expect("authenticated test client is ready");
        let result: Result<tonic::Response<HeartbeatResponse>, tonic::Status> = grpc
            .unary(
                tonic::Request::new(HeartbeatRequest {
                    expected_process_id: host.process_descriptor().as_proto().process_id.clone(),
                }),
                "/novarocks.NovaRocksGrpc/Unknown"
                    .parse()
                    .expect("valid unknown native RPC path"),
                tonic::codec::ProstCodec::default(),
            )
            .await;
        let error = result.expect_err("valid JWT must reach the Native fallback");
        assert_eq!(error.code(), tonic::Code::Unimplemented);

        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_malformed_abort_query_returns_invalid_argument() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host =
            BackendApplicationHost::open(backend_config(grpc_port, grpc_port), test_data_runtime())
                .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;

        let error = client
            .abort_query(ProtoAbortQueryRequest::default())
            .await
            .expect_err("malformed AbortQuery must be a transport-visible error");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        host.shutdown().expect("native backend shutdown");
    }

    #[test]
    fn application_exposes_metrics_only_on_the_management_listener() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let metrics_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        config.metrics_http_port = metrics_port;
        let host = BackendApplicationHost::open(config, test_data_runtime())
            .expect("native backend host starts");

        if let Ok(native_response) = http_get(grpc_port, "/metrics") {
            assert!(!native_response.contains("novarocks_backend_query_lifecycle_entries"));
        }

        let management_response =
            http_get(metrics_port, "/metrics").expect("read management metrics");
        assert!(management_response.starts_with("HTTP/1.1 200"));
        assert!(management_response.contains("novarocks_backend_query_lifecycle_entries"));

        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_abort_digest_mismatch_is_rejected_without_terminating_entry() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host =
            BackendApplicationHost::open(backend_config(grpc_port, grpc_port), test_data_runtime())
                .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;
        let process_id = host
            .process_descriptor()
            .process_id()
            .expect("host process identity");
        let init = live_query_init_request(process_id, 904);
        let different = live_query_init_request(process_id, 905);
        let protocol_init = protocol_init_request(&init);
        let protocol_different = protocol_init_request(&different);
        client
            .init_query(protocol_init.as_proto().clone())
            .await
            .expect("InitQuery succeeds");

        let error = client
            .abort_query(protocol_abort_request(
                &protocol_init,
                protocol_different
                    .manifest()
                    .expect("validated init manifest")
                    .digest()
                    .expect("validated InitQuery has digest")
                    .as_bytes(),
                "mismatched digest",
            ))
            .await
            .expect_err("digest mismatch must be rejected");
        assert_eq!(error.code(), tonic::Code::AlreadyExists);

        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        commands
            .send(protocol_control_attach(&protocol_init, 9))
            .await
            .expect("send Attach");
        let mut events = client
            .query_control_stream(ReceiverStream::new(command_rx))
            .await
            .expect("mismatched abort leaves entry attachable")
            .into_inner();
        assert_event(
            events
                .message()
                .await
                .expect("read ControlReady")
                .expect("ControlReady"),
            |event| {
                matches!(
                    event,
                    protocol::query_control_response::Event::ControlReady(_)
                )
            },
        );

        drop(events);
        drop(commands);
        host.shutdown().expect("native backend shutdown");
    }

    #[test]
    fn supervision_error_remains_primary_when_shutdown_also_fails() {
        let error = combine_primary_and_shutdown(
            Err(BackendApplicationError::new(
                BackendApplicationErrorKind::Supervision,
                "gRPC server exited",
            )),
            Err(BackendApplicationError::new(
                BackendApplicationErrorKind::Shutdown,
                "gRPC join failed",
            )),
        )
        .expect_err("supervision failure must be returned");

        assert_eq!(error.kind(), BackendApplicationErrorKind::Supervision);
        assert!(
            error
                .to_string()
                .contains("cleanup failed: Shutdown: gRPC join failed")
        );
    }
}
