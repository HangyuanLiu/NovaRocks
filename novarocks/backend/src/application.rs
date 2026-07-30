use std::fmt;
use std::future::Future;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks::common::app_config::{self, NovaRocksConfig};
use novarocks::common::network;
use novarocks::connector::ConnectorRegistry;
use novarocks::query_execution::lifecycle::{
    QueryAbortRequest, QueryControlAttach, QueryControlAttachment, QueryInitAck, QueryInitRequest,
    QueryLifecycleError, QueryLifecycleIngress, QueryStageAck, QueryStageOutcome,
    QueryStageRequest, QueryStartAck, QueryStartRequest, QueryTerminationAck,
};
use novarocks::query_execution::report::{NativeReportHandler, NativeReportHandlerError};
use novarocks::service::grpc_server;

use crate::fragment::control::FragmentControlRegistry;
use crate::fragment::{
    NativeFragmentService, grpc_exchange_transmitter, grpc_fragment_lookup_client,
    native_fragment_event_sink, native_result_writer,
};
use crate::query_lifecycle::{
    NativeQueryLifecycleLocalRuntime, QueryLifecycleRegistry, QueryLifecycleRegistryConfig,
};

const READINESS_TIMEOUT: Duration = Duration::from_secs(5);
const SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(50);
const BACKEND_REPORT_ROLE_REJECTION: &str =
    "native backend role does not own coordinator report ingress";

struct BackendNativeReportHandler;

impl NativeReportHandler for BackendNativeReportHandler {
    fn handle_native_report(
        &self,
        _report: novarocks::proto::novarocks::ExecStatusReport,
    ) -> Result<(), NativeReportHandlerError> {
        Err(NativeReportHandlerError::role_rejected(
            BACKEND_REPORT_ROLE_REJECTION,
        ))
    }
}

pub fn backend_native_report_handler() -> Arc<dyn NativeReportHandler> {
    Arc::new(BackendNativeReportHandler)
}

pub struct BackendServerConfig {
    pub config: NovaRocksConfig,
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
    _native_fragment_service: Arc<NativeFragmentService>,
    _query_lifecycle_registry: Arc<QueryLifecycleRegistry>,
    execution_host: Arc<crate::ConnectorExecutionHost>,
    query_lifecycle_sweep: QueryLifecycleSweepTask,
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
    fn bind_backend_identity(&self, backend_id: u64) -> Result<(), QueryLifecycleError> {
        self.registry.bind_backend_identity(backend_id)
    }

    fn init_query(&self, request: QueryInitRequest) -> QueryInitAck {
        self.registry.init_query(request)
    }

    fn stage_fragments(&self, request: QueryStageRequest) -> QueryStageAck {
        match self.registry.begin_stage(request.clone()) {
            crate::query_lifecycle::StageBuildDecision::Complete(ack) => ack,
            crate::query_lifecycle::StageBuildDecision::Build(permit) => {
                let execution_id = request.execution_id();
                let build = self.fragments.stage_fragments(
                    execution_id,
                    request.fragments(),
                    permit.gate(),
                );
                match build {
                    Ok(()) => permit.commit(),
                    Err(error) => QueryStageAck::new(
                        execution_id,
                        request.digest_version(),
                        request.digest(),
                        QueryStageOutcome::RejectedLocalFailure,
                        error.to_string(),
                    ),
                }
            }
        }
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
    join_handle: Option<std::thread::JoinHandle<()>>,
}

impl QueryLifecycleSweepTask {
    fn start(registry: Arc<QueryLifecycleRegistry>, interval: Duration) -> Result<Self, String> {
        let (stop_tx, stop_rx) = std::sync::mpsc::channel();
        let join_handle = std::thread::Builder::new()
            .name("query-lifecycle-sweep".to_string())
            .spawn(move || {
                loop {
                    match stop_rx.recv_timeout(interval) {
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            registry.sweep_expired(Instant::now());
                        }
                        Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .map_err(|error| format!("spawn query lifecycle sweep task: {error}"))?;
        Ok(Self {
            stop_tx: Some(stop_tx),
            join_handle: Some(join_handle),
        })
    }

    fn stop(&mut self) -> Result<(), String> {
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
    config: &NovaRocksConfig,
) -> Result<BackendApplicationServices, BackendApplicationError> {
    let controls = Arc::new(FragmentControlRegistry::default());
    let execution_host = Arc::new(crate::ConnectorExecutionHost::new());
    let local_runtime = Arc::new(NativeQueryLifecycleLocalRuntime::new(
        Arc::clone(&controls),
        Arc::clone(&execution_host),
    ));
    let query_lifecycle_registry = QueryLifecycleRegistry::new_unbound(
        novarocks::runtime::start_epoch::start_epoch(),
        local_runtime,
        QueryLifecycleRegistryConfig::from_runtime_config(&config.runtime),
    );
    let connector_registry = Arc::new(ConnectorRegistry::new());
    let default_object_store = config.connector.object_store_config().map_err(|error| {
        BackendApplicationError::new(
            BackendApplicationErrorKind::Configuration,
            format!("resolve connector startup object-store binding: {error}"),
        )
    })?;
    for installer in
        novarocks::connector::compose_backend_connector_execution_installers(default_object_store)
            .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("compose connector execution installers: {error}"),
            )
        })?
    {
        execution_host
            .register_installer(installer)
            .map_err(|error| {
                BackendApplicationError::new(
                    BackendApplicationErrorKind::Configuration,
                    format!("register connector execution installer: {error}"),
                )
            })?;
    }
    let native_fragment_service = Arc::new(NativeFragmentService::new_with_controls(
        grpc_exchange_transmitter(),
        grpc_fragment_lookup_client(),
        native_result_writer(),
        native_fragment_event_sink(),
        controls,
        Arc::clone(&query_lifecycle_registry),
        connector_registry,
        Arc::clone(&execution_host),
    ));
    let query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress> =
        Arc::new(BackendStageLifecycleIngress {
            registry: Arc::clone(&query_lifecycle_registry),
            fragments: Arc::clone(&native_fragment_service),
        });
    Ok(BackendApplicationServices {
        native_fragment_service,
        query_lifecycle_registry,
        execution_host,
        query_lifecycle_ingress,
    })
}

impl BackendApplicationHost {
    pub fn open(config: BackendServerConfig) -> Result<Self, BackendApplicationError> {
        Self::open_with_readiness_timeout(config, READINESS_TIMEOUT)
    }

    /// Starts a native backend whose report ingress is owned by the supplied
    /// coordinator. This is used only by the all-in-one composition root.
    pub fn open_with_native_report_handler(
        config: BackendServerConfig,
        native_report_handler: Arc<dyn NativeReportHandler>,
    ) -> Result<Self, BackendApplicationError> {
        Self::open_with_readiness_timeout_and_report_handler(
            config,
            READINESS_TIMEOUT,
            native_report_handler,
        )
    }

    pub fn ready_marker(&self) -> &str {
        &self.ready_marker
    }

    pub fn poll_failure(
        &mut self,
    ) -> Result<Option<BackendApplicationError>, BackendApplicationError> {
        grpc_server::poll_grpc_server_failure()
            .map_err(|error| {
                BackendApplicationError::new(BackendApplicationErrorKind::Supervision, error)
            })
            .map(|failure| {
                failure.map(|error| {
                    BackendApplicationError::new(BackendApplicationErrorKind::Supervision, error)
                })
            })
    }

    pub fn shutdown(mut self) -> Result<(), BackendApplicationError> {
        let execution_shutdown = self
            .execution_host
            .shutdown()
            .map_err(|error| error.to_string());
        let resource_result = stop_backend_resources();
        let sweep_result = self.query_lifecycle_sweep.stop();
        combine_shutdown_results(sweep_result, resource_result)
            .and_then(|()| execution_shutdown)
            .map_err(|error| {
                BackendApplicationError::new(BackendApplicationErrorKind::Shutdown, error)
            })
    }

    fn open_with_readiness_timeout(
        config: BackendServerConfig,
        readiness_timeout: Duration,
    ) -> Result<Self, BackendApplicationError> {
        Self::open_with_readiness_timeout_and_report_handler(
            config,
            readiness_timeout,
            backend_native_report_handler(),
        )
    }

    fn open_with_readiness_timeout_and_report_handler(
        config: BackendServerConfig,
        readiness_timeout: Duration,
        native_report_handler: Arc<dyn NativeReportHandler>,
    ) -> Result<Self, BackendApplicationError> {
        let config = config.config;
        app_config::install_preloaded_config(config.clone());

        let advertise_endpoint = network::standalone_advertise_endpoint_for_config(&config)
            .map_err(|error| {
                BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error)
            })?;
        let readiness_addr =
            advertised_probe_addr(&advertise_endpoint.host, advertise_endpoint.port).map_err(
                |error| {
                    BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error)
                },
            )?;
        let bind_host = config.server.host.clone();
        let grpc_port = config.server.grpc_port;
        let services = compose_backend_application_services(&config)?;
        let native_fragment_service = Arc::clone(&services.native_fragment_service);
        let mut query_lifecycle_sweep = QueryLifecycleSweepTask::start(
            Arc::clone(&services.query_lifecycle_registry),
            Duration::from_millis(config.runtime.query_control_heartbeat_interval_ms),
        )
        .map_err(|error| BackendApplicationError::new(BackendApplicationErrorKind::Start, error))?;

        grpc_server::start_grpc_exchange_server(
            &bind_host,
            grpc_port,
            native_fragment_service.clone(),
            services.query_lifecycle_ingress.clone(),
            native_report_handler,
        )
        .map_err(|error| {
            let _ = query_lifecycle_sweep.stop();
            BackendApplicationError::new(
                BackendApplicationErrorKind::Start,
                format!("start native backend gRPC server on {bind_host}:{grpc_port}: {error}"),
            )
        })?;

        if let Err(error) = wait_for_tcp_ready(readiness_addr, readiness_timeout) {
            let _ = query_lifecycle_sweep.stop();
            return Err(cleanup_after_primary_error(BackendApplicationError::new(
                BackendApplicationErrorKind::Readiness,
                format!("advertised endpoint readiness failed: {error}"),
            )));
        }

        Ok(Self {
            ready_marker: format!(
                "NOVAROCKS_READY role=be grpc_port={grpc_port} advertise_host={} pid={}",
                advertise_endpoint.host,
                std::process::id()
            ),
            _native_fragment_service: native_fragment_service,
            _query_lifecycle_registry: services.query_lifecycle_registry,
            execution_host: services.execution_host,
            query_lifecycle_sweep,
        })
    }
}

pub fn run_backend_server(config: BackendServerConfig) -> Result<(), BackendApplicationError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Start,
                format!("build backend Tokio runtime failed: {error}"),
            )
        })?;
    runtime.block_on(run_backend_server_until_signal(config))
}

pub async fn run_backend_server_until_shutdown<F>(
    config: BackendServerConfig,
    shutdown: F,
) -> Result<(), BackendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    run_backend_server_until(config, async move {
        shutdown.await;
        Ok(())
    })
    .await
}

async fn run_backend_server_until_signal(
    config: BackendServerConfig,
) -> Result<(), BackendApplicationError> {
    run_backend_server_until(config, async {
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
    shutdown: F,
) -> Result<(), BackendApplicationError>
where
    F: Future<Output = Result<(), BackendApplicationError>> + Send,
{
    let mut host = BackendApplicationHost::open(config)?;
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

fn cleanup_after_primary_error(primary: BackendApplicationError) -> BackendApplicationError {
    match stop_backend_resources() {
        Ok(()) => primary,
        Err(cleanup_error) => primary.with_cleanup_context(cleanup_error),
    }
}

fn stop_backend_resources() -> Result<(), String> {
    let grpc_result = grpc_server::stop_grpc_server();
    novarocks::query_execution::native_fragment_report::stop();
    grpc_result
}

fn combine_shutdown_results(
    sweep: Result<(), String>,
    resources: Result<(), String>,
) -> Result<(), String> {
    match (sweep, resources) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(sweep), Err(resources)) => Err(format!("{sweep}; {resources}")),
    }
}

fn advertised_probe_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let host = host
        .trim()
        .trim_matches(|character| character == '[' || character == ']');
    (host, port)
        .to_socket_addrs()
        .map_err(|error| format!("resolve advertised endpoint {host}:{port}: {error}"))?
        .next()
        .ok_or_else(|| format!("advertised endpoint {host}:{port} resolved no addresses"))
}

fn wait_for_tcp_ready(addr: SocketAddr, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let attempt_timeout = remaining.min(Duration::from_millis(100));
        match TcpStream::connect_timeout(&addr, attempt_timeout) {
            Ok(_) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
        std::thread::sleep(remaining.min(Duration::from_millis(10)));
    }
    match last_error {
        Some(error) => Err(format!(
            "advertised endpoint {addr} did not become ready within {}ms: {error}",
            timeout.as_millis()
        )),
        None => Err(format!(
            "advertised endpoint {addr} did not become ready within {}ms",
            timeout.as_millis()
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::{Arc, LazyLock, Mutex};

    use super::{
        BackendApplicationError, BackendApplicationErrorKind, BackendApplicationHost,
        BackendServerConfig, combine_primary_and_shutdown, compose_backend_application_services,
    };
    use novarocks::common::app_config::NovaRocksConfig;
    use novarocks::proto::common::{Status, UniqueId};
    use novarocks::proto::novarocks::{
        AbortQueryRequest as ProtoAbortQueryRequest, ExecStatusReport, HeartbeatRequest,
        InitQueryRequest as ProtoInitQueryRequest, ReportExecStatusRequest,
    };
    use novarocks::query_execution::contract::QueryId;
    use novarocks::query_execution::lifecycle::contract::{
        decode_query_control_event, encode_abort_query_request, encode_query_control_attach,
        encode_query_control_command, encode_query_init_request,
    };
    use novarocks::query_execution::lifecycle::{
        AttemptId, ParticipantBackendIdentity, ParticipantManifest, ParticipantQueryOptions,
        ParticipantRole, QueryAbortRequest, QueryControlAttach, QueryControlCommand,
        QueryControlEndpoint, QueryControlEvent, QueryExecutionId, QueryInitRequest,
        QueryTerminationReason,
    };
    use novarocks::runtime::query_options::QueryOptions;
    use novarocks::service::grpc_client::NovaRocksGrpcRemoteClient;
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

    fn backend_config(grpc_port: u16, advertise_port: u16) -> BackendServerConfig {
        let mut config = NovaRocksConfig::default();
        config.server.host = "127.0.0.1".to_string();
        config.server.grpc_port = grpc_port;
        config.cluster.advertise_host = "127.0.0.1".to_string();
        config.cluster.advertise_port = advertise_port;
        BackendServerConfig { config }
    }

    fn live_query_init_request(start_epoch: u64, query_low: i64) -> QueryInitRequest {
        let execution_id = QueryExecutionId::new(
            QueryId::new(0x514c_4302, query_low),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("valid execution id");
        QueryInitRequest::from_manifest(
            ParticipantManifest::new(
                execution_id,
                ParticipantBackendIdentity::new(
                    7,
                    QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid backend endpoint"),
                    start_epoch,
                )
                .expect("valid backend identity"),
                [ParticipantRole::FragmentExecutor],
                [novarocks::UniqueId {
                    hi: query_low,
                    lo: 1,
                }],
                ParticipantQueryOptions::new(QueryOptions::default()),
                10_000,
                [],
                None,
                std::time::Duration::from_secs(30),
                QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
            )
            .expect("valid participant manifest"),
        )
    }

    async fn connect_live_client(
        grpc_port: u16,
    ) -> novarocks::proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient<
        tonic::transport::Channel,
    > {
        novarocks::proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient::connect(format!(
            "http://127.0.0.1:{grpc_port}"
        ))
        .await
        .expect("connect native backend gRPC")
    }

    #[test]
    fn application_composition_owns_one_query_lifecycle_registry() {
        let config = NovaRocksConfig::default();
        let services = compose_backend_application_services(&config)
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
        config.config.cluster.advertise_host = "127.0.0.2".to_string();
        let error = BackendApplicationHost::open_with_readiness_timeout(
            config,
            std::time::Duration::from_millis(25),
        )
        .expect_err("unreachable advertised endpoint must fail readiness");

        assert_eq!(error.kind(), BackendApplicationErrorKind::Readiness);
        TcpListener::bind(("127.0.0.1", grpc_port))
            .expect("readiness cleanup must release the started listener");
    }

    #[test]
    fn native_backend_rejects_coordinator_reports_with_role_error() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host = BackendApplicationHost::open(backend_config(grpc_port, grpc_port))
            .expect("native backend host starts");
        let client = NovaRocksGrpcRemoteClient::new(
            format!("127.0.0.1:{grpc_port}")
                .parse()
                .expect("backend address"),
        )
        .expect("gRPC client");

        let response = client
            .blocking_report_exec_status(ReportExecStatusRequest {
                report: Some(ExecStatusReport {
                    query_id: Some(UniqueId { hi: 41, lo: 73 }),
                    fragment_instance_id: Some(UniqueId { hi: 41, lo: 74 }),
                    status: Some(Status::default()),
                    done: true,
                    ..Default::default()
                }),
            })
            .expect("role rejection is returned as a business response");

        assert_eq!(response.status_code, 1);
        assert_eq!(response.error_code, "NativeReportRoleRejected");
        assert_eq!(
            response.message,
            "native backend role does not own coordinator report ingress"
        );
        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn application_query_control_attachment_live_loopback_round_trip() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host = BackendApplicationHost::open(backend_config(grpc_port, grpc_port))
            .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;
        let heartbeat = client
            .heartbeat(HeartbeatRequest {
                assigned_be_id: 7,
                fe_epoch: 1,
            })
            .await
            .expect("bind backend identity")
            .into_inner();
        let init = live_query_init_request(heartbeat.start_epoch, 901);
        client
            .init_query(encode_query_init_request(&init).expect("encode InitQuery"))
            .await
            .expect("InitQuery succeeds");

        let attach = QueryControlAttach::new(init.manifest().execution_id(), init.digest(), 9)
            .expect("valid Attach");
        let (commands, command_rx) = tokio::sync::mpsc::channel(4);
        commands
            .send(encode_query_control_attach(&attach))
            .await
            .expect("send Attach");
        let mut events = client
            .query_control_stream(ReceiverStream::new(command_rx))
            .await
            .expect("attach QueryControlStream")
            .into_inner();
        assert_eq!(
            decode_query_control_event(
                &events
                    .message()
                    .await
                    .expect("read ControlReady")
                    .expect("ControlReady")
            )
            .expect("decode ControlReady"),
            QueryControlEvent::ControlReady
        );
        commands
            .send(encode_query_control_command(
                &QueryControlCommand::Heartbeat {
                    sequence: 77,
                    sent_mono_ns: 123,
                },
            ))
            .await
            .expect("send heartbeat");
        assert_eq!(
            decode_query_control_event(
                &events
                    .message()
                    .await
                    .expect("read HeartbeatAck")
                    .expect("HeartbeatAck")
            )
            .expect("decode HeartbeatAck"),
            QueryControlEvent::HeartbeatAck { sequence: 77 }
        );
        commands
            .send(encode_query_control_command(&QueryControlCommand::Abort {
                reason: "live loopback cancellation".to_string(),
            }))
            .await
            .expect("send Abort");
        assert_eq!(
            decode_query_control_event(
                &events
                    .message()
                    .await
                    .expect("read TerminationAccepted")
                    .expect("TerminationAccepted")
            )
            .expect("decode TerminationAccepted"),
            QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorAbort
            }
        );
        drop(events);
        drop(commands);
        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn application_query_control_heartbeat_timeout_fails_closed_with_open_socket() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        config.config.runtime.query_control_heartbeat_interval_ms = 50;
        config.config.runtime.query_control_heartbeat_timeout_ms = 250;
        let host = BackendApplicationHost::open(config).expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;
        let heartbeat = client
            .heartbeat(HeartbeatRequest {
                assigned_be_id: 7,
                fe_epoch: 1,
            })
            .await
            .expect("bind backend identity")
            .into_inner();
        let init = live_query_init_request(heartbeat.start_epoch, 902);
        client
            .init_query(encode_query_init_request(&init).expect("encode InitQuery"))
            .await
            .expect("InitQuery succeeds");
        let attach = QueryControlAttach::new(init.manifest().execution_id(), init.digest(), 9)
            .expect("valid Attach");
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        commands
            .send(encode_query_control_attach(&attach))
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
        assert_eq!(
            decode_query_control_event(
                &tokio::time::timeout(std::time::Duration::from_secs(1), events.message())
                    .await
                    .expect("timeout termination event arrives")
                    .expect("read timeout termination event")
                    .expect("timeout TerminationAccepted")
            )
            .expect("decode timeout termination event"),
            QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorHeartbeatTimeout
            }
        );
        let termination = client
            .abort_query(encode_abort_query_request(
                &QueryAbortRequest::new(
                    init.manifest().execution_id(),
                    init.digest(),
                    "probe latched timeout",
                )
                .expect("valid abort request"),
            ))
            .await
            .expect("AbortQuery observes termination")
            .into_inner();
        assert_eq!(
            termination.accepted_reason,
            novarocks::proto::novarocks::QueryTerminationReason::
                QueryTerminationCoordinatorHeartbeatTimeout as i32
        );

        drop(events);
        drop(commands);
        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn application_shutdown_closes_live_query_control_stream_and_fails_closed() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host = BackendApplicationHost::open(backend_config(grpc_port, grpc_port))
            .expect("native backend host starts");
        let registry = Arc::clone(&host._query_lifecycle_registry);
        let mut client = connect_live_client(grpc_port).await;
        let heartbeat = client
            .heartbeat(HeartbeatRequest {
                assigned_be_id: 7,
                fe_epoch: 1,
            })
            .await
            .expect("bind backend identity")
            .into_inner();
        let init = live_query_init_request(heartbeat.start_epoch, 903);
        client
            .init_query(encode_query_init_request(&init).expect("encode InitQuery"))
            .await
            .expect("InitQuery succeeds");
        let attach = QueryControlAttach::new(init.manifest().execution_id(), init.digest(), 9)
            .expect("valid Attach");
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        commands
            .send(encode_query_control_attach(&attach))
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
                .send(encode_query_control_command(
                    &QueryControlCommand::Heartbeat {
                        sequence,
                        sent_mono_ns: sequence,
                    },
                ))
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
            .abort_query(
                QueryAbortRequest::new(
                    init.manifest().execution_id(),
                    init.digest(),
                    "observe fail-closed shutdown",
                )
                .expect("valid abort request"),
            )
            .expect("observe latched shutdown termination");
        assert_eq!(
            termination.accepted_reason(),
            QueryTerminationReason::CoordinatorStreamLost
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn application_malformed_init_query_returns_invalid_argument() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host = BackendApplicationHost::open(backend_config(grpc_port, grpc_port))
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
    async fn application_malformed_abort_query_returns_invalid_argument() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host = BackendApplicationHost::open(backend_config(grpc_port, grpc_port))
            .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;

        let error = client
            .abort_query(ProtoAbortQueryRequest::default())
            .await
            .expect_err("malformed AbortQuery must be a transport-visible error");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn application_abort_digest_mismatch_is_rejected_without_terminating_entry() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host = BackendApplicationHost::open(backend_config(grpc_port, grpc_port))
            .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;
        let heartbeat = client
            .heartbeat(HeartbeatRequest {
                assigned_be_id: 7,
                fe_epoch: 1,
            })
            .await
            .expect("bind backend identity")
            .into_inner();
        let init = live_query_init_request(heartbeat.start_epoch, 904);
        let different = live_query_init_request(heartbeat.start_epoch, 905);
        client
            .init_query(encode_query_init_request(&init).expect("encode InitQuery"))
            .await
            .expect("InitQuery succeeds");

        let mismatch = QueryAbortRequest::new(
            init.manifest().execution_id(),
            different.digest(),
            "mismatched digest",
        )
        .expect("valid mismatched abort");
        let error = client
            .abort_query(encode_abort_query_request(&mismatch))
            .await
            .expect_err("digest mismatch must be rejected");
        assert_eq!(error.code(), tonic::Code::AlreadyExists);

        let attach = QueryControlAttach::new(init.manifest().execution_id(), init.digest(), 9)
            .expect("valid Attach");
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        commands
            .send(encode_query_control_attach(&attach))
            .await
            .expect("send Attach");
        let mut events = client
            .query_control_stream(ReceiverStream::new(command_rx))
            .await
            .expect("mismatched abort leaves entry attachable")
            .into_inner();
        assert_eq!(
            decode_query_control_event(
                &events
                    .message()
                    .await
                    .expect("read ControlReady")
                    .expect("ControlReady")
            )
            .expect("decode ControlReady"),
            QueryControlEvent::ControlReady
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
