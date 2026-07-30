use std::fmt;
use std::sync::Arc;
#[cfg(test)]
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use axum::Router;
use novarocks::common::app_config::{self, NovaRocksConfig};
use novarocks::common::network;
use novarocks::query_execution::report::{NativeReportHandler, NativeReportHandlerError};
use novarocks::runtime::fragment::io::SyncFragmentExecutor;
use novarocks::service::{backend_service, grpc_server, heartbeat_service};
use novarocks::thrift::{master_service, types};

use crate::brpc;
use crate::fragment::{
    CompatFragmentService, brpc_exchange_transmitter, brpc_fragment_lookup_client,
    compat_result_writer, lake_meta_storage_resolver,
};
use crate::frontend_rpc;
use crate::load::{
    CompatLoadRegistry, CompatLoadService, LoadTrackingStore, router as load_router,
};
use crate::report::{CompatReportService, new_report_service_with_tracking};

const SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(100);
const COMPAT_REPORT_ROLE_REJECTION: &str =
    "compat backend role does not own native coordinator report ingress";

#[cfg(test)]
static APPLICATION_TEST_LOCK: Mutex<()> = Mutex::new(());

struct CompatNativeReportHandler;

impl NativeReportHandler for CompatNativeReportHandler {
    fn handle_native_report(
        &self,
        _report: novarocks::proto::novarocks::ExecStatusReport,
    ) -> Result<(), NativeReportHandlerError> {
        Err(NativeReportHandlerError::role_rejected(
            COMPAT_REPORT_ROLE_REJECTION,
        ))
    }
}

fn compat_native_report_handler() -> Arc<dyn NativeReportHandler> {
    Arc::new(CompatNativeReportHandler)
}

struct CompatFinishTaskSender;

impl backend_service::FinishTaskSender for CompatFinishTaskSender {
    fn send_finish_task(
        &self,
        fe_addr: &types::TNetworkAddress,
        request: &master_service::TFinishTaskRequest,
    ) -> Result<master_service::TMasterResult, backend_service::FinishTaskSendError> {
        frontend_rpc::finish_task(fe_addr, request.clone()).map_err(|error| {
            if frontend_rpc::is_transport_error(&error) {
                backend_service::FinishTaskSendError::transport(error)
            } else {
                backend_service::FinishTaskSendError::failed(error)
            }
        })
    }
}

struct CompatDiskReportSender;

impl novarocks::service::disk_report::DiskReportSender for CompatDiskReportSender {
    fn send_disk_report(
        &self,
        fe_addr: &types::TNetworkAddress,
        request: &master_service::TReportRequest,
    ) -> Result<(), String> {
        frontend_rpc::report_disk(fe_addr, request.clone())
    }
}

pub struct CompatServerConfig {
    pub config: NovaRocksConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompatApplicationErrorKind {
    Configuration,
    ReportStart,
    GrpcStart,
    HeartbeatStart,
    BackendServiceStart,
    BrpcStart,
    Supervision,
    Shutdown,
}

#[derive(Debug)]
pub struct CompatApplicationError {
    kind: CompatApplicationErrorKind,
    message: String,
}

impl CompatApplicationError {
    fn new(kind: CompatApplicationErrorKind, error: impl fmt::Display) -> Self {
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

    pub const fn kind(&self) -> CompatApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for CompatApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for CompatApplicationError {}

#[derive(Default)]
struct StartedResources {
    grpc: bool,
    heartbeat: bool,
    backend: bool,
    brpc: bool,
    report: bool,
    starlet_metadata: bool,
    frontend_rpc: bool,
}

pub struct CompatApplicationHost {
    ports: Box<dyn CompatPorts>,
    started: StartedResources,
    fragment_service: Arc<CompatFragmentService>,
    report_service: Arc<CompatReportService>,
    load_service: Arc<CompatLoadService>,
    tracking: Arc<LoadTrackingStore>,
    ready_marker: String,
    startup_summary: String,
    #[cfg(test)]
    _test_lifecycle_lock: MutexGuard<'static, ()>,
}

impl fmt::Debug for CompatApplicationHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompatApplicationHost")
            .field("started_grpc", &self.started.grpc)
            .field("started_heartbeat", &self.started.heartbeat)
            .field("started_backend", &self.started.backend)
            .field("started_brpc", &self.started.brpc)
            .field("started_report", &self.started.report)
            .finish_non_exhaustive()
    }
}

impl Drop for CompatApplicationHost {
    fn drop(&mut self) {
        let _ = self.cleanup_started();
    }
}

impl CompatApplicationHost {
    pub fn open(config: CompatServerConfig) -> Result<Self, CompatApplicationError> {
        Self::open_with_ports(config, LiveCompatPorts)
    }

    pub fn ready_marker(&self) -> &str {
        &self.ready_marker
    }

    pub fn startup_summary(&self) -> &str {
        &self.startup_summary
    }

    pub fn poll_failure(
        &mut self,
    ) -> Result<Option<CompatApplicationError>, CompatApplicationError> {
        self.ports
            .poll_grpc_failure()
            .map_err(|error| {
                CompatApplicationError::new(
                    CompatApplicationErrorKind::Supervision,
                    format!("poll compat grpc supervisor failed: {error}"),
                )
            })
            .map(|failure| {
                failure.map(|error| {
                    CompatApplicationError::new(CompatApplicationErrorKind::Supervision, error)
                })
            })
    }

    pub fn shutdown(mut self) -> Result<(), CompatApplicationError> {
        self.cleanup_started().map_err(|error| {
            CompatApplicationError::new(CompatApplicationErrorKind::Shutdown, error)
        })
    }

    fn open_with_ports(
        config: CompatServerConfig,
        ports: impl CompatPorts + 'static,
    ) -> Result<Self, CompatApplicationError> {
        #[cfg(test)]
        let test_lifecycle_lock = APPLICATION_TEST_LOCK
            .lock()
            .expect("compat application test lifecycle lock");
        let config = config.config;
        let advertise_endpoint =
            network::advertise_endpoint_for_config(&config).map_err(|error| {
                CompatApplicationError::new(CompatApplicationErrorKind::Configuration, error)
            })?;
        let memory_limit = config
            .runtime
            .effective_be_mem_limit_bytes()
            .map_err(|error| {
                CompatApplicationError::new(CompatApplicationErrorKind::Configuration, error)
            })?;
        let query_threads = config
            .runtime
            .actual_internal_service_query_rpc_threads()
            .min(u32::MAX as usize) as u32;
        let log_level = compat_log_level(&config.log_level);
        app_config::install_preloaded_config(config.clone());

        let server = &config.server;
        let ready_marker = format!(
            "NOVAROCKS_READY role=compat-be heartbeat_port={} brpc_port={} grpc_port={} pid={}",
            server.heartbeat_port,
            server.brpc_port,
            server.grpc_port,
            std::process::id()
        );
        let startup_summary = format!(
            "novarocksd started (bind_host={}, advertise_host={}, advertise_port={}, heartbeat_port={}, be_port={}, brpc_port={}, http_port={}, grpc_port={}, starlet_port={})",
            server.host,
            advertise_endpoint.host,
            advertise_endpoint.port,
            server.heartbeat_port,
            server.be_port,
            server.brpc_port,
            server.http_port,
            server.grpc_port,
            server.starlet_port
        );
        let heartbeat_config = heartbeat_service::HeartbeatConfig {
            host: server.host.clone(),
            advertise_host: advertise_endpoint.host,
            heartbeat_port: server.heartbeat_port,
            be_port: server.be_port,
            brpc_port: server.brpc_port,
            http_port: server.http_port,
            starlet_port: advertise_endpoint.port,
            mem_limit_bytes: memory_limit,
        };
        let backend_config = backend_service::BackendServiceConfig {
            host: server.host.clone(),
            be_port: server.be_port,
        };
        let load_registry = Arc::new(CompatLoadRegistry::default());
        let tracking = Arc::new(LoadTrackingStore::default());
        let report_service = new_report_service_with_tracking(Arc::clone(&tracking));
        let frontend_rpc_manager = frontend_rpc::create_manager();
        let starlet_metadata_adapter = crate::starlet_metadata::starlet_metadata_adapter();
        let starlet_metadata_provider: Arc<
            dyn novarocks::connector::starrocks::ports::StarletMetadataProvider,
        > = starlet_metadata_adapter.clone();
        let storage_metadata_provider = crate::storage_wire::storage_metadata_provider();
        let fragment_service = Arc::new(CompatFragmentService::new_with_connector_dependencies(
            novarocks::runtime::starrocks_fragment_query::StarRocksFragmentQueryRuntime::new(),
            brpc_exchange_transmitter(),
            brpc_fragment_lookup_client(),
            compat_result_writer(),
            Arc::clone(&report_service),
            Arc::clone(&load_registry),
            tracking.clone(),
            lake_meta_storage_resolver(
                Arc::clone(&starlet_metadata_provider),
                Arc::clone(&storage_metadata_provider),
            ),
            Some(crate::frontend_rpc::table_schema_provider()),
            Some(crate::schema_provider::schema_load_provider()),
            Some(crate::sink_frontend::sink_frontend_provider()),
            Some(Arc::clone(&starlet_metadata_provider)),
            Some(Arc::clone(&storage_metadata_provider)),
        ));
        let fragment_sync_executor: Arc<dyn SyncFragmentExecutor> = fragment_service.clone();
        let load_service = Arc::new(CompatLoadService::new(
            load_registry,
            fragment_sync_executor,
        ));
        let compat_routes = load_router(Arc::clone(&load_service), Arc::clone(&tracking));
        fragment_service
            .compose_connector_bindings(config.connector.object_store_config().map_err(
                |error| {
                    CompatApplicationError::new(
                        CompatApplicationErrorKind::Configuration,
                        format!("resolve connector startup object-store binding: {error}"),
                    )
                },
            )?)
            .map_err(|error| {
                CompatApplicationError::new(
                    CompatApplicationErrorKind::Configuration,
                    format!("compose connector instance bindings: {error}"),
                )
            })?;
        let brpc_config = brpc::CompatConfig {
            host: &server.host,
            heartbeat_port: server.heartbeat_port,
            brpc_port: server.brpc_port,
            internal_service_query_rpc_thread_num: query_threads,
            debug_exec_batch_plan_json: config.debug.exec_batch_plan_json,
            log_level,
            fragment_service_context: Arc::as_ptr(&fragment_service).cast(),
        };

        let mut host = Self {
            ports: Box::new(ports),
            started: StartedResources::default(),
            fragment_service,
            report_service,
            load_service,
            tracking,
            ready_marker,
            startup_summary,
            #[cfg(test)]
            _test_lifecycle_lock: test_lifecycle_lock,
        };
        if let Err(error) = novarocks::service::starlet_metadata::install(starlet_metadata_provider)
        {
            return Err(host.start_failure(CompatApplicationErrorKind::Configuration, error));
        }
        host.started.starlet_metadata = true;
        if let Err(error) = frontend_rpc::install(frontend_rpc_manager) {
            return Err(host.start_failure(CompatApplicationErrorKind::Configuration, error));
        }
        host.started.frontend_rpc = true;
        if let Err(error) = host.report_service.start() {
            return Err(host.start_failure(
                CompatApplicationErrorKind::ReportStart,
                format!("start compat report service: {error}"),
            ));
        }
        host.started.report = true;
        if let Err(error) = host.ports.start_grpc(
            &server.host,
            compat_routes,
            compat_native_report_handler(),
            starlet_metadata_adapter,
        ) {
            return Err(host.start_failure(
                CompatApplicationErrorKind::GrpcStart,
                format!("start grpc/http/starlet listeners: {error}"),
            ));
        }
        host.started.grpc = true;
        let disk_report_sender: Arc<dyn novarocks::service::disk_report::DiskReportSender> =
            Arc::new(CompatDiskReportSender);
        if let Err(error) = host
            .ports
            .start_heartbeat(heartbeat_config, disk_report_sender)
        {
            return Err(host.start_failure(
                CompatApplicationErrorKind::HeartbeatStart,
                format!("start heartbeat listener: {error}"),
            ));
        }
        host.started.heartbeat = true;
        let load_channel_finisher: Arc<dyn backend_service::LoadChannelFinisher> =
            host.load_service.clone();
        let finish_task_sender: Arc<dyn backend_service::FinishTaskSender> =
            Arc::new(CompatFinishTaskSender);
        let lake_agent_task_adapter =
            crate::lake_agent_tasks::lake_agent_task_adapter(storage_metadata_provider);
        if let Err(error) = host.ports.start_backend(
            backend_config,
            load_channel_finisher,
            finish_task_sender,
            lake_agent_task_adapter,
        ) {
            return Err(host.start_failure(
                CompatApplicationErrorKind::BackendServiceStart,
                format!("start backend listener: {error}"),
            ));
        }
        host.started.backend = true;
        if let Err(error) = host.ports.start_brpc(&brpc_config) {
            return Err(host.start_failure(
                CompatApplicationErrorKind::BrpcStart,
                format!("start brpc listener: {error}"),
            ));
        }
        host.started.brpc = true;
        Ok(host)
    }

    fn start_failure(
        mut self,
        kind: CompatApplicationErrorKind,
        error: impl fmt::Display,
    ) -> CompatApplicationError {
        let primary = CompatApplicationError::new(kind, error);
        match self.cleanup_started() {
            Ok(()) => primary,
            Err(cleanup) => primary.with_cleanup_context(cleanup),
        }
    }

    fn cleanup_started(&mut self) -> Result<(), String> {
        let mut failures = Vec::new();
        if self.started.brpc {
            self.ports.stop_brpc();
            self.started.brpc = false;
        }
        self.load_service.begin_shutdown();
        if self.started.backend {
            if let Err(error) = self.ports.stop_backend() {
                failures.push(format!("stop backend service failed: {error}"));
            }
            self.started.backend = false;
        }
        if self.started.heartbeat {
            if let Err(error) = self.ports.stop_heartbeat() {
                failures.push(format!("stop heartbeat service failed: {error}"));
            }
            self.started.heartbeat = false;
        }
        if self.started.grpc {
            if let Err(error) = self.ports.stop_grpc() {
                failures.push(format!("stop grpc server failed: {error}"));
            }
            self.started.grpc = false;
        }
        if self.started.starlet_metadata {
            if let Err(error) = novarocks::service::starlet_metadata::clear() {
                failures.push(format!("clear Starlet metadata callback failed: {error}"));
            }
            self.started.starlet_metadata = false;
        }
        self.load_service.finish_shutdown();
        self.tracking.clear();
        if self.started.report {
            self.report_service.stop();
            self.started.report = false;
        }
        if self.started.frontend_rpc {
            if let Err(error) = frontend_rpc::clear() {
                failures.push(format!("clear frontend RPC manager failed: {error}"));
            }
            self.started.frontend_rpc = false;
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}

pub fn run_compat_server_until_shutdown<F>(
    config: CompatServerConfig,
    shutdown_requested: F,
) -> Result<(), CompatApplicationError>
where
    F: FnMut() -> bool,
{
    run_compat_server_until_shutdown_with_ports(config, shutdown_requested, LiveCompatPorts)
}

fn run_compat_server_until_shutdown_with_ports<F>(
    config: CompatServerConfig,
    mut shutdown_requested: F,
    ports: impl CompatPorts + 'static,
) -> Result<(), CompatApplicationError>
where
    F: FnMut() -> bool,
{
    let mut host = CompatApplicationHost::open_with_ports(config, ports)?;
    println!("{}", host.ready_marker());
    println!("{}", host.startup_summary());

    let mut primary = Ok(());
    while !shutdown_requested() {
        match host.poll_failure() {
            Ok(Some(error)) | Err(error) => {
                primary = Err(error);
                break;
            }
            Ok(None) => {}
        }
        thread::sleep(SUPERVISION_POLL_INTERVAL);
    }
    if primary.is_ok() {
        match host.poll_failure() {
            Ok(Some(error)) | Err(error) => primary = Err(error),
            Ok(None) => {}
        }
    }
    combine_primary_and_shutdown(primary, host.shutdown())
}

fn combine_primary_and_shutdown(
    primary: Result<(), CompatApplicationError>,
    shutdown: Result<(), CompatApplicationError>,
) -> Result<(), CompatApplicationError> {
    match (primary, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(shutdown)) => Err(shutdown),
        (Err(primary), Err(shutdown)) => Err(primary.with_cleanup_context(shutdown)),
    }
}

fn compat_log_level(level: &str) -> u8 {
    match level {
        "warn" => 1,
        "error" => 2,
        _ => 0,
    }
}

trait CompatPorts: Send {
    fn start_grpc(
        &mut self,
        host: &str,
        compat_routes: Router,
        report_handler: Arc<dyn NativeReportHandler>,
        starlet_control: Arc<dyn grpc_server::StarletControl>,
    ) -> Result<(), String>;
    fn start_heartbeat(
        &mut self,
        config: heartbeat_service::HeartbeatConfig,
        disk_report_sender: Arc<dyn novarocks::service::disk_report::DiskReportSender>,
    ) -> Result<(), String>;
    fn start_backend(
        &mut self,
        config: backend_service::BackendServiceConfig,
        load_channel_finisher: Arc<dyn backend_service::LoadChannelFinisher>,
        finish_task_sender: Arc<dyn backend_service::FinishTaskSender>,
        lake_agent_task_adapter: Arc<dyn backend_service::LakeAgentTaskAdapter>,
    ) -> Result<(), String>;
    fn start_brpc(&mut self, config: &brpc::CompatConfig<'_>) -> Result<(), String>;
    fn poll_grpc_failure(&mut self) -> Result<Option<String>, String>;
    fn stop_brpc(&mut self);
    fn stop_backend(&mut self) -> Result<(), String>;
    fn stop_heartbeat(&mut self) -> Result<(), String>;
    fn stop_grpc(&mut self) -> Result<(), String>;
}

struct LiveCompatPorts;

impl CompatPorts for LiveCompatPorts {
    fn start_grpc(
        &mut self,
        host: &str,
        compat_routes: Router,
        report_handler: Arc<dyn NativeReportHandler>,
        starlet_control: Arc<dyn grpc_server::StarletControl>,
    ) -> Result<(), String> {
        grpc_server::start_grpc_server(host, compat_routes, report_handler, starlet_control)
    }

    fn start_heartbeat(
        &mut self,
        config: heartbeat_service::HeartbeatConfig,
        disk_report_sender: Arc<dyn novarocks::service::disk_report::DiskReportSender>,
    ) -> Result<(), String> {
        heartbeat_service::start_heartbeat_server(config, disk_report_sender)
    }

    fn start_backend(
        &mut self,
        config: backend_service::BackendServiceConfig,
        load_channel_finisher: Arc<dyn backend_service::LoadChannelFinisher>,
        finish_task_sender: Arc<dyn backend_service::FinishTaskSender>,
        lake_agent_task_adapter: Arc<dyn backend_service::LakeAgentTaskAdapter>,
    ) -> Result<(), String> {
        backend_service::start_backend_service(
            config,
            load_channel_finisher,
            finish_task_sender,
            lake_agent_task_adapter,
        )
    }

    fn start_brpc(&mut self, config: &brpc::CompatConfig<'_>) -> Result<(), String> {
        brpc::start(config).map_err(|error| error.to_string())
    }

    fn poll_grpc_failure(&mut self) -> Result<Option<String>, String> {
        grpc_server::poll_grpc_server_failure()
    }

    fn stop_brpc(&mut self) {
        brpc::stop();
    }

    fn stop_backend(&mut self) -> Result<(), String> {
        backend_service::stop_backend_service()
    }

    fn stop_heartbeat(&mut self) -> Result<(), String> {
        heartbeat_service::stop_heartbeat_server()
    }

    fn stop_grpc(&mut self) -> Result<(), String> {
        grpc_server::stop_grpc_server()
    }
}

#[cfg(test)]
#[path = "../tests/application_host.rs"]
mod tests;
