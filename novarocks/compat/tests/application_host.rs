use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use super::*;

#[derive(Clone)]
struct FakePorts {
    events: Arc<Mutex<Vec<String>>>,
    fail_start: Option<&'static str>,
    fail_stops: HashSet<&'static str>,
    polls: Arc<Mutex<VecDeque<Result<Option<String>, String>>>>,
    brpc_fragment_contexts: Arc<Mutex<Vec<usize>>>,
    brpc_lake_contexts: Arc<Mutex<Vec<usize>>>,
    native_report_rejection:
        Arc<Mutex<Option<novarocks::query_execution::report::NativeReportHandlerError>>>,
}

impl FakePorts {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            fail_start: None,
            fail_stops: HashSet::new(),
            polls: Arc::new(Mutex::new(VecDeque::new())),
            brpc_fragment_contexts: Arc::new(Mutex::new(Vec::new())),
            brpc_lake_contexts: Arc::new(Mutex::new(Vec::new())),
            native_report_rejection: Arc::new(Mutex::new(None)),
        }
    }

    fn record(&self, event: &str) {
        self.events.lock().unwrap().push(event.to_string());
    }

    fn start(&self, stage: &'static str) -> Result<(), String> {
        self.record(&format!("start-{stage}"));
        if self.fail_start == Some(stage) {
            Err(format!("{stage} failed"))
        } else {
            Ok(())
        }
    }

    fn stop(&self, stage: &'static str) -> Result<(), String> {
        self.record(&format!("stop-{stage}"));
        if self.fail_stops.contains(stage) {
            Err(format!("{stage} cleanup failed"))
        } else {
            Ok(())
        }
    }
}

impl CompatPorts for FakePorts {
    fn start_grpc(
        &mut self,
        _config: crate::listeners::CompatListenerConfig,
        _compat_routes: axum::Router,
        report_handler: Arc<dyn novarocks::query_execution::report::NativeReportHandler>,
        _starlet_control: Arc<dyn crate::listeners::StarletControl>,
    ) -> Result<(), String> {
        let rejection = report_handler
            .handle_native_report(Default::default())
            .expect_err("compat host must inject a rejecting native report handler");
        *self.native_report_rejection.lock().unwrap() = Some(rejection);
        self.start("grpc")
    }

    fn start_heartbeat(
        &mut self,
        _config: crate::heartbeat_service::HeartbeatConfig,
        _control: Arc<crate::control::FrontendControlState>,
        _disk_report_worker: Arc<crate::disk_report::DiskReportWorker>,
    ) -> Result<(), String> {
        self.start("heartbeat")
    }

    fn start_backend(
        &mut self,
        _config: crate::backend_service::BackendServiceConfig,
        _control: Arc<crate::control::FrontendControlState>,
        _load_service: Arc<crate::load::CompatLoadService>,
        _lake_agent_task_adapter: Arc<crate::lake_agent_tasks::CompatLakeAgentTaskAdapter>,
    ) -> Result<(), String> {
        self.start("backend")
    }

    fn start_brpc(&mut self, config: &crate::brpc::CompatConfig<'_>) -> Result<(), String> {
        self.brpc_fragment_contexts
            .lock()
            .unwrap()
            .push(config.fragment_service_context as usize);
        self.brpc_lake_contexts
            .lock()
            .unwrap()
            .push(config.lake_service_context as usize);
        self.start("brpc")
    }

    fn poll_heartbeat_failure(&mut self) -> Result<Option<String>, String> {
        Ok(None)
    }

    fn poll_backend_failure(&mut self) -> Result<Option<String>, String> {
        Ok(None)
    }

    fn poll_grpc_failure(&mut self) -> Result<Option<String>, String> {
        self.record("poll-grpc");
        self.polls.lock().unwrap().pop_front().unwrap_or(Ok(None))
    }

    fn stop_brpc(&mut self) {
        self.record("stop-brpc");
    }

    fn stop_backend(&mut self) -> Result<(), String> {
        self.stop("backend")
    }

    fn stop_heartbeat(&mut self) -> Result<(), String> {
        self.stop("heartbeat")
    }

    fn stop_grpc(&mut self) -> Result<(), String> {
        self.stop("grpc")
    }
}

fn test_config() -> CompatServerConfig {
    let mut config = novarocks::common::app_config::NovaRocksConfig::default();
    config.server.host = "127.0.0.1".to_string();
    config.server.heartbeat_port = 19050;
    config.server.be_port = 19060;
    config.server.brpc_port = 18060;
    config.server.http_port = 18040;
    config.server.grpc_port = 19080;
    config.server.starlet_port = 19070;
    config.cluster.advertise_host = "be.example.test".to_string();
    config.cluster.advertise_port = 19071;
    config.runtime.be_mem_limit_bytes = 8 * 1024 * 1024 * 1024;
    config.runtime.internal_service_query_rpc_thread_num = 11;
    CompatServerConfig { config }
}

#[test]
fn compat_backend_rejects_native_coordinator_reports_with_role_error() {
    let ports = FakePorts::new();
    let native_report_rejection = Arc::clone(&ports.native_report_rejection);
    let host = CompatApplicationHost::open_with_ports(test_config(), ports)
        .expect("open compat application");
    let error = native_report_rejection
        .lock()
        .unwrap()
        .clone()
        .expect("compat gRPC port receives the host-selected report handler");

    assert_eq!(error.status_code(), 1);
    assert_eq!(error.error_code(), "NativeReportRoleRejected");
    assert_eq!(
        error.message(),
        "compat backend role does not own native coordinator report ingress"
    );
    host.shutdown().expect("shutdown compat application");
}

#[test]
fn starts_ports_in_frozen_order_and_preserves_marker_and_summary() {
    let ports = FakePorts::new();
    let events = ports.events.clone();
    let brpc_fragment_contexts = ports.brpc_fragment_contexts.clone();
    let brpc_lake_contexts = ports.brpc_lake_contexts.clone();
    let host = CompatApplicationHost::open_with_ports(test_config(), ports)
        .expect("open compat application");

    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "start-grpc",
            "start-heartbeat",
            "start-backend",
            "start-brpc",
        ]
    );
    assert_eq!(
        host.ready_marker(),
        format!(
            "NOVAROCKS_READY role=compat-be heartbeat_port=19050 brpc_port=18060 grpc_port=19080 pid={}",
            std::process::id()
        )
    );
    assert_eq!(
        host.startup_summary(),
        "novarocksd started (bind_host=127.0.0.1, advertise_host=be.example.test, advertise_port=19071, heartbeat_port=19050, be_port=19060, brpc_port=18060, http_port=18040, grpc_port=19080, starlet_port=19070)"
    );
    assert_eq!(brpc_fragment_contexts.lock().unwrap().len(), 1);
    assert_eq!(brpc_lake_contexts.lock().unwrap().len(), 1);
    assert_ne!(brpc_lake_contexts.lock().unwrap()[0], 0);

    host.shutdown().expect("shutdown compat application");
}

#[test]
fn heartbeat_start_failure_rolls_back_grpc_only() {
    assert_start_failure_rollback(
        "heartbeat",
        CompatApplicationErrorKind::HeartbeatStart,
        &["start-grpc", "start-heartbeat", "stop-grpc"],
    );
}

#[test]
fn backend_start_failure_rolls_back_heartbeat_then_grpc() {
    assert_start_failure_rollback(
        "backend",
        CompatApplicationErrorKind::BackendServiceStart,
        &[
            "start-grpc",
            "start-heartbeat",
            "start-backend",
            "stop-heartbeat",
            "stop-grpc",
        ],
    );
}

#[test]
fn brpc_start_failure_rolls_back_backend_heartbeat_and_grpc() {
    assert_start_failure_rollback(
        "brpc",
        CompatApplicationErrorKind::BrpcStart,
        &[
            "start-grpc",
            "start-heartbeat",
            "start-backend",
            "start-brpc",
            "stop-backend",
            "stop-heartbeat",
            "stop-grpc",
        ],
    );
}

fn assert_start_failure_rollback(
    stage: &'static str,
    expected_kind: CompatApplicationErrorKind,
    expected_events: &[&str],
) {
    let mut ports = FakePorts::new();
    ports.fail_start = Some(stage);
    let events = ports.events.clone();

    let error =
        CompatApplicationHost::open_with_ports(test_config(), ports).expect_err("start must fail");

    assert_eq!(error.kind(), expected_kind);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        expected_events,
        "rollback order"
    );
}

#[test]
fn cleanup_failure_does_not_skip_later_stops() {
    let mut ports = FakePorts::new();
    ports.fail_stops.insert("backend");
    ports.fail_stops.insert("grpc");
    let events = ports.events.clone();
    let host = CompatApplicationHost::open_with_ports(test_config(), ports)
        .expect("open compat application");

    let error = host.shutdown().expect_err("cleanup must report failures");

    assert_eq!(error.kind(), CompatApplicationErrorKind::Shutdown);
    assert!(error.to_string().contains("backend cleanup failed"));
    assert!(error.to_string().contains("grpc cleanup failed"));
    assert_eq!(
        &events.lock().unwrap()[4..],
        ["stop-brpc", "stop-backend", "stop-heartbeat", "stop-grpc",]
    );
}

#[test]
fn final_poll_closes_shutdown_boundary_race() {
    let ports = FakePorts::new();
    ports
        .polls
        .lock()
        .unwrap()
        .push_back(Ok(Some("grpc exited at shutdown boundary".to_string())));
    let events = ports.events.clone();

    let error = run_compat_server_until_shutdown_with_ports(test_config(), || true, ports)
        .expect_err("final poll must observe failure");

    assert_eq!(error.kind(), CompatApplicationErrorKind::Supervision);
    assert!(
        error
            .to_string()
            .contains("grpc exited at shutdown boundary")
    );
    assert!(events.lock().unwrap().contains(&"poll-grpc".to_string()));
}

#[test]
fn supervision_failure_remains_primary_when_cleanup_fails() {
    let mut ports = FakePorts::new();
    ports.fail_stops.insert("heartbeat");
    ports
        .polls
        .lock()
        .unwrap()
        .push_back(Ok(Some("grpc supervisor failed".to_string())));

    let error = run_compat_server_until_shutdown_with_ports(test_config(), || true, ports)
        .expect_err("supervision and cleanup must fail");

    assert_eq!(error.kind(), CompatApplicationErrorKind::Supervision);
    assert!(error.to_string().contains("grpc supervisor failed"));
    assert!(
        error
            .to_string()
            .contains("cleanup failed: Shutdown: stop heartbeat service failed")
    );
}
