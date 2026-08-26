use std::net::TcpListener;

use std::time::Duration;

use novarocks_backend::{
    BackendApplicationErrorKind, BackendApplicationHost, BackendDataRuntime, BackendServerConfig,
    QueryLifecycleRegistryConfig,
};
use novarocks_execution::runtime::execution_runtime::{
    ExecutionRuntimeConfig, ExecutionSpillStorageConfig,
};
use novarocks_types::AdvertiseEndpoint;

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
    BackendServerConfig {
        bind_host: "127.0.0.1".to_string(),
        grpc_port,
        metrics_http_port: unused_port(),
        advertise_endpoint: AdvertiseEndpoint {
            host: "127.0.0.1".to_string(),
            port: advertise_port,
        },
        query_lifecycle_sweep_interval: Duration::from_millis(1_000),
        query_lifecycle_config: QueryLifecycleRegistryConfig::new(
            4_096,
            16_384,
            Duration::from_millis(120_000),
            Duration::from_millis(5_000),
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
        ),
        write_commit_evidence_limits: novarocks_spi::connector::WriteCommitEvidenceLimits::default(
        ),
        execution_runtime_config: ExecutionRuntimeConfig {
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
        },
        execution_installers: Vec::new(),
    }
}

#[test]
fn host_rejects_an_unsealed_connector_installer_set() {
    let grpc_port = unused_port();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .expect("build Backend application host runtime");
    let error = BackendApplicationHost::open(
        backend_config(grpc_port, grpc_port),
        BackendDataRuntime::new(runtime.handle().clone()),
    )
    .expect_err("unsealed connector installer set must fail startup");
    assert_eq!(error.kind(), BackendApplicationErrorKind::Configuration);
}
