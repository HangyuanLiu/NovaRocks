use std::net::TcpListener;

use novarocks::common::app_config::NovaRocksConfig;
use novarocks_backend::{BackendApplicationHost, BackendServerConfig};

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
    BackendServerConfig {
        config,
        execution_installers: Vec::new(),
    }
}

#[test]
fn host_preserves_native_backend_ready_marker() {
    let grpc_port = unused_port();
    let host = BackendApplicationHost::open(backend_config(grpc_port, grpc_port))
        .expect("open native backend host");

    assert_eq!(
        host.ready_marker(),
        format!(
            "NOVAROCKS_READY role=be grpc_port={grpc_port} advertise_host=127.0.0.1 pid={}",
            std::process::id()
        )
    );

    host.shutdown().expect("shutdown native backend host");
}
