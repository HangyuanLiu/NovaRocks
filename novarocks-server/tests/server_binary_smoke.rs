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

//! Thin smoke coverage for the `novarocks` server binary.
//!
//! This target owns exactly two closures, and nothing else:
//!
//! 1. all-in-one composition/readiness: the binary wires the FE/BE application
//!    boundary and the MySQL protocol entrypoint together;
//! 2. BE signal/exit/restart: the binary honours SIGINT, releases its ports and
//!    restarts from the same frozen config.
//!
//! Everything else belongs to a lower canonical owner:
//!
//! - real 1FE+NBE topology, restart, faults and artifacts live in
//!   `novarocks-cluster-harness`, driven by `novarocks-system-test-runner`;
//! - SQL, result, expected-error and plan-shape contracts live in the SQL suites;
//! - Frontend/Backend/Connector/StateStore state machines live in their own
//!   owner-local component tests;
//! - process, readiness, log and TCP-port mechanics live in
//!   `novarocks-test-support`.
//!
//! The helpers below exist only to serve these two smoke closures. They must not
//! grow into a second cluster lifecycle owner.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder};
use novarocks_test_support::{ManagedProcess, ReadyMarker, ReservedTcpPort};
use tempfile::{Builder as TempFileBuilder, NamedTempFile};

static SERVER_BINARY_SMOKE_LOCK: Mutex<()> = Mutex::new(());

fn lock_server_binary_smoke() -> MutexGuard<'static, ()> {
    SERVER_BINARY_SMOKE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn runtime_dir() -> PathBuf {
    let dir = PathBuf::from(".server-binary-smoke-runtime");
    std::fs::create_dir_all(&dir).expect("create server binary smoke runtime dir");
    dir
}

/// Freeze a port through the canonical reservation owner. The reservation is
/// released before the child spawns; the mechanics themselves are asserted by
/// `novarocks-test-support`, not here.
fn reserve_port() -> ReservedTcpPort {
    ReservedTcpPort::new().expect("reserve TCP port")
}

fn write_config(name: &str, content: &str) -> NamedTempFile {
    let file = TempFileBuilder::new()
        .prefix(name)
        .suffix(".toml")
        .tempfile_in(runtime_dir())
        .expect("create config temp file");
    std::fs::write(file.path(), content).expect("write config");
    file
}

const PROCESS_READY_TIMEOUT: Duration = Duration::from_secs(30);

fn spawn_novarocks(
    config_path: &Path,
    ready_marker: &str,
    debug_env: &[(&str, &str)],
) -> ManagedProcess {
    let mut command = Command::new(env!("CARGO_BIN_EXE_novarocks"));
    command.arg("standalone").arg("--config").arg(config_path);
    for (name, value) in debug_env {
        command.env(name, value);
    }
    ManagedProcess::spawn(
        format!("novarocks {}", config_path.display()),
        command,
        ReadyMarker::StdoutContains(ready_marker.to_string()),
        PROCESS_READY_TIMEOUT,
        config_path.with_extension("process.log"),
    )
    .unwrap_or_else(|error| panic!("spawn novarocks {}: {error:#}", config_path.display()))
}

fn connect_mysql(port: u16) -> MysqlConn {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let builder = OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1".to_string()))
            .tcp_port(port)
            .prefer_socket(false)
            .user(Some("root".to_string()))
            .read_timeout(Some(Duration::from_secs(10)))
            .write_timeout(Some(Duration::from_secs(10)));
        match MysqlConn::new(builder) {
            Ok(conn) => return conn,
            Err(err) => {
                if Instant::now() >= deadline {
                    panic!("mysql connection failed: {err}");
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn start_all_in_one_with_debug_env(debug_env: &[(&str, &str)]) -> (ManagedProcess, u16, u16) {
    let mysql = reserve_port();
    let http = reserve_port();
    let grpc = reserve_port();
    let mysql_port = mysql.port();
    let http_port = http.port();
    let grpc_port = grpc.port();
    let config = write_config(
        "all-in-one",
        &format!(
            r#"
[server]
host = "127.0.0.1"
http_port = {http_port}
grpc_port = {grpc_port}

[standalone_server]
mysql_port = {mysql_port}

[cluster]
role = "all-in-one"
"#
        ),
    );
    let _ = mysql.release();
    let _ = http.release();
    let _ = grpc.release();
    let process = spawn_novarocks(config.path(), "NOVAROCKS_READY mysql_port=", debug_env);
    (process, mysql_port, http_port)
}

fn scrape_metrics(port: u16) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect metrics endpoint");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set metrics read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .expect("set metrics write timeout");
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .expect("request metrics");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read metrics response");
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("split metrics HTTP response");
    assert!(
        headers.starts_with("HTTP/1.1 200") || headers.starts_with("HTTP/1.0 200"),
        "metrics endpoint returned {headers}"
    );
    body.to_string()
}

/// Smoke closure 1: the binary composes FE and BE into one process, publishes the
/// ready marker, answers a minimal query over the public MySQL entrypoint, and
/// reaches the native typed gRPC fetch path.
#[test]
fn all_in_one_loopback_stage_start_select_succeeds() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_server_binary_smoke();

    let (mut srv, mysql_port, _) =
        start_all_in_one_with_debug_env(&[("NOVAROCKS_SQL_TEST_EMIT_GRPC_FRAGMENT_MARKER", "1")]);
    let mut conn = connect_mysql(mysql_port);
    let rows: Vec<i64> = conn.query("SELECT 1").expect("SELECT 1");
    assert_eq!(rows, vec![1]);
    srv.wait_for_log_contains("NOVAROCKS_GRPC_FETCH_TYPED status=", Duration::from_secs(3))
        .expect("wait for typed gRPC fetch marker");
}

/// All-in-one keeps the production FE/BE application boundary while exposing
/// one process metrics endpoint, so both role-owned metric families must be
/// visible through that endpoint.
#[test]
fn all_in_one_metrics_surface_contains_both_role_families() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_server_binary_smoke();

    let (_srv, _, http_port) = start_all_in_one_with_debug_env(&[]);
    let metrics = scrape_metrics(http_port);
    assert!(
        metrics.contains("novarocks_live_backends"),
        "all-in-one metrics omitted frontend family: {metrics}"
    );
    assert!(
        metrics.contains("novarocks_backend_query_lifecycle_entries"),
        "all-in-one metrics omitted backend family: {metrics}"
    );
}

/// Smoke closure 2: `role=be` reaches readiness, shuts down cleanly on SIGINT,
/// frees its gRPC port immediately, and restarts from the same frozen config.
#[cfg(unix)]
#[test]
fn native_be_signal_shutdown_releases_port_for_restart() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_server_binary_smoke();

    let grpc = reserve_port();
    let grpc_port = grpc.port();
    let http = reserve_port();
    let http_port = http.port();
    let config = write_config(
        "native-be-signal-restart",
        &format!(
            r#"
[server]
host = "127.0.0.1"
grpc_port = {grpc_port}
http_port = {http_port}

[cluster]
role = "be"
"#
        ),
    );

    let _ = grpc.release();
    let _ = http.release();
    let mut first = spawn_novarocks(config.path(), "NOVAROCKS_READY role=be", &[]);
    first
        .interrupt_and_wait(Duration::from_secs(10))
        .expect("shut down first BE cleanly");

    let rebound = TcpListener::bind(("127.0.0.1", grpc_port))
        .expect("native BE gRPC port must be reusable immediately after SIGINT shutdown");
    drop(rebound);

    let mut restarted = spawn_novarocks(config.path(), "NOVAROCKS_READY role=be", &[]);
    restarted
        .interrupt_and_wait(Duration::from_secs(10))
        .expect("shut down restarted BE cleanly");
}
