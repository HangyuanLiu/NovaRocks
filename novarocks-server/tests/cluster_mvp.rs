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

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder};
use novarocks_test_support::{ManagedProcess, ReadyMarker, ReservedTcpPort};
use tempfile::{Builder as TempFileBuilder, NamedTempFile};

static CLUSTER_MVP_TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock_cluster_mvp() -> MutexGuard<'static, ()> {
    CLUSTER_MVP_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn runtime_dir() -> PathBuf {
    let dir = PathBuf::from(".cluster_mvp_runtime");
    std::fs::create_dir_all(&dir).expect("create cluster mvp runtime dir");
    dir
}

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
    backend_index: Option<usize>,
    debug_env: &[(&str, &str)],
) -> ManagedProcess {
    let mut command = Command::new(env!("CARGO_BIN_EXE_novarocks"));
    command.arg("standalone").arg("--config").arg(config_path);
    for (name, value) in debug_env {
        command.env(name, value);
    }
    if let Some(backend_index) = backend_index {
        command.env(
            "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX",
            backend_index.to_string(),
        );
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

fn start_all_in_one_with_debug_env(
    extra: &str,
    debug_env: &[(&str, &str)],
) -> (ManagedProcess, u16) {
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

{extra}
"#
        ),
    );
    let _ = mysql.release();
    let _ = http.release();
    let _ = grpc.release();
    let process = spawn_novarocks(
        config.path(),
        "NOVAROCKS_READY mysql_port=",
        None,
        debug_env,
    );
    (process, mysql_port)
}

#[test]
fn all_in_one_loopback_stage_start_select_succeeds() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let (mut srv, mysql_port) = start_all_in_one_with_debug_env(
        "",
        &[("NOVAROCKS_SQL_TEST_EMIT_GRPC_FRAGMENT_MARKER", "1")],
    );
    let mut conn = connect_mysql(mysql_port);
    let rows: Vec<i64> = conn.query("SELECT 1").expect("SELECT 1");
    assert_eq!(rows, vec![1]);
    srv.wait_for_log_contains("NOVAROCKS_GRPC_FETCH_TYPED status=", Duration::from_secs(3))
        .expect("wait for typed gRPC fetch marker");
}

#[cfg(unix)]
#[test]
fn native_be_signal_shutdown_releases_port_for_restart() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

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
    let mut first = spawn_novarocks(config.path(), "NOVAROCKS_READY role=be", None, &[]);
    first
        .interrupt_and_wait(Duration::from_secs(10))
        .expect("shut down first BE cleanly");

    let rebound = TcpListener::bind(("127.0.0.1", grpc_port))
        .expect("native BE gRPC port must be reusable immediately after SIGINT shutdown");
    drop(rebound);

    let mut restarted = spawn_novarocks(config.path(), "NOVAROCKS_READY role=be", None, &[]);
    restarted
        .interrupt_and_wait(Duration::from_secs(10))
        .expect("shut down restarted BE cleanly");
}

#[test]
fn reserved_port_blocks_rebinding_until_release() {
    let port = reserve_port();
    let addr = ("127.0.0.1", port.port());

    assert!(
        std::net::TcpListener::bind(addr).is_err(),
        "reserved port must remain bound until release"
    );

    assert_eq!(port.release(), addr.1);
}
