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
use std::sync::{Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder, Row};
use novarocks_frontend::{
    ClusterBackendOpenConfig, FrontendApplicationHost, FrontendExecutionConfig,
};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHostConfig, StateStoreLimitOverrides,
    StateStoreProviderConfig,
};
use novarocks_test_support::{ManagedProcess, ReadyMarker, ReservedTcpPort};
use tempfile::{Builder as TempFileBuilder, NamedTempFile, TempDir};

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

struct EnvironmentValueGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvironmentValueGuard {
    fn set_path(key: &'static str, value: &Path) -> Self {
        let previous = std::env::var_os(key);
        // SAFETY: this integration target serializes its process-spawning
        // tests with `CLUSTER_MVP_TEST_LOCK`; the guard restores the caller
        // environment before the test returns.
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvironmentValueGuard {
    fn drop(&mut self) {
        // SAFETY: see `set_path`; restoring the inherited environment is part
        // of the runner-owned fault scope cleanup.
        unsafe {
            if let Some(value) = self.previous.as_ref() {
                std::env::set_var(self.key, value);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }
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

struct MultiBeClusterHarness {
    #[allow(dead_code)]
    bes: Vec<ManagedProcess>,
    fe: Option<ManagedProcess>,
    fe_mysql: u16,
    #[allow(dead_code)]
    _be_configs: Vec<NamedTempFile>,
    fe_config: NamedTempFile,
    be_log_dirs: Vec<PathBuf>,
    fe_log_dir: PathBuf,
    _log_root: TempDir,
}

impl MultiBeClusterHarness {
    fn start_n_be_with_options(
        n: usize,
        be_debug: &str,
        be_debug_env: &[(&str, &str)],
        fe_extra: &str,
        default_state_store: bool,
    ) -> Self {
        Self::start_n_be_with_options_and_standalone_extra(
            n,
            be_debug,
            be_debug_env,
            fe_extra,
            default_state_store,
            "",
        )
    }

    fn start_n_be_with_options_and_standalone_extra(
        n: usize,
        be_debug: &str,
        be_debug_env: &[(&str, &str)],
        fe_extra: &str,
        default_state_store: bool,
        standalone_server_extra: &str,
    ) -> Self {
        assert!(n >= 1, "must spawn at least one BE");

        // Reserve all ports up front before releasing any of them.
        struct BePortSet {
            http: ReservedTcpPort,
            grpc: ReservedTcpPort,
        }
        let mut be_port_sets: Vec<BePortSet> = (0..n)
            .map(|_| BePortSet {
                http: reserve_port(),
                grpc: reserve_port(),
            })
            .collect();
        let fe_mysql = reserve_port();
        let fe_http = reserve_port();
        let fe_grpc = reserve_port();

        // Collect port numbers before consuming the ReservedTcpPort structs.
        let be_http_ports: Vec<u16> = be_port_sets.iter().map(|s| s.http.port()).collect();
        let be_grpc_ports: Vec<u16> = be_port_sets.iter().map(|s| s.grpc.port()).collect();
        let fe_mysql_port = fe_mysql.port();
        let fe_http_port = fe_http.port();
        let fe_grpc_port = fe_grpc.port();
        let log_root = TempFileBuilder::new()
            .prefix("cluster-logs-")
            .tempdir_in(runtime_dir())
            .expect("create cluster log root");
        let be_log_dirs = (0..n)
            .map(|index| log_root.path().join(format!("be-{index}")))
            .collect::<Vec<_>>();
        let fe_log_dir = log_root.path().join("fe");
        let default_state_store_config = if default_state_store {
            format!(
                r#"
[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "cluster-mvp-{}"
deployment_owner = "fe-1"
"#,
                log_root.path().join("frontend-state.sqlite").display(),
                fe_mysql_port,
            )
        } else {
            String::new()
        };

        // Write all BE configs (while ports are still reserved).
        let be_configs: Vec<NamedTempFile> = be_port_sets
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let http_port = be_http_ports[i];
                let grpc_port = be_grpc_ports[i];
                write_config(
                    &format!("be{i}"),
                    &format!(
                        r#"
sys_log_dir = "{}"

[server]
host = "127.0.0.1"
http_port = {http_port}
grpc_port = {grpc_port}

[cluster]
role = "be"
{be_debug}
"#,
                        be_log_dirs[i].display()
                    ),
                )
            })
            .collect();

        // Build the backends list for the FE config.
        let backends_list: String = be_grpc_ports
            .iter()
            .map(|p| format!("\"127.0.0.1:{p}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let fe_config = write_config(
            "fe",
            &format!(
                r#"
sys_log_dir = "{}"

[server]
host = "127.0.0.1"
http_port = {fe_http_port}
grpc_port = {fe_grpc_port}

[standalone_server]
mysql_port = {fe_mysql_port}
{standalone_server_extra}

[cluster]
role = "fe"
backends = [{backends_list}]
{default_state_store_config}
{fe_extra}
"#,
                fe_log_dir.display()
            ),
        );

        // Spawn all BEs first (releasing each BE's reserved ports immediately
        // before its own spawn), then wait for all readiness in a second pass.
        let mut bes: Vec<ManagedProcess> = Vec::with_capacity(n);
        for (i, port_set) in be_port_sets.drain(..).enumerate() {
            let _ = port_set.http.release();
            let _ = port_set.grpc.release();
            bes.push(spawn_novarocks(
                be_configs[i].path(),
                "NOVAROCKS_READY role=be",
                Some(i),
                be_debug_env,
            ));
        }

        // Release FE ports and spawn FE.
        let _ = fe_mysql.release();
        let _ = fe_http.release();
        let _ = fe_grpc.release();
        let fe = spawn_novarocks(fe_config.path(), "NOVAROCKS_READY mysql_port=", None, &[]);

        Self {
            bes,
            fe: Some(fe),
            fe_mysql: fe_mysql_port,
            _be_configs: be_configs,
            fe_config,
            be_log_dirs,
            fe_log_dir,
            _log_root: log_root,
        }
    }

    fn start_three_be_sqlite_state_store(state_store_path: &Path, cluster_id: &str) -> Self {
        Self::start_three_be_sqlite_state_store_with_extra(state_store_path, cluster_id, "")
    }

    fn start_three_be_sqlite_state_store_with_extra(
        state_store_path: &Path,
        cluster_id: &str,
        fe_extra: &str,
    ) -> Self {
        assert!(
            state_store_path.is_absolute(),
            "SQLite StateStore path must be absolute: {}",
            state_store_path.display()
        );
        let state_store_config = format!(
            r#"
[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "{cluster_id}"
deployment_owner = "fe-1"

{fe_extra}
"#,
            state_store_path.display()
        );
        Self::start_n_be_with_options(3, "", &[], &state_store_config, false)
    }

    fn start_three_be_sqlite_state_store_with_extras(
        state_store_path: &Path,
        cluster_id: &str,
        be_extra: &str,
        be_debug_env: &[(&str, &str)],
        frontend_extra: &str,
    ) -> Self {
        assert!(
            state_store_path.is_absolute(),
            "SQLite StateStore path must be absolute: {}",
            state_store_path.display()
        );
        let fe_extra = format!(
            r#"
[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "{cluster_id}"
deployment_owner = "fe-1"
"#,
            state_store_path.display(),
        );
        Self::start_n_be_with_options_and_standalone_extra(
            3,
            be_extra,
            be_debug_env,
            &fe_extra,
            false,
            frontend_extra,
        )
    }

    fn start_three_be_sqlite_state_store_with_fault_dir(
        state_store_path: &Path,
        cluster_id: &str,
        fault_dir: &Path,
    ) -> Self {
        // Fault injection is armed through NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR;
        // the process config carries no fault-injection key.
        let _ = fault_dir;
        let debug = String::new();
        let fe_extra = format!(
            r#"
[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "{cluster_id}"
deployment_owner = "fe-1"

{debug}
"#,
            state_store_path.display(),
        );
        Self::start_n_be_with_options(3, &debug, &[], &fe_extra, false)
    }

    fn fe_mysql_port(&self) -> u16 {
        self.fe_mysql
    }

    fn log_diagnostics(&self) -> String {
        format!(
            "FE log dir={}; BE log dirs={:?}",
            self.fe_log_dir.display(),
            self.be_log_dirs
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
        )
    }

    #[cfg(unix)]
    fn shutdown_fe_cleanly(&mut self, timeout: Duration) {
        let mut fe = self.fe.take().expect("FE process must be running");
        fe.interrupt_and_wait(timeout)
            .expect("shut down frontend process with SIGINT");
    }

    #[cfg(unix)]
    fn kill_fe(&mut self) {
        let mut fe = self.fe.take().expect("FE process must be running");
        fe.kill_now().expect("kill frontend process");
    }

    #[cfg(unix)]
    fn restart_fe(&mut self) {
        assert!(self.fe.is_none(), "old FE process must be stopped");
        let fe = spawn_novarocks(
            self.fe_config.path(),
            "NOVAROCKS_READY mysql_port=",
            None,
            &[],
        );
        self.fe = Some(fe);
    }

    #[cfg(unix)]
    fn wait_for_fe_output_contains(&mut self, marker: &str, timeout: Duration) {
        self.fe
            .as_mut()
            .expect("FE process must be running")
            .wait_for_log_contains(marker, timeout)
            .unwrap_or_else(|error| panic!("wait for FE output marker {marker:?}: {error:#}"));
    }
}

fn show_backends(conn: &mut MysqlConn) -> Vec<Row> {
    conn.query("SHOW BACKENDS").expect("SHOW BACKENDS")
}

fn assert_exact_live_backends(conn: &mut MysqlConn, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rows = show_backends(conn);
        if rows.len() == expected
            && rows
                .iter()
                .all(|row| row.get::<String, usize>(3).as_deref() == Some("Live"))
        {
            println!("SHOW BACKENDS {expected}/{expected} Live");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected exactly {expected} Live backends; rows={rows:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_mv_rows(conn: &mut MysqlConn, sql: &str, expected: &[(i32, i64)], diagnostics: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let rows: Result<Vec<(i32, i64)>, mysql::Error> = conn.query(sql);
        if matches!(&rows, Ok(rows) if rows.as_slice() == expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "scheduled MV did not converge; expected={expected:?}; observed={rows:?}; {diagnostics}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_scheduler_marker_count(directory: &Path, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let count = std::fs::read_dir(directory)
            .expect("read MVX-4 scheduler barrier directory")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("mvx4-scheduler-admitted-")
            })
            .count();
        if count == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected} frontend MV scheduler barrier markers, observed {count}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
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

#[cfg(unix)]
#[test]
fn cross_process_three_be_mv_state_store_restart() {
    let _guard = lock_cluster_mvp();
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create StateStore tempdir");
    let state_store_path = state_store_dir.path().join("frontend-mv.sqlite");
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store(
        &state_store_path,
        "mv-state-store-restart",
    );
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create MV warehouse");

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        "CREATE EXTERNAL CATALOG mv_restart_ice PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
        warehouse.path().display(),
    ))
    .expect("create restart Iceberg catalog");
    conn.query_drop("CREATE DATABASE mv_restart_ice.ns")
        .expect("create restart namespace");
    conn.query_drop("SET CATALOG mv_restart_ice")
        .expect("use restart catalog");
    conn.query_drop("USE ns").expect("use restart namespace");
    conn.query_drop(
        "CREATE TABLE orders (k1 INT, v2 BIGINT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create restart base table");
    conn.query_drop("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .expect("seed restart base table");
    let base_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders ORDER BY k1")
        .expect("read restart base table before MV refresh");
    assert_eq!(base_rows, vec![(1, 10), (2, 20)]);
    let row_ids: Vec<i64> = conn
        .query("SELECT _row_id FROM orders ORDER BY k1")
        .expect("read restart base row lineage before MV refresh");
    assert_eq!(row_ids.len(), 2);
    let physical_rows: Vec<(i32, i64, i64)> = conn
        .query("SELECT k1, v2, _row_id AS __nova_base_row_id FROM orders ORDER BY k1")
        .expect("read restart base physical projection before MV refresh");
    assert_eq!(physical_rows.len(), 2);
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 \
         AS SELECT k1, v2 FROM orders",
    )
    .expect("create first MV through frontend StateStore service");
    conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv")
        .expect("refresh first MV");
    let rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read first MV before FE restart");
    assert_eq!(rows, vec![(1, 10), (2, 20)]);
    drop(conn);

    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    assert!(
        state_store_path.is_file(),
        "MV StateStore persists across FE restart"
    );
    cluster.restart_fe();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop("SET CATALOG mv_restart_ice")
        .expect("restore restart catalog");
    conn.query_drop("USE ns")
        .expect("restore restart namespace");
    let restored: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read existing MV after FE restart");
    assert_eq!(restored, vec![(1, 10), (2, 20)]);
    conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv")
        .expect("refresh existing MV after FE restart");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv_2 DISTRIBUTED BY HASH(k1) BUCKETS 2 \
         AS SELECT k1, v2 FROM orders",
    )
    .expect("create second MV after FE restart");
    let rows: Vec<Row> = conn
        .query("SHOW MATERIALIZED VIEWS FROM ns")
        .expect("show MVs after restart");
    let names: Vec<String> = rows
        .iter()
        .map(|row| row.get::<String, _>(0).expect("MV name column"))
        .collect();
    assert_eq!(names, vec!["orders_mv", "orders_mv_2"]);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build StateStore inspection runtime");
    let host = runtime
        .block_on(FrontendApplicationHost::open(
            Some(sqlite_state_store_config(
                &state_store_path,
                "mv-state-store-restart",
            )),
            frontend_execution_config(),
            ClusterBackendOpenConfig::new(
                novarocks_types::ClusterRole::AllInOne,
                Vec::new(),
                Duration::from_secs(1),
                1,
                Duration::from_secs(1),
            )
            .expect("valid StateStore inspection backend config"),
        ))
        .expect("reopen MV StateStore after clean FE shutdown");
    let definitions = host
        .mv_repository()
        .list_definitions()
        .expect("list MV definitions from StateStore");
    let first_id = definitions
        .iter()
        .find(|definition| definition.target_table.as_deref() == Some("orders_mv"))
        .map(|definition| definition.mv_id)
        .expect("first MV definition persists");
    let second_id = definitions
        .iter()
        .find(|definition| definition.target_table.as_deref() == Some("orders_mv_2"))
        .map(|definition| definition.mv_id)
        .expect("second MV definition persists");
    assert!(
        second_id > first_id,
        "StateStore-backed MV IDs must increase across FE restart: first={first_id}, second={second_id}"
    );
    runtime
        .block_on(host.shutdown())
        .expect("inspection host shutdown");
}

/// Exercises the frontend-owned scheduler in the native deployment shape.
///
/// The worker has no all-in-one branch: catalog facts are frozen by the FE and
/// refreshes are submitted through the three live BEs.  A debug-only FE
/// barrier holds the first admitted refresh, proving the configured permit is
/// an execution bound rather than merely queue bookkeeping.
#[cfg(unix)]
#[test]
#[ignore = "requires native 1FE+3BE processes and scheduler debug barriers"]
fn cross_process_three_be_mvx4_scheduler_catches_up_and_bounds_concurrency() {
    let _guard = lock_cluster_mvp();
    let barrier_dir = tempfile::tempdir_in(runtime_dir()).expect("create scheduler barrier dir");
    let _barrier_environment =
        EnvironmentValueGuard::set_path("NOVAROCKS_MVX4_SCHEDULER_TEST_DIR", barrier_dir.path());
    let hold_trigger = barrier_dir.path().join("mvx4-scheduler-hold.trigger");
    std::fs::write(&hold_trigger, "hold\n").expect("arm scheduler concurrency barrier");
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create StateStore tempdir");
    let state_store_path = state_store_dir.path().join("frontend-mvx4.sqlite");
    let scheduler_config = r#"
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 100
mv_refresh_scheduler_max_concurrent = 1
mv_refresh_scheduler_failure_backoff_ms = 100
mv_refresh_scheduler_max_failure_backoff_ms = 1_000
"#;
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_extras(
        &state_store_path,
        "mvx4-scheduler",
        "",
        &[],
        scheduler_config,
    );
    let diagnostics = cluster.log_diagnostics();
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create MVX-4 warehouse");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        "CREATE EXTERNAL CATALOG mvx4_sched_ice PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
        warehouse.path().display(),
    ))
    .expect("create MVX-4 scheduler catalog");
    conn.query_drop("CREATE DATABASE mvx4_sched_ice.ns")
        .expect("create MVX-4 scheduler namespace");
    conn.query_drop("SET CATALOG mvx4_sched_ice")
        .expect("select MVX-4 scheduler catalog");
    conn.query_drop("USE ns")
        .expect("select MVX-4 scheduler namespace");
    conn.query_drop(
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\",\"write.row-lineage\"=\"true\")",
    )
    .expect("create MVX-4 scheduler base table");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv_a DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
    )
    .expect("create first scheduled MV");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv_b DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
    )
    .expect("create second scheduled MV");
    wait_for_scheduler_marker_count(barrier_dir.path(), 1);
    std::thread::sleep(Duration::from_millis(300));
    let admitted = std::fs::read_dir(barrier_dir.path())
        .expect("read scheduler barrier directory")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("mvx4-scheduler-admitted-")
        })
        .count();
    assert_eq!(
        admitted, 1,
        "max_concurrent_refreshes=1 must admit only one real scheduler refresh"
    );
    conn.query_drop("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .expect("seed scheduled MV base table");
    std::fs::remove_file(&hold_trigger).expect("release scheduler concurrency barrier");
    let initial = [(1, 10), (2, 20)];
    wait_for_mv_rows(
        &mut conn,
        "SELECT k1, v2 FROM orders_mv_a ORDER BY k1",
        &initial,
        &diagnostics,
    );
    wait_for_mv_rows(
        &mut conn,
        "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
        &initial,
        &diagnostics,
    );
    conn.query_drop("INSERT INTO orders VALUES (3, 30)")
        .expect("mutate scheduled MV base table");
    let caught_up = [(1, 10), (2, 20), (3, 30)];
    wait_for_mv_rows(
        &mut conn,
        "SELECT k1, v2 FROM orders_mv_a ORDER BY k1",
        &caught_up,
        &diagnostics,
    );
    wait_for_mv_rows(
        &mut conn,
        "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
        &caught_up,
        &diagnostics,
    );
    assert_exact_live_backends(&mut conn, 3);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
}

/// Verifies that a clean FE shutdown cancels a frontend-owned, pre-dispatch
/// worker attempt and that restart rebinds only after StateStore/catalog
/// recovery before catching the durable watermark up.
#[cfg(unix)]
#[test]
#[ignore = "requires native 1FE+3BE processes and scheduler debug barriers"]
fn cross_process_three_be_mvx4_shutdown_cancels_and_recovers_background_work() {
    let _guard = lock_cluster_mvp();
    let barrier_dir = tempfile::tempdir_in(runtime_dir()).expect("create scheduler barrier dir");
    let _barrier_environment =
        EnvironmentValueGuard::set_path("NOVAROCKS_MVX4_SCHEDULER_TEST_DIR", barrier_dir.path());
    let hold_trigger = barrier_dir.path().join("mvx4-scheduler-hold.trigger");
    std::fs::write(&hold_trigger, "hold\n").expect("arm scheduler shutdown barrier");
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create StateStore tempdir");
    let state_store_path = state_store_dir.path().join("frontend-mvx4.sqlite");
    let scheduler_config = r#"
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 100
mv_refresh_scheduler_max_concurrent = 1
mv_refresh_scheduler_failure_backoff_ms = 100
mv_refresh_scheduler_max_failure_backoff_ms = 1_000
"#;
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_extras(
        &state_store_path,
        "mvx4-shutdown-recovery",
        "",
        &[],
        scheduler_config,
    );
    let diagnostics = cluster.log_diagnostics();
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create MVX-4 warehouse");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        "CREATE EXTERNAL CATALOG mvx4_recovery_ice PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
        warehouse.path().display(),
    ))
    .expect("create MVX-4 recovery catalog");
    conn.query_drop("CREATE DATABASE mvx4_recovery_ice.ns")
        .expect("create MVX-4 recovery namespace");
    conn.query_drop("SET CATALOG mvx4_recovery_ice")
        .expect("select MVX-4 recovery catalog");
    conn.query_drop("USE ns")
        .expect("select MVX-4 recovery namespace");
    conn.query_drop(
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\",\"write.row-lineage\"=\"true\")",
    )
    .expect("create MVX-4 recovery base table");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
    )
    .expect("create scheduled MV for shutdown recovery");
    wait_for_scheduler_marker_count(barrier_dir.path(), 1);
    conn.query_drop("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .expect("seed recovery base table");
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    std::fs::remove_file(&hold_trigger).expect("release scheduler shutdown barrier");
    cluster.restart_fe();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop("SET CATALOG mvx4_recovery_ice")
        .expect("restore recovery catalog after FE restart");
    conn.query_drop("USE ns")
        .expect("restore recovery namespace after FE restart");
    let caught_up = [(1, 10), (2, 20)];
    wait_for_mv_rows(
        &mut conn,
        "SELECT k1, v2 FROM orders_mv ORDER BY k1",
        &caught_up,
        &diagnostics,
    );
    assert_exact_live_backends(&mut conn, 3);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
}

/// Refreshes an MV whose previous owner crashed while holding its refresh lease.
///
/// The crashed frontend never released the lease, and a lease is precisely the
/// mechanism that cannot distinguish "crashed" from "partitioned but still
/// publishing". So the restarted frontend must wait it out rather than reclaim
/// it -- reclaiming early is the split-brain this ownership exists to prevent.
///
/// Only the ownership refusal is tolerated. Any other error fails immediately,
/// so this stays a wait for takeover and does not become a blanket retry that
/// would paper over a real recovery bug.
#[cfg(unix)]
fn refresh_after_owner_crash(conn: &mut mysql::Conn, mv: &str) {
    use std::time::Instant;

    // The frontend lease is 15s with a 2s takeover observation; this leaves room
    // for both plus scheduling slack.
    let deadline = Instant::now() + Duration::from_secs(40);
    let statement = format!("REFRESH MATERIALIZED VIEW {mv}");
    loop {
        match conn.query_drop(&statement) {
            Ok(()) => return,
            Err(error) => {
                let message = error.to_string();
                let awaiting_takeover = message.contains("another frontend currently owns");
                assert!(
                    awaiting_takeover,
                    "recovery must release staged attempt fence for a fresh refresh: {error}"
                );
                assert!(
                    Instant::now() < deadline,
                    "the crashed owner's refresh lease never aged out: {error}"
                );
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

/// Exercises the two crash windows that startup recovery must converge without
/// replaying a historical write or publication: a staged write before main
/// publication, and a published main snapshot before staging cleanup/finalize.
#[cfg(unix)]
#[test]
#[ignore = "requires native 1FE+3BE processes and debug recovery barriers"]
fn cross_process_three_be_mvx3_recovery_reconciles_staged_and_published_attempts() {
    let _guard = lock_cluster_mvp();
    let fault_dir = tempfile::tempdir_in(runtime_dir()).expect("create MV recovery fault dir");
    let _fault_environment = EnvironmentValueGuard::set_path(
        "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR",
        fault_dir.path(),
    );
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create StateStore tempdir");
    let state_store_path = state_store_dir.path().join("frontend-mv.sqlite");
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_fault_dir(
        &state_store_path,
        "mvx3-recovery-reconciliation",
        fault_dir.path(),
    );
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create MV recovery warehouse");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        "CREATE EXTERNAL CATALOG mvx3_recovery_ice PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
        warehouse.path().display(),
    ))
    .expect("create recovery Iceberg catalog");
    conn.query_drop("CREATE DATABASE mvx3_recovery_ice.ns")
        .expect("create recovery namespace");
    conn.query_drop("SET CATALOG mvx3_recovery_ice")
        .expect("select recovery catalog");
    conn.query_drop("USE ns")
        .expect("select recovery namespace");
    conn.query_drop(
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\",\"write.row-lineage\"=\"true\")",
    )
    .expect("create recovery base table");
    conn.query_drop("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .expect("seed recovery base table");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
    )
    .expect("create recovery MV");

    let write_trigger = fault_dir
        .path()
        .join("mv-refresh-at-write-committed.trigger");
    std::fs::write(&write_trigger, "token=staged-before-publication\n")
        .expect("arm staged recovery crash barrier");
    let mysql_port = cluster.fe_mysql_port();
    let (write_tx, write_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| -> Result<(), String> {
            let mut refresh_conn = connect_mysql(mysql_port);
            refresh_conn
                .query_drop("SET CATALOG mvx3_recovery_ice")
                .map_err(|error| error.to_string())?;
            refresh_conn
                .query_drop("USE ns")
                .map_err(|error| error.to_string())?;
            refresh_conn
                .query_drop("REFRESH MATERIALIZED VIEW orders_mv")
                .map_err(|error| error.to_string())
        })();
        let _ = write_tx.send(result);
    });
    cluster.wait_for_fe_output_contains(
        "NOVAROCKS_MV_RECOVERY_PHASE phase=write-committed token=staged-before-publication",
        Duration::from_secs(30),
    );
    cluster.kill_fe();
    std::fs::remove_file(&write_trigger).expect("remove staged recovery crash barrier");
    assert!(
        write_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("staged refresh client must observe FE kill")
            .is_err(),
        "a killed frontend must not report staged refresh success"
    );
    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    conn.query_drop("SET CATALOG mvx3_recovery_ice")
        .expect("restore recovery catalog after staged crash");
    conn.query_drop("USE ns")
        .expect("restore recovery namespace after staged crash");
    let after_staged_restart: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read MV after staged recovery");
    assert!(
        after_staged_restart.is_empty(),
        "staged-only recovery must not publish main: {after_staged_restart:?}"
    );
    refresh_after_owner_crash(&mut conn, "orders_mv");
    let first_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read recovered first refresh");
    assert_eq!(first_rows, vec![(1, 10), (2, 20)]);
    conn.query_drop("INSERT INTO orders VALUES (3, 30)")
        .expect("add incremental recovery source row");

    let publication_trigger = fault_dir
        .path()
        .join("mv-refresh-at-publication-committed.trigger");
    std::fs::write(&publication_trigger, "token=published-before-cleanup\n")
        .expect("arm published recovery crash barrier");
    let mysql_port = cluster.fe_mysql_port();
    let (publication_tx, publication_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| -> Result<(), String> {
            let mut refresh_conn = connect_mysql(mysql_port);
            refresh_conn
                .query_drop("SET CATALOG mvx3_recovery_ice")
                .map_err(|error| error.to_string())?;
            refresh_conn
                .query_drop("USE ns")
                .map_err(|error| error.to_string())?;
            refresh_conn
                .query_drop("REFRESH MATERIALIZED VIEW orders_mv")
                .map_err(|error| error.to_string())
        })();
        let _ = publication_tx.send(result);
    });
    cluster.wait_for_fe_output_contains(
        "NOVAROCKS_MV_RECOVERY_PHASE phase=publication-committed token=published-before-cleanup",
        Duration::from_secs(30),
    );
    cluster.kill_fe();
    std::fs::remove_file(&publication_trigger).expect("remove published recovery crash barrier");
    assert!(
        publication_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("published refresh client must observe FE kill")
            .is_err(),
        "a killed frontend must not report publication refresh success"
    );
    cluster.restart_fe();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    conn.query_drop("SET CATALOG mvx3_recovery_ice")
        .expect("restore recovery catalog after publication crash");
    conn.query_drop("USE ns")
        .expect("restore recovery namespace after publication crash");
    let published_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read MV after published recovery");
    assert_eq!(published_rows, vec![(1, 10), (2, 20), (3, 30)]);
    conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv")
        .expect("published recovery must finalize durable MV metadata");
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
}

fn sqlite_state_store_config(state_store_path: &Path, cluster_id: &str) -> StateStoreHostConfig {
    StateStoreHostConfig {
        state_store: StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: cluster_id.to_owned(),
                limits: StateStoreLimitOverrides::default(),
                provider: StateStoreProviderConfig::Sqlite {
                    path: state_store_path.to_owned(),
                    deployment_owner: "fe-1".to_owned(),
                },
            },
            mysql_client: None,
        },
        foundationdb_client: None,
    }
}

fn frontend_execution_config() -> FrontendExecutionConfig {
    FrontendExecutionConfig::new("127.0.0.1", 19090, std::num::NonZeroUsize::new(1).unwrap())
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
