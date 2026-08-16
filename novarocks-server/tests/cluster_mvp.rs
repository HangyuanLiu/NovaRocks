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
use novarocks_frontend::dml::{
    AddFilesLifecyclePhase, ConnectorWriteFinalizationRecord, ConnectorWriteLifecycleRecord,
    CtasSagaPhase, ExternalFactOutcome, OperationKind, OperationPayload, OperationState,
    StatementNextAction, TruncateLifecyclePhase,
};
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
    fn start_n_be_without_state_store(n: usize, be_debug: &str, fe_extra: &str) -> Self {
        Self::start_n_be_with_options(n, be_debug, &[], fe_extra, false)
    }

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

fn scheduled_fragments(conn: &mut MysqlConn) -> u64 {
    let rows = show_backends(conn);
    rows.iter()
        .filter(|row| row.get::<String, usize>(3).as_deref() == Some("Live"))
        .map(|row| {
            let value = row.get::<String, usize>(4).unwrap_or_else(|| {
                panic!("Live backend must expose ScheduledFragments; rows={rows:?}")
            });
            value.parse::<u64>().unwrap_or_else(|error| {
                panic!(
                    "Live backend ScheduledFragments must be an unsigned integer \
                     ({value:?}): {error}; rows={rows:?}"
                )
            })
        })
        .sum()
}

fn show_optimize_jobs(
    conn: &mut MysqlConn,
    catalog: &str,
    database: &str,
    table: &str,
) -> Vec<Row> {
    conn.query(format!(
        "SHOW ALTER TABLE OPTIMIZE FROM {catalog}.{database} \
         WHERE TableName = '{table}' ORDER BY CreateTime DESC"
    ))
    .expect("SHOW ALTER TABLE OPTIMIZE")
}

fn wait_for_latest_optimize_finished(
    conn: &mut MysqlConn,
    catalog: &str,
    database: &str,
    table: &str,
    minimum_job_count: usize,
    diagnostics: &str,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let rows = show_optimize_jobs(conn, catalog, database, table);
        let row_summaries = rows
            .iter()
            .map(|row| {
                (
                    row.get::<String, usize>(0),
                    row.get::<String, usize>(2),
                    row.get::<String, usize>(5),
                )
            })
            .collect::<Vec<_>>();
        if rows.len() >= minimum_job_count {
            let job_id = rows[0].get::<String, usize>(0).expect("optimize JobId");
            let state = rows[0].get::<String, usize>(2).expect("optimize State");
            match state.as_str() {
                "FINISHED" => {
                    println!(
                        "SHOW ALTER TABLE OPTIMIZE latest job {job_id} FINISHED ({}/{minimum_job_count} jobs)",
                        rows.len()
                    );
                    return job_id;
                }
                "FAILED" => {
                    panic!("optimize job {job_id} failed; rows={row_summaries:?}; {diagnostics}");
                }
                _ => {}
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for latest optimize job to finish; rows={row_summaries:?}; {diagnostics}"
        );
        std::thread::sleep(Duration::from_millis(100));
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
fn cross_process_three_be_frontend_insert_service_lifecycle() {
    let _guard = lock_cluster_mvp();
    let fixture_dir =
        tempfile::tempdir_in(runtime_dir()).expect("create INSERT lifecycle fixture directory");
    let state_store_path = fixture_dir.path().join("frontend-state.sqlite");
    let legacy_metadata_path = fixture_dir.path().join("frontend-metadata.sqlite");
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create INSERT lifecycle warehouse");
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store(
        &state_store_path,
        "frontend-insert-lifecycle",
    );

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG insert_lifecycle_ice PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create INSERT lifecycle catalog");
    conn.query_drop("CREATE DATABASE insert_lifecycle_ice.ns")
        .expect("create INSERT lifecycle namespace");
    conn.query_drop(
        "CREATE TABLE insert_lifecycle_ice.ns.orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create INSERT lifecycle table");
    let scheduled_before = scheduled_fragments(&mut conn);

    conn.query_drop("INSERT INTO insert_lifecycle_ice.ns.orders VALUES (1, 10), (2, 20)")
        .expect("execute INSERT VALUES through frontend DML service");
    conn.query_drop(
        "INSERT INTO insert_lifecycle_ice.ns.orders \
         SELECT id + 2, amount + 20 FROM insert_lifecycle_ice.ns.orders",
    )
    .expect("execute INSERT SELECT through frontend DML service");
    let appended: Vec<(i32, i32)> = conn
        .query("SELECT id, amount FROM insert_lifecycle_ice.ns.orders ORDER BY id")
        .expect("read appended INSERT lifecycle rows");
    assert_eq!(appended, vec![(1, 10), (2, 20), (3, 30), (4, 40)]);

    conn.query_drop("INSERT OVERWRITE insert_lifecycle_ice.ns.orders VALUES (10, 100), (20, 200)")
        .expect("execute full INSERT OVERWRITE through frontend DML service");
    let snapshots_before_empty: Vec<i64> = conn
        .query(
            "SELECT count(*) \
             FROM insert_lifecycle_ice.ns.orders$snapshots",
        )
        .expect("count snapshots before empty overwrite");
    conn.query_drop(
        "INSERT OVERWRITE insert_lifecycle_ice.ns.orders \
         SELECT id, amount FROM insert_lifecycle_ice.ns.orders WHERE 1 = 0",
    )
    .expect("execute empty INSERT OVERWRITE through frontend DML service");
    let snapshots_after_empty: Vec<i64> = conn
        .query(
            "SELECT count(*) \
             FROM insert_lifecycle_ice.ns.orders$snapshots",
        )
        .expect("count snapshots after empty overwrite");
    assert_eq!(snapshots_before_empty.len(), 1);
    assert_eq!(snapshots_after_empty.len(), 1);
    assert_eq!(
        snapshots_after_empty[0],
        snapshots_before_empty[0] + 1,
        "empty full overwrite must commit a replacement snapshot"
    );
    let final_rows: Vec<(i32, i32)> = conn
        .query("SELECT id, amount FROM insert_lifecycle_ice.ns.orders ORDER BY id")
        .expect("read final INSERT lifecycle rows");
    assert!(
        final_rows.is_empty(),
        "empty full overwrite must replace all visible rows"
    );
    assert_exact_live_backends(&mut conn, 3);
    let scheduled_after = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after > scheduled_before,
        "frontend INSERT lifecycle must schedule remote fragments: \
         before={scheduled_before}, after={scheduled_after}"
    );

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    assert!(state_store_path.is_file(), "DML StateStore must persist");
    assert!(
        !legacy_metadata_path.exists(),
        "the retired legacy metadata database must not be created"
    );
    cluster.restart_fe();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let restored_rows: Vec<(i32, i32)> = conn
        .query("SELECT id, amount FROM insert_lifecycle_ice.ns.orders ORDER BY id")
        .expect("read INSERT lifecycle table after FE restart");
    assert!(
        restored_rows.is_empty(),
        "empty overwrite result must survive FE restart"
    );
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build DML StateStore inspection runtime");
    let host = runtime
        .block_on(FrontendApplicationHost::open(
            Some(sqlite_state_store_config(
                &state_store_path,
                "frontend-insert-lifecycle",
            )),
            frontend_execution_config(),
            ClusterBackendOpenConfig::new(
                novarocks_types::ClusterRole::AllInOne,
                Vec::new(),
                Duration::from_secs(1),
                1,
                Duration::from_secs(1),
            )
            .expect("valid DML StateStore inspection backend config"),
        ))
        .expect("reopen DML StateStore after clean FE shutdown");
    let dml = host.dml_service();
    let operations = dml
        .list_operations()
        .expect("list durable INSERT operations");
    assert_eq!(
        operations.len(),
        4,
        "VALUES, SELECT, full overwrite, and empty overwrite must each be journaled"
    );
    assert!(
        operations
            .iter()
            .all(|operation| operation.state == OperationState::Finalized),
        "every successful INSERT operation must be terminal: {operations:?}"
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| operation.operation_kind == OperationKind::InsertAppend)
            .count(),
        2
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| operation.operation_kind == OperationKind::InsertOverwrite)
            .count(),
        2
    );
    assert!(
        operations.iter().all(|operation| {
            operation.target.catalog == "insert_lifecycle_ice"
                && operation.target.namespace == "ns"
                && operation.target.table == "orders"
        }),
        "durable operations must preserve the INSERT target: {operations:?}"
    );
    assert!(
        dml.list_unfinished_operations()
            .expect("list unfinished INSERT operations")
            .is_empty(),
        "successful INSERT lifecycle must leave no recovery work"
    );
    drop(dml);
    runtime
        .block_on(host.shutdown())
        .expect("inspection host shutdown");
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_frontend_delete_service_lifecycle() {
    let _guard = lock_cluster_mvp();
    let fixture_dir = tempfile::tempdir_in(runtime_dir()).expect("create DELETE lifecycle fixture");
    let state_store_path = fixture_dir.path().join("frontend-state.sqlite");
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create DELETE lifecycle warehouse");
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store(
        &state_store_path,
        "frontend-delete-lifecycle",
    );

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG delete_lifecycle_ice PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    )).expect("create DELETE lifecycle catalog");
    conn.query_drop("CREATE DATABASE delete_lifecycle_ice.ns")
        .expect("create namespace");
    conn.query_drop(
        "CREATE TABLE delete_lifecycle_ice.ns.orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create v3 row-lineage table");
    conn.query_drop("INSERT INTO delete_lifecycle_ice.ns.orders VALUES (1, 10), (2, 20), (3, 30)")
        .expect("seed DELETE lifecycle rows");
    let scheduled_before = scheduled_fragments(&mut conn);

    conn.query_drop("DELETE FROM delete_lifecycle_ice.ns.orders WHERE id = 1")
        .expect("execute standard DELETE through frontend DML service");
    let scheduled_after_standard = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_standard > scheduled_before,
        "standard DELETE must schedule fragments"
    );

    conn.query_drop(
        "ALTER TABLE delete_lifecycle_ice.ns.orders \
         ADD EQUALITY DELETE (id) VALUES (2)",
    )
    .expect("execute equality DELETE through frontend DML service");
    let scheduled_after_equality = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_equality > scheduled_after_standard,
        "equality DELETE must schedule fragments"
    );

    let snapshots_before_noop: Vec<i64> = conn
        .query("SELECT count(*) FROM delete_lifecycle_ice.ns.orders$snapshots")
        .expect("count snapshots before no-op DELETE");
    conn.query_drop("DELETE FROM delete_lifecycle_ice.ns.orders WHERE id = 999")
        .expect("execute no-op DELETE");
    let snapshots_after_noop: Vec<i64> = conn
        .query("SELECT count(*) FROM delete_lifecycle_ice.ns.orders$snapshots")
        .expect("count snapshots after no-op DELETE");
    assert_eq!(
        snapshots_after_noop, snapshots_before_noop,
        "no-match DELETE must not commit a snapshot"
    );
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, amount FROM delete_lifecycle_ice.ns.orders ORDER BY id")
        .expect("read remaining DELETE lifecycle rows");
    assert_eq!(rows, vec![(3, 30)]);

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let restored: Vec<(i32, i32)> = conn
        .query("SELECT id, amount FROM delete_lifecycle_ice.ns.orders ORDER BY id")
        .expect("read DELETE lifecycle table after FE restart");
    assert_eq!(restored, vec![(3, 30)]);
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
                "frontend-delete-lifecycle",
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
        .expect("reopen DML StateStore");
    let operations = host
        .dml_service()
        .list_operations()
        .expect("list durable DELETE operations");
    let row_deltas = operations
        .iter()
        .filter(|operation| operation.operation_kind == OperationKind::RowDelta)
        .collect::<Vec<_>>();
    assert_eq!(
        row_deltas.len(),
        3,
        "standard, equality, and no-op DELETE must be journaled"
    );
    assert_eq!(
        row_deltas
            .iter()
            .filter(|operation| operation.state == OperationState::Finalized)
            .count(),
        3,
        "standard, equality, and known-empty DELETE terminalize exactly once"
    );
    assert_eq!(
        row_deltas
            .iter()
            .filter(|operation| operation.state == OperationState::Aborted)
            .count(),
        0,
        "known-empty DELETE is a terminal no-op, not an abort"
    );
    runtime
        .block_on(host.shutdown())
        .expect("inspection host shutdown");
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_frontend_ctas_truncate_lifecycle() {
    let Ok(rest_uri) = std::env::var("NOVAROCKS_ICEBERG_REST_URI") else {
        eprintln!(
            "SKIP cross_process_three_be_frontend_ctas_truncate_lifecycle: \
             NOVAROCKS_ICEBERG_REST_URI is not configured"
        );
        return;
    };
    let rest_warehouse = std::env::var("NOVAROCKS_ICEBERG_REST_WAREHOUSE")
        .expect("REST lifecycle acceptance requires NOVAROCKS_ICEBERG_REST_WAREHOUSE");
    let s3_endpoint = std::env::var("AWS_S3_ENDPOINT")
        .expect("REST lifecycle acceptance requires AWS_S3_ENDPOINT");
    let s3_access_key = std::env::var("AWS_S3_ACCESS_KEY_ID")
        .expect("REST lifecycle acceptance requires AWS_S3_ACCESS_KEY_ID");
    let s3_secret_key = std::env::var("AWS_S3_SECRET_ACCESS_KEY")
        .expect("REST lifecycle acceptance requires AWS_S3_SECRET_ACCESS_KEY");

    let _guard = lock_cluster_mvp();
    let fixture_dir = tempfile::tempdir_in(runtime_dir())
        .expect("create CTAS/TRUNCATE lifecycle fixture directory");
    let state_store_path = fixture_dir.path().join("frontend-state.sqlite");
    let legacy_metadata_path = fixture_dir.path().join("frontend-metadata.sqlite");
    let namespace = format!("dml3_cluster_{}", std::process::id());
    let catalog = "ctas_truncate_lifecycle_ice";
    let cluster_id = "frontend-ctas-truncate-lifecycle";
    let be_object_store = format!(
        r#"
[connector.object_store]
endpoint = "{s3_endpoint}"
access_key_id = "{s3_access_key}"
access_key_secret = "{s3_secret_key}"
region = "us-east-1"
enable_path_style_access = true
"#
    );
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_extras(
        &state_store_path,
        cluster_id,
        &be_object_store,
        &[],
        &be_object_store,
    );

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG {catalog} PROPERTIES(
            "type"="iceberg",
            "iceberg.catalog.type"="rest",
            "uri"="{rest_uri}",
            "warehouse"="{rest_warehouse}",
            "aws.s3.endpoint"="{s3_endpoint}",
            "aws.s3.access_key"="{s3_access_key}",
            "aws.s3.secret_key"="{s3_secret_key}",
            "aws.s3.region"="us-east-1",
            "aws.s3.enable_path_style_access"="true")"#,
    ))
    .expect("create REST CTAS/TRUNCATE lifecycle catalog");
    conn.query_drop(format!(
        "DROP DATABASE IF EXISTS {catalog}.{namespace} FORCE"
    ))
    .expect("remove stale REST lifecycle namespace");
    conn.query_drop(format!("CREATE DATABASE {catalog}.{namespace}"))
        .expect("create REST lifecycle namespace");
    conn.query_drop(format!(
        "CREATE TABLE {catalog}.{namespace}.source_orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
    ))
    .expect("create CTAS source table");
    conn.query_drop(format!(
        "INSERT INTO {catalog}.{namespace}.source_orders VALUES (1, 10), (2, 20), (3, 30)"
    ))
    .expect("seed CTAS source exactly once");

    let scheduled_before_ctas = scheduled_fragments(&mut conn);
    conn.query_drop(format!(
        "CREATE TABLE {catalog}.{namespace}.published_orders AS \
         SELECT id, amount FROM {catalog}.{namespace}.source_orders"
    ))
    .expect("execute REST staged-publication CTAS through frontend DML service");
    let scheduled_after_ctas = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_ctas > scheduled_before_ctas,
        "CTAS must schedule real remote fragments: before={scheduled_before_ctas}, \
         after={scheduled_after_ctas}"
    );
    let ctas_rows: Vec<(i32, i32)> = conn
        .query(format!(
            "SELECT id, amount FROM {catalog}.{namespace}.published_orders ORDER BY id"
        ))
        .expect("read atomically published CTAS table");
    assert_eq!(
        ctas_rows,
        vec![(1, 10), (2, 20), (3, 30)],
        "the admitted CTAS source must execute exactly once"
    );

    conn.query_drop(format!(
        "CREATE TABLE {catalog}.{namespace}.protected_orders (id INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
    ))
    .expect("create visible table protected from CTAS failure compensation");
    conn.query_drop(format!(
        "INSERT INTO {catalog}.{namespace}.protected_orders VALUES (99)"
    ))
    .expect("seed protected visible table");
    let conflict = conn
        .query_drop(format!(
            "CREATE TABLE {catalog}.{namespace}.protected_orders AS \
             SELECT id FROM {catalog}.{namespace}.source_orders"
        ))
        .expect_err("CTAS must reject an existing target without destructive compensation");
    assert!(
        conflict.to_string().contains("already exists"),
        "unexpected CTAS conflict error: {conflict}"
    );
    let protected_rows: Vec<i32> = conn
        .query(format!(
            "SELECT id FROM {catalog}.{namespace}.protected_orders ORDER BY id"
        ))
        .expect("read protected visible table after CTAS conflict");
    assert_eq!(
        protected_rows,
        vec![99],
        "CTAS failure must never drop the visible target"
    );

    let rows_before_truncate: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders"
        ))
        .expect("count CTAS rows before TRUNCATE");
    assert_eq!(rows_before_truncate, vec![3]);
    let snapshots_before_truncate: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders$snapshots"
        ))
        .expect("count snapshots before TRUNCATE");
    let scheduled_before_truncate = scheduled_fragments(&mut conn);
    conn.query_drop(format!(
        "TRUNCATE TABLE {catalog}.{namespace}.published_orders"
    ))
    .expect("execute frontend direct-mutation TRUNCATE");
    let scheduled_after_truncate = scheduled_fragments(&mut conn);
    assert_eq!(
        scheduled_after_truncate, scheduled_before_truncate,
        "TRUNCATE must not initialize or schedule backend fragments"
    );
    let rows_after_truncate: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders"
        ))
        .expect("count rows after TRUNCATE");
    assert_eq!(rows_after_truncate, vec![0]);
    let snapshots_after_truncate: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders$snapshots"
        ))
        .expect("count snapshots after TRUNCATE");
    assert_eq!(snapshots_before_truncate.len(), 1);
    assert_eq!(snapshots_after_truncate.len(), 1);
    assert_eq!(
        snapshots_after_truncate[0],
        snapshots_before_truncate[0] + 1,
        "TRUNCATE must commit exactly one audit snapshot"
    );

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    assert!(state_store_path.is_file(), "DML StateStore must persist");
    assert!(
        !legacy_metadata_path.exists(),
        "the retired legacy metadata database must not be created"
    );
    cluster.restart_fe();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let restored_rows: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders"
        ))
        .expect("read truncated CTAS table after FE restart");
    assert_eq!(restored_rows, vec![0]);
    let restored_snapshots: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders$snapshots"
        ))
        .expect("read TRUNCATE snapshot chain after FE restart");
    assert_eq!(restored_snapshots, snapshots_after_truncate);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build CTAS/TRUNCATE StateStore inspection runtime");
    let host = runtime
        .block_on(FrontendApplicationHost::open(
            Some(sqlite_state_store_config(&state_store_path, cluster_id)),
            frontend_execution_config(),
            ClusterBackendOpenConfig::new(
                novarocks_types::ClusterRole::AllInOne,
                Vec::new(),
                Duration::from_secs(1),
                1,
                Duration::from_secs(1),
            )
            .expect("valid CTAS/TRUNCATE inspection backend config"),
        ))
        .expect("reopen CTAS/TRUNCATE DML StateStore");
    let dml = host.dml_service();
    let operations = dml
        .list_operations()
        .expect("list durable CTAS/TRUNCATE operations");
    let committed_ctas = operations
        .iter()
        .find(|operation| {
            operation.operation_kind == OperationKind::CreateTableAsSelect
                && operation.target.table == "published_orders"
        })
        .expect("durable successful CTAS operation");
    assert_eq!(committed_ctas.state, OperationState::Finalized);
    let OperationPayload::CtasSaga(ctas) = &committed_ctas.payload else {
        panic!("successful CTAS must persist a CTAS saga payload")
    };
    assert_eq!(ctas.phase, CtasSagaPhase::Committed);
    assert_eq!(ctas.next_action, StatementNextAction::None);
    assert!(
        ctas.source_plan_digest
            .as_deref()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(
        ctas.source_schema_digest
            .as_deref()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(
        ctas.source_execution_identity
            .as_deref()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(
        ctas.write_cohort_id
            .as_deref()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(
        ctas.aggregate_write_digest
            .as_deref()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(
        ctas.prepare_fact.is_some() && ctas.write_fact.is_some() && ctas.publish_fact.is_some()
    );
    for fact in [
        ctas.prepare_fact.as_ref(),
        ctas.write_fact.as_ref(),
        ctas.publish_fact.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        assert_eq!(fact.outcome, ExternalFactOutcome::KnownCommitted);
        for encoded in [
            fact.receipt.as_ref(),
            fact.evidence.as_ref(),
            fact.finalization_failure.as_ref(),
            fact.failure.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            assert!(
                encoded.len() <= 8 * 1024,
                "durable CTAS fact must stay bounded"
            );
        }
    }
    let child_ids = [
        ctas.prepare_operation_id,
        ctas.write_operation_id,
        ctas.publish_operation_id,
        ctas.abort_staging_operation_id,
    ];
    for (index, child_id) in child_ids.iter().enumerate() {
        assert!(
            child_ids[index + 1..].iter().all(|other| other != child_id),
            "CTAS child operation IDs must be stable and distinct"
        );
    }

    let truncate = operations
        .iter()
        .find(|operation| operation.operation_kind == OperationKind::Truncate)
        .expect("durable TRUNCATE operation");
    assert_eq!(truncate.state, OperationState::Finalized);
    let OperationPayload::TruncateLifecycle(truncate_record) = &truncate.payload else {
        panic!("TRUNCATE must persist a direct-mutation lifecycle payload")
    };
    assert_eq!(truncate_record.phase, TruncateLifecyclePhase::Committed);
    assert_eq!(truncate_record.next_action, StatementNextAction::None);
    assert_eq!(
        truncate_record.outcome.as_ref().map(|fact| fact.outcome),
        Some(ExternalFactOutcome::KnownCommitted)
    );
    assert!(
        dml.list_unfinished_operations()
            .expect("list unfinished CTAS/TRUNCATE operations")
            .is_empty(),
        "successful lifecycle plus terminal conflict must leave no recovery work"
    );
    drop(dml);
    runtime
        .block_on(host.shutdown())
        .expect("CTAS/TRUNCATE inspection host shutdown");
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_frontend_add_files_lifecycle() {
    let Ok(rest_uri) = std::env::var("NOVAROCKS_ICEBERG_REST_URI") else {
        eprintln!(
            "SKIP cross_process_three_be_frontend_add_files_lifecycle: \
             NOVAROCKS_ICEBERG_REST_URI is not configured"
        );
        return;
    };
    let rest_warehouse = std::env::var("NOVAROCKS_ICEBERG_REST_WAREHOUSE")
        .expect("ADD FILES lifecycle acceptance requires NOVAROCKS_ICEBERG_REST_WAREHOUSE");
    let s3_endpoint = std::env::var("AWS_S3_ENDPOINT")
        .expect("ADD FILES lifecycle acceptance requires AWS_S3_ENDPOINT");
    let s3_access_key = std::env::var("AWS_S3_ACCESS_KEY_ID")
        .expect("ADD FILES lifecycle acceptance requires AWS_S3_ACCESS_KEY_ID");
    let s3_secret_key = std::env::var("AWS_S3_SECRET_ACCESS_KEY")
        .expect("ADD FILES lifecycle acceptance requires AWS_S3_SECRET_ACCESS_KEY");
    let spark_sql = std::env::var("NOVAROCKS_SPARK_SQL")
        .expect("ADD FILES lifecycle acceptance requires NOVAROCKS_SPARK_SQL");

    let _guard = lock_cluster_mvp();
    let fixture_dir =
        tempfile::tempdir_in(runtime_dir()).expect("create ADD FILES lifecycle fixture directory");
    let state_store_path = fixture_dir.path().join("frontend-state.sqlite");
    let suffix = std::process::id();
    let namespace = format!("dml_add_files_cluster_{suffix}");
    let table = "imported_orders";
    let source = format!("cp3-add-files-{suffix}");
    let catalog = "add_files_lifecycle_ice";
    let cluster_id = "frontend-add-files-lifecycle";

    let spark_program = format!(
        r#"CREATE NAMESPACE IF NOT EXISTS ice_rest.{namespace};
DROP TABLE IF EXISTS ice_rest.{namespace}.{table};
CREATE TABLE ice_rest.{namespace}.{table} (
  new_id BIGINT,
  new_note STRING
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.row-lineage' = 'true',
  'schema.name-mapping.default' = '[{{"field-id":1,"names":["new_id","old_id"]}},{{"field-id":2,"names":["new_note","old_note"]}}]'
);
INSERT OVERWRITE DIRECTORY 's3a://warehouse/{source}'
USING parquet
SELECT old_note, old_id
FROM VALUES
  ('alpha', CAST(11 AS BIGINT)),
  ('beta', CAST(22 AS BIGINT))
AS source(old_note, old_id);
"#
    );
    let spark_file = TempFileBuilder::new()
        .prefix("cp3-add-files-")
        .suffix(".sql")
        .tempfile_in(runtime_dir())
        .expect("create ADD FILES Spark SQL file");
    std::fs::write(spark_file.path(), spark_program).expect("write ADD FILES Spark SQL");
    let spark_output = Command::new(spark_sql)
        .arg(spark_file.path())
        .output()
        .expect("run Spark ADD FILES fixture");
    assert!(
        spark_output.status.success(),
        "Spark ADD FILES fixture failed: stdout={} stderr={}",
        String::from_utf8_lossy(&spark_output.stdout),
        String::from_utf8_lossy(&spark_output.stderr)
    );

    let be_object_store = format!(
        r#"
[connector.object_store]
endpoint = "{s3_endpoint}"
access_key_id = "{s3_access_key}"
access_key_secret = "{s3_secret_key}"
region = "us-east-1"
enable_path_style_access = true
"#
    );
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_extras(
        &state_store_path,
        cluster_id,
        &be_object_store,
        &[],
        &be_object_store,
    );

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG {catalog} PROPERTIES(
            "type"="iceberg",
            "iceberg.catalog.type"="rest",
            "uri"="{rest_uri}",
            "warehouse"="{rest_warehouse}",
            "aws.s3.endpoint"="{s3_endpoint}",
            "aws.s3.access_key"="{s3_access_key}",
            "aws.s3.secret_key"="{s3_secret_key}",
            "aws.s3.region"="us-east-1",
            "aws.s3.enable_path_style_access"="true")"#,
    ))
    .expect("create REST ADD FILES lifecycle catalog");
    let scheduled_before = scheduled_fragments(&mut conn);
    conn.query_drop(format!(
        "ALTER TABLE {catalog}.{namespace}.{table} ADD FILES FROM 's3://warehouse/{source}'"
    ))
    .expect("execute frontend-only ADD FILES");
    assert_eq!(
        scheduled_fragments(&mut conn),
        scheduled_before,
        "ADD FILES must not initialize or schedule backend fragments"
    );
    let rows: Vec<(i64, String)> = conn
        .query(format!(
            "SELECT new_id, new_note FROM {catalog}.{namespace}.{table} ORDER BY new_id"
        ))
        .expect("read ADD FILES rows");
    assert_eq!(
        rows,
        vec![(11, "alpha".to_string()), (22, "beta".to_string())]
    );

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let restored_rows: Vec<(i64, String)> = conn
        .query(format!(
            "SELECT new_id, new_note FROM {catalog}.{namespace}.{table} ORDER BY new_id"
        ))
        .expect("read ADD FILES rows after FE restart");
    assert_eq!(restored_rows, rows);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build ADD FILES StateStore inspection runtime");
    let host = runtime
        .block_on(FrontendApplicationHost::open(
            Some(sqlite_state_store_config(&state_store_path, cluster_id)),
            frontend_execution_config(),
            ClusterBackendOpenConfig::new(
                novarocks_types::ClusterRole::AllInOne,
                Vec::new(),
                Duration::from_secs(1),
                1,
                Duration::from_secs(1),
            )
            .expect("valid ADD FILES inspection backend config"),
        ))
        .expect("reopen ADD FILES DML StateStore");
    let dml = host.dml_service();
    let operations = dml
        .list_operations()
        .expect("list durable ADD FILES operations");
    let operation = operations
        .iter()
        .find(|operation| {
            operation.operation_kind == OperationKind::AddFiles
                && operation.target.catalog == catalog
                && operation.target.namespace == namespace
                && operation.target.table == table
        })
        .expect("durable ADD FILES operation");
    assert_eq!(operation.state, OperationState::Finalized);
    assert!(operation.coordination_provenance.is_some());
    let OperationPayload::AddFilesLifecycle(record) = &operation.payload else {
        panic!("ADD FILES must persist its typed lifecycle payload")
    };
    assert_eq!(record.phase, AddFilesLifecyclePhase::Committed);
    assert_eq!(record.next_action, StatementNextAction::None);
    assert!(operation.recovery_due_at_ms.is_none());
    drop(dml);
    runtime
        .block_on(host.shutdown())
        .expect("ADD FILES inspection host shutdown");
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_frontend_update_merge_lifecycle() {
    let rest_uri = std::env::var("NOVAROCKS_ICEBERG_REST_URI")
        .expect("UPDATE/MERGE lifecycle acceptance requires NOVAROCKS_ICEBERG_REST_URI");
    let rest_warehouse = std::env::var("NOVAROCKS_ICEBERG_REST_WAREHOUSE")
        .expect("UPDATE/MERGE lifecycle acceptance requires NOVAROCKS_ICEBERG_REST_WAREHOUSE");
    let s3_endpoint = std::env::var("AWS_S3_ENDPOINT")
        .expect("UPDATE/MERGE lifecycle acceptance requires AWS_S3_ENDPOINT");
    let s3_access_key = std::env::var("AWS_S3_ACCESS_KEY_ID")
        .expect("UPDATE/MERGE lifecycle acceptance requires AWS_S3_ACCESS_KEY_ID");
    let s3_secret_key = std::env::var("AWS_S3_SECRET_ACCESS_KEY")
        .expect("UPDATE/MERGE lifecycle acceptance requires AWS_S3_SECRET_ACCESS_KEY");

    let _guard = lock_cluster_mvp();
    let fixture_dir = tempfile::tempdir_in(runtime_dir())
        .expect("create UPDATE/MERGE lifecycle fixture directory");
    let state_store_path = fixture_dir.path().join("frontend-state.sqlite");
    let namespace = format!("dml6_cluster_{}", std::process::id());
    let catalog = "update_merge_lifecycle_ice";
    let cluster_id = "frontend-update-merge-lifecycle";
    let be_object_store = format!(
        r#"
[connector.object_store]
endpoint = "{s3_endpoint}"
access_key_id = "{s3_access_key}"
access_key_secret = "{s3_secret_key}"
region = "us-east-1"
enable_path_style_access = true
"#
    );
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_extras(
        &state_store_path,
        cluster_id,
        &be_object_store,
        &[],
        &be_object_store,
    );
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG {catalog} PROPERTIES(
            "type"="iceberg",
            "iceberg.catalog.type"="rest",
            "uri"="{rest_uri}",
            "warehouse"="{rest_warehouse}",
            "aws.s3.endpoint"="{s3_endpoint}",
            "aws.s3.access_key"="{s3_access_key}",
            "aws.s3.secret_key"="{s3_secret_key}",
            "aws.s3.region"="us-east-1",
            "aws.s3.enable_path_style_access"="true")"#,
    ))
    .expect("create REST UPDATE/MERGE catalog");
    conn.query_drop(format!(
        "DROP DATABASE IF EXISTS {catalog}.{namespace} FORCE"
    ))
    .expect("remove stale UPDATE/MERGE namespace");
    conn.query_drop(format!("CREATE DATABASE {catalog}.{namespace}"))
        .expect("create UPDATE/MERGE namespace");
    conn.query_drop(format!(
        "CREATE TABLE {catalog}.{namespace}.target_orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\", \
         \"novarocks.update.mode\"=\"merge-on-read\")"
    ))
    .expect("create UPDATE/MERGE target table");
    conn.query_drop(format!(
        "CREATE TABLE {catalog}.{namespace}.source_orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
    ))
    .expect("create UPDATE/MERGE source table");
    conn.query_drop(format!(
        "INSERT INTO {catalog}.{namespace}.target_orders VALUES (1, 10), (2, 20)"
    ))
    .expect("seed UPDATE/MERGE target");
    conn.query_drop(format!(
        "INSERT INTO {catalog}.{namespace}.source_orders VALUES (2, 200), (3, 300)"
    ))
    .expect("seed UPDATE/MERGE source");

    let snapshots_before_empty_update: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots before zero-effect UPDATE");
    conn.query_drop(format!(
        "UPDATE {catalog}.{namespace}.target_orders SET amount = 999 WHERE id = 999"
    ))
    .expect("execute zero-effect MOR UPDATE");
    let snapshots_after_empty_update: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots after zero-effect UPDATE");
    assert_eq!(
        snapshots_after_empty_update, snapshots_before_empty_update,
        "zero-effect MOR UPDATE must not create a snapshot"
    );

    let scheduled_before_update = scheduled_fragments(&mut conn);
    conn.query_drop(format!(
        "UPDATE {catalog}.{namespace}.target_orders SET amount = 100 WHERE id = 1"
    ))
    .expect("execute frontend UPDATE");
    let scheduled_after_update = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_update > scheduled_before_update,
        "UPDATE must schedule remote fragments: before={scheduled_before_update}, \
         after={scheduled_after_update}"
    );
    let snapshots_before_first_merge: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots before MERGE");
    let scheduled_before_first_merge = scheduled_fragments(&mut conn);
    conn.query_drop(format!(
        "MERGE INTO {catalog}.{namespace}.target_orders AS t \
         USING {catalog}.{namespace}.source_orders AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET amount = s.amount \
         WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)"
    ))
    .expect("execute frontend matched-update/not-matched-insert MERGE");
    let scheduled_after_first_merge = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_first_merge > scheduled_before_first_merge,
        "first MOR MERGE must schedule remote fragments: before={scheduled_before_first_merge}, \
         after={scheduled_after_first_merge}"
    );
    let snapshots_after_first_merge: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots after MERGE");
    assert_eq!(snapshots_after_first_merge.len(), 1);
    assert_eq!(snapshots_before_first_merge.len(), 1);
    assert_eq!(
        snapshots_after_first_merge[0],
        snapshots_before_first_merge[0] + 1,
        "matched UPDATE and not-matched INSERT must produce one MERGE snapshot"
    );
    conn.query_drop(format!(
        "INSERT INTO {catalog}.{namespace}.source_orders VALUES (4, 400)"
    ))
    .expect("seed delete/insert MOR MERGE source row");
    let snapshots_before_second_merge: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots before delete/insert MERGE");
    let scheduled_before_second_merge = scheduled_fragments(&mut conn);
    conn.query_drop(format!(
        "MERGE INTO {catalog}.{namespace}.target_orders AS t \
         USING {catalog}.{namespace}.source_orders AS s ON t.id = s.id \
         WHEN MATCHED AND s.id = 2 THEN DELETE \
         WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)"
    ))
    .expect("execute frontend matched-delete/not-matched-insert MOR MERGE");
    let scheduled_after_second_merge = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_second_merge > scheduled_before_second_merge,
        "second MOR MERGE must schedule remote fragments: before={scheduled_before_second_merge}, \
         after={scheduled_after_second_merge}"
    );
    let snapshots_after_second_merge: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots after delete/insert MERGE");
    assert_eq!(snapshots_after_second_merge.len(), 1);
    assert_eq!(snapshots_before_second_merge.len(), 1);
    assert_eq!(
        snapshots_after_second_merge[0],
        snapshots_before_second_merge[0] + 1,
        "matched DELETE and not-matched INSERT must produce one MERGE snapshot"
    );
    let rows: Vec<(i32, i32)> = conn
        .query(format!(
            "SELECT id, amount FROM {catalog}.{namespace}.target_orders ORDER BY id"
        ))
        .expect("read UPDATE/MERGE target rows");
    assert_eq!(rows, vec![(1, 100), (3, 300), (4, 400)]);

    let scheduled_before_compat = scheduled_fragments(&mut conn);
    let compat_error = conn
        .query_drop("UPDATE information_schema.be_configs SET Value = 'x'")
        .expect_err("UPDATE information_schema.be_configs must not retain a compatibility no-op");
    assert!(
        compat_error.to_string().contains("Iceberg")
            || compat_error.to_string().contains("unsupported")
            || compat_error.to_string().contains("Unsupported"),
        "be_configs UPDATE must fail as an unsupported/non-Iceberg target: {compat_error}"
    );
    assert_eq!(
        scheduled_fragments(&mut conn),
        scheduled_before_compat,
        "rejected be_configs UPDATE must not schedule fragments"
    );

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let restored_rows: Vec<(i32, i32)> = conn
        .query(format!(
            "SELECT id, amount FROM {catalog}.{namespace}.target_orders ORDER BY id"
        ))
        .expect("read UPDATE/MERGE rows after FE restart");
    assert_eq!(restored_rows, rows);
    let restored_snapshots: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots after FE restart");
    assert_eq!(restored_snapshots, snapshots_after_second_merge);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build UPDATE/MERGE StateStore inspection runtime");
    let host = runtime
        .block_on(FrontendApplicationHost::open(
            Some(sqlite_state_store_config(&state_store_path, cluster_id)),
            frontend_execution_config(),
            ClusterBackendOpenConfig::new(
                novarocks_types::ClusterRole::AllInOne,
                Vec::new(),
                Duration::from_secs(1),
                1,
                Duration::from_secs(1),
            )
            .expect("valid UPDATE/MERGE inspection backend config"),
        ))
        .expect("reopen UPDATE/MERGE StateStore");
    let dml = host.dml_service();
    let row_deltas = dml
        .list_operations()
        .expect("list durable UPDATE/MERGE operations")
        .into_iter()
        .filter(|operation| {
            operation.operation_kind == OperationKind::RowDelta
                && operation.target.catalog == catalog
                && operation.target.namespace == namespace
                && operation.target.table == "target_orders"
        })
        .collect::<Vec<_>>();
    assert_eq!(
        row_deltas.len(),
        4,
        "zero-effect UPDATE, non-empty UPDATE and two MERGEs must each be journaled"
    );
    assert!(
        row_deltas
            .iter()
            .filter(|operation| !matches!(
                &operation.payload,
                OperationPayload::ConnectorWriteLifecycle(
                    ConnectorWriteLifecycleRecord::KnownEmpty
                )
            ))
            .all(|operation| matches!(
                &operation.payload,
                OperationPayload::ConnectorWriteLifecycle(
                    ConnectorWriteLifecycleRecord::KnownCommitted {
                        finalization: ConnectorWriteFinalizationRecord::Complete,
                        ..
                    }
                )
            )),
        "non-empty UPDATE/MERGE must retain committed terminal facts: {row_deltas:?}"
    );
    assert!(
        row_deltas
            .iter()
            .filter(|operation| matches!(
                &operation.payload,
                OperationPayload::ConnectorWriteLifecycle(
                    ConnectorWriteLifecycleRecord::KnownEmpty
                )
            ))
            .count()
            == 1,
        "zero-effect UPDATE must persist a provider-neutral known-empty terminal fact: {row_deltas:?}"
    );
    assert_eq!(
        row_deltas
            .iter()
            .filter(|operation| operation.state == OperationState::Finalized)
            .count(),
        4,
        "zero-effect UPDATE, non-empty UPDATE, and both MERGEs must be finalized: {row_deltas:?}"
    );
    assert_eq!(
        row_deltas
            .iter()
            .filter(|operation| operation.state == OperationState::Aborted)
            .count(),
        0,
        "known-empty UPDATE must not synthesize an aborted terminal record: {row_deltas:?}"
    );
    assert!(
        row_deltas
            .iter()
            .any(|operation| operation.operation_subkind.as_deref() == Some("UPDATE"))
            && row_deltas
                .iter()
                .any(|operation| operation.operation_subkind.as_deref() == Some("MERGE")),
        "RowDelta records must retain UPDATE and MERGE subkinds: {row_deltas:?}"
    );
    assert!(
        dml.list_unfinished_operations()
            .expect("list unfinished UPDATE/MERGE operations")
            .is_empty(),
        "successful UPDATE/MERGE must leave no unresolved StateStore record"
    );
    drop(dml);
    runtime
        .block_on(host.shutdown())
        .expect("UPDATE/MERGE inspection host shutdown");
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_insert_without_state_store_fails_before_side_effect() {
    let _guard = lock_cluster_mvp();
    let failure = match std::panic::catch_unwind(|| {
        MultiBeClusterHarness::start_n_be_without_state_store(3, "", "")
    }) {
        Ok(_cluster) => panic!("role=fe without StateStore must fail before serving SQL"),
        Err(failure) => failure,
    };
    let message = failure
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            failure
                .downcast_ref::<&'static str>()
                .map(|message| (*message).to_string())
        })
        .unwrap_or_else(|| "non-string panic payload".to_string());
    assert!(
        message.contains("role=fe requires StateStore for durable cluster backend membership"),
        "1FE+3BE must reject missing StateStore before the SQL endpoint opens: {message}"
    );
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_table_maintenance_lifecycle() {
    let _guard = lock_cluster_mvp();
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create state store tempdir");
    let state_store_path = state_store_dir.path().join("frontend-state.sqlite");
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create fixture warehouse");
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store(
        &state_store_path,
        "table-maintenance",
    );
    let diagnostics = cluster.log_diagnostics();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG maintenance_ice PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create maintenance fixture catalog");
    conn.query_drop("CREATE DATABASE maintenance_ice.maintenance_db")
        .expect("create maintenance fixture database");
    conn.query_drop(
        "CREATE TABLE maintenance_ice.maintenance_db.orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create maintenance fixture table");
    conn.query_drop("INSERT INTO maintenance_ice.maintenance_db.orders VALUES (1, 10), (2, 20)")
        .expect("insert first maintenance fixture data file");
    conn.query_drop("INSERT INTO maintenance_ice.maintenance_db.orders VALUES (3, 30), (4, 40)")
        .expect("insert second maintenance fixture data file");
    conn.query_drop("DELETE FROM maintenance_ice.maintenance_db.orders WHERE id = 2")
        .expect("create deletion vector before optimize");

    let before_optimize: Vec<(Option<i32>, Option<i64>)> = conn
        .query("SELECT id, _row_id FROM maintenance_ice.maintenance_db.orders ORDER BY id")
        .expect("query fixture row lineage before optimize");
    conn.query_drop("ALTER TABLE maintenance_ice.maintenance_db.orders OPTIMIZE")
        .expect("submit first optimize job");
    let first_job_id = wait_for_latest_optimize_finished(
        &mut conn,
        "maintenance_ice",
        "maintenance_db",
        "orders",
        1,
        &diagnostics,
    );
    let after_optimize: Vec<(Option<i32>, Option<i64>)> = conn
        .query("SELECT id, _row_id FROM maintenance_ice.maintenance_db.orders ORDER BY id")
        .expect("query fixture row lineage after optimize");
    assert_eq!(
        after_optimize, before_optimize,
        "OPTIMIZE must preserve visible rows and row lineage"
    );

    conn.query_drop("ALTER TABLE maintenance_ice.maintenance_db.orders REWRITE MANIFESTS")
        .expect("rewrite manifests through frontend maintenance route");
    conn.query_drop(
        "ALTER TABLE maintenance_ice.maintenance_db.orders \
         EXPIRE SNAPSHOTS RETAIN LAST 1",
    )
    .expect("expire snapshots through frontend maintenance route");
    conn.query_drop(
        "ALTER TABLE maintenance_ice.maintenance_db.orders \
         REMOVE ORPHAN FILES OLDER THAN '2000-01-01 00:00:00'",
    )
    .expect("remove orphan files through frontend maintenance route");
    conn.query_drop("DELETE FROM maintenance_ice.maintenance_db.orders WHERE id = 3")
        .expect("create deletion vector before position-delete rewrite");
    let position_delete_result: Vec<Row> = conn
        .query(
            "CALL maintenance_ice.system.rewrite_position_delete_files(\
             table => 'maintenance_db.orders', options => map('rewrite-all', 'true'))",
        )
        .expect("rewrite position delete files through frontend maintenance route");
    assert_eq!(
        position_delete_result.len(),
        1,
        "rewrite position delete files must return one outcome row"
    );
    println!(
        "TABLE MAINTENANCE direct actions completed: REWRITE MANIFESTS, \
         EXPIRE SNAPSHOTS, REMOVE ORPHAN FILES, rewrite_position_delete_files"
    );
    assert_exact_live_backends(&mut conn, 3);

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    assert!(
        state_store_path.is_file(),
        "SQLite StateStore must survive the first FE lifecycle"
    );

    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let restored_jobs =
        show_optimize_jobs(&mut conn, "maintenance_ice", "maintenance_db", "orders");
    assert!(
        restored_jobs.iter().any(|row| {
            row.get::<String, usize>(0).as_deref() == Some(first_job_id.as_str())
                && row.get::<String, usize>(2).as_deref() == Some("FINISHED")
        }),
        "terminal optimize history must survive FE restart; rows={restored_jobs:?}; {diagnostics}"
    );
    println!(
        "SHOW ALTER TABLE OPTIMIZE restored job {first_job_id} FINISHED after clean FE restart"
    );

    conn.query_drop("INSERT INTO maintenance_ice.maintenance_db.orders VALUES (5, 50), (6, 60)")
        .expect("persisted catalog must accept inserts after FE restart");
    conn.query_drop("ALTER TABLE maintenance_ice.maintenance_db.orders OPTIMIZE")
        .expect("submit second optimize job after FE restart");
    let second_job_id = wait_for_latest_optimize_finished(
        &mut conn,
        "maintenance_ice",
        "maintenance_db",
        "orders",
        2,
        &diagnostics,
    );
    assert_ne!(
        second_job_id, first_job_id,
        "FE restart must enqueue a distinct optimize job"
    );
    let final_rows: Vec<Option<i32>> = conn
        .query("SELECT id FROM maintenance_ice.maintenance_db.orders ORDER BY id")
        .expect("query maintenance fixture after second optimize");
    assert_eq!(
        final_rows,
        vec![Some(1), Some(4), Some(5), Some(6)],
        "maintenance lifecycle must preserve the expected visible row set"
    );
    assert_exact_live_backends(&mut conn, 3);

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
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
