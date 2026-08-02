// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

#![cfg(feature = "mv-first-refresh-staging-test-support")]

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder};
use novarocks::common::app_config::{ClusterRole, NovaRocksConfig};
use novarocks::server::StandaloneGrpcEndpointOwnership;
use novarocks_frontend::{
    ClusterBackendOpenConfig, FrontendApplicationHost, FrontendExecutionConfig,
    FrontendQueryService,
};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHostConfig, StateStoreLimitOverrides,
    StateStoreProviderConfig,
};
use tempfile::{NamedTempFile, TempDir};

struct ReservedPort {
    _listener: TcpListener,
    port: u16,
}

impl ReservedPort {
    fn new() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve TCP port");
        let port = listener.local_addr().expect("reserved port address").port();
        Self {
            _listener: listener,
            port,
        }
    }

    fn release(self) -> u16 {
        self.port
    }
}

struct BackendProcess {
    child: Child,
}

impl BackendProcess {
    fn spawn(config: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_novarocks"))
            .arg("standalone")
            .arg("--config")
            .arg(config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn backend process");
        Self { child }
    }
}

impl Drop for BackendProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

struct ThreeBackendFixture {
    _root: TempDir,
    _configs: Vec<NamedTempFile>,
    _processes: Vec<BackendProcess>,
    endpoints: Vec<SocketAddr>,
}

impl ThreeBackendFixture {
    fn start() -> Self {
        let root = tempfile::tempdir().expect("create backend fixture root");
        let mut reservations = (0..3)
            .map(|_| (ReservedPort::new(), ReservedPort::new()))
            .collect::<Vec<_>>();
        let endpoints = reservations
            .iter()
            .map(|(_, grpc)| SocketAddr::from(([127, 0, 0, 1], grpc.port)))
            .collect::<Vec<_>>();
        let mut configs = Vec::new();
        for (index, (http, grpc)) in reservations.iter().enumerate() {
            let config = tempfile::Builder::new()
                .prefix(&format!("mvx2w-be-{index}-"))
                .suffix(".toml")
                .tempfile_in(root.path())
                .expect("create backend config");
            std::fs::write(
                config.path(),
                format!(
                    r#"
sys_log_dir = "{}"

[server]
host = "127.0.0.1"
http_port = {}
grpc_port = {}

[cluster]
role = "be"
"#,
                    root.path().join(format!("be-{index}")).display(),
                    http.port,
                    grpc.port,
                ),
            )
            .expect("write backend config");
            configs.push(config);
        }
        let mut processes = Vec::new();
        for ((http, grpc), config) in reservations.drain(..).zip(configs.iter()) {
            let _ = http.release();
            let grpc_port = grpc.release();
            processes.push(BackendProcess::spawn(config.path()));
            wait_for_tcp(grpc_port, "backend gRPC endpoint");
        }
        Self {
            _root: root,
            _configs: configs,
            _processes: processes,
            endpoints,
        }
    }
}

fn wait_for_tcp(port: u16, label: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {label} on {port}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn connect_mysql(port: u16) -> MysqlConn {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let builder = OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(port)
            .user(Some("root"));
        match MysqlConn::new(builder) {
            Ok(connection) => return connection,
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for MVX-2W test MySQL server: {error}");
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => panic!("connect MVX-2W test MySQL server: {error}"),
        }
    }
}

/// This uses three independent BE processes and the ordinary frontend host.
/// The only test-only seam is the feature-gated consumer invoked after MV SQL
/// preparation; all fragment submission, report collection and connector
/// staging use the production native wire/QES path.
#[cfg(unix)]
#[test]
#[ignore = "requires native 1FE+3BE processes"]
fn projection_first_refresh_stages_on_three_backend_processes() {
    let backends = ThreeBackendFixture::start();
    let fe_mysql = ReservedPort::new();
    let fe_http = ReservedPort::new();
    let fe_grpc = ReservedPort::new();
    let fe_mysql_port = fe_mysql.port;
    let fe_http_port = fe_http.port;
    let fe_grpc_port = fe_grpc.port;
    let state_root = tempfile::tempdir().expect("create frontend state root");
    let state_path = state_root.path().join("state.sqlite");
    let metadata_path = state_root.path().join("metadata.sqlite");
    let config_file = tempfile::NamedTempFile::new().expect("create frontend config");
    let backend_list = backends
        .endpoints
        .iter()
        .map(|endpoint| format!("\"{endpoint}\""))
        .collect::<Vec<_>>()
        .join(", ");
    std::fs::write(
        config_file.path(),
        format!(
            r#"
[server]
host = "127.0.0.1"
http_port = {}
grpc_port = {}

[standalone_server]
mysql_port = {}

[cluster]
role = "fe"
backends = [{}]

[metadata]
provider = "sqlite"
path = "{}"

[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "mvx2w-native-staging"
deployment_owner = "fe-1"
"#,
            fe_http_port,
            fe_grpc_port,
            fe_mysql_port,
            backend_list,
            metadata_path.display(),
            state_path.display(),
        ),
    )
    .expect("write frontend config");
    let config = NovaRocksConfig::load_from_file(config_file.path()).expect("load frontend config");
    novarocks::common::app_config::install_preloaded_config(config.clone());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build frontend runtime");
    let frontend = runtime
        .block_on(FrontendApplicationHost::open(
            Some(StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "mvx2w-native-staging".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: state_path.clone(),
                            deployment_owner: "fe-1".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            }),
            FrontendExecutionConfig::new(
                "127.0.0.1",
                fe_grpc_port,
                NonZeroUsize::new(3).expect("non-zero coordinator concurrency"),
            ),
            ClusterBackendOpenConfig::new(
                ClusterRole::Fe,
                backends.endpoints.clone(),
                Duration::from_secs(1),
                3,
                Duration::from_secs(1),
            )
            .expect("build frontend backend config"),
        ))
        .expect("open frontend host");
    let services = novarocks_frontend::standalone_open_services_for_server(&frontend);
    let query_control = services.query_control.clone();
    let query_execution = services.query_execution.clone();
    let topology = services.backend_topology.clone();
    let role = services.execution_role;
    let dml = frontend.dml_service();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (engine_tx, engine_rx) = mpsc::channel();
    let _ = fe_mysql.release();
    let _ = fe_http.release();
    let _ = fe_grpc.release();
    let server =
        novarocks::server::run_standalone_server_with_config_until_shutdown_with_session_factory(
            config,
            Some(config_file.path().to_path_buf()),
            None,
            // This fixture owns the report-only endpoint.  A bare
            // FrontendApplicationHost deliberately does not bind it; treating it
            // as externally hosted would register the FE as a fake backend.
            StandaloneGrpcEndpointOwnership::HostedReportOnly,
            services,
            move |engine| {
                engine_tx
                    .send(engine.clone())
                    .map_err(|error| error.to_string())?;
                let insert_engine = engine.insert_engine();
                let delete_engine = engine.delete_engine();
                Ok(std::sync::Arc::new(FrontendQueryService::new(
                    engine,
                    query_control,
                    query_execution,
                    role,
                    topology,
                    dml,
                    insert_engine,
                    delete_engine,
                )))
            },
            async move {
                let _ = shutdown_rx.await;
            },
        );
    let server_task = runtime.spawn(server);
    let engine = match engine_rx.recv_timeout(Duration::from_secs(30)) {
        Ok(engine) => engine,
        Err(error) => {
            let terminal = runtime
                .block_on(server_task)
                .expect("join prematurely terminated frontend server task");
            panic!(
                "native frontend server did not construct an engine: {error}; terminal result: {terminal:?}"
            );
        }
    };
    let mut conn = connect_mysql(fe_mysql_port);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG staging_ice PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        state_root.path().join("warehouse").display(),
    ))
    .expect("create Iceberg catalog");
    conn.query_drop("CREATE DATABASE staging_ice.ns")
        .expect("create Iceberg namespace");
    conn.query_drop("SET CATALOG staging_ice")
        .expect("select Iceberg catalog");
    conn.query_drop("USE ns").expect("select Iceberg namespace");
    conn.query_drop(
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create base table");
    conn.query_drop("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .expect("seed base table");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
    )
    .expect("create MV target");

    let outcome = engine
        .stage_iceberg_mv_first_refresh_for_test(Some("staging_ice"), "ns", "orders_mv")
        .expect("stage projection first refresh through native QES");
    assert_eq!(outcome.input_rows, 2);
    assert!(
        outcome.staged_bytes > 0,
        "native writer must stage bytes: {outcome:?}"
    );
    assert!(
        outcome.artifact_count > 0,
        "native writer must stage artifacts: {outcome:?}"
    );
    assert!(
        (1..=3).contains(&outcome.writer_count),
        "writer reports must belong to the admitted live BE topology: {outcome:?}"
    );
    let main_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read un-published MV main ref");
    assert!(
        main_rows.is_empty(),
        "fixture may commit only its staging branch"
    );

    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_agg_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, SUM(v2) AS total_v2 FROM orders GROUP BY k1",
    )
    .expect("create aggregate MV target");
    let aggregate_outcome = engine
        .stage_iceberg_mv_first_refresh_for_test(Some("staging_ice"), "ns", "orders_agg_mv")
        .expect("stage aggregate first refresh through native QES");
    assert_eq!(aggregate_outcome.input_rows, 2);
    assert!(
        aggregate_outcome.artifact_count > 0,
        "aggregate native writer must stage artifacts: {aggregate_outcome:?}"
    );
    let aggregate_main_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, total_v2 FROM orders_agg_mv ORDER BY k1")
        .expect("read un-published aggregate MV main ref");
    assert!(
        aggregate_main_rows.is_empty(),
        "aggregate fixture may commit only its staging branch"
    );

    conn.query_drop(
        "CREATE TABLE customers (k1 INT, region VARCHAR(16)) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create join base table");
    conn.query_drop("INSERT INTO customers VALUES (1, 'east'), (2, 'west')")
        .expect("seed join base table");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_join_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT o.k1, o.v2, c.region FROM orders o JOIN customers c ON o.k1 = c.k1",
    )
    .expect("create join MV target");
    let join_outcome = engine
        .stage_iceberg_mv_first_refresh_for_test(Some("staging_ice"), "ns", "orders_join_mv")
        .expect("stage join first refresh through native QES");
    assert_eq!(join_outcome.input_rows, 2);
    assert!(
        join_outcome.artifact_count > 0,
        "join native writer must stage artifacts: {join_outcome:?}"
    );
    let join_main_rows: Vec<(i32, i64, String)> = conn
        .query("SELECT k1, v2, region FROM orders_join_mv ORDER BY k1")
        .expect("read un-published join MV main ref");
    assert!(
        join_main_rows.is_empty(),
        "join fixture may commit only its staging branch"
    );

    drop(engine);
    drop(conn);
    shutdown_tx
        .send(())
        .expect("request frontend server shutdown");
    let server_result = runtime
        .block_on(server_task)
        .expect("join frontend server task");
    server_result.expect("shutdown frontend server");
    runtime
        .block_on(frontend.shutdown())
        .expect("shutdown frontend host");
}
