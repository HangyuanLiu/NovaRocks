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

use crate::compat_artifact::CompatArtifact;
use crate::managed_process::{ManagedProcess, ReadyMarker};
use crate::types::{CompatBeEndpoint, RunnerConfig};
use anyhow::{Context, Result, bail};
use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder};
use std::fs;
use std::net::TcpListener;
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use toml::Value;

use crate::cluster::ServerHandle;

pub(crate) const COMPAT_BE_COUNT: usize = 3;
const FE_START_MARKER: &str = "using java version";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const FE_CONFIG_KEYS: &[&str] = &[
    "cloud_native_storage_type",
    "cloud_native_hdfs_url",
    "aws_s3_path",
    "aws_s3_endpoint",
    "aws_s3_region",
    "aws_s3_access_key",
    "aws_s3_secret_key",
    "aws_s3_use_aws_sdk_default_behavior",
    "aws_s3_use_instance_profile",
    "aws_s3_iam_role_arn",
    "aws_s3_external_id",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FePorts {
    pub(crate) http: u16,
    pub(crate) rpc: u16,
    pub(crate) query: u16,
    pub(crate) edit_log: u16,
}

impl FePorts {
    fn all_ports(&self) -> Vec<u16> {
        vec![self.http, self.rpc, self.query, self.edit_log]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompatBePorts {
    pub(crate) heartbeat: u16,
    pub(crate) be: u16,
    pub(crate) brpc: u16,
    pub(crate) http: u16,
    pub(crate) grpc: u16,
    pub(crate) starlet: u16,
}

impl CompatBePorts {
    fn all_ports(&self) -> Vec<u16> {
        vec![
            self.heartbeat,
            self.be,
            self.brpc,
            self.http,
            self.grpc,
            self.starlet,
        ]
    }

    fn endpoint(&self) -> CompatBeEndpoint {
        CompatBeEndpoint {
            host: "127.0.0.1".to_string(),
            heartbeat_port: self.heartbeat,
            be_port: self.be,
            brpc_port: self.brpc,
            http_port: self.http,
            grpc_port: self.grpc,
            starlet_port: self.starlet,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompatTopology {
    fe: FePorts,
    be: Vec<CompatBePorts>,
}

struct ReservedCompatPorts {
    listeners: Vec<TcpListener>,
    topology: CompatTopology,
}

impl ReservedCompatPorts {
    fn new() -> Result<Self> {
        let mut listeners = Vec::with_capacity(4 + COMPAT_BE_COUNT * 6);
        for _ in 0..listeners.capacity() {
            listeners
                .push(TcpListener::bind(("127.0.0.1", 0)).context("reserve compatibility port")?);
        }
        let ports = listeners
            .iter()
            .map(|listener| {
                listener
                    .local_addr()
                    .map(|address| address.port())
                    .context("read reserved compatibility port")
            })
            .collect::<Result<Vec<_>>>()?;
        let fe = FePorts {
            http: ports[0],
            rpc: ports[1],
            query: ports[2],
            edit_log: ports[3],
        };
        let be = ports[4..]
            .chunks_exact(6)
            .map(|chunk| CompatBePorts {
                heartbeat: chunk[0],
                be: chunk[1],
                brpc: chunk[2],
                http: chunk[3],
                grpc: chunk[4],
                starlet: chunk[5],
            })
            .collect();
        Ok(Self {
            listeners,
            topology: CompatTopology { fe, be },
        })
    }

    fn topology(&self) -> CompatTopology {
        self.topology.clone()
    }

    fn release_ports(&mut self, ports: &[u16]) -> Result<()> {
        for port in ports {
            let index = self
                .listeners
                .iter()
                .position(|listener| {
                    listener
                        .local_addr()
                        .is_ok_and(|address| address.port() == *port)
                })
                .with_context(|| format!("reserved port {port} is unavailable for release"))?;
            self.listeners.swap_remove(index);
        }
        Ok(())
    }
}

pub(crate) fn validate_fe_home(home: &Path) -> Result<()> {
    let start_script = home.join("bin/start_fe.sh");
    if !start_script.is_file() {
        bail!(
            "StarRocks FE home is missing bin/start_fe.sh: {}",
            start_script.display()
        );
    }
    #[cfg(unix)]
    if fs::metadata(&start_script)?.permissions().mode() & 0o111 == 0 {
        bail!(
            "StarRocks FE start script is not executable: {}",
            start_script.display()
        );
    }
    let core_jar = home.join("lib/fe-core-main.jar");
    if !core_jar.is_file() {
        bail!(
            "StarRocks FE home is missing lib/fe-core-main.jar: {}",
            core_jar.display()
        );
    }
    Ok(())
}

pub(crate) fn select_fe_home_path(explicit: Option<PathBuf>, user_home: &Path) -> PathBuf {
    explicit.unwrap_or_else(|| user_home.join("starrocks-on-novarocks/fe"))
}

fn discover_fe_home() -> Result<PathBuf> {
    let explicit = std::env::var_os("STARROCKS_FE_HOME").map(PathBuf::from);
    let user_home = match explicit.as_ref() {
        Some(_) => PathBuf::new(),
        None => std::env::var_os("HOME")
            .map(PathBuf::from)
            .context("HOME is unset and STARROCKS_FE_HOME was not provided")?,
    };
    let home = select_fe_home_path(explicit, &user_home);
    let home = home
        .canonicalize()
        .with_context(|| format!("resolve StarRocks FE home {}", home.display()))?;
    validate_fe_home(&home)?;
    Ok(home)
}

pub(crate) fn parse_java_major_version(output: &str) -> Option<u32> {
    let version = output.split('"').nth(1)?;
    let mut components = version.split('.');
    let first = components.next()?.parse::<u32>().ok()?;
    if first == 1 {
        components.next()?.parse().ok()
    } else {
        Some(first)
    }
}

pub(crate) fn validate_java_version_output(output: &str) -> Result<()> {
    let version = parse_java_major_version(output)
        .with_context(|| format!("parse Java version output: {output:?}"))?;
    if version < 17 {
        bail!("StarRocks FE requires Java 17 or newer; detected Java {version}");
    }
    Ok(())
}

fn validate_java_runtime() -> Result<()> {
    let java = match std::env::var_os("JAVA_HOME") {
        Some(home) => PathBuf::from(home).join("bin/java"),
        None => PathBuf::from("java"),
    };
    let output = Command::new(&java)
        .arg("-version")
        .output()
        .with_context(|| format!("run {} -version", java.display()))?;
    if !output.status.success() {
        bail!(
            "{} -version failed with {}: {}",
            java.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    validate_java_version_output(&combined)
}

pub(crate) fn render_fe_conf(base: &str, runtime_fe_home: &Path, ports: &FePorts) -> String {
    let mut copied = Vec::new();
    for line in base.lines() {
        let Some((key, _)) = line.split_once('=') else {
            continue;
        };
        if FE_CONFIG_KEYS.contains(&key.trim()) {
            copied.push(line.trim().to_string());
        }
    }
    copied.extend([
        format!("LOG_DIR = {}/log", runtime_fe_home.display()),
        "JAVA_OPTS = \"-Xms1g -Xmx2g -XX:+UseG1GC\"".to_string(),
        format!("meta_dir = {}/meta", runtime_fe_home.display()),
        format!("http_port = {}", ports.http),
        format!("rpc_port = {}", ports.rpc),
        format!("query_port = {}", ports.query),
        format!("edit_log_port = {}", ports.edit_log),
        "run_mode = shared_data".to_string(),
        "priority_networks = 127.0.0.1/32".to_string(),
        "proc_profile_cpu_enable = false".to_string(),
        "proc_profile_mem_enable = false".to_string(),
    ]);
    format!("{}\n", copied.join("\n"))
}

pub(crate) fn create_isolated_fe_home(
    source_home: &Path,
    runtime_home: &Path,
    ports: &FePorts,
) -> Result<()> {
    fs::create_dir_all(runtime_home)
        .with_context(|| format!("create isolated FE home {}", runtime_home.display()))?;
    #[cfg(unix)]
    for name in ["bin", "lib", "spark-dpp"] {
        symlink(source_home.join(name), runtime_home.join(name))
            .with_context(|| format!("symlink immutable StarRocks FE directory {name}"))?;
    }
    let source_conf = source_home.join("conf");
    let runtime_conf = runtime_home.join("conf");
    fs::create_dir_all(&runtime_conf)?;
    fs::create_dir_all(runtime_home.join("log"))?;
    fs::create_dir_all(runtime_home.join("meta"))?;
    let base_conf_path = source_conf.join("fe.conf");
    let base_conf = if base_conf_path.is_file() {
        fs::read_to_string(&base_conf_path)
            .with_context(|| format!("read {}", base_conf_path.display()))?
    } else {
        String::new()
    };
    let rendered = render_fe_conf(&base_conf, runtime_home, ports);
    if !rendered.lines().any(|line| {
        line.split_once('=')
            .is_some_and(|(key, _)| key.trim() == "cloud_native_storage_type")
    }) {
        bail!(
            "StarRocks FE shared-data config is missing cloud_native_storage_type in {}",
            base_conf_path.display()
        );
    }
    fs::write(runtime_conf.join("fe.conf"), rendered)
        .context("write isolated StarRocks fe.conf")?;
    Ok(())
}

pub(crate) fn render_be_config(ports: &CompatBePorts) -> Result<String> {
    let mut root = toml::map::Map::new();
    let mut server = toml::map::Map::new();
    server.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
    server.insert(
        "priority_networks".to_string(),
        Value::String("127.0.0.1/32".to_string()),
    );
    for (key, port) in [
        ("heartbeat_port", ports.heartbeat),
        ("be_port", ports.be),
        ("brpc_port", ports.brpc),
        ("http_port", ports.http),
        ("grpc_port", ports.grpc),
        ("starlet_port", ports.starlet),
    ] {
        server.insert(key.to_string(), Value::Integer(i64::from(port)));
    }
    root.insert("server".to_string(), Value::Table(server));
    toml::to_string(&Value::Table(root)).context("serialize compatibility BE config")
}

pub(crate) fn validate_compat_topology<S: AsRef<str>>(
    expected_ports: &[u16],
    headers: &[S],
    rows: &[Vec<String>],
) -> Result<()> {
    let column = |name: &str| {
        headers
            .iter()
            .position(|header| header.as_ref().eq_ignore_ascii_case(name))
    };
    let heartbeat_index = column("HeartbeatPort").context("SHOW BACKENDS missing HeartbeatPort")?;
    let alive_index = column("Alive");
    let status_index = column("Status");
    if alive_index.is_none() && status_index.is_none() {
        bail!("SHOW BACKENDS missing Alive and Status columns");
    }
    let mut expected = expected_ports.to_vec();
    expected.sort_unstable();
    let mut observed = Vec::with_capacity(rows.len());
    let mut dead = Vec::new();
    for (row_index, row) in rows.iter().enumerate() {
        let heartbeat = row
            .get(heartbeat_index)
            .with_context(|| format!("SHOW BACKENDS row {row_index} missing HeartbeatPort"))?
            .parse::<u16>()
            .with_context(|| format!("parse SHOW BACKENDS row {row_index} HeartbeatPort"))?;
        observed.push(heartbeat);
        let live = if let Some(index) = alive_index {
            row.get(index)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        } else {
            row.get(status_index.expect("Status index checked"))
                .is_some_and(|value| value.eq_ignore_ascii_case("Live"))
        };
        if !live {
            dead.push(heartbeat);
        }
    }
    observed.sort_unstable();
    if rows.len() != COMPAT_BE_COUNT || expected.len() != COMPAT_BE_COUNT || observed != expected {
        bail!(
            "SHOW BACKENDS expected heartbeat ports {expected:?} in exactly {COMPAT_BE_COUNT} rows, observed {observed:?}"
        );
    }
    if !dead.is_empty() {
        bail!("SHOW BACKENDS backends are not alive: heartbeat_ports={dead:?}");
    }
    Ok(())
}

pub(crate) fn spawn_managed_process(
    label: String,
    command: Command,
    marker: ReadyMarker,
    timeout: Duration,
    log_path: PathBuf,
) -> Result<ManagedProcess> {
    ManagedProcess::spawn(label, command, marker, timeout, log_path)
}

fn create_runtime_dir(repo_root: &Path) -> Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = repo_root.join(format!(
        ".sql-test-runner-runtime/starrocks-compat-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&path)
        .with_context(|| format!("create compatibility runtime {}", path.display()))?;
    Ok(path)
}

struct RuntimeDirGuard(Option<PathBuf>);

impl RuntimeDirGuard {
    fn new(path: PathBuf) -> Self {
        Self(Some(path))
    }

    fn path(&self) -> &Path {
        self.0.as_deref().expect("runtime directory is available")
    }

    fn into_path(mut self) -> PathBuf {
        self.0.take().expect("runtime directory is available")
    }
}

impl Drop for RuntimeDirGuard {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_dir_all(path);
        }
    }
}

fn mysql_options(
    user: &str,
    password: Option<&str>,
    query_port: u16,
    timeout: Duration,
) -> OptsBuilder {
    OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(query_port)
        .prefer_socket(false)
        .user(Some(user))
        .pass(password)
        .tcp_connect_timeout(Some(timeout))
        .read_timeout(Some(timeout))
        .write_timeout(Some(timeout))
}

fn ensure_processes_healthy(
    fe_process: &mut ManagedProcess,
    fe_config: &Path,
    query_port: u16,
    be_processes: &mut [ManagedProcess],
    be_configs: &[PathBuf],
    endpoints: &[CompatBeEndpoint],
) -> Result<String> {
    let mut diagnostics = vec![fe_process.runtime_diagnostic(
        "StarRocks FE",
        &format!("mysql://127.0.0.1:{query_port}"),
        fe_config,
    )?];
    for (index, process) in be_processes.iter_mut().enumerate() {
        diagnostics.push(process.runtime_diagnostic(
            &format!("compat BE[{index}]"),
            &format!("heartbeat://127.0.0.1:{}", endpoints[index].heartbeat_port),
            &be_configs[index],
        )?);
    }
    Ok(diagnostics.join("; "))
}

fn wait_for_fe_mysql(
    user: &str,
    password: Option<&str>,
    query_port: u16,
    fe_process: &mut ManagedProcess,
    fe_config: &Path,
) -> Result<()> {
    wait_for_fe_mysql_with(
        STARTUP_TIMEOUT,
        || {
            fe_process
                .runtime_diagnostic(
                    "StarRocks FE",
                    &format!("mysql://127.0.0.1:{query_port}"),
                    fe_config,
                )
                .map(|_| ())
        },
        |connect_timeout| {
            MysqlConn::new(mysql_options(user, password, query_port, connect_timeout))
                .context("connect and authenticate StarRocks FE MySQL")
        },
        |connection, select_timeout| {
            set_mysql_connection_io_timeout(connection, select_timeout)
                .context("apply remaining deadline to StarRocks FE MySQL SELECT 1")?;
            connection
                .query_drop("SELECT 1")
                .context("execute StarRocks FE MySQL health check SELECT 1")
        },
        thread::sleep,
    )
    .with_context(|| format!("StarRocks FE MySQL did not become ready at 127.0.0.1:{query_port}"))
}

#[cfg(unix)]
fn set_mysql_connection_io_timeout(connection: &MysqlConn, timeout: Duration) -> Result<()> {
    let seconds = timeout.as_secs().min(libc::time_t::MAX as u64) as libc::time_t;
    let mut microseconds = timeout.subsec_micros() as libc::suseconds_t;
    if seconds == 0 && microseconds == 0 {
        microseconds = 1;
    }
    let value = libc::timeval {
        tv_sec: seconds,
        tv_usec: microseconds,
    };
    let value_ptr = std::ptr::from_ref(&value).cast::<libc::c_void>();
    let value_len = std::mem::size_of::<libc::timeval>() as libc::socklen_t;
    for option in [libc::SO_RCVTIMEO, libc::SO_SNDTIMEO] {
        // SAFETY: `connection` owns a live TCP socket for the duration of this call, and
        // `value_ptr` references a correctly sized `timeval`.
        let result = unsafe {
            libc::setsockopt(
                connection.as_raw_fd(),
                libc::SOL_SOCKET,
                option,
                value_ptr,
                value_len,
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error()).context("set MySQL socket I/O timeout");
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_mysql_connection_io_timeout(_connection: &MysqlConn, _timeout: Duration) -> Result<()> {
    bail!("StarRocks compatibility MySQL deadline enforcement requires a Unix TCP socket")
}

fn wait_for_fe_mysql_with<C, H, N, Q, S>(
    timeout: Duration,
    process_health: H,
    connect: N,
    select: Q,
    sleep: S,
) -> Result<()>
where
    H: FnMut() -> Result<()>,
    N: FnMut(Duration) -> Result<C>,
    Q: FnMut(&mut C, Duration) -> Result<()>,
    S: FnMut(Duration),
{
    wait_for_fe_mysql_with_clock(
        Instant::now() + timeout,
        process_health,
        connect,
        select,
        sleep,
        Instant::now,
    )
}

fn wait_for_fe_mysql_with_clock<C, H, N, Q, S, T>(
    deadline: Instant,
    mut process_health: H,
    mut connect: N,
    mut select: Q,
    mut sleep: S,
    mut now: T,
) -> Result<()>
where
    H: FnMut() -> Result<()>,
    N: FnMut(Duration) -> Result<C>,
    Q: FnMut(&mut C, Duration) -> Result<()>,
    S: FnMut(Duration),
    T: FnMut() -> Instant,
{
    loop {
        if now() >= deadline {
            bail!("timed out waiting for MySQL connect plus SELECT 1 before starting I/O")
        }
        process_health()?;
        let before_connect = now();
        if before_connect >= deadline {
            bail!("timed out waiting for MySQL connect plus SELECT 1 before connect")
        }
        let connect_budget = deadline
            .duration_since(before_connect)
            .min(Duration::from_secs(2));
        let attempt = match connect(connect_budget) {
            Ok(mut connection) => {
                let before_select = now();
                if before_select >= deadline {
                    Err(anyhow::anyhow!(
                        "MySQL connect completed at the absolute deadline before SELECT 1"
                    ))
                } else {
                    let select_budget = deadline
                        .duration_since(before_select)
                        .min(Duration::from_secs(2));
                    select(&mut connection, select_budget)
                }
            }
            Err(error) => Err(error),
        };
        match attempt {
            Ok(()) => return Ok(()),
            Err(error) => {
                let before_sleep = now();
                if before_sleep >= deadline {
                    bail!("timed out waiting for MySQL connect plus SELECT 1: {error:#}")
                }
                let sleep_budget = POLL_INTERVAL.min(deadline.duration_since(before_sleep));
                if !sleep_budget.is_zero() {
                    sleep(sleep_budget);
                }
            }
        }
    }
}

fn query_backends(
    user: &str,
    password: Option<&str>,
    query_port: u16,
    timeout: Duration,
) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut connection = MysqlConn::new(mysql_options(user, password, query_port, timeout))
        .context("connect to StarRocks FE for SHOW BACKENDS")?;
    let rows: Vec<mysql::Row> = connection
        .query("SHOW BACKENDS")
        .context("execute SHOW BACKENDS")?;
    let headers = rows
        .first()
        .map(|row| {
            row.columns_ref()
                .iter()
                .map(|column| column.name_str().into_owned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let values = rows
        .into_iter()
        .map(|row| {
            (0..row.len())
                .map(|index| row.get::<String, usize>(index).unwrap_or_default())
                .collect::<Vec<_>>()
        })
        .collect();
    Ok((headers, values))
}

fn wait_for_topology(
    user: &str,
    password: Option<&str>,
    query_port: u16,
    endpoints: &[CompatBeEndpoint],
    fe_process: &mut ManagedProcess,
    fe_config: &Path,
    be_processes: &mut [ManagedProcess],
    be_configs: &[PathBuf],
) -> Result<()> {
    let expected_ports = endpoints
        .iter()
        .map(|endpoint| endpoint.heartbeat_port)
        .collect::<Vec<_>>();
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        let diagnostics = ensure_processes_healthy(
            fe_process,
            fe_config,
            query_port,
            be_processes,
            be_configs,
            endpoints,
        )?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let io_timeout = remaining
            .min(Duration::from_secs(2))
            .max(Duration::from_millis(1));
        let last_observation = match query_backends(user, password, query_port, io_timeout) {
            Ok((headers, rows)) => match validate_compat_topology(&expected_ports, &headers, &rows)
            {
                Ok(()) => {
                    let mut sorted = expected_ports;
                    sorted.sort_unstable();
                    println!(
                        "starrocks-compat topology barrier PASS: SHOW BACKENDS 3/3 Alive; heartbeat_ports={sorted:?}"
                    );
                    return Ok(());
                }
                Err(error) => error.to_string(),
            },
            Err(error) => format!("SHOW BACKENDS failed: {error:#}"),
        };
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for StarRocks compatibility topology: {last_observation}; {diagnostics}"
            );
        }
        thread::sleep(POLL_INTERVAL.min(remaining));
    }
}

fn build_be_command(binary: &Path, config: &Path, workdir: &Path) -> Command {
    let mut command = Command::new(binary);
    command
        .arg("run")
        .arg("--config")
        .arg(config)
        .current_dir(workdir);
    command
}

fn start_be(
    index: usize,
    binary: &Path,
    config: &Path,
    workdir: &Path,
    endpoint: &CompatBeEndpoint,
    log_path: PathBuf,
) -> Result<ManagedProcess> {
    let marker = format!(
        "NOVAROCKS_READY role=compat-be heartbeat_port={} brpc_port={} grpc_port={}",
        endpoint.heartbeat_port, endpoint.brpc_port, endpoint.grpc_port
    );
    spawn_managed_process(
        format!("compat BE[{index}]"),
        build_be_command(binary, config, workdir),
        ReadyMarker::StdoutContains(marker),
        STARTUP_TIMEOUT,
        log_path,
    )
}

fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .output()
        .is_ok_and(|output: Output| output.status.success())
}

pub(crate) struct StarRocksCompatServerHandle {
    target_host: String,
    target_port: u16,
    runtime_dir: Option<PathBuf>,
    artifact_binary: PathBuf,
    probe_binary: PathBuf,
    endpoints: Vec<CompatBeEndpoint>,
    be_workdirs: Vec<PathBuf>,
    be_config_paths: Vec<PathBuf>,
    be_processes: Vec<ManagedProcess>,
    fe_config_path: PathBuf,
    fe_process: Option<ManagedProcess>,
    mysql_user: String,
    mysql_password: Option<String>,
    process_ids: Vec<u32>,
}

impl StarRocksCompatServerHandle {
    pub(crate) fn launch(
        repo_root: &Path,
        runner_config: &RunnerConfig,
        artifact: CompatArtifact,
    ) -> Result<Self> {
        let source_fe_home = discover_fe_home()?;
        validate_java_runtime()?;
        let runtime_dir = RuntimeDirGuard::new(create_runtime_dir(repo_root)?);
        let mut reserved = ReservedCompatPorts::new()?;
        let topology = reserved.topology();
        let runtime_fe_home = runtime_dir.path().join("fe");
        create_isolated_fe_home(&source_fe_home, &runtime_fe_home, &topology.fe)?;
        let fe_config_path = runtime_fe_home.join("conf/fe.conf");
        reserved.release_ports(&topology.fe.all_ports())?;

        let mut fe_command = Command::new(runtime_fe_home.join("bin/start_fe.sh"));
        fe_command.arg("--logconsole").current_dir(&runtime_fe_home);
        let mut fe_process = spawn_managed_process(
            "StarRocks FE".to_string(),
            fe_command,
            ReadyMarker::StdoutContains(FE_START_MARKER.to_string()),
            STARTUP_TIMEOUT,
            runtime_dir.path().join("fe-process.log"),
        )?;
        let mysql_user = runner_config
            .cluster
            .get("user")
            .filter(|user| !user.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| "root".to_string());
        let mysql_password = runner_config
            .cluster
            .get("password")
            .filter(|password| !password.is_empty())
            .cloned();
        wait_for_fe_mysql(
            &mysql_user,
            mysql_password.as_deref(),
            topology.fe.query,
            &mut fe_process,
            &fe_config_path,
        )?;

        let endpoints = topology
            .be
            .iter()
            .map(CompatBePorts::endpoint)
            .collect::<Vec<_>>();
        let mut be_workdirs = Vec::with_capacity(COMPAT_BE_COUNT);
        let mut be_config_paths = Vec::with_capacity(COMPAT_BE_COUNT);
        let mut be_processes = Vec::with_capacity(COMPAT_BE_COUNT);
        for (index, ports) in topology.be.iter().enumerate() {
            let workdir = runtime_dir.path().join(format!("be-{index}"));
            fs::create_dir_all(&workdir)?;
            let config = workdir.join("novarocks.toml");
            fs::write(&config, render_be_config(ports)?)?;
            reserved.release_ports(&ports.all_ports())?;
            let process = start_be(
                index,
                &artifact.binary,
                &config,
                &workdir,
                &endpoints[index],
                runtime_dir.path().join(format!("be-{index}.log")),
            )?;
            be_workdirs.push(workdir);
            be_config_paths.push(config);
            be_processes.push(process);
        }

        let mut connection = MysqlConn::new(mysql_options(
            &mysql_user,
            mysql_password.as_deref(),
            topology.fe.query,
            Duration::from_secs(2),
        ))
        .context("connect to StarRocks FE to register compatibility backends")?;
        for endpoint in &endpoints {
            connection
                .query_drop(format!(
                    "ALTER SYSTEM ADD BACKEND '127.0.0.1:{}'",
                    endpoint.heartbeat_port
                ))
                .with_context(|| {
                    format!(
                        "register compatibility BE at heartbeat port {}",
                        endpoint.heartbeat_port
                    )
                })?;
        }
        drop(connection);
        wait_for_topology(
            &mysql_user,
            mysql_password.as_deref(),
            topology.fe.query,
            &endpoints,
            &mut fe_process,
            &fe_config_path,
            &mut be_processes,
            &be_config_paths,
        )?;
        let mut process_ids = be_processes
            .iter()
            .map(ManagedProcess::pid)
            .collect::<Vec<_>>();
        process_ids.push(fe_process.pid());
        let probe_binary = std::env::var_os("NOVAROCKS_COMPAT_PROBE_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                artifact
                    .binary
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join("starrocks-compat-probe")
            });
        Ok(Self {
            target_host: "127.0.0.1".to_string(),
            target_port: topology.fe.query,
            runtime_dir: Some(runtime_dir.into_path()),
            artifact_binary: artifact.binary,
            probe_binary,
            endpoints,
            be_workdirs,
            be_config_paths,
            be_processes,
            fe_config_path,
            fe_process: Some(fe_process),
            mysql_user,
            mysql_password,
            process_ids,
        })
    }

    fn ensure_be_index(&self, index: usize) -> Result<()> {
        if index >= self.be_processes.len() {
            bail!(
                "BE index {index} is out of bounds for StarRocks compatibility cluster with {} BEs",
                self.be_processes.len()
            );
        }
        Ok(())
    }

    fn recheck_topology(&mut self) -> Result<()> {
        let fe_process = self
            .fe_process
            .as_mut()
            .context("StarRocks FE is stopped")?;
        wait_for_topology(
            &self.mysql_user,
            self.mysql_password.as_deref(),
            self.target_port,
            &self.endpoints,
            fe_process,
            &self.fe_config_path,
            &mut self.be_processes,
            &self.be_config_paths,
        )
    }

    fn shutdown_inner(&mut self) -> Result<()> {
        let mut errors = Vec::new();
        for (index, process) in self.be_processes.iter_mut().enumerate() {
            if let Err(error) = process.stop() {
                errors.push(format!("stop compat BE[{index}]: {error:#}"));
            }
        }
        if let Some(fe_process) = self.fe_process.as_mut()
            && let Err(error) = fe_process.stop()
        {
            errors.push(format!("stop StarRocks FE: {error:#}"));
        }
        self.fe_process = None;
        if let Some(runtime_dir) = self.runtime_dir.take()
            && let Err(error) = fs::remove_dir_all(&runtime_dir)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            errors.push(format!("remove runtime {}: {error}", runtime_dir.display()));
        }
        let residual = self.residual_process_ids();
        if !residual.is_empty() {
            errors.push(format!(
                "residual StarRocks compatibility process IDs: {residual:?}"
            ));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            bail!(errors.join("; "))
        }
    }
}

impl ServerHandle for StarRocksCompatServerHandle {
    fn target_host(&self) -> Option<&str> {
        Some(&self.target_host)
    }

    fn target_port(&self) -> Option<u16> {
        Some(self.target_port)
    }

    fn supports_fault_injection(&self) -> bool {
        true
    }

    fn kill_be(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        self.be_processes[index]
            .kill_now()
            .with_context(|| format!("kill compatibility BE[{index}]"))
    }

    fn restart_be(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let endpoint = &self.endpoints[index];
        let marker = format!(
            "NOVAROCKS_READY role=compat-be heartbeat_port={} brpc_port={} grpc_port={}",
            endpoint.heartbeat_port, endpoint.brpc_port, endpoint.grpc_port
        );
        let command = build_be_command(
            &self.artifact_binary,
            &self.be_config_paths[index],
            &self.be_workdirs[index],
        );
        self.be_processes[index]
            .restart(
                command,
                ReadyMarker::StdoutContains(marker),
                STARTUP_TIMEOUT,
                self.runtime_dir
                    .as_ref()
                    .context("compatibility runtime is removed")?
                    .join(format!("be-{index}.log")),
            )
            .with_context(|| format!("restart compatibility BE[{index}]"))?;
        self.process_ids[index] = self.be_processes[index].pid();
        self.recheck_topology()
    }

    fn be_endpoints(&self) -> &[CompatBeEndpoint] {
        &self.endpoints
    }

    fn assert_be_log(&self, index: usize, needle: &str) -> Result<()> {
        self.ensure_be_index(index)?;
        self.be_processes[index].assert_log_contains(needle)
    }

    fn be_log_count(&self, index: usize, needle: &str) -> Result<usize> {
        self.ensure_be_index(index)?;
        self.be_processes[index].log_count(needle)
    }

    fn run_compat_probe(&self, probe: &str, endpoint: &CompatBeEndpoint) -> Result<()> {
        if !self.endpoints.contains(endpoint) {
            bail!(
                "compatibility probe endpoint is not a managed BE: {}:{}",
                endpoint.host,
                endpoint.brpc_port
            );
        }
        if !self.probe_binary.is_file() {
            bail!(
                "compatibility probe binary is missing: {}",
                self.probe_binary.display()
            );
        }
        let marker = format!(
            "probe={probe} status=PASS endpoint={}:{}",
            endpoint.host, endpoint.brpc_port
        );
        let mut command = Command::new(&self.probe_binary);
        command
            .args(["--host", endpoint.host.as_str(), "--brpc-port"])
            .arg(endpoint.brpc_port.to_string())
            .args(["--probe", probe]);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let log_path = self
            .runtime_dir
            .as_ref()
            .context("compatibility runtime is removed")?
            .join(format!("probe-{probe}-{nonce}.log"));
        let mut process = ManagedProcess::spawn(
            format!("compatibility probe {probe}"),
            command,
            ReadyMarker::StdoutContains(marker),
            PROBE_TIMEOUT,
            log_path,
        )?;
        process.stop().context("wait for compatibility probe")
    }

    fn residual_process_ids(&self) -> Vec<u32> {
        self.process_ids
            .iter()
            .copied()
            .filter(|pid| process_exists(*pid))
            .collect()
    }

    fn shutdown(&mut self) -> Result<()> {
        self.shutdown_inner()
    }
}

impl Drop for StarRocksCompatServerHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_inner();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::managed_process::ReadyMarker;
    use std::cell::Cell;
    use std::collections::BTreeSet;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(label: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "novarocks-starrocks-compat-{label}-{}-{nanos}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create test dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_executable(path: &Path, contents: &str) {
        fs::write(path, contents).expect("write executable fixture");
        let mut permissions = fs::metadata(path).expect("fixture metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("make fixture executable");
    }

    fn create_fe_home(root: &Path) -> PathBuf {
        let home = root.join("fe-dist");
        fs::create_dir_all(home.join("bin")).expect("create bin");
        fs::create_dir_all(home.join("lib")).expect("create lib");
        fs::create_dir_all(home.join("spark-dpp")).expect("create spark-dpp");
        fs::create_dir_all(home.join("conf")).expect("create conf");
        write_executable(
            &home.join("bin/start_fe.sh"),
            "#!/bin/sh\necho 'using java version 17'\nwhile :; do sleep 1; done\n",
        );
        fs::write(home.join("lib/fe-core-main.jar"), b"jar").expect("write jar");
        fs::write(
            home.join("conf/fe.conf"),
            "run_mode = shared_nothing\ncloud_native_storage_type = S3\n",
        )
        .expect("write fe.conf");
        home
    }

    #[test]
    fn fe_home_requires_start_script_and_core_jar() {
        let temp = TestDir::new("fe-home-validation");
        let home = temp.path().join("missing");
        fs::create_dir_all(&home).expect("create missing home");

        let error = validate_fe_home(&home).expect_err("missing distribution must fail");
        assert!(error.to_string().contains("bin/start_fe.sh"), "{error:#}");

        fs::create_dir_all(home.join("bin")).expect("create bin");
        write_executable(&home.join("bin/start_fe.sh"), "#!/bin/sh\nexit 0\n");
        let error = validate_fe_home(&home).expect_err("missing jar must fail");
        assert!(
            error.to_string().contains("lib/fe-core-main.jar"),
            "{error:#}"
        );

        fs::create_dir_all(home.join("lib")).expect("create lib");
        fs::write(home.join("lib/fe-core-main.jar"), b"jar").expect("write jar");
        validate_fe_home(&home).expect("complete distribution");

        let mut permissions = fs::metadata(home.join("bin/start_fe.sh"))
            .expect("start script metadata")
            .permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(home.join("bin/start_fe.sh"), permissions)
            .expect("remove executable bit");
        let error = validate_fe_home(&home).expect_err("non-executable start script must fail");
        assert!(error.to_string().contains("not executable"), "{error:#}");
    }

    #[test]
    fn fe_home_selection_prefers_explicit_env_and_falls_back_under_home() {
        let user_home = Path::new("/tmp/compat-user");
        assert_eq!(
            select_fe_home_path(Some(PathBuf::from("/opt/starrocks/fe")), user_home),
            PathBuf::from("/opt/starrocks/fe")
        );
        assert_eq!(
            select_fe_home_path(None, user_home),
            user_home.join("starrocks-on-novarocks/fe")
        );
    }

    #[test]
    fn java_version_parser_requires_seventeen_or_newer() {
        assert_eq!(
            parse_java_major_version("openjdk version \"17.0.12\""),
            Some(17)
        );
        assert_eq!(parse_java_major_version("java version \"21\""), Some(21));
        assert_eq!(
            parse_java_major_version("openjdk version \"1.8.0_412\""),
            Some(8)
        );
        validate_java_version_output("openjdk version \"17.0.12\"").expect("Java 17 is supported");
        let error = validate_java_version_output("openjdk version \"11.0.24\"")
            .expect_err("Java 11 must fail");
        assert!(error.to_string().contains("Java 17 or newer"), "{error:#}");
    }

    #[test]
    fn reserved_topology_has_four_fe_ports_and_six_distinct_ports_per_be() {
        let reserved = ReservedCompatPorts::new().expect("reserve topology ports");
        let topology = reserved.topology();
        let fe_ports = topology.fe.all_ports();
        assert_eq!(fe_ports.len(), 4);
        assert_eq!(topology.be.len(), COMPAT_BE_COUNT);

        let mut all = BTreeSet::new();
        for port in fe_ports {
            assert!(all.insert(port), "duplicate FE port {port}");
        }
        for be in &topology.be {
            let ports = be.all_ports();
            assert_eq!(ports.len(), 6);
            assert_eq!(ports.iter().copied().collect::<BTreeSet<_>>().len(), 6);
            for port in ports {
                assert!(all.insert(port), "cluster-wide duplicate port {port}");
            }
        }
        assert_eq!(all.len(), 4 + COMPAT_BE_COUNT * 6);
    }

    #[test]
    fn fe_conf_copies_only_object_store_keys_and_applies_isolated_overrides() {
        let temp = TestDir::new("fe-conf");
        let fe_home = temp.path().join("runtime/fe");
        let ports = FePorts {
            http: 18030,
            rpc: 19020,
            query: 19030,
            edit_log: 19010,
        };
        let base = r#"
sys_log_level = DEBUG
run_mode = shared_nothing
cloud_native_storage_type = S3
aws_s3_path = s3://bucket/prefix
aws_s3_endpoint = http://127.0.0.1:9000
aws_s3_region = us-east-1
aws_s3_access_key = admin
aws_s3_secret_key = secret
query_port = 9030
"#;

        let rendered = render_fe_conf(base, &fe_home, &ports);
        assert!(rendered.contains(&format!("LOG_DIR = {}/log", fe_home.display())));
        assert!(rendered.contains("JAVA_OPTS = \"-Xms1g -Xmx2g -XX:+UseG1GC\""));
        assert!(rendered.contains(&format!("meta_dir = {}/meta", fe_home.display())));
        assert!(rendered.contains("http_port = 18030"));
        assert!(rendered.contains("rpc_port = 19020"));
        assert!(rendered.contains("query_port = 19030"));
        assert!(rendered.contains("edit_log_port = 19010"));
        assert!(rendered.contains("run_mode = shared_data"));
        assert!(rendered.contains("priority_networks = 127.0.0.1/32"));
        assert!(rendered.contains("proc_profile_cpu_enable = false"));
        assert!(rendered.contains("proc_profile_mem_enable = false"));
        assert!(rendered.contains("cloud_native_storage_type = S3"));
        assert!(rendered.contains("aws_s3_endpoint = http://127.0.0.1:9000"));
        assert!(!rendered.contains("sys_log_level"));
        assert_eq!(rendered.matches("query_port = ").count(), 1);
    }

    #[test]
    fn be_toml_assigns_all_six_unique_ports() {
        let ports = CompatBePorts {
            heartbeat: 19050,
            be: 19060,
            brpc: 18060,
            http: 18040,
            grpc: 19080,
            starlet: 19070,
        };
        let rendered = render_be_config(&ports).expect("render BE config");
        let value: toml::Value = rendered.parse().expect("parse BE TOML");
        let server = value.get("server").expect("server table");
        assert_eq!(
            server.get("host").and_then(toml::Value::as_str),
            Some("127.0.0.1")
        );
        assert_eq!(
            server
                .get("priority_networks")
                .and_then(toml::Value::as_str),
            Some("127.0.0.1/32")
        );
        for (key, expected) in [
            ("heartbeat_port", 19050),
            ("be_port", 19060),
            ("brpc_port", 18060),
            ("http_port", 18040),
            ("grpc_port", 19080),
            ("starlet_port", 19070),
        ] {
            assert_eq!(
                server.get(key).and_then(toml::Value::as_integer),
                Some(expected)
            );
        }
    }

    fn topology_rows(ports: &[u16], alive: &str) -> Vec<Vec<String>> {
        ports
            .iter()
            .enumerate()
            .map(|(index, port)| {
                vec![
                    (index + 1).to_string(),
                    "127.0.0.1".to_string(),
                    port.to_string(),
                    alive.to_string(),
                ]
            })
            .collect()
    }

    #[test]
    fn topology_requires_exact_three_rows_and_exact_heartbeat_multiset() {
        let headers = ["BackendId", "IP", "HeartbeatPort", "Alive"];
        let expected = [19050, 19051, 19052];
        validate_compat_topology(&expected, &headers, &topology_rows(&expected, "true"))
            .expect("exact live topology");

        for (label, rows) in [
            ("missing", topology_rows(&expected[..2], "true")),
            (
                "extra",
                topology_rows(&[19050, 19051, 19052, 19053], "true"),
            ),
            ("wrong", topology_rows(&[19050, 19051, 19053], "true")),
        ] {
            let error = validate_compat_topology(&expected, &headers, &rows).expect_err(label);
            assert!(
                error.to_string().contains("expected heartbeat ports"),
                "{label}: {error:#}"
            );
        }
    }

    #[test]
    fn topology_rejects_alive_false_and_supports_status_live_fallback() {
        let expected = [19050, 19051, 19052];
        let alive_headers = ["BackendId", "IP", "HeartbeatPort", "Alive"];
        let mut rows = topology_rows(&expected, "true");
        rows[1][3] = "false".to_string();
        let error = validate_compat_topology(&expected, &alive_headers, &rows)
            .expect_err("Alive=false must fail");
        assert!(error.to_string().contains("not alive"), "{error:#}");

        let status_headers = ["BackendId", "IP", "HeartbeatPort", "Status"];
        let live_rows = topology_rows(&expected, "Live");
        validate_compat_topology(&expected, &status_headers, &live_rows)
            .expect("Status=Live fallback");
    }

    #[test]
    fn fake_fe_early_exit_is_reported_before_readiness() {
        let temp = TestDir::new("fake-fe-exit");
        let script = temp.path().join("fake-fe.sh");
        write_executable(&script, "#!/bin/sh\necho fatal-fe >&2\nexit 17\n");
        let command = Command::new(&script);
        let error = spawn_managed_process(
            "StarRocks FE".to_string(),
            command,
            ReadyMarker::StdoutContains("using java version".to_string()),
            Duration::from_secs(2),
            temp.path().join("fe.log"),
        )
        .expect_err("early FE exit must fail");
        let message = format!("{error:#}");
        assert!(
            message.contains("StarRocks FE exited before readiness marker"),
            "{message}"
        );
        assert!(message.contains("fatal-fe"), "{message}");
    }

    #[test]
    fn fake_be_early_exit_is_reported_before_readiness() {
        let temp = TestDir::new("fake-be-exit");
        let script = temp.path().join("fake-be.sh");
        write_executable(&script, "#!/bin/sh\necho fatal-be >&2\nexit 23\n");
        let command = Command::new(&script);
        let error = spawn_managed_process(
            "compat BE[0]".to_string(),
            command,
            ReadyMarker::StdoutContains("NOVAROCKS_READY role=compat-be".to_string()),
            Duration::from_secs(2),
            temp.path().join("be.log"),
        )
        .expect_err("early BE exit must fail");
        let message = format!("{error:#}");
        assert!(
            message.contains("compat BE[0] exited before readiness marker"),
            "{message}"
        );
        assert!(message.contains("fatal-be"), "{message}");
    }

    #[test]
    fn isolated_fe_runtime_symlinks_immutable_distribution_content() {
        let temp = TestDir::new("isolated-fe");
        let source = create_fe_home(temp.path());
        let runtime = temp.path().join("runtime/fe");
        let ports = FePorts {
            http: 18030,
            rpc: 19020,
            query: 19030,
            edit_log: 19010,
        };
        create_isolated_fe_home(&source, &runtime, &ports).expect("create isolated FE home");

        for name in ["bin", "lib", "spark-dpp"] {
            assert!(
                fs::symlink_metadata(runtime.join(name))
                    .expect("runtime entry")
                    .file_type()
                    .is_symlink(),
                "{name} must be immutable symlink"
            );
        }
        for name in ["conf", "log", "meta"] {
            assert!(
                runtime.join(name).is_dir(),
                "{name} must be writable directory"
            );
        }
        let conf = fs::read_to_string(runtime.join("conf/fe.conf")).expect("runtime fe.conf");
        assert!(conf.contains("run_mode = shared_data"));
    }

    #[test]
    fn isolated_fe_conf_is_fresh_allowlisted_and_accepts_readonly_source() {
        let temp = TestDir::new("isolated-fe-conf");
        let source = create_fe_home(temp.path());
        fs::write(
            source.join("conf/hadoop_env.sh"),
            "export SHOULD_NOT_COPY=1\n",
        )
        .expect("write optional hadoop env");
        fs::create_dir_all(source.join("conf/nested")).expect("create nested source conf");
        fs::write(source.join("conf/nested/secret.txt"), "do not copy")
            .expect("write unexpected source conf");
        let mut source_conf_permissions = fs::metadata(source.join("conf"))
            .expect("source conf metadata")
            .permissions();
        source_conf_permissions.set_mode(0o555);
        fs::set_permissions(source.join("conf"), source_conf_permissions)
            .expect("make source conf readonly");
        let mut source_fe_conf_permissions = fs::metadata(source.join("conf/fe.conf"))
            .expect("source fe.conf metadata")
            .permissions();
        source_fe_conf_permissions.set_mode(0o444);
        fs::set_permissions(source.join("conf/fe.conf"), source_fe_conf_permissions)
            .expect("make source fe.conf readonly");

        let runtime = temp.path().join("runtime/fe");
        create_isolated_fe_home(
            &source,
            &runtime,
            &FePorts {
                http: 18030,
                rpc: 19020,
                query: 19030,
                edit_log: 19010,
            },
        )
        .expect("readonly source must still create isolated FE home");

        let copied = fs::read_dir(runtime.join("conf"))
            .expect("read runtime conf")
            .map(|entry| entry.expect("runtime conf entry").file_name())
            .collect::<Vec<_>>();
        assert_eq!(copied, vec![std::ffi::OsString::from("fe.conf")]);
    }

    #[test]
    fn fe_mysql_health_check_retries_transient_select_failure() {
        let mut health_checks = 0usize;
        let mut mysql_attempts = 0usize;
        wait_for_fe_mysql_with(
            Duration::from_secs(1),
            || {
                health_checks += 1;
                Ok(())
            },
            |_| Ok(()),
            |_, _| {
                mysql_attempts += 1;
                if mysql_attempts == 1 {
                    bail!("transient SELECT 1 failure");
                }
                Ok(())
            },
            |_| {},
        )
        .expect("transient MySQL health failure must be retried");
        assert_eq!(mysql_attempts, 2);
        assert_eq!(health_checks, 2);
    }

    #[test]
    fn fe_mysql_connect_and_select_share_one_absolute_deadline() {
        let base = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let select_budget = Cell::new(None);

        let error = wait_for_fe_mysql_with_clock(
            base + Duration::from_millis(100),
            || Ok(()),
            |connect_budget| {
                assert_eq!(connect_budget, Duration::from_millis(100));
                elapsed.set(elapsed.get() + Duration::from_millis(70));
                Ok(())
            },
            |_, budget| {
                select_budget.set(Some(budget));
                elapsed.set(elapsed.get() + budget);
                bail!("SELECT 1 remained blocked until its I/O budget expired")
            },
            |_| panic!("an expired absolute deadline must not sleep before returning"),
            || base + elapsed.get(),
        )
        .expect_err("SELECT 1 must fail at the shared absolute deadline");

        assert!(error.to_string().contains("timed out"), "{error:#}");
        assert_eq!(select_budget.get(), Some(Duration::from_millis(30)));
        assert_eq!(elapsed.get(), Duration::from_millis(100));
    }
}
