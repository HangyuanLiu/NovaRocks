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
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer};
use std::path::{Path, PathBuf};

use novarocks_state_store::StateStoreLimitOverrides;
use novarocks_state_store::config::{
    FoundationDbClientConfig, MySqlClientConfig, MySqlTlsMode, StateStoreAppConfig,
    StateStoreConfig, StateStoreProviderConfig,
};
use novarocks_types::ClusterRole;
use uuid::Uuid;

pub use novarocks::common::memory_limit::DEFAULT_MEM_LIMIT_SPEC;

fn default_log_level() -> String {
    "info".to_string()
}

fn default_sys_log_dir() -> String {
    "log".to_string()
}

fn default_sys_log_roll_mode() -> String {
    "SIZE-MB-1024".to_string()
}

fn default_sys_log_roll_num() -> usize {
    10
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StateStoreProviderKindWire {
    Sqlite,
    Foundationdb,
    Mysql,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreAppConfigWire {
    provider: StateStoreProviderKindWire,
    cluster_id: String,
    path: Option<PathBuf>,
    deployment_owner: Option<String>,
    cluster_file: Option<PathBuf>,
    keyspace_id: Option<Uuid>,
    database: Option<String>,
    #[serde(default)]
    limits: StateStoreLimitOverridesWire,
    mysql_client: Option<MySqlClientConfigWire>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreLimitOverridesWire {
    max_key_bytes: Option<usize>,
    max_value_bytes: Option<usize>,
    max_page_size: Option<usize>,
    max_transaction_operations: Option<usize>,
    max_transaction_bytes: Option<usize>,
    transaction_deadline_ms: Option<u64>,
    runner_max_attempts: Option<usize>,
}
impl From<StateStoreLimitOverridesWire> for StateStoreLimitOverrides {
    fn from(w: StateStoreLimitOverridesWire) -> Self {
        Self {
            max_key_bytes: w.max_key_bytes,
            max_value_bytes: w.max_value_bytes,
            max_page_size: w.max_page_size,
            max_transaction_operations: w.max_transaction_operations,
            max_transaction_bytes: w.max_transaction_bytes,
            transaction_deadline_ms: w.transaction_deadline_ms,
            runner_max_attempts: w.runner_max_attempts,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum MySqlTlsModeWire {
    Disabled,
    Required,
    VerifyIdentity,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MySqlClientConfigWire {
    host: String,
    port: u16,
    username: String,
    password_env: String,
    tls_mode: MySqlTlsModeWire,
    tls_ca_path: Option<PathBuf>,
    tls_cert_path: Option<PathBuf>,
    tls_key_path: Option<PathBuf>,
    connect_timeout_ms: u64,
    pool_min: usize,
    pool_max: usize,
    inactive_connection_ttl_ms: u64,
}
impl From<MySqlClientConfigWire> for MySqlClientConfig {
    fn from(w: MySqlClientConfigWire) -> Self {
        Self {
            host: w.host,
            port: w.port,
            username: w.username,
            password_env: w.password_env,
            tls_mode: match w.tls_mode {
                MySqlTlsModeWire::Disabled => MySqlTlsMode::Disabled,
                MySqlTlsModeWire::Required => MySqlTlsMode::Required,
                MySqlTlsModeWire::VerifyIdentity => MySqlTlsMode::VerifyIdentity,
            },
            tls_ca_path: w.tls_ca_path,
            tls_cert_path: w.tls_cert_path,
            tls_key_path: w.tls_key_path,
            connect_timeout_ms: w.connect_timeout_ms,
            pool_min: w.pool_min,
            pool_max: w.pool_max,
            inactive_connection_ttl_ms: w.inactive_connection_ttl_ms,
        }
    }
}

fn state_store_from_wire<E: serde::de::Error>(
    wire: StateStoreAppConfigWire,
) -> std::result::Result<StateStoreAppConfig, E> {
    let provider = match wire.provider {
        StateStoreProviderKindWire::Sqlite => {
            if wire.cluster_file.is_some() || wire.keyspace_id.is_some() || wire.database.is_some()
            {
                return Err(E::custom(
                    "non-SQLite fields are not valid for the sqlite state store provider",
                ));
            }
            StateStoreProviderConfig::Sqlite {
                path: wire.path.ok_or_else(|| E::missing_field("path"))?,
                deployment_owner: wire
                    .deployment_owner
                    .ok_or_else(|| E::missing_field("deployment_owner"))?,
            }
        }
        StateStoreProviderKindWire::Foundationdb => {
            if wire.path.is_some() || wire.deployment_owner.is_some() || wire.database.is_some() {
                return Err(E::custom(
                    "non-FoundationDB fields are not valid for the foundationdb state store provider",
                ));
            }
            StateStoreProviderConfig::Foundationdb {
                cluster_file: wire
                    .cluster_file
                    .ok_or_else(|| E::missing_field("cluster_file"))?,
                keyspace_id: wire
                    .keyspace_id
                    .ok_or_else(|| E::missing_field("keyspace_id"))?,
            }
        }
        StateStoreProviderKindWire::Mysql => {
            if wire.path.is_some()
                || wire.deployment_owner.is_some()
                || wire.cluster_file.is_some()
                || wire.keyspace_id.is_some()
            {
                return Err(E::custom(
                    "non-MySQL fields are not valid for the mysql state store provider",
                ));
            }
            StateStoreProviderConfig::Mysql {
                database: wire.database.ok_or_else(|| E::missing_field("database"))?,
            }
        }
    };
    Ok(StateStoreAppConfig {
        store: StateStoreConfig {
            cluster_id: wire.cluster_id,
            limits: wire.limits.into(),
            provider,
        },
        mysql_client: wire.mysql_client.map(Into::into),
    })
}
fn deserialize_state_store<'de, D: Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<StateStoreAppConfig>, D::Error> {
    Option::<StateStoreAppConfigWire>::deserialize(d)?
        .map(state_store_from_wire)
        .transpose()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FoundationDbClientConfigWire {
    disable_multi_version_client: bool,
    tls_cert_path: Option<PathBuf>,
    tls_key_path: Option<PathBuf>,
    tls_ca_path: Option<PathBuf>,
    tls_verify_peers: Option<String>,
    tls_password_env: Option<String>,
}
fn deserialize_foundationdb_client<'de, D: Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<FoundationDbClientConfig>, D::Error> {
    Ok(
        Option::<FoundationDbClientConfigWire>::deserialize(d)?.map(|w| FoundationDbClientConfig {
            disable_multi_version_client: w.disable_multi_version_client,
            tls_cert_path: w.tls_cert_path,
            tls_key_path: w.tls_key_path,
            tls_ca_path: w.tls_ca_path,
            tls_verify_peers: w.tls_verify_peers,
            tls_password_env: w.tls_password_env,
        }),
    )
}

/// Configuration for the `[cluster]` TOML section.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub role: ClusterRole,
    pub backends: Vec<String>,
    pub advertise_host: String,
    pub advertise_port: u16,
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_heartbeat_timeout_retries")]
    pub heartbeat_timeout_retries: u32,
    #[serde(default = "default_decommission_timeout_secs")]
    pub decommission_timeout_secs: u64,
}

fn default_heartbeat_interval_ms() -> u64 {
    5000
}

fn default_heartbeat_timeout_retries() -> u32 {
    3
}

fn default_decommission_timeout_secs() -> u64 {
    300
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            role: ClusterRole::default(),
            backends: Vec::new(),
            advertise_host: String::new(),
            advertise_port: 0,
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            heartbeat_timeout_retries: default_heartbeat_timeout_retries(),
            decommission_timeout_secs: default_decommission_timeout_secs(),
        }
    }
}

impl ClusterConfig {
    /// Validate cluster config consistency. Called at startup after parsing.
    pub fn validate(&self) -> Result<(), String> {
        match self.role {
            ClusterRole::Fe => {
                let mut seen = std::collections::HashSet::new();
                for b in &self.backends {
                    let canonical = b
                        .parse::<std::net::SocketAddr>()
                        .map_err(|e| format!("invalid backend addr '{}': {}", b, e))?
                        .to_string();
                    if !seen.insert(canonical) {
                        return Err(format!("duplicate backend in [cluster].backends: {}", b));
                    }
                }
            }
            ClusterRole::Be => {
                if !self.backends.is_empty() {
                    return Err(format!(
                        "role=be must not configure [cluster].backends (got {} entries)",
                        self.backends.len()
                    ));
                }
            }
            ClusterRole::AllInOne => {
                if !self.backends.is_empty() {
                    return Err(format!(
                        "role=all-in-one must not configure [cluster].backends (got {} entries)",
                        self.backends.len()
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod cluster_hb_tests {
    use super::ClusterConfig;

    #[test]
    fn cluster_config_heartbeat_defaults() {
        let c = ClusterConfig::default();
        assert_eq!(c.heartbeat_interval_ms, 5000);
        assert_eq!(c.heartbeat_timeout_retries, 3);
        assert_eq!(c.decommission_timeout_secs, 300);
    }

    #[test]
    fn cluster_config_parses_heartbeat_overrides() {
        let toml = r#"
            role = "fe"
            backends = ["127.0.0.1:9070"]
            heartbeat_interval_ms = 2000
            heartbeat_timeout_retries = 5
        "#;
        let c: ClusterConfig = toml::from_str(toml).unwrap();
        assert_eq!(c.heartbeat_interval_ms, 2000);
        assert_eq!(c.heartbeat_timeout_retries, 5);
        assert_eq!(c.decommission_timeout_secs, 300);
    }
}

/// Resolve the config file path using the standard search order:
/// 1. `explicit` – a path supplied directly by the caller (e.g. `--config`).
/// 2. `NOVAROCKS_CONFIG` environment variable.
/// 3. `./novarocks.toml` in the current working directory (only if the file exists).
/// 4. `None` – the caller should fall back to built-in defaults.
pub fn resolve_config_path(explicit: Option<&Path>) -> Option<PathBuf> {
    explicit
        .map(Path::to_path_buf)
        .or_else(|| {
            std::env::var("NOVAROCKS_CONFIG")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
        })
        .or_else(|| {
            let default_path = PathBuf::from("novarocks.toml");
            default_path.exists().then_some(default_path)
        })
}

/// Loads the config at `path`, falling back to built-in defaults when the file
/// is absent.
///
/// The result is a value the caller owns. There is no process-wide active
/// config: whoever loads the config hands it to the components that need it.
pub fn load_from_path(path: impl AsRef<Path>) -> Result<NovaRocksConfig> {
    let path = path.as_ref().to_path_buf();
    if !path.exists() {
        eprintln!(
            "WARNING: config file '{}' not found, using built-in defaults",
            path.display()
        );
        return Ok(NovaRocksConfig::default());
    }
    NovaRocksConfig::load_from_file(&path)
}

/// Loads the config named by `NOVAROCKS_CONFIG`, else `./novarocks.toml`, else
/// the built-in defaults.
pub fn load_from_env_or_default() -> Result<NovaRocksConfig> {
    if let Ok(p) = std::env::var("NOVAROCKS_CONFIG") {
        let p = p.trim();
        if !p.is_empty() {
            return load_from_path(PathBuf::from(p));
        }
    }

    let default_path = PathBuf::from("novarocks.toml");
    if default_path.exists() {
        return NovaRocksConfig::load_from_file(&default_path);
    }

    eprintln!("WARNING: config file 'novarocks.toml' not found, using built-in defaults");
    Ok(NovaRocksConfig::default())
}

/// Load only the resolved object-store input needed by offline tooling.
pub fn load_object_store_config(
    explicit: Option<&Path>,
) -> Result<novarocks_fs::ObjectStoreConfig> {
    let config = match explicit {
        Some(path) => load_from_path(path)?,
        None => load_from_env_or_default()?,
    };
    config
        .connector
        .object_store_config(&config.runtime.object_storage.retry_settings())
        .map_err(anyhow::Error::msg)?
        .context("missing [connector.object_store] config")
}

#[derive(Clone, Deserialize)]
pub struct NovaRocksConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// Optional full tracing EnvFilter expression.
    /// If set, this takes precedence over `log_level`.
    /// Example: "novarocks=debug,h2=off,hyper=off,tonic=off"
    #[serde(default)]
    pub log_filter: Option<String>,

    #[serde(default = "default_sys_log_dir")]
    pub sys_log_dir: String,

    #[serde(default = "default_sys_log_roll_mode")]
    pub sys_log_roll_mode: String,

    #[serde(default = "default_sys_log_roll_num")]
    pub sys_log_roll_num: usize,

    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub runtime: RuntimeConfig,

    #[serde(default, deserialize_with = "deserialize_state_store")]
    pub state_store: Option<StateStoreAppConfig>,

    #[serde(default, deserialize_with = "deserialize_foundationdb_client")]
    pub foundationdb_client: Option<FoundationDbClientConfig>,

    #[serde(default)]
    pub standalone_server: Option<StandaloneServerConfig>,

    #[serde(default)]
    pub connector: ConnectorConfig,

    #[serde(default)]
    pub spill: SpillStorageConfig,

    #[serde(default)]
    pub cluster: ClusterConfig,
}

impl NovaRocksConfig {
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("read config file: {}", path.display()))?;
        let cfg: NovaRocksConfig =
            toml::from_str(&s).with_context(|| format!("parse toml: {}", path.display()))?;
        validate_state_store_configuration(&cfg)?;
        validate_query_control_config(&cfg.runtime)?;
        #[cfg(not(debug_assertions))]
        reject_fault_injection_environment()?;
        Ok(cfg)
    }
}

/// Reject runner-owned fault-injection environment variables in release builds.
///
/// The fault hooks themselves read these variables directly (see
/// `common::query_lifecycle_fault` and `common::cleanup_fault`) and are compiled
/// out of release builds. Failing startup here keeps a release binary from
/// silently ignoring an armed fault and letting a cross-process test pass
/// vacuously.
#[cfg(not(debug_assertions))]
fn reject_fault_injection_environment() -> Result<()> {
    for name in [
        "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR",
        "NOVAROCKS_SQL_TEST_CLEANUP_FAULT_DIR",
        "NOVAROCKS_SQL_TEST_FAULT_INJECT_FETCH_NOT_READY_COUNT",
        "NOVAROCKS_SQL_TEST_EMIT_CANCEL_MARKER",
        "NOVAROCKS_SQL_TEST_EMIT_GRPC_FRAGMENT_MARKER",
        "NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER",
        "NOVAROCKS_DEBUG_EXEC_NODE_OUTPUT",
    ] {
        if std::env::var_os(name).is_some() {
            bail!("{name} is only available in debug builds");
        }
    }
    Ok(())
}

impl Default for NovaRocksConfig {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            log_filter: None,
            sys_log_dir: default_sys_log_dir(),
            sys_log_roll_mode: default_sys_log_roll_mode(),
            sys_log_roll_num: default_sys_log_roll_num(),
            server: ServerConfig::default(),
            runtime: RuntimeConfig::default(),
            state_store: None,
            foundationdb_client: None,
            standalone_server: None,
            connector: ConnectorConfig::default(),
            spill: SpillStorageConfig::default(),
            cluster: ClusterConfig::default(),
        }
    }
}

fn validate_state_store_configuration(config: &NovaRocksConfig) -> Result<()> {
    if let Some(state_store) = &config.state_store {
        state_store.validate()?;
    }

    match (
        config
            .state_store
            .as_ref()
            .map(|state_store| &state_store.store.provider),
        &config.foundationdb_client,
    ) {
        (None, None)
        | (Some(StateStoreProviderConfig::Sqlite { .. }), None)
        | (Some(StateStoreProviderConfig::Mysql { .. }), None) => Ok(()),
        (Some(StateStoreProviderConfig::Foundationdb { .. }), Some(client)) => client.validate(),
        (Some(StateStoreProviderConfig::Foundationdb { .. }), None) => {
            bail!("InvalidStateStoreConfig: foundationdb provider requires [foundationdb_client]")
        }
        (None, Some(_))
        | (Some(StateStoreProviderConfig::Sqlite { .. }), Some(_))
        | (Some(StateStoreProviderConfig::Mysql { .. }), Some(_)) => bail!(
            "InvalidStateStoreConfig: [foundationdb_client] requires the foundationdb state store provider"
        ),
    }
}

#[derive(Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_server_host")]
    pub host: String,
    #[serde(default)]
    pub priority_networks: String,
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,
}

fn default_server_host() -> String {
    "127.0.0.1".to_string()
}
fn default_http_port() -> u16 {
    8040
}
fn default_grpc_port() -> u16 {
    9080
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_server_host(),
            priority_networks: String::new(),
            http_port: default_http_port(),
            grpc_port: default_grpc_port(),
        }
    }
}

/// Shared object-store credentials loaded independently by every backend at
/// startup. Native plans may reference this binding but must never carry its
/// values.
#[derive(Clone, Debug, Deserialize, Default, PartialEq, Eq)]
pub struct ConnectorObjectStoreConfig {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub access_key_secret: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub enable_path_style_access: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Default, PartialEq, Eq)]
#[serde(default)]
pub struct ConnectorConfig {
    pub object_store: Option<ConnectorObjectStoreConfig>,
}

impl ConnectorConfig {
    /// Project the `[connector.object_store]` credentials onto a neutral
    /// filesystem config, filling unset retry knobs from `retry`.
    ///
    /// The retry defaults arrive as an argument rather than being read from a
    /// process-global config, so that `novarocks-fs` owns no configuration
    /// source of its own.
    pub fn object_store_config(
        &self,
        retry: &novarocks_fs::ObjectStoreRetrySettings,
    ) -> std::result::Result<Option<novarocks_fs::ObjectStoreConfig>, String> {
        let Some(object_store) = self.object_store.as_ref() else {
            return Ok(None);
        };
        let credentials = novarocks_fs::ObjectStoreCredentials::from_parts(
            novarocks_fs::ObjectStoreCredentialsSource::ConnectorStartupConfig,
            object_store.endpoint.as_deref().unwrap_or_default(),
            object_store.access_key_id.as_deref().unwrap_or_default(),
            object_store
                .access_key_secret
                .as_deref()
                .unwrap_or_default(),
            object_store.region.as_deref(),
            object_store.enable_path_style_access,
        )?;
        let mut config = credentials.to_object_store_config();
        retry.apply_if_absent(&mut config);
        Ok(Some(config))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct StandaloneServerConfig {
    #[serde(default = "default_standalone_server_mysql_port")]
    pub mysql_port: u16,
    #[serde(default = "default_standalone_server_user")]
    pub user: String,
    #[serde(default)]
    pub mv_refresh_scheduler_enabled: bool,
    #[serde(default = "default_standalone_mv_refresh_scheduler_interval_ms")]
    pub mv_refresh_scheduler_interval_ms: u64,
    #[serde(default = "default_standalone_mv_refresh_scheduler_max_concurrent")]
    pub mv_refresh_scheduler_max_concurrent: usize,
    #[serde(default = "default_standalone_mv_refresh_scheduler_failure_backoff_ms")]
    pub mv_refresh_scheduler_failure_backoff_ms: i64,
    #[serde(default = "default_standalone_mv_refresh_scheduler_max_failure_backoff_ms")]
    pub mv_refresh_scheduler_max_failure_backoff_ms: i64,
    #[serde(default = "default_standalone_mv_refresh_max_touched_groups")]
    pub mv_refresh_max_touched_groups: usize,
    #[serde(default = "default_standalone_mv_refresh_max_affected_partitions")]
    pub mv_refresh_max_affected_partitions: usize,
    #[serde(default = "default_standalone_mv_partition_state_max_entries")]
    pub mv_partition_state_max_entries: usize,
    #[serde(default = "default_standalone_iceberg_maintenance_enabled")]
    pub iceberg_maintenance_enabled: bool,
    #[serde(default = "default_standalone_iceberg_maintenance_tick_interval_ms")]
    pub iceberg_maintenance_tick_interval_ms: u64,
    #[serde(default = "default_standalone_iceberg_maintenance_max_concurrent")]
    pub iceberg_maintenance_max_concurrent: usize,
    #[serde(default = "default_standalone_iceberg_maintenance_compaction_min_data_files")]
    pub iceberg_maintenance_compaction_min_data_files: u64,
    #[serde(default = "default_standalone_iceberg_maintenance_dv_min_delete_files")]
    pub iceberg_maintenance_dv_min_delete_files: u64,
    #[serde(default = "default_standalone_iceberg_maintenance_action_cooldown_ms")]
    pub iceberg_maintenance_action_cooldown_ms: i64,
    #[serde(default = "default_standalone_iceberg_maintenance_max_consecutive_failures")]
    pub iceberg_maintenance_max_consecutive_failures: u32,
}

fn default_standalone_server_mysql_port() -> u16 {
    9030
}

fn default_standalone_server_user() -> String {
    "root".to_string()
}

fn default_standalone_mv_refresh_scheduler_interval_ms() -> u64 {
    30_000
}

fn default_standalone_mv_refresh_scheduler_max_concurrent() -> usize {
    1
}

fn default_standalone_mv_refresh_scheduler_failure_backoff_ms() -> i64 {
    60_000
}

fn default_standalone_mv_refresh_scheduler_max_failure_backoff_ms() -> i64 {
    1_800_000
}

fn default_standalone_mv_refresh_max_touched_groups() -> usize {
    100_000
}

fn default_standalone_mv_refresh_max_affected_partitions() -> usize {
    4_096
}

fn default_standalone_mv_partition_state_max_entries() -> usize {
    10_000
}

fn default_standalone_iceberg_maintenance_enabled() -> bool {
    true
}

fn default_standalone_iceberg_maintenance_tick_interval_ms() -> u64 {
    600_000
}

fn default_standalone_iceberg_maintenance_max_concurrent() -> usize {
    1
}

fn default_standalone_iceberg_maintenance_compaction_min_data_files() -> u64 {
    100
}

fn default_standalone_iceberg_maintenance_dv_min_delete_files() -> u64 {
    10
}

fn default_standalone_iceberg_maintenance_action_cooldown_ms() -> i64 {
    3_600_000
}

fn default_standalone_iceberg_maintenance_max_consecutive_failures() -> u32 {
    4
}

impl Default for StandaloneServerConfig {
    fn default() -> Self {
        Self {
            mysql_port: default_standalone_server_mysql_port(),
            user: default_standalone_server_user(),
            mv_refresh_scheduler_enabled: false,
            mv_refresh_scheduler_interval_ms: default_standalone_mv_refresh_scheduler_interval_ms(),
            mv_refresh_scheduler_max_concurrent:
                default_standalone_mv_refresh_scheduler_max_concurrent(),
            mv_refresh_scheduler_failure_backoff_ms:
                default_standalone_mv_refresh_scheduler_failure_backoff_ms(),
            mv_refresh_scheduler_max_failure_backoff_ms:
                default_standalone_mv_refresh_scheduler_max_failure_backoff_ms(),
            mv_refresh_max_touched_groups: default_standalone_mv_refresh_max_touched_groups(),
            mv_refresh_max_affected_partitions:
                default_standalone_mv_refresh_max_affected_partitions(),
            mv_partition_state_max_entries: default_standalone_mv_partition_state_max_entries(),
            iceberg_maintenance_enabled: default_standalone_iceberg_maintenance_enabled(),
            iceberg_maintenance_tick_interval_ms:
                default_standalone_iceberg_maintenance_tick_interval_ms(),
            iceberg_maintenance_max_concurrent:
                default_standalone_iceberg_maintenance_max_concurrent(),
            iceberg_maintenance_compaction_min_data_files:
                default_standalone_iceberg_maintenance_compaction_min_data_files(),
            iceberg_maintenance_dv_min_delete_files:
                default_standalone_iceberg_maintenance_dv_min_delete_files(),
            iceberg_maintenance_action_cooldown_ms:
                default_standalone_iceberg_maintenance_action_cooldown_ms(),
            iceberg_maintenance_max_consecutive_failures:
                default_standalone_iceberg_maintenance_max_consecutive_failures(),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_exchange_wait_ms")]
    pub exchange_wait_ms: u64,
    #[serde(default = "default_exchange_max_transmit_batched_bytes")]
    pub exchange_max_transmit_batched_bytes: usize,
    #[serde(default = "default_exchange_io_threads")]
    pub exchange_io_threads: usize,
    #[serde(default = "default_exchange_io_max_inflight_bytes")]
    pub exchange_io_max_inflight_bytes: usize,
    #[serde(default = "default_query_control_heartbeat_interval_ms")]
    pub query_control_heartbeat_interval_ms: u64,
    #[serde(default = "default_query_control_heartbeat_timeout_ms")]
    pub query_control_heartbeat_timeout_ms: u64,
    #[serde(default = "default_query_control_init_rpc_timeout_ms")]
    pub query_control_init_rpc_timeout_ms: u64,
    #[serde(default = "default_query_control_attach_timeout_ms")]
    pub query_control_attach_timeout_ms: u64,
    #[serde(default = "default_query_control_stage_rpc_timeout_ms")]
    pub query_control_stage_rpc_timeout_ms: u64,
    #[serde(default = "default_query_control_start_rpc_timeout_ms")]
    pub query_control_start_rpc_timeout_ms: u64,
    #[serde(default = "default_query_control_pre_start_timeout_ms")]
    pub query_control_pre_start_timeout_ms: u64,
    #[serde(default = "default_query_control_tombstone_retention_ms")]
    pub query_control_tombstone_retention_ms: u64,
    #[serde(default = "default_query_control_tombstone_capacity")]
    pub query_control_tombstone_capacity: usize,
    #[serde(default = "default_query_control_terminal_drain_timeout_ms")]
    pub query_control_terminal_drain_timeout_ms: u64,
    #[serde(default = "default_query_control_terminal_ack_timeout_ms")]
    pub query_control_terminal_ack_timeout_ms: u64,
    #[serde(default = "default_query_control_terminal_fallback_rpc_timeout_ms")]
    pub query_control_terminal_fallback_rpc_timeout_ms: u64,
    #[serde(default = "default_query_control_terminal_fallback_max_attempts")]
    pub query_control_terminal_fallback_max_attempts: usize,
    #[serde(default = "default_query_control_terminal_fallback_initial_backoff_ms")]
    pub query_control_terminal_fallback_initial_backoff_ms: u64,
    #[serde(default = "default_query_control_terminal_fallback_max_backoff_ms")]
    pub query_control_terminal_fallback_max_backoff_ms: u64,
    #[serde(default = "default_query_control_terminal_max_encoded_bytes")]
    pub query_control_terminal_max_encoded_bytes: usize,
    #[serde(default = "default_query_control_terminal_max_retained_bytes")]
    pub query_control_terminal_max_retained_bytes: usize,
    #[serde(default = "default_query_control_terminal_retained_capacity")]
    pub query_control_terminal_retained_capacity: usize,
    #[serde(default = "default_query_control_terminal_retention_ms")]
    pub query_control_terminal_retention_ms: u64,
    #[serde(default = "default_query_control_max_active_entries")]
    pub query_control_max_active_entries: usize,
    #[serde(default = "default_query_control_stage_max_encoded_bytes")]
    pub query_control_stage_max_encoded_bytes: usize,
    #[serde(default = "default_query_control_stage_max_fragments")]
    pub query_control_stage_max_fragments: usize,
    #[serde(default = "default_query_control_max_active_staging")]
    pub query_control_max_active_staging: usize,
    #[serde(default = "default_query_control_stage_max_inflight_encoded_bytes")]
    pub query_control_stage_max_inflight_encoded_bytes: usize,
    #[serde(default = "default_query_control_stage_max_dormant_workers")]
    pub query_control_stage_max_dormant_workers: usize,
    #[serde(default = "default_write_commit_evidence_max_bytes")]
    pub write_commit_evidence_max_bytes: usize,
    #[serde(default = "default_write_commit_evidence_max_entries")]
    pub write_commit_evidence_max_entries: usize,
    #[serde(default = "default_mem_limit")]
    pub mem_limit: String,
    #[serde(default = "default_be_mem_limit_bytes")]
    pub be_mem_limit_bytes: u64,
    #[serde(default = "default_optimizer_query_mem_limit_bytes")]
    pub optimizer_query_mem_limit_bytes: u64,
    /// `0` means derive the backend count from the live BE registry (the normal path).
    #[serde(default = "default_optimizer_effective_backend_count")]
    pub optimizer_effective_backend_count: u64,
    #[serde(default = "default_local_exchange_buffer_mem_limit_per_driver")]
    pub local_exchange_buffer_mem_limit_per_driver: usize,
    #[serde(default = "default_local_exchange_max_buffered_rows")]
    pub local_exchange_max_buffered_rows: i64,
    #[serde(default = "default_operator_buffer_chunks")]
    pub operator_buffer_chunks: usize,
    #[serde(default = "default_olap_sink_write_buffer_size_bytes")]
    pub olap_sink_write_buffer_size_bytes: usize,
    #[serde(default = "default_olap_sink_max_tablet_write_chunk_bytes")]
    pub olap_sink_max_tablet_write_chunk_bytes: usize,
    #[serde(default = "default_pipeline_scan_thread_pool_thread_num")]
    pub pipeline_scan_thread_pool_thread_num: usize,
    #[serde(default = "default_enable_tablet_write_log")]
    pub enable_tablet_write_log: bool,
    #[serde(default = "default_tablet_write_log_buffer_size")]
    pub tablet_write_log_buffer_size: usize,
    #[serde(default = "default_be_txn_info_history_size")]
    pub be_txn_info_history_size: usize,
    #[serde(default = "default_connector_io_tasks_per_scan_operator")]
    pub connector_io_tasks_per_scan_operator: i32,
    #[serde(default = "default_io_coalesce_read_enable")]
    pub io_coalesce_read_enable: bool,
    #[serde(default = "default_io_coalesce_read_max_buffer_size")]
    pub io_coalesce_read_max_buffer_size: u64,
    #[serde(default = "default_io_coalesce_read_max_distance_size")]
    pub io_coalesce_read_max_distance_size: u64,
    #[serde(default = "default_io_coalesce_adaptive_lazy_active")]
    pub io_coalesce_adaptive_lazy_active: bool,
    #[serde(default = "default_pipeline_scan_thread_pool_queue_size")]
    pub pipeline_scan_thread_pool_queue_size: usize,
    #[serde(default = "default_pipeline_exec_thread_pool_thread_num")]
    pub pipeline_exec_thread_pool_thread_num: usize,
    #[serde(default = "default_data_runtime_worker_threads")]
    pub data_runtime_worker_threads: usize,
    #[serde(default = "default_data_runtime_max_blocking_threads")]
    pub data_runtime_max_blocking_threads: usize,
    #[serde(default = "default_spill_io_threads")]
    pub spill_io_threads: usize,
    #[serde(default = "default_spill_io_queue_size")]
    pub spill_io_queue_size: usize,
    #[serde(default = "default_scan_submit_fail_max")]
    pub scan_submit_fail_max: usize,
    #[serde(default = "default_scan_submit_fail_timeout_ms")]
    pub scan_submit_fail_timeout_ms: u64,
    #[serde(default = "default_profile_report_interval")]
    pub profile_report_interval: i64,
    #[serde(default = "default_table_schema_service_max_retries")]
    pub table_schema_service_max_retries: usize,
    #[serde(default = "default_table_schema_service_cache_capacity")]
    pub table_schema_service_cache_capacity: u64,
    #[serde(default)]
    pub runtime_filter_scan_wait_time_ms_override: Option<i64>,
    #[serde(default)]
    pub runtime_filter_wait_timeout_ms_override: Option<i64>,
    #[serde(default)]
    pub object_storage: ObjectStorageConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub path_rewrite: PathRewriteConfig,
    #[serde(default)]
    pub execution_services: ExecutionServicesConfig,
}

#[derive(Clone, Deserialize)]
pub struct ObjectStorageConfig {
    #[serde(default)]
    pub retry_max_times: Option<usize>,
    #[serde(default)]
    pub retry_min_delay_ms: Option<u64>,
    #[serde(default)]
    pub retry_max_delay_ms: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub io_timeout_ms: Option<u64>,
    #[serde(default = "default_object_storage_retry_log_summary_interval_ms")]
    pub retry_log_summary_interval_ms: u64,
    #[serde(default = "default_object_storage_retry_log_first_n")]
    pub retry_log_first_n: u32,
}

impl ObjectStorageConfig {
    /// The retry knobs this section contributes to filesystem resources.
    pub fn retry_settings(&self) -> novarocks_fs::ObjectStoreRetrySettings {
        novarocks_fs::ObjectStoreRetrySettings {
            retry_max_times: self.retry_max_times,
            retry_min_delay_ms: self.retry_min_delay_ms,
            retry_max_delay_ms: self.retry_max_delay_ms,
            timeout_ms: self.timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct SpillStorageConfig {
    #[serde(default = "default_spill_enable")]
    pub enable: bool,
    #[serde(default)]
    pub local_dirs: Vec<String>,
    #[serde(default = "default_spill_dir_max_bytes")]
    pub dir_max_bytes: u64,
    #[serde(default = "default_spill_block_size_bytes")]
    pub block_size_bytes: u64,
    #[serde(default = "default_spill_ipc_compression")]
    pub ipc_compression: String,
}

fn default_spill_enable() -> bool {
    true
}

fn default_spill_dir_max_bytes() -> u64 {
    0
}

fn default_spill_block_size_bytes() -> u64 {
    134_217_728
}

fn default_spill_ipc_compression() -> String {
    "lz4".to_string()
}

impl Default for SpillStorageConfig {
    fn default() -> Self {
        Self {
            enable: default_spill_enable(),
            local_dirs: Vec::new(),
            dir_max_bytes: default_spill_dir_max_bytes(),
            block_size_bytes: default_spill_block_size_bytes(),
            ipc_compression: default_spill_ipc_compression(),
        }
    }
}

fn default_exchange_wait_ms() -> u64 {
    120_000
}

fn default_exchange_max_transmit_batched_bytes() -> usize {
    262_144 // 256KB, aligned with StarRocks `max_transmit_batched_bytes`
}

fn default_exchange_io_threads() -> usize {
    4
}

fn default_exchange_io_max_inflight_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_query_control_heartbeat_interval_ms() -> u64 {
    1_000
}

fn default_query_control_heartbeat_timeout_ms() -> u64 {
    5_000
}

fn default_query_control_init_rpc_timeout_ms() -> u64 {
    5_000
}

fn default_query_control_attach_timeout_ms() -> u64 {
    5_000
}

fn default_query_control_stage_rpc_timeout_ms() -> u64 {
    5_000
}

fn default_query_control_start_rpc_timeout_ms() -> u64 {
    2_000
}

fn default_query_control_pre_start_timeout_ms() -> u64 {
    30_000
}

fn default_query_control_tombstone_retention_ms() -> u64 {
    120_000
}

fn default_query_control_tombstone_capacity() -> usize {
    16_384
}

fn default_query_control_terminal_drain_timeout_ms() -> u64 {
    30_000
}
fn default_query_control_terminal_ack_timeout_ms() -> u64 {
    5_000
}
fn default_query_control_terminal_fallback_rpc_timeout_ms() -> u64 {
    5_000
}
fn default_query_control_terminal_fallback_max_attempts() -> usize {
    5
}
fn default_query_control_terminal_fallback_initial_backoff_ms() -> u64 {
    100
}
fn default_query_control_terminal_fallback_max_backoff_ms() -> u64 {
    1_000
}
fn default_query_control_terminal_max_encoded_bytes() -> usize {
    48 * 1024 * 1024
}
fn default_query_control_terminal_max_retained_bytes() -> usize {
    256 * 1024 * 1024
}
fn default_query_control_terminal_retained_capacity() -> usize {
    4_096
}
fn default_query_control_terminal_retention_ms() -> u64 {
    120_000
}

fn default_query_control_max_active_entries() -> usize {
    4_096
}

fn default_query_control_stage_max_encoded_bytes() -> usize {
    48 * 1024 * 1024
}

fn default_query_control_stage_max_fragments() -> usize {
    256
}

fn default_query_control_max_active_staging() -> usize {
    32
}

fn default_query_control_stage_max_inflight_encoded_bytes() -> usize {
    256 * 1024 * 1024
}

fn default_query_control_stage_max_dormant_workers() -> usize {
    512
}

fn validate_query_control_config(runtime: &RuntimeConfig) -> Result<()> {
    let nonzero_durations = [
        (
            "runtime.query_control_heartbeat_interval_ms",
            runtime.query_control_heartbeat_interval_ms,
        ),
        (
            "runtime.query_control_heartbeat_timeout_ms",
            runtime.query_control_heartbeat_timeout_ms,
        ),
        (
            "runtime.query_control_init_rpc_timeout_ms",
            runtime.query_control_init_rpc_timeout_ms,
        ),
        (
            "runtime.query_control_attach_timeout_ms",
            runtime.query_control_attach_timeout_ms,
        ),
        (
            "runtime.query_control_stage_rpc_timeout_ms",
            runtime.query_control_stage_rpc_timeout_ms,
        ),
        (
            "runtime.query_control_start_rpc_timeout_ms",
            runtime.query_control_start_rpc_timeout_ms,
        ),
        (
            "runtime.query_control_pre_start_timeout_ms",
            runtime.query_control_pre_start_timeout_ms,
        ),
        (
            "runtime.query_control_tombstone_retention_ms",
            runtime.query_control_tombstone_retention_ms,
        ),
        (
            "runtime.query_control_terminal_drain_timeout_ms",
            runtime.query_control_terminal_drain_timeout_ms,
        ),
        (
            "runtime.query_control_terminal_ack_timeout_ms",
            runtime.query_control_terminal_ack_timeout_ms,
        ),
        (
            "runtime.query_control_terminal_fallback_rpc_timeout_ms",
            runtime.query_control_terminal_fallback_rpc_timeout_ms,
        ),
        (
            "runtime.query_control_terminal_fallback_initial_backoff_ms",
            runtime.query_control_terminal_fallback_initial_backoff_ms,
        ),
        (
            "runtime.query_control_terminal_fallback_max_backoff_ms",
            runtime.query_control_terminal_fallback_max_backoff_ms,
        ),
    ];
    for (field, value) in nonzero_durations {
        if value == 0 {
            bail!("{field} must be greater than 0");
        }
    }
    if runtime.query_control_tombstone_capacity == 0 {
        bail!("runtime.query_control_tombstone_capacity must be greater than 0");
    }
    if runtime.query_control_max_active_entries == 0 {
        bail!("runtime.query_control_max_active_entries must be greater than 0");
    }
    let terminal_limits = [
        (
            "runtime.query_control_terminal_fallback_max_attempts",
            runtime.query_control_terminal_fallback_max_attempts,
        ),
        (
            "runtime.query_control_terminal_max_encoded_bytes",
            runtime.query_control_terminal_max_encoded_bytes,
        ),
        (
            "runtime.query_control_terminal_max_retained_bytes",
            runtime.query_control_terminal_max_retained_bytes,
        ),
        (
            "runtime.query_control_terminal_retained_capacity",
            runtime.query_control_terminal_retained_capacity,
        ),
    ];
    for (field, value) in terminal_limits {
        if value == 0 {
            bail!("{field} must be greater than 0");
        }
    }
    if runtime.query_control_terminal_fallback_initial_backoff_ms
        > runtime.query_control_terminal_fallback_max_backoff_ms
    {
        bail!(
            "runtime.query_control_terminal_fallback_initial_backoff_ms must not exceed runtime.query_control_terminal_fallback_max_backoff_ms"
        );
    }
    let nonzero_limits = [
        (
            "runtime.query_control_stage_max_encoded_bytes",
            runtime.query_control_stage_max_encoded_bytes,
        ),
        (
            "runtime.query_control_stage_max_fragments",
            runtime.query_control_stage_max_fragments,
        ),
        (
            "runtime.query_control_max_active_staging",
            runtime.query_control_max_active_staging,
        ),
        (
            "runtime.query_control_stage_max_inflight_encoded_bytes",
            runtime.query_control_stage_max_inflight_encoded_bytes,
        ),
        (
            "runtime.query_control_stage_max_dormant_workers",
            runtime.query_control_stage_max_dormant_workers,
        ),
        (
            "runtime.write_commit_evidence_max_bytes",
            runtime.write_commit_evidence_max_bytes,
        ),
        (
            "runtime.write_commit_evidence_max_entries",
            runtime.write_commit_evidence_max_entries,
        ),
    ];
    for (field, value) in nonzero_limits {
        if value == 0 {
            bail!("{field} must be greater than 0");
        }
    }
    const TONIC_MAX_STAGE_REQUEST_BYTES: usize = 64 * 1024 * 1024;
    if runtime.query_control_stage_max_encoded_bytes >= TONIC_MAX_STAGE_REQUEST_BYTES {
        bail!(
            "runtime.query_control_stage_max_encoded_bytes must be smaller than the 64MiB gRPC limit"
        );
    }
    if runtime.query_control_stage_max_inflight_encoded_bytes
        < runtime.query_control_stage_max_encoded_bytes
    {
        bail!(
            "runtime.query_control_stage_max_inflight_encoded_bytes must be at least runtime.query_control_stage_max_encoded_bytes"
        );
    }
    if runtime.query_control_stage_max_dormant_workers < runtime.query_control_stage_max_fragments {
        bail!(
            "runtime.query_control_stage_max_dormant_workers must be at least runtime.query_control_stage_max_fragments"
        );
    }
    let minimum_timeout = runtime
        .query_control_heartbeat_interval_ms
        .checked_mul(3)
        .ok_or_else(|| {
            anyhow::anyhow!("runtime.query_control_heartbeat_interval_ms is too large to validate")
        })?;
    if runtime.query_control_heartbeat_timeout_ms < minimum_timeout {
        bail!(
            "runtime.query_control_heartbeat_timeout_ms must be at least 3 times runtime.query_control_heartbeat_interval_ms"
        );
    }
    Ok(())
}

fn default_mem_limit() -> String {
    DEFAULT_MEM_LIMIT_SPEC.to_string()
}

fn default_be_mem_limit_bytes() -> u64 {
    0
}

fn default_optimizer_query_mem_limit_bytes() -> u64 {
    2 * 1024 * 1024 * 1024
}

fn default_optimizer_effective_backend_count() -> u64 {
    0
}

fn default_local_exchange_buffer_mem_limit_per_driver() -> usize {
    128 * 1024 * 1024
}

fn default_local_exchange_max_buffered_rows() -> i64 {
    -1
}

fn default_operator_buffer_chunks() -> usize {
    8
}

fn default_olap_sink_write_buffer_size_bytes() -> usize {
    100 * 1024 * 1024 // 100MB, aligned with StarRocks `write_buffer_size`
}

fn default_olap_sink_max_tablet_write_chunk_bytes() -> usize {
    512 * 1024 * 1024 // 512MB, aligned with StarRocks `max_tablet_write_chunk_bytes`
}

fn default_write_commit_evidence_max_bytes() -> usize {
    novarocks_spi::connector::DEFAULT_WRITE_COMMIT_EVIDENCE_MAX_BYTES
}

fn default_write_commit_evidence_max_entries() -> usize {
    novarocks_spi::connector::DEFAULT_WRITE_COMMIT_EVIDENCE_MAX_ENTRIES
}

fn default_pipeline_exec_thread_pool_thread_num() -> usize {
    0 // 0 means use CPU cores
}

fn default_data_runtime_worker_threads() -> usize {
    0 // 0 means use CPU cores for global data runtime
}

fn default_data_runtime_max_blocking_threads() -> usize {
    64
}

fn default_spill_io_threads() -> usize {
    0 // 0 means use actual exec thread count
}

fn default_spill_io_queue_size() -> usize {
    1024
}

fn default_pipeline_scan_thread_pool_thread_num() -> usize {
    0 // 0 means use CPU cores, aligned with StarRocks pipeline_scan_thread_pool_thread_num
}

fn default_enable_tablet_write_log() -> bool {
    false // aligned with StarRocks enable_tablet_write_log
}

fn default_tablet_write_log_buffer_size() -> usize {
    100_000 // aligned with StarRocks tablet_write_log_buffer_size
}

fn default_be_txn_info_history_size() -> usize {
    20_000 // aligned with StarRocks txn_info_history_size
}

fn default_connector_io_tasks_per_scan_operator() -> i32 {
    16 // aligned with StarRocks BE config::connector_io_tasks_per_scan_operator
}

fn default_io_coalesce_read_enable() -> bool {
    true
}

fn default_io_coalesce_read_max_buffer_size() -> u64 {
    8 * 1024 * 1024 // aligned with StarRocks io_coalesce_read_max_buffer_size
}

fn default_io_coalesce_read_max_distance_size() -> u64 {
    1024 * 1024 // aligned with StarRocks io_coalesce_read_max_distance_size
}

fn default_io_coalesce_adaptive_lazy_active() -> bool {
    true // aligned with StarRocks io_coalesce_adaptive_lazy_active
}

fn default_pipeline_scan_thread_pool_queue_size() -> usize {
    102_400 // Aligned with StarRocks pipeline_scan_thread_pool_queue_size
}

fn default_scan_submit_fail_max() -> usize {
    128
}

fn default_scan_submit_fail_timeout_ms() -> u64 {
    2000
}

fn default_profile_report_interval() -> i64 {
    30
}

fn default_table_schema_service_max_retries() -> usize {
    3
}

fn default_table_schema_service_cache_capacity() -> u64 {
    4_096
}

fn default_object_storage_retry_log_summary_interval_ms() -> u64 {
    30_000
}

fn default_object_storage_retry_log_first_n() -> u32 {
    3
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            exchange_wait_ms: default_exchange_wait_ms(),
            exchange_max_transmit_batched_bytes: default_exchange_max_transmit_batched_bytes(),
            exchange_io_threads: default_exchange_io_threads(),
            exchange_io_max_inflight_bytes: default_exchange_io_max_inflight_bytes(),
            query_control_heartbeat_interval_ms: default_query_control_heartbeat_interval_ms(),
            query_control_heartbeat_timeout_ms: default_query_control_heartbeat_timeout_ms(),
            query_control_init_rpc_timeout_ms: default_query_control_init_rpc_timeout_ms(),
            query_control_attach_timeout_ms: default_query_control_attach_timeout_ms(),
            query_control_stage_rpc_timeout_ms: default_query_control_stage_rpc_timeout_ms(),
            query_control_start_rpc_timeout_ms: default_query_control_start_rpc_timeout_ms(),
            query_control_pre_start_timeout_ms: default_query_control_pre_start_timeout_ms(),
            query_control_tombstone_retention_ms: default_query_control_tombstone_retention_ms(),
            query_control_tombstone_capacity: default_query_control_tombstone_capacity(),
            query_control_terminal_drain_timeout_ms:
                default_query_control_terminal_drain_timeout_ms(),
            query_control_terminal_ack_timeout_ms: default_query_control_terminal_ack_timeout_ms(),
            query_control_terminal_fallback_rpc_timeout_ms:
                default_query_control_terminal_fallback_rpc_timeout_ms(),
            query_control_terminal_fallback_max_attempts:
                default_query_control_terminal_fallback_max_attempts(),
            query_control_terminal_fallback_initial_backoff_ms:
                default_query_control_terminal_fallback_initial_backoff_ms(),
            query_control_terminal_fallback_max_backoff_ms:
                default_query_control_terminal_fallback_max_backoff_ms(),
            query_control_terminal_max_encoded_bytes:
                default_query_control_terminal_max_encoded_bytes(),
            query_control_terminal_max_retained_bytes:
                default_query_control_terminal_max_retained_bytes(),
            query_control_terminal_retained_capacity:
                default_query_control_terminal_retained_capacity(),
            query_control_terminal_retention_ms: default_query_control_terminal_retention_ms(),
            query_control_max_active_entries: default_query_control_max_active_entries(),
            query_control_stage_max_encoded_bytes: default_query_control_stage_max_encoded_bytes(),
            query_control_stage_max_fragments: default_query_control_stage_max_fragments(),
            query_control_max_active_staging: default_query_control_max_active_staging(),
            query_control_stage_max_inflight_encoded_bytes:
                default_query_control_stage_max_inflight_encoded_bytes(),
            query_control_stage_max_dormant_workers:
                default_query_control_stage_max_dormant_workers(),
            write_commit_evidence_max_bytes: default_write_commit_evidence_max_bytes(),
            write_commit_evidence_max_entries: default_write_commit_evidence_max_entries(),
            mem_limit: default_mem_limit(),
            be_mem_limit_bytes: default_be_mem_limit_bytes(),
            optimizer_query_mem_limit_bytes: default_optimizer_query_mem_limit_bytes(),
            optimizer_effective_backend_count: default_optimizer_effective_backend_count(),
            local_exchange_buffer_mem_limit_per_driver:
                default_local_exchange_buffer_mem_limit_per_driver(),
            local_exchange_max_buffered_rows: default_local_exchange_max_buffered_rows(),
            operator_buffer_chunks: default_operator_buffer_chunks(),
            olap_sink_write_buffer_size_bytes: default_olap_sink_write_buffer_size_bytes(),
            olap_sink_max_tablet_write_chunk_bytes: default_olap_sink_max_tablet_write_chunk_bytes(
            ),
            pipeline_scan_thread_pool_thread_num: default_pipeline_scan_thread_pool_thread_num(),
            enable_tablet_write_log: default_enable_tablet_write_log(),
            tablet_write_log_buffer_size: default_tablet_write_log_buffer_size(),
            be_txn_info_history_size: default_be_txn_info_history_size(),
            connector_io_tasks_per_scan_operator: default_connector_io_tasks_per_scan_operator(),
            io_coalesce_read_enable: default_io_coalesce_read_enable(),
            io_coalesce_read_max_buffer_size: default_io_coalesce_read_max_buffer_size(),
            io_coalesce_read_max_distance_size: default_io_coalesce_read_max_distance_size(),
            io_coalesce_adaptive_lazy_active: default_io_coalesce_adaptive_lazy_active(),
            pipeline_scan_thread_pool_queue_size: default_pipeline_scan_thread_pool_queue_size(),
            pipeline_exec_thread_pool_thread_num: default_pipeline_exec_thread_pool_thread_num(),
            data_runtime_worker_threads: default_data_runtime_worker_threads(),
            data_runtime_max_blocking_threads: default_data_runtime_max_blocking_threads(),
            spill_io_threads: default_spill_io_threads(),
            spill_io_queue_size: default_spill_io_queue_size(),
            scan_submit_fail_max: default_scan_submit_fail_max(),
            scan_submit_fail_timeout_ms: default_scan_submit_fail_timeout_ms(),
            profile_report_interval: default_profile_report_interval(),
            table_schema_service_max_retries: default_table_schema_service_max_retries(),
            table_schema_service_cache_capacity: default_table_schema_service_cache_capacity(),
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            object_storage: ObjectStorageConfig::default(),
            cache: CacheConfig::default(),
            path_rewrite: PathRewriteConfig::default(),
            execution_services: ExecutionServicesConfig::default(),
        }
    }
}

impl Default for ObjectStorageConfig {
    fn default() -> Self {
        Self {
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
            retry_log_summary_interval_ms: default_object_storage_retry_log_summary_interval_ms(),
            retry_log_first_n: default_object_storage_retry_log_first_n(),
        }
    }
}

#[derive(Clone, Default, Deserialize)]
pub struct PathRewriteConfig {
    #[serde(default)]
    pub enable: bool,
    #[serde(default)]
    pub from_prefix: String,
    #[serde(default)]
    pub to_prefix: String,
}

/// Execution-service resource boundaries (IW-1).
///
/// These knobs size the dedicated `sink_io` runtime and the async-sink queue.
/// Defaults add only a few (mostly idle) threads and do not change all-in-one
/// behavior. `metadata_io` / `commit` / `scan_io` currently alias `data_runtime`
/// and therefore have no size knobs yet.
#[derive(Clone, Deserialize)]
pub struct ExecutionServicesConfig {
    /// Worker threads for the dedicated sink I/O runtime. 0 = min(4, cores).
    #[serde(default = "default_sink_io_worker_threads")]
    pub sink_io_worker_threads: usize,
    /// Max blocking threads for the dedicated sink I/O runtime.
    #[serde(default = "default_sink_io_max_blocking_threads")]
    pub sink_io_max_blocking_threads: usize,
    /// Bounded queue capacity (chunks) for `AsyncSinkOperator` backpressure.
    #[serde(default = "default_async_sink_queue_capacity")]
    pub async_sink_queue_capacity: usize,
}

fn default_sink_io_worker_threads() -> usize {
    0
}

fn default_sink_io_max_blocking_threads() -> usize {
    16
}

fn default_async_sink_queue_capacity() -> usize {
    8
}

impl Default for ExecutionServicesConfig {
    fn default() -> Self {
        Self {
            sink_io_worker_threads: default_sink_io_worker_threads(),
            sink_io_max_blocking_threads: default_sink_io_max_blocking_threads(),
            async_sink_queue_capacity: default_async_sink_queue_capacity(),
        }
    }
}

impl ExecutionServicesConfig {
    /// Resolve sink I/O worker threads; 0 means min(4, cores).
    pub fn actual_sink_io_worker_threads(&self) -> usize {
        if self.sink_io_worker_threads > 0 {
            self.sink_io_worker_threads
        } else {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            cores.clamp(1, 4)
        }
    }
}

impl RuntimeConfig {
    pub fn effective_be_mem_limit_bytes(&self) -> Result<u64> {
        if self.be_mem_limit_bytes > 0 {
            return Ok(self.be_mem_limit_bytes);
        }

        novarocks::common::memory_limit::resolve_starrocks_process_mem_limit_bytes(&self.mem_limit)
            .with_context(|| format!("resolve runtime.mem_limit '{}'", self.mem_limit))
    }

    pub fn effective_be_mem_limit_bytes_for_visible_memory(
        &self,
        visible_memory_bytes: u64,
    ) -> Result<u64> {
        if self.be_mem_limit_bytes > 0 {
            return Ok(self.be_mem_limit_bytes);
        }

        novarocks::common::memory_limit::resolve_starrocks_process_mem_limit_bytes_for_visible_memory(
            &self.mem_limit,
            visible_memory_bytes,
        )
        .with_context(|| format!("resolve runtime.mem_limit '{}'", self.mem_limit))
    }

    /// Get the actual number of executor threads.
    /// Returns CPU cores if configured as 0.
    pub fn actual_exec_threads(&self) -> usize {
        if self.pipeline_exec_thread_pool_thread_num > 0 {
            self.pipeline_exec_thread_pool_thread_num
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        }
    }

    /// Get the actual number of scan threads.
    /// Returns CPU cores if configured as 0.
    pub fn actual_scan_threads(&self) -> usize {
        if self.pipeline_scan_thread_pool_thread_num > 0 {
            self.pipeline_scan_thread_pool_thread_num
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        }
    }

    /// Get the actual number of data-runtime worker threads.
    /// Returns CPU cores if configured as 0.
    pub fn actual_data_runtime_threads(&self) -> usize {
        if self.data_runtime_worker_threads > 0 {
            self.data_runtime_worker_threads
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_page_cache_enable")]
    pub page_cache_enable: bool,
    #[serde(default = "default_page_cache_capacity")]
    pub page_cache_capacity: usize,
    #[serde(default = "default_page_cache_evict_probability")]
    pub page_cache_evict_probability: u32,
    #[serde(default = "default_parquet_meta_cache_enable")]
    pub parquet_meta_cache_enable: bool,
    #[serde(default = "default_parquet_meta_cache_capacity")]
    pub parquet_meta_cache_capacity: usize,
    #[serde(default = "default_parquet_meta_cache_ttl_seconds")]
    pub parquet_meta_cache_ttl_seconds: u64,
    #[serde(default = "default_parquet_page_cache_enable")]
    pub parquet_page_cache_enable: bool,
    #[serde(default = "default_parquet_page_cache_capacity")]
    pub parquet_page_cache_capacity: usize,
    #[serde(default = "default_parquet_page_cache_ttl_seconds")]
    pub parquet_page_cache_ttl_seconds: u64,
    #[serde(default = "default_parquet_page_cache_decompress_threshold")]
    pub parquet_page_cache_decompress_threshold: f64,
    #[serde(default = "default_datacache_enable")]
    pub datacache_enable: bool,
    #[serde(default = "default_datacache_disk_path")]
    pub datacache_disk_path: String,
    #[serde(default = "default_datacache_disk_size")]
    pub datacache_disk_size: u64,
    #[serde(default = "default_datacache_block_size")]
    pub datacache_block_size: u64,
    #[serde(default = "default_datacache_checksum_enable")]
    pub datacache_checksum_enable: bool,
    #[serde(default = "default_datacache_direct_io_enable")]
    pub datacache_direct_io_enable: bool,
    #[serde(default = "default_datacache_io_align_unit_size")]
    pub datacache_io_align_unit_size: u64,
}

fn default_parquet_meta_cache_enable() -> bool {
    true
}

fn default_page_cache_enable() -> bool {
    true
}

fn default_page_cache_capacity() -> usize {
    11_000
}

fn default_page_cache_evict_probability() -> u32 {
    100
}

fn default_parquet_meta_cache_capacity() -> usize {
    1000 // Cache up to 1000 file metadata entries
}

fn default_parquet_meta_cache_ttl_seconds() -> u64 {
    3600 // 1 hour TTL
}

fn default_parquet_page_cache_enable() -> bool {
    true
}

fn default_parquet_page_cache_capacity() -> usize {
    10000 // Cache up to 10000 page entries (each entry can be several KB to MB)
}

fn default_parquet_page_cache_ttl_seconds() -> u64 {
    1800 // 30 minutes TTL (shorter than metadata cache)
}

fn default_parquet_page_cache_decompress_threshold() -> f64 {
    2.0 // Cache decompressed data if uncompressed_size <= 2.0 * compressed_size
}

fn default_datacache_enable() -> bool {
    false
}

fn default_datacache_disk_path() -> String {
    "/tmp/novarocks_datacache".to_string()
}

fn default_datacache_disk_size() -> u64 {
    0
}

fn default_datacache_block_size() -> u64 {
    1024 * 1024
}

fn default_datacache_checksum_enable() -> bool {
    true
}

fn default_datacache_direct_io_enable() -> bool {
    false
}

fn default_datacache_io_align_unit_size() -> u64 {
    4096
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            page_cache_enable: default_page_cache_enable(),
            page_cache_capacity: default_page_cache_capacity(),
            page_cache_evict_probability: default_page_cache_evict_probability(),
            parquet_meta_cache_enable: default_parquet_meta_cache_enable(),
            parquet_meta_cache_capacity: default_parquet_meta_cache_capacity(),
            parquet_meta_cache_ttl_seconds: default_parquet_meta_cache_ttl_seconds(),
            parquet_page_cache_enable: default_parquet_page_cache_enable(),
            parquet_page_cache_capacity: default_parquet_page_cache_capacity(),
            parquet_page_cache_ttl_seconds: default_parquet_page_cache_ttl_seconds(),
            parquet_page_cache_decompress_threshold:
                default_parquet_page_cache_decompress_threshold(),
            datacache_enable: default_datacache_enable(),
            datacache_disk_path: default_datacache_disk_path(),
            datacache_disk_size: default_datacache_disk_size(),
            datacache_block_size: default_datacache_block_size(),
            datacache_checksum_enable: default_datacache_checksum_enable(),
            datacache_direct_io_enable: default_datacache_direct_io_enable(),
            datacache_io_align_unit_size: default_datacache_io_align_unit_size(),
        }
    }
}

#[cfg(test)]
mod tests {
    use novarocks_state_store::config::StateStoreProviderConfig;

    use super::{
        DEFAULT_MEM_LIMIT_SPEC, NovaRocksConfig, RuntimeConfig, StandaloneServerConfig,
        validate_query_control_config,
    };

    #[test]
    fn query_control_config_defaults_are_fixed() {
        let runtime = RuntimeConfig::default();

        assert_eq!(runtime.query_control_heartbeat_interval_ms, 1_000);
        assert_eq!(runtime.query_control_heartbeat_timeout_ms, 5_000);
        assert_eq!(runtime.query_control_init_rpc_timeout_ms, 5_000);
        assert_eq!(runtime.query_control_attach_timeout_ms, 5_000);
        assert_eq!(runtime.query_control_stage_rpc_timeout_ms, 5_000);
        assert_eq!(runtime.query_control_start_rpc_timeout_ms, 2_000);
        assert_eq!(runtime.query_control_pre_start_timeout_ms, 30_000);
        assert_eq!(runtime.query_control_tombstone_retention_ms, 120_000);
        assert_eq!(runtime.query_control_tombstone_capacity, 16_384);
        assert_eq!(runtime.query_control_terminal_drain_timeout_ms, 30_000);
        assert_eq!(runtime.query_control_terminal_ack_timeout_ms, 5_000);
        assert_eq!(
            runtime.query_control_terminal_fallback_rpc_timeout_ms,
            5_000
        );
        assert_eq!(runtime.query_control_terminal_fallback_max_attempts, 5);
        assert_eq!(
            runtime.query_control_terminal_fallback_initial_backoff_ms,
            100
        );
        assert_eq!(
            runtime.query_control_terminal_fallback_max_backoff_ms,
            1_000
        );
        assert_eq!(
            runtime.query_control_terminal_max_encoded_bytes,
            48 * 1024 * 1024
        );
        assert_eq!(
            runtime.query_control_terminal_max_retained_bytes,
            256 * 1024 * 1024
        );
        assert_eq!(runtime.query_control_terminal_retained_capacity, 4_096);
        assert_eq!(runtime.query_control_terminal_retention_ms, 120_000);
        assert_eq!(runtime.query_control_max_active_entries, 4_096);
        assert_eq!(
            runtime.query_control_stage_max_encoded_bytes,
            48 * 1024 * 1024
        );
        assert_eq!(runtime.query_control_stage_max_fragments, 256);
        assert_eq!(runtime.query_control_max_active_staging, 32);
        assert_eq!(
            runtime.query_control_stage_max_inflight_encoded_bytes,
            256 * 1024 * 1024
        );
        assert_eq!(runtime.query_control_stage_max_dormant_workers, 512);
    }

    #[test]
    fn query_control_config_rejects_zero_values() {
        let cases: [(&str, fn(&mut RuntimeConfig)); 8] = [
            ("query_control_heartbeat_interval_ms", |runtime| {
                runtime.query_control_heartbeat_interval_ms = 0;
            }),
            ("query_control_heartbeat_timeout_ms", |runtime| {
                runtime.query_control_heartbeat_timeout_ms = 0;
            }),
            ("query_control_init_rpc_timeout_ms", |runtime| {
                runtime.query_control_init_rpc_timeout_ms = 0;
            }),
            ("query_control_attach_timeout_ms", |runtime| {
                runtime.query_control_attach_timeout_ms = 0;
            }),
            ("query_control_pre_start_timeout_ms", |runtime| {
                runtime.query_control_pre_start_timeout_ms = 0;
            }),
            ("query_control_tombstone_retention_ms", |runtime| {
                runtime.query_control_tombstone_retention_ms = 0;
            }),
            ("query_control_tombstone_capacity", |runtime| {
                runtime.query_control_tombstone_capacity = 0;
            }),
            ("query_control_max_active_entries", |runtime| {
                runtime.query_control_max_active_entries = 0;
            }),
        ];

        for (field, mutate) in cases {
            let mut runtime = RuntimeConfig::default();
            mutate(&mut runtime);
            let error = validate_query_control_config(&runtime)
                .expect_err("zero query-control values must be rejected");
            assert!(
                error.to_string().contains(field),
                "error must identify {field}: {error}"
            );
        }
    }

    #[test]
    fn query_control_config_rejects_short_heartbeat_timeout() {
        let mut runtime = RuntimeConfig::default();
        runtime.query_control_heartbeat_interval_ms = 1_000;
        runtime.query_control_heartbeat_timeout_ms = 2_999;

        let error = validate_query_control_config(&runtime)
            .expect_err("heartbeat timeout must cover at least three intervals");
        assert!(
            error
                .to_string()
                .contains("query_control_heartbeat_timeout_ms")
        );
    }

    #[test]
    fn query_control_config_load_rejects_invalid_capacity() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(
            temp.path(),
            r#"
[runtime]
query_control_max_active_entries = 0
"#,
        )?;

        let error = match NovaRocksConfig::load_from_file(temp.path()) {
            Ok(_) => panic!("load must validate query-control capacity"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("query_control_max_active_entries")
        );
        Ok(())
    }

    #[test]
    fn state_store_config_loads_explicit_sqlite_provider() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(
            temp.path(),
            r#"
[state_store]
provider = "sqlite"
path = "meta/state-store.sqlite"
cluster_id = "cluster-a"
deployment_owner = "fe-a"
"#,
        )?;

        let cfg = NovaRocksConfig::load_from_file(temp.path())?;

        assert!(matches!(
            cfg.state_store.expect("state store config").store.provider,
            StateStoreProviderConfig::Sqlite { .. }
        ));
        Ok(())
    }

    #[test]
    fn state_store_config_requires_provider() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(
            temp.path(),
            r#"
[state_store]
path = "meta/state-store.sqlite"
cluster_id = "cluster-a"
deployment_owner = "fe-a"
"#,
        )?;

        let error = match NovaRocksConfig::load_from_file(temp.path()) {
            Ok(_) => panic!("state_store.provider must be explicit"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("parse toml"));
        Ok(())
    }

    #[test]
    fn state_store_config_rejects_cross_provider_fields() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(
            temp.path(),
            r#"
[state_store]
provider = "foundationdb"
path = "meta/state-store.sqlite"
cluster_id = "cluster-a"
deployment_owner = "fe-a"
"#,
        )?;

        let error = match NovaRocksConfig::load_from_file(temp.path()) {
            Ok(_) => panic!("cross-provider fields must fail closed"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("parse toml"));
        Ok(())
    }

    #[test]
    fn state_store_config_rejects_relaxed_key_limit() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(
            temp.path(),
            r#"
[state_store]
provider = "sqlite"
path = "meta/state-store.sqlite"
cluster_id = "cluster-a"
deployment_owner = "fe-a"

[state_store.limits]
max_key_bytes = 8193
"#,
        )?;

        let error = match NovaRocksConfig::load_from_file(temp.path()) {
            Ok(_) => panic!("provider limits may only tighten the common contract"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("InvalidStateStoreConfig"));
        Ok(())
    }

    #[test]
    fn state_store_config_is_disabled_when_section_is_absent() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(temp.path(), "log_level = \"info\"\n")?;

        let cfg = NovaRocksConfig::load_from_file(temp.path())?;

        assert!(cfg.state_store.is_none());
        Ok(())
    }

    #[test]
    fn test_server_priority_networks_default_is_empty() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
http_port = 8040
"#,
        )
        .expect("parse config");
        assert!(cfg.server.priority_networks.is_empty());
    }

    #[test]
    fn test_server_host_default_is_loopback() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
http_port = 8040
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.server.host, "127.0.0.1");
    }

    #[test]
    fn test_server_priority_networks_can_be_overridden() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
http_port = 8040
priority_networks = "10.10.10.0/24;192.168.0.0/16"
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.server.priority_networks, "10.10.10.0/24;192.168.0.0/16");
    }

    #[test]
    fn test_server_grpc_port_default_is_9080() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
http_port = 8040
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.server.grpc_port, 9080);
    }

    #[test]
    fn test_server_grpc_port_can_be_overridden() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
grpc_port = 19080
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.server.grpc_port, 19080);
    }

    #[test]
    fn test_standalone_server_defaults() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[standalone_server]
"#,
        )
        .expect("parse config");
        assert_eq!(
            cfg.standalone_server,
            Some(StandaloneServerConfig {
                mysql_port: 9030,
                user: "root".to_string(),
                mv_refresh_scheduler_enabled: false,
                mv_refresh_scheduler_interval_ms: 30_000,
                mv_refresh_scheduler_max_concurrent: 1,
                mv_refresh_scheduler_failure_backoff_ms: 60_000,
                mv_refresh_scheduler_max_failure_backoff_ms: 1_800_000,
                mv_refresh_max_touched_groups: 100_000,
                mv_refresh_max_affected_partitions: 4_096,
                mv_partition_state_max_entries: 10_000,
                iceberg_maintenance_enabled: true,
                iceberg_maintenance_tick_interval_ms: 600_000,
                iceberg_maintenance_max_concurrent: 1,
                iceberg_maintenance_compaction_min_data_files: 100,
                iceberg_maintenance_dv_min_delete_files: 10,
                iceberg_maintenance_action_cooldown_ms: 3_600_000,
                iceberg_maintenance_max_consecutive_failures: 4,
            })
        );
    }

    #[test]
    fn standalone_server_config_iceberg_maintenance_defaults() {
        let cfg: StandaloneServerConfig =
            toml::from_str("").expect("empty standalone_server section parses");
        assert!(cfg.iceberg_maintenance_enabled);
        assert_eq!(cfg.iceberg_maintenance_tick_interval_ms, 600_000);
        assert_eq!(cfg.iceberg_maintenance_max_concurrent, 1);
        assert_eq!(cfg.iceberg_maintenance_compaction_min_data_files, 100);
        assert_eq!(cfg.iceberg_maintenance_dv_min_delete_files, 10);
        assert_eq!(cfg.iceberg_maintenance_action_cooldown_ms, 3_600_000);
        assert_eq!(cfg.iceberg_maintenance_max_consecutive_failures, 4);
        assert_eq!(cfg.mv_refresh_max_touched_groups, 100_000);
        assert_eq!(cfg.mv_refresh_max_affected_partitions, 4_096);
        assert_eq!(cfg.mv_partition_state_max_entries, 10_000);
        assert_eq!(cfg, StandaloneServerConfig::default());
    }

    #[test]
    fn standalone_server_config_mv_refresh_pruning_overrides() {
        let cfg: StandaloneServerConfig = toml::from_str(
            r#"
mv_refresh_max_touched_groups = 7
mv_refresh_max_affected_partitions = 3
mv_partition_state_max_entries = 11
"#,
        )
        .expect("standalone_server pruning thresholds parse");

        assert_eq!(cfg.mv_refresh_max_touched_groups, 7);
        assert_eq!(cfg.mv_refresh_max_affected_partitions, 3);
        assert_eq!(cfg.mv_partition_state_max_entries, 11);
    }

    #[test]
    fn test_runtime_olap_sink_threshold_defaults() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.olap_sink_write_buffer_size_bytes, 104_857_600);
        assert_eq!(
            cfg.runtime.olap_sink_max_tablet_write_chunk_bytes,
            536_870_912
        );
    }

    #[test]
    fn test_runtime_olap_sink_threshold_can_be_overridden() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
olap_sink_write_buffer_size_bytes = 33554432
olap_sink_max_tablet_write_chunk_bytes = 67108864
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.olap_sink_write_buffer_size_bytes, 33_554_432);
        assert_eq!(
            cfg.runtime.olap_sink_max_tablet_write_chunk_bytes,
            67_108_864
        );
    }

    #[test]
    fn test_runtime_data_runtime_defaults() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.data_runtime_worker_threads, 0);
        assert_eq!(cfg.runtime.data_runtime_max_blocking_threads, 64);
    }

    #[test]
    fn test_runtime_data_runtime_can_be_overridden() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
data_runtime_worker_threads = 6
data_runtime_max_blocking_threads = 99
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.data_runtime_worker_threads, 6);
        assert_eq!(cfg.runtime.data_runtime_max_blocking_threads, 99);
    }

    #[test]
    fn test_actual_data_runtime_threads_behavior() {
        let mut runtime = RuntimeConfig {
            data_runtime_worker_threads: 0,
            ..Default::default()
        };
        let expected = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(runtime.actual_data_runtime_threads(), expected);
        runtime.data_runtime_worker_threads = 3;
        assert_eq!(runtime.actual_data_runtime_threads(), 3);
    }

    #[test]
    fn test_runtime_mem_limit_defaults_to_starrocks_spec() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.mem_limit, DEFAULT_MEM_LIMIT_SPEC);
        assert_eq!(cfg.runtime.be_mem_limit_bytes, 0);
    }

    #[test]
    fn test_runtime_be_mem_limit_bytes_override_wins() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
mem_limit = "10%"
be_mem_limit_bytes = 34359738368
"#,
        )
        .expect("parse config");
        assert_eq!(
            cfg.runtime
                .effective_be_mem_limit_bytes_for_visible_memory(128 * 1024 * 1024 * 1024)
                .expect("resolve mem limit"),
            32 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_runtime_mem_limit_derives_starrocks_soft_limit_from_percentage() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
mem_limit = "90%"
"#,
        )
        .expect("parse config");
        assert_eq!(
            cfg.runtime
                .effective_be_mem_limit_bytes_for_visible_memory(1000)
                .expect("resolve mem limit"),
            810
        );
    }

    #[test]
    fn test_runtime_mem_limit_derives_starrocks_soft_limit_from_units_and_clamps() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
mem_limit = "200G"
"#,
        )
        .expect("parse config");
        assert_eq!(
            cfg.runtime
                .effective_be_mem_limit_bytes_for_visible_memory(100 * 1024 * 1024 * 1024)
                .expect("resolve mem limit"),
            100 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_runtime_mem_limit_rejects_zero_effective_limit() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
mem_limit = "0"
"#,
        )
        .expect("parse config");
        assert!(
            cfg.runtime
                .effective_be_mem_limit_bytes_for_visible_memory(100 * 1024 * 1024 * 1024)
                .is_err()
        );
    }

    #[test]
    fn test_cluster_default_is_all_in_one() {
        let toml = r#"
[server]
host = "127.0.0.1"
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse default");
        assert_eq!(cfg.cluster.role, super::ClusterRole::AllInOne);
        assert!(cfg.cluster.backends.is_empty());
    }

    #[test]
    fn test_cluster_role_fe_with_single_backend() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["127.0.0.1:9070"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse fe");
        assert_eq!(cfg.cluster.role, super::ClusterRole::Fe);
        assert_eq!(cfg.cluster.backends, vec!["127.0.0.1:9070".to_string()]);
    }

    #[test]
    fn test_cluster_role_be_rejects_backends() {
        let toml = r#"
[cluster]
role = "be"
backends = ["127.0.0.1:9070"]
"#;
        let parsed: NovaRocksConfig = toml::from_str(toml).expect("parse be with backends");
        let err = parsed
            .cluster
            .validate()
            .expect_err("be with backends should fail");
        assert!(err.contains("backends"));
    }

    #[test]
    fn test_cluster_role_fe_with_three_backends_passes() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["10.0.0.1:9070", "10.0.0.2:9070", "10.0.0.3:9070"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse fe with 3 backends");
        cfg.cluster
            .validate()
            .expect("3 backends should pass D2 validate");
    }

    #[test]
    fn test_cluster_role_fe_rejects_duplicate_backends() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["10.0.0.1:9070", "10.0.0.1:9070"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
        let err = cfg
            .cluster
            .validate()
            .expect_err("duplicate backends should fail");
        assert!(err.contains("duplicate") || err.contains("10.0.0.1:9070"));
    }

    #[test]
    fn test_cluster_role_fe_rejects_malformed_backend() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["not-a-socket-addr"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
        let err = cfg
            .cluster
            .validate()
            .expect_err("malformed addr should fail");
        assert!(err.contains("not-a-socket-addr") || err.contains("invalid"));
    }

    #[test]
    fn test_cluster_role_fe_empty_backends_allowed() {
        let toml = r#"
[cluster]
role = "fe"
backends = []
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
        cfg.cluster
            .validate()
            .expect("role=fe may start with no configured backends");
    }

    #[test]
    fn test_cluster_role_invalid_rejected() {
        let toml = r#"
[cluster]
role = "leader"
"#;
        let result: Result<NovaRocksConfig, _> = toml::from_str(toml);
        assert!(result.is_err(), "invalid role string should fail parse");
    }

    #[test]
    fn test_all_in_one_rejects_non_empty_backends() {
        // I2: role=all-in-one with non-empty backends must be rejected.
        let toml = r#"
[cluster]
role = "all-in-one"
backends = ["127.0.0.1:9070"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse all-in-one with backends");
        let err = cfg
            .cluster
            .validate()
            .expect_err("all-in-one with backends should fail");
        assert!(
            err.contains("all-in-one") && err.contains("backends"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_all_in_one_with_no_backends_passes_validation() {
        // Default all-in-one with no backends must still pass.
        let toml = r#"
[cluster]
role = "all-in-one"
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse all-in-one");
        cfg.cluster
            .validate()
            .expect("all-in-one with no backends should pass");
    }

    #[test]
    fn test_all_in_one_rejects_multiple_backends() {
        // I2: multiple backends should also be rejected for all-in-one.
        let toml = r#"
[cluster]
role = "all-in-one"
backends = ["127.0.0.1:9070", "127.0.0.1:9071"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
        let err = cfg
            .cluster
            .validate()
            .expect_err("all-in-one with 2 backends should fail");
        assert!(
            err.contains("all-in-one") && err.contains("backends"),
            "unexpected error: {err}"
        );
        // Error message should contain the count.
        assert!(err.contains('2'), "expected count 2 in error: {err}");
    }

    #[test]
    fn execution_services_defaults_are_sane() {
        let cfg = RuntimeConfig::default();
        assert_eq!(cfg.execution_services.sink_io_max_blocking_threads, 16);
        assert_eq!(cfg.execution_services.async_sink_queue_capacity, 8);
        // 0 means "derive from cores"; resolved value must be >= 1.
        assert!(cfg.execution_services.actual_sink_io_worker_threads() >= 1);
        assert!(cfg.execution_services.actual_sink_io_worker_threads() <= 4);
    }
}
