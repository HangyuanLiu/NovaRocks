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

use std::num::NonZeroUsize;
use std::path::PathBuf;

use bytes::Bytes;
use novarocks::common::app_config::NovaRocksConfig;
use novarocks_spi::state_store::StateStoreErrorKind;
use novarocks_state_store::{
    FeDeploymentView, MySqlTlsMode, StateStoreProviderConfig, StateStoreRuntime, open_state_store,
};
use uuid::Uuid;

#[test]
fn mysql_config_parses_exact_nested_client_shape() -> anyhow::Result<()> {
    let fixture_dir = tempfile::tempdir()?;
    let ca = fixture_dir.path().join("ca.pem");
    let cert = fixture_dir.path().join("client.pem");
    let key = fixture_dir.path().join("client-key.pem");
    for path in [&ca, &cert, &key] {
        std::fs::write(path, b"test fixture")?;
    }
    let config_path = fixture_dir.path().join("novarocks.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
[state_store]
provider = "mysql"
cluster_id = "production-cluster"
database = "novarocks_control_plane"

[state_store.mysql_client]
host = "mysql.internal.example"
port = 3306
username = "novarocks_state_store"
password_env = "NOVAROCKS_STATE_STORE_MYSQL_PASSWORD_UNSET"
tls_mode = "verify_identity"
tls_ca_path = "{}"
tls_cert_path = "{}"
tls_key_path = "{}"
connect_timeout_ms = 1000
pool_min = 1
pool_max = 16
inactive_connection_ttl_ms = 30000
"#,
            ca.display(),
            cert.display(),
            key.display()
        ),
    )?;

    let loaded = NovaRocksConfig::load_from_file(&config_path)?;
    let app = loaded.state_store.expect("state store config");
    assert!(matches!(
        app.store.provider,
        StateStoreProviderConfig::Mysql { ref database }
            if database == "novarocks_control_plane"
    ));
    let client = app.mysql_client.expect("nested MySQL client");
    assert_eq!(client.host, "mysql.internal.example");
    assert_eq!(client.port, 3306);
    assert_eq!(client.username, "novarocks_state_store");
    assert_eq!(
        client.password_env,
        "NOVAROCKS_STATE_STORE_MYSQL_PASSWORD_UNSET"
    );
    assert_eq!(client.tls_mode, MySqlTlsMode::VerifyIdentity);
    assert_eq!(client.tls_ca_path.as_deref(), Some(ca.as_path()));
    assert_eq!(client.tls_cert_path.as_deref(), Some(cert.as_path()));
    assert_eq!(client.tls_key_path.as_deref(), Some(key.as_path()));
    assert_eq!(client.connect_timeout_ms, 1_000);
    assert_eq!(client.pool_min, 1);
    assert_eq!(client.pool_max, 16);
    assert_eq!(client.inactive_connection_ttl_ms, 30_000);
    Ok(())
}

#[test]
fn mysql_config_rejects_cross_provider_and_unknown_fields() -> anyhow::Result<()> {
    let fixtures = [
        (
            "non-MySQL fields are not valid",
            r#"
[state_store]
provider = "mysql"
cluster_id = "cluster-a"
database = "novarocks_control_plane"
path = "meta/state-store.sqlite"

[state_store.mysql_client]
host = "mysql.internal.example"
port = 3306
username = "novarocks"
password_env = "NOVAROCKS_MYSQL_PASSWORD"
tls_mode = "required"
connect_timeout_ms = 1000
pool_min = 1
pool_max = 16
inactive_connection_ttl_ms = 30000
"#,
        ),
        (
            "[state_store.mysql_client] requires the mysql state store provider",
            r#"
[state_store]
provider = "sqlite"
cluster_id = "cluster-a"
path = "meta/state-store.sqlite"
deployment_owner = "fe-a"

[state_store.mysql_client]
host = "mysql.internal.example"
port = 3306
username = "novarocks"
password_env = "NOVAROCKS_MYSQL_PASSWORD"
tls_mode = "required"
connect_timeout_ms = 1000
pool_min = 1
pool_max = 16
inactive_connection_ttl_ms = 30000
"#,
        ),
        (
            "mysql provider requires [state_store.mysql_client]",
            r#"
[state_store]
provider = "mysql"
cluster_id = "cluster-a"
database = "novarocks_control_plane"
"#,
        ),
        (
            "unknown field",
            r#"
[state_store]
provider = "mysql"
cluster_id = "cluster-a"
database = "novarocks_control_plane"

[state_store.mysql_client]
host = "mysql.internal.example"
port = 3306
username = "novarocks"
password_env = "NOVAROCKS_MYSQL_PASSWORD"
tls_mode = "required"
connect_timeout_ms = 1000
pool_min = 1
pool_max = 16
inactive_connection_ttl_ms = 30000
dsn = "mysql://plaintext-secret"
"#,
        ),
        (
            "database",
            r#"
[state_store]
provider = "mysql"
cluster_id = "cluster-a"
database = "invalid-database-name"

[state_store.mysql_client]
host = "mysql.internal.example"
port = 3306
username = "novarocks"
password_env = "NOVAROCKS_MYSQL_PASSWORD"
tls_mode = "required"
connect_timeout_ms = 1000
pool_min = 1
pool_max = 16
inactive_connection_ttl_ms = 30000
"#,
        ),
    ];

    for (expected, fixture) in fixtures {
        let config_path = tempfile::NamedTempFile::new()?;
        std::fs::write(config_path.path(), fixture)?;
        let error = match NovaRocksConfig::load_from_file(config_path.path()) {
            Ok(_) => {
                panic!("cross-provider, missing-client, and unknown fields must fail closed")
            }
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(
            message.contains(expected),
            "expected {expected:?}, got {message:?} for fixture: {fixture}"
        );
    }
    Ok(())
}

#[test]
fn foundationdb_config_rejects_invalid_client_configuration() -> anyhow::Result<()> {
    let cluster_file = tempfile::NamedTempFile::new()?;
    let config_path = tempfile::NamedTempFile::new()?;
    let state_store = format!(
        r#"
[state_store]
provider = "foundationdb"
cluster_id = "cluster-a"
cluster_file = "{}"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"
"#,
        cluster_file.path().display()
    );

    for client in [
        r#"
[foundationdb_client]
disable_multi_version_client = false
"#,
        r#"
[foundationdb_client]
"#,
        r#"
[foundationdb_client]
disable_multi_version_client = true
tls_cert_path = "/tmp/client.crt"
"#,
        r#"
[foundationdb_client]
disable_multi_version_client = true
tls_password = "plaintext-secret"
"#,
    ] {
        std::fs::write(config_path.path(), format!("{state_store}{client}"))?;
        if NovaRocksConfig::load_from_file(config_path.path()).is_ok() {
            panic!("invalid FoundationDB client configuration must fail closed");
        }
    }
    Ok(())
}

#[test]
fn foundationdb_config_rejects_missing_or_orphaned_client_configuration() -> anyhow::Result<()> {
    let cluster_file = tempfile::NamedTempFile::new()?;
    let config_path = tempfile::NamedTempFile::new()?;
    let fixtures = [
        format!(
            r#"
[state_store]
provider = "foundationdb"
cluster_id = "cluster-a"
cluster_file = "{}"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"
"#,
            cluster_file.path().display()
        ),
        r#"
[state_store]
provider = "sqlite"
cluster_id = "cluster-a"
path = "meta/state-store.sqlite"
deployment_owner = "fe-a"

[foundationdb_client]
disable_multi_version_client = true
"#
        .to_owned(),
        r#"
[foundationdb_client]
disable_multi_version_client = true
"#
        .to_owned(),
    ];

    for fixture in fixtures {
        std::fs::write(config_path.path(), fixture)?;
        if NovaRocksConfig::load_from_file(config_path.path()).is_ok() {
            panic!("missing or orphaned FoundationDB client config must fail closed");
        }
    }
    Ok(())
}

#[test]
fn foundationdb_config_loads_complete_tls_client_configuration() -> anyhow::Result<()> {
    const FIXTURE_ENV: &str = "NOVAROCKS_TEST_FDB_TLS_CONFIG_FIXTURE";
    const PASSWORD_ENV: &str = "NOVAROCKS_TEST_FDB_TLS_PASSWORD";

    if let Some(config_path) = std::env::var_os(FIXTURE_ENV) {
        return assert_complete_tls_client_configuration(PathBuf::from(config_path), PASSWORD_ENV);
    }

    let fixture_dir = tempfile::tempdir()?;
    let cluster_file = fixture_dir.path().join("fdb.cluster");
    let tls_cert = fixture_dir.path().join("client.crt");
    let tls_key = fixture_dir.path().join("client.key");
    let tls_ca = fixture_dir.path().join("ca.crt");
    let config_path = fixture_dir.path().join("novarocks.toml");
    for path in [&cluster_file, &tls_cert, &tls_key, &tls_ca] {
        std::fs::write(path, b"")?;
    }
    let config_text = format!(
        r#"
[state_store]
provider = "foundationdb"
cluster_id = "cluster-a"
cluster_file = "{}"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"

[foundationdb_client]
disable_multi_version_client = true
tls_cert_path = "{}"
tls_key_path = "{}"
tls_ca_path = "{}"
tls_verify_peers = "Check.Valid=1"
tls_password_env = "{}"
"#,
        cluster_file.display(),
        tls_cert.display(),
        tls_key.display(),
        tls_ca.display(),
        PASSWORD_ENV,
    );
    std::fs::write(&config_path, config_text)?;

    let output = std::process::Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("foundationdb_config_loads_complete_tls_client_configuration")
        .arg("--nocapture")
        .env(FIXTURE_ENV, &config_path)
        .env(PASSWORD_ENV, "test-password")
        .output()?;
    assert!(
        output.status.success(),
        "isolated TLS config test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn assert_complete_tls_client_configuration(
    config_path: PathBuf,
    password_env: &str,
) -> anyhow::Result<()> {
    let fixture_dir = config_path.parent().expect("fixture directory");
    let cluster_file = fixture_dir.join("fdb.cluster");
    let tls_cert = fixture_dir.join("client.crt");
    let tls_key = fixture_dir.join("client.key");
    let tls_ca = fixture_dir.join("ca.crt");
    let loaded = NovaRocksConfig::load_from_file(&config_path)?;

    let state_store = loaded.state_store.expect("state store config");
    assert!(matches!(
        state_store.store.provider,
        StateStoreProviderConfig::Foundationdb { cluster_file: loaded, keyspace_id }
            if loaded == cluster_file
                && keyspace_id == Uuid::parse_str("22db595e-3031-48eb-8212-f56d3626ee41")?
    ));

    let client = loaded
        .foundationdb_client
        .expect("FoundationDB client config");
    assert_eq!(client.tls_cert_path.as_deref(), Some(tls_cert.as_path()));
    assert_eq!(client.tls_key_path.as_deref(), Some(tls_key.as_path()));
    assert_eq!(client.tls_ca_path.as_deref(), Some(tls_ca.as_path()));
    assert_eq!(client.tls_verify_peers.as_deref(), Some("Check.Valid=1"));
    assert_eq!(client.tls_password_env.as_deref(), Some(password_env));

    let debug = format!("{client:?}");
    for secret in [
        tls_cert.to_string_lossy().as_ref(),
        tls_key.to_string_lossy().as_ref(),
        tls_ca.to_string_lossy().as_ref(),
        "Check.Valid=1",
        password_env,
    ] {
        assert!(!debug.contains(secret), "Debug leaked {secret}: {debug}");
    }
    Ok(())
}

#[cfg(not(feature = "foundationdb-provider"))]
#[tokio::test]
async fn foundationdb_config_feature_off_open_fails_without_fallback() {
    let cluster_file = tempfile::NamedTempFile::new().expect("cluster file");
    let config_path = tempfile::NamedTempFile::new().expect("config file");
    std::fs::write(
        config_path.path(),
        format!(
            r#"
[state_store]
provider = "foundationdb"
cluster_id = "cluster-a"
cluster_file = "{}"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"

[foundationdb_client]
disable_multi_version_client = true
"#,
            cluster_file.path().display()
        ),
    )
    .expect("write config");
    let loaded = NovaRocksConfig::load_from_file(config_path.path())
        .expect("load FoundationDB config from TOML");
    let runtime = StateStoreRuntime::local().expect("create feature-off local runtime");
    let error = match open_state_store(
        &runtime,
        loaded.state_store.expect("state store config").store,
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(3).expect("three FEs"),
            topology_revision: Bytes::from_static(b"topology-r1"),
        },
    )
    .await
    {
        Ok(_) => panic!("feature-off FoundationDB open must fail without SQLite fallback"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    assert_eq!(
        error.to_string(),
        "InvalidConfiguration: FoundationDB provider is not compiled in"
    );
}
