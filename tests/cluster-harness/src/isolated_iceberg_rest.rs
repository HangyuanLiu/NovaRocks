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

//! An explicitly isolated Iceberg REST plus MinIO fixture for system scenarios.
//!
//! The ordinary `docker/iceberg-rest` environment intentionally shares Docker
//! services across worktrees.  Credential-lease scenarios need the opposite:
//! a unique compose project, volume, warehouse, runtime entry, and teardown.
//! This module is therefore test-only and deliberately drives the existing
//! fixture scripts with `NOVA_ENV_SHARED_DOCKER=false`; it never starts or
//! falls back to the shared project.

use anyhow::{Context, Result, bail, ensure};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const FIXTURE_PREFIX: &str = "cca1-vended-rest";
const MAX_DIAGNOSTIC_BYTES: usize = 8 * 1024;
const MINIO_STS_DURATION_SECONDS: u32 = 900;
static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(1);

/// The non-secret endpoint facts consumed by a vended REST scenario.
#[derive(Clone, Eq, PartialEq)]
pub struct IsolatedIcebergRestEndpoints {
    pub rest_uri: String,
    pub rest_warehouse: String,
    pub minio_endpoint: String,
    pub compose_project: String,
}

impl fmt::Debug for IsolatedIcebergRestEndpoints {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IsolatedIcebergRestEndpoints")
            .field("rest_uri", &self.rest_uri)
            .field("rest_warehouse", &self.rest_warehouse)
            .field("minio_endpoint", &self.minio_endpoint)
            .field("compose_project", &self.compose_project)
            .finish()
    }
}

/// One test-only S3 access-key identity provisioned by the isolated MinIO.
///
/// The secret is needed to construct a vended credential response, but its
/// `Debug` representation remains redacted so it cannot leak through scenario
/// diagnostics.
#[derive(Clone, Eq, PartialEq)]
pub struct IsolatedS3Identity {
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl fmt::Debug for IsolatedS3Identity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IsolatedS3Identity")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

/// One short-lived S3 identity issued by the fixture's own MinIO STS endpoint.
///
/// A vended Iceberg response carries all three AWS STS scalars, so a normal
/// MinIO access key plus an invented token is not a valid test substitute.
#[derive(Clone, Eq, PartialEq)]
pub struct IsolatedStsS3Identity {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub not_after_unix_ms: u64,
}

impl fmt::Debug for IsolatedStsS3Identity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IsolatedStsS3Identity")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("session_token", &"<redacted>")
            .field("not_after_unix_ms", &self.not_after_unix_ms)
            .finish()
    }
}

/// The two distinct usable identities required to verify initial credential
/// use and the subsequent refresh.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsolatedVendedS3Identities {
    pub initial: IsolatedStsS3Identity,
    pub rotated: IsolatedStsS3Identity,
}

/// A per-scenario REST Catalog and object-store environment.
///
/// The fixture owns exactly one generated workspace root below the supplied
/// scenario runtime root.  `shutdown` is idempotent and `Drop` makes a best
/// effort to destroy only that fixture's compose project and runtime entry.
pub struct IsolatedIcebergRestFixture {
    repo_root: PathBuf,
    scenario_root: PathBuf,
    workspace_root: PathBuf,
    config_file: PathBuf,
    compose_project: String,
    endpoints: IsolatedIcebergRestEndpoints,
    minio_root_identity: IsolatedS3Identity,
    vended_s3_identities: Option<IsolatedVendedS3Identities>,
    active: bool,
}

impl IsolatedIcebergRestFixture {
    /// Starts a fresh REST Catalog and MinIO compose project below
    /// `scenario_root`.  The caller must retain the fixture for the entire
    /// lifetime of any cluster that uses the returned endpoints.
    pub fn start(scenario_root: impl AsRef<Path>) -> Result<Self> {
        let repo_root = repository_root()?;
        let scenario_root = ensure_absolute_directory(scenario_root.as_ref())?;
        let fixture_id = unique_fixture_id();
        let workspace_root = scenario_root.join(&fixture_id);
        let config_file = workspace_root.join("isolated-compose.env");
        let compose_project = format!("nr-{fixture_id}");
        let (access_key_id, secret_access_key) = fixture_credentials(&fixture_id);

        fs::create_dir_all(&workspace_root).with_context(|| {
            format!("create isolated fixture root {}", workspace_root.display())
        })?;
        write_config(
            &config_file,
            &compose_project,
            &access_key_id,
            &secret_access_key,
        )?;

        let mut fixture = Self {
            repo_root,
            scenario_root,
            workspace_root,
            config_file,
            compose_project,
            endpoints: IsolatedIcebergRestEndpoints {
                rest_uri: String::new(),
                rest_warehouse: String::new(),
                minio_endpoint: String::new(),
                compose_project: String::new(),
            },
            minio_root_identity: IsolatedS3Identity {
                access_key_id,
                secret_access_key,
            },
            vended_s3_identities: None,
            active: true,
        };

        if let Err(error) = fixture.run_script("up.sh", &[]) {
            let cleanup = fixture.shutdown();
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(error.context(format!(
                    "isolated fixture cleanup also failed: {cleanup_error:#}"
                ))),
            };
        }

        let endpoints = fixture.read_endpoints().with_context(|| {
            format!(
                "read isolated Iceberg REST manifest for compose project {}",
                fixture.compose_project
            )
        });
        match endpoints {
            Ok((endpoints, minio_root_identity)) => {
                fixture.endpoints = endpoints;
                fixture.minio_root_identity = minio_root_identity;
            }
            Err(error) => {
                let cleanup = fixture.shutdown();
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "isolated fixture cleanup also failed: {cleanup_error:#}"
                    ))),
                };
            }
        }
        Ok(fixture)
    }

    pub fn endpoints(&self) -> &IsolatedIcebergRestEndpoints {
        &self.endpoints
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Creates two distinct, valid MinIO STS identities. The fixture first
    /// provisions ordinary users and then signs AssumeRole requests as those
    /// users, yielding the access key, secret, and session token that MinIO
    /// itself will verify on the S3 data plane.
    pub fn provision_vended_s3_identities(&mut self) -> Result<IsolatedVendedS3Identities> {
        if let Some(identities) = &self.vended_s3_identities {
            return Ok(identities.clone());
        }
        self.assert_owned_paths()?;
        let initial = self.new_sts_identity("ccai")?;
        let rotated = self.new_sts_identity("ccar")?;
        ensure!(
            initial.access_key_id != rotated.access_key_id,
            "isolated fixture generated duplicate vended access keys"
        );
        let identities = IsolatedVendedS3Identities { initial, rotated };
        self.vended_s3_identities = Some(identities.clone());
        Ok(identities)
    }

    /// Creates an empty Iceberg table through the isolated fixture's own
    /// privileged Spark catalog before a vended client is admitted.
    ///
    /// This is intentionally fixture setup rather than a NovaRocks DDL helper:
    /// a vended catalog must not receive the fixture's MinIO root credential.
    /// The resulting table is subsequently accessed only through the vended
    /// REST proxy.
    pub fn provision_empty_table(&self, namespace: &str, table: &str) -> Result<()> {
        self.assert_owned_paths()?;
        validate_sql_identifier("namespace", namespace)?;
        validate_sql_identifier("table", table)?;

        let sql_path = self
            .workspace_root
            .join(format!("provision-{namespace}-{table}.sql"));
        let sql = format!(
            "CREATE NAMESPACE IF NOT EXISTS ice_rest.{namespace};\n\
             CREATE TABLE ice_rest.{namespace}.{table} (v BIGINT) USING iceberg;\n"
        );
        fs::write(&sql_path, sql)
            .with_context(|| format!("write isolated fixture Spark SQL {}", sql_path.display()))?;

        let manifest_path = self.find_manifest()?;
        let manifest = read_manifest(&manifest_path)?;
        self.assert_isolated_manifest(&manifest)?;
        let output_result = self.run_spark_sql(&manifest, &sql_path);
        let cleanup_result = fs::remove_file(&sql_path);

        if let Err(error) = cleanup_result {
            return Err(error).with_context(|| {
                format!(
                    "remove isolated fixture Spark SQL after provisioning {}",
                    sql_path.display()
                )
            });
        }
        output_result
            .with_context(|| format!("provision empty isolated Iceberg table {namespace}.{table}"))
    }

    /// Stops the exact compose project created by this fixture and removes its
    /// generated runtime entry.  It never addresses `nr-iceberg-rest`.
    pub fn shutdown(&mut self) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        self.assert_owned_paths()?;
        let result = self.run_script("down.sh", &["--docker", "--purge"]);
        self.active = false;
        result
    }

    fn read_endpoints(&self) -> Result<(IsolatedIcebergRestEndpoints, IsolatedS3Identity)> {
        self.assert_owned_paths()?;
        let manifest_path = self.find_manifest()?;
        let contents = fs::read_to_string(&manifest_path).with_context(|| {
            format!(
                "read generated fixture manifest {}",
                manifest_path.display()
            )
        })?;
        let manifest: Manifest = serde_json::from_str(&contents).with_context(|| {
            format!(
                "decode generated fixture manifest {}",
                manifest_path.display()
            )
        })?;
        ensure!(
            !manifest.shared_docker,
            "isolated fixture manifest unexpectedly enables shared Docker"
        );
        ensure!(
            manifest.compose_project == self.compose_project,
            "isolated fixture manifest compose project mismatch"
        );
        ensure!(
            manifest.workspace_root == self.workspace_root.to_string_lossy(),
            "isolated fixture manifest workspace root mismatch"
        );
        ensure!(
            !manifest.iceberg_rest.uri.trim().is_empty()
                && !manifest.iceberg_rest.warehouse.trim().is_empty()
                && !manifest.minio.endpoint.trim().is_empty()
                && !manifest.minio.access_key_id.trim().is_empty()
                && !manifest.minio.secret_access_key.trim().is_empty(),
            "isolated fixture manifest is missing REST or MinIO endpoint facts"
        );
        let endpoints = IsolatedIcebergRestEndpoints {
            rest_uri: manifest.iceberg_rest.uri,
            rest_warehouse: manifest.iceberg_rest.warehouse,
            minio_endpoint: manifest.minio.endpoint,
            compose_project: manifest.compose_project,
        };
        let root_identity = IsolatedS3Identity {
            access_key_id: manifest.minio.access_key_id,
            secret_access_key: manifest.minio.secret_access_key,
        };
        Ok((endpoints, root_identity))
    }

    fn find_manifest(&self) -> Result<PathBuf> {
        let runtime_base = self.repo_root.join("docker/iceberg-rest/runtime");
        let expected_workspace_root = self.workspace_root.to_string_lossy();
        let mut matches = Vec::new();
        for entry in fs::read_dir(&runtime_base)
            .with_context(|| format!("read fixture runtime base {}", runtime_base.display()))?
        {
            let entry = entry.context("read fixture runtime entry")?;
            if !entry
                .file_type()
                .context("read fixture runtime entry type")?
                .is_dir()
            {
                continue;
            }
            let manifest_path = entry.path().join("manifest.json");
            let Ok(contents) = fs::read_to_string(&manifest_path) else {
                continue;
            };
            let Ok(manifest) = serde_json::from_str::<Manifest>(&contents) else {
                continue;
            };
            if manifest.workspace_root == expected_workspace_root
                && manifest.compose_project == self.compose_project
                && !manifest.shared_docker
            {
                matches.push(manifest_path);
            }
        }
        match matches.len() {
            1 => Ok(matches.remove(0)),
            0 => bail!(
                "no isolated fixture manifest exists for compose project {}",
                self.compose_project
            ),
            _ => bail!(
                "multiple isolated fixture manifests exist for compose project {}",
                self.compose_project
            ),
        }
    }

    fn new_sts_identity(&self, prefix: &str) -> Result<IsolatedStsS3Identity> {
        let user = self.new_builtin_user_identity(prefix)?;
        mint_minio_sts_identity(&self.endpoints.minio_endpoint, &user)
            .context("mint isolated MinIO STS credential")
    }

    fn new_builtin_user_identity(&self, prefix: &str) -> Result<IsolatedS3Identity> {
        let identity = IsolatedS3Identity {
            access_key_id: access_key(prefix, &self.compose_project),
            // MinIO built-in-user secrets are bounded to 8..=40 bytes.
            // Derive a compact test-only value rather than embedding the
            // unbounded unique compose-project name.
            secret_access_key: secret_key(prefix, &self.compose_project),
        };
        let manifest_path = self.find_manifest()?;
        let manifest = read_manifest(&manifest_path)?;
        self.assert_isolated_manifest(&manifest)
            .context("isolated fixture manifest changed before access-key provisioning")?;
        let mut command = Command::new("docker");
        command
            .current_dir(&self.repo_root)
            .arg("compose")
            .arg("--env-file")
            .arg(&manifest.compose_env)
            .arg("-p")
            .arg(&self.compose_project)
            .arg("-f")
            .arg(&manifest.compose_file)
            .args(["run", "--rm", "--no-deps", "-T"])
            .args(["-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD"])
            .args(["-e", "VENDED_ACCESS_KEY", "-e", "VENDED_SECRET_KEY"])
            .args(["--entrypoint", "/bin/sh", "mc", "-c"])
            .arg(
                "set -eu; \\
                 /usr/bin/mc alias set minio http://minio:9000 \"$MINIO_ROOT_USER\" \"$MINIO_ROOT_PASSWORD\" >/dev/null; \\
                 /usr/bin/mc admin user add minio \"$VENDED_ACCESS_KEY\" \"$VENDED_SECRET_KEY\" >/dev/null; \\
                 /usr/bin/mc admin policy attach minio readwrite --user \"$VENDED_ACCESS_KEY\" >/dev/null",
            )
            .env("MINIO_ROOT_USER", &self.minio_root_identity.access_key_id)
            .env("MINIO_ROOT_PASSWORD", &self.minio_root_identity.secret_access_key)
            .env("VENDED_ACCESS_KEY", &identity.access_key_id)
            .env("VENDED_SECRET_KEY", &identity.secret_access_key);
        let output = command
            .output()
            .context("provision isolated MinIO built-in user")?;
        if !output.status.success() {
            bail!(
                "provision isolated MinIO built-in user exited with {}; diagnostics: {}",
                output.status,
                safe_diagnostics(
                    &output,
                    &[
                        &self.minio_root_identity.secret_access_key,
                        &identity.secret_access_key,
                    ],
                )
            );
        }
        Ok(identity)
    }

    fn assert_isolated_manifest(&self, manifest: &Manifest) -> Result<()> {
        let runtime_base = self.repo_root.join("docker/iceberg-rest/runtime");
        ensure!(
            manifest.compose_project == self.compose_project && !manifest.shared_docker,
            "isolated fixture manifest does not belong to this fixture"
        );
        ensure!(
            Path::new(&manifest.compose_file)
                == self.repo_root.join("docker/iceberg-rest/compose.yml"),
            "isolated fixture manifest references an unexpected compose file"
        );
        ensure!(
            Path::new(&manifest.compose_env).starts_with(&runtime_base)
                && Path::new(&manifest.runtime_dir).starts_with(&runtime_base),
            "isolated fixture manifest references generated files outside its runtime base"
        );
        Ok(())
    }

    fn run_spark_sql(&self, manifest: &Manifest, sql_path: &Path) -> Result<()> {
        let defaults_path = Path::new(&manifest.runtime_dir).join("spark-defaults.conf");
        let defaults = fs::read(&defaults_path).with_context(|| {
            format!(
                "read isolated fixture Spark defaults {}",
                defaults_path.display()
            )
        })?;
        let sql = fs::read(sql_path)
            .with_context(|| format!("read isolated fixture Spark SQL {}", sql_path.display()))?;
        let temp_dir = format!("/tmp/novarocks-cca1-spark-{}", self.compose_project);
        let defaults_in_container = format!("{temp_dir}/spark-defaults.conf");
        let sql_in_container = format!("{temp_dir}/query.sql");
        let cleanup_command = format!("rm -rf {}", shell_literal(&temp_dir));

        self.run_compose_spark(
            manifest,
            &format!("mkdir -p {}", shell_literal(&temp_dir)),
            None,
        )?;
        self.run_compose_spark(
            manifest,
            &format!("cat > {}", shell_literal(&defaults_in_container)),
            Some(&defaults),
        )?;
        self.run_compose_spark(
            manifest,
            &format!("cat > {}", shell_literal(&sql_in_container)),
            Some(&sql),
        )?;
        self.run_compose_spark(
            manifest,
            &format!(
                "set -eu; \\
                 trap {} EXIT; \\
                 spark_sql_bin=\"${{SPARK_SQL_BIN:-}}\"; \\
                 if [ -z \"$spark_sql_bin\" ]; then spark_sql_bin=\"$(command -v spark-sql || true)\"; fi; \\
                 if [ -z \"$spark_sql_bin\" ] && [ -x /opt/spark/bin/spark-sql ]; then spark_sql_bin=/opt/spark/bin/spark-sql; fi; \\
                 if [ -z \"$spark_sql_bin\" ]; then echo 'spark-sql binary not found' >&2; exit 127; fi; \\
                 \"$spark_sql_bin\" --properties-file {} -f {}",
                shell_literal(&cleanup_command),
                shell_literal(&defaults_in_container),
                shell_literal(&sql_in_container),
            ),
            None,
        )
    }

    fn run_compose_spark(
        &self,
        manifest: &Manifest,
        shell_command: &str,
        stdin: Option<&[u8]>,
    ) -> Result<()> {
        let mut command = Command::new("docker");
        command
            .current_dir(&self.repo_root)
            .args(["compose", "--env-file"])
            .arg(&manifest.compose_env)
            .args(["-p", &self.compose_project, "-f"])
            .arg(&manifest.compose_file)
            .args(["exec", "-T", "spark", "/bin/bash", "-lc", shell_command]);
        if stdin.is_some() {
            command.stdin(Stdio::piped());
        }
        let mut child = command
            .spawn()
            .context("start isolated Spark compose command")?;
        if let Some(stdin_bytes) = stdin {
            let mut child_stdin = child
                .stdin
                .take()
                .context("open isolated Spark compose command stdin")?;
            child_stdin
                .write_all(stdin_bytes)
                .context("write isolated Spark compose command stdin")?;
        }
        let output = child
            .wait_with_output()
            .context("wait for isolated Spark compose command")?;
        if output.status.success() {
            return Ok(());
        }
        bail!(
            "isolated Spark compose command exited with {}; diagnostics: {}",
            output.status,
            safe_diagnostics(&output, &[&self.minio_root_identity.secret_access_key])
        );
    }

    fn run_script(&self, script: &str, args: &[&str]) -> Result<()> {
        let script_path = self.repo_root.join("docker/iceberg-rest").join(script);
        let output = fixture_command(
            &script_path,
            &self.repo_root,
            &self.workspace_root,
            &self.config_file,
            &self.compose_project,
            args,
        )
        .output()
        .with_context(|| {
            format!(
                "run isolated Iceberg REST fixture script {}",
                script_path.display()
            )
        })?;
        if output.status.success() {
            return Ok(());
        }
        bail!(
            "isolated Iceberg REST fixture script {} exited with {}; diagnostics: {}",
            script_path.display(),
            output.status,
            safe_diagnostics(&output, &[&self.minio_root_identity.secret_access_key])
        );
    }

    fn assert_owned_paths(&self) -> Result<()> {
        ensure!(
            self.workspace_root.starts_with(&self.scenario_root),
            "refusing to operate isolated fixture outside its scenario root"
        );
        ensure!(
            self.config_file.starts_with(&self.workspace_root),
            "refusing to operate isolated fixture config outside its workspace root"
        );
        ensure!(
            self.compose_project.starts_with("nr-cca1-vended-rest-"),
            "refusing to operate unexpected compose project"
        );
        Ok(())
    }
}

impl Drop for IsolatedIcebergRestFixture {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            eprintln!("isolated Iceberg REST fixture cleanup failed: {error:#}");
        }
    }
}

/// Mints a real temporary MinIO credential through its AWS-compatible STS
/// endpoint. This fixture uses a deliberately small SigV4 implementation so
/// the test harness does not need to introduce an AWS SDK only to issue two
/// local credentials.
fn mint_minio_sts_identity(
    endpoint: &str,
    user: &IsolatedS3Identity,
) -> Result<IsolatedStsS3Identity> {
    let endpoint = endpoint
        .parse::<reqwest::Url>()
        .context("parse isolated MinIO STS endpoint")?;
    ensure!(
        endpoint.scheme() == "http" || endpoint.scheme() == "https",
        "isolated MinIO STS endpoint must be HTTP(S)"
    );
    ensure!(
        endpoint.path() == "/" && endpoint.query().is_none(),
        "isolated MinIO STS endpoint must not include a path or query"
    );
    let host = endpoint
        .host_str()
        .context("isolated MinIO STS endpoint has no host")?;
    let canonical_host = endpoint
        .port()
        .map(|port| format!("{host}:{port}"))
        .unwrap_or_else(|| host.to_owned());
    let timestamp = aws_amz_timestamp()?;
    let date = &timestamp[..8];
    let body = format!(
        "Action=AssumeRole&Version=2011-06-15&DurationSeconds={MINIO_STS_DURATION_SECONDS}&RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fcca1-vended&RoleSessionName=cca1-vended"
    );
    let payload_hash = sha256_hex(body.as_bytes());
    let canonical_headers = format!(
        "content-type:application/x-www-form-urlencoded\nhost:{canonical_host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{timestamp}\n"
    );
    let signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date";
    let canonical_request =
        format!("POST\n/\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date}/us-east-1/sts/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = aws_v4_signing_key(&user.secret_access_key, date, "us-east-1", "sts");
    let signature = hex_encode(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        user.access_key_id
    );

    let response = reqwest::blocking::Client::new()
        .post(endpoint)
        .header("content-type", "application/x-www-form-urlencoded")
        .header("host", canonical_host)
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", timestamp)
        .header("authorization", authorization)
        .body(body)
        .send()
        .context("send isolated MinIO STS AssumeRole request")?;
    let status = response.status();
    ensure!(
        status.is_success(),
        "isolated MinIO STS AssumeRole returned HTTP {status}"
    );
    let xml = response
        .text()
        .context("read isolated MinIO STS AssumeRole response")?;
    let expiration = sts_xml_value(&xml, "Expiration")?;
    let not_after_unix_ms = OffsetDateTime::parse(&expiration, &Rfc3339)
        .context("parse isolated MinIO STS credential expiration")?
        .unix_timestamp_nanos()
        .checked_div(1_000_000)
        .and_then(|milliseconds| u64::try_from(milliseconds).ok())
        .context("isolated MinIO STS credential expiration is before the Unix epoch")?;
    Ok(IsolatedStsS3Identity {
        access_key_id: sts_xml_value(&xml, "AccessKeyId")?,
        secret_access_key: sts_xml_value(&xml, "SecretAccessKey")?,
        session_token: sts_xml_value(&xml, "SessionToken")?,
        not_after_unix_ms,
    })
}

fn aws_amz_timestamp() -> Result<String> {
    let output = Command::new("date")
        .args(["-u", "+%Y%m%dT%H%M%SZ"])
        .output()
        .context("read UTC time for isolated MinIO STS request")?;
    ensure!(
        output.status.success(),
        "read UTC time for isolated MinIO STS request exited with {}",
        output.status
    );
    let value = String::from_utf8(output.stdout)
        .context("decode UTC time for isolated MinIO STS request")?
        .trim()
        .to_owned();
    ensure!(
        value.len() == 16
            && value.as_bytes()[8] == b'T'
            && value.as_bytes()[15] == b'Z'
            && value
                .bytes()
                .enumerate()
                .all(|(index, byte)| { index == 8 || index == 15 || byte.is_ascii_digit() }),
        "UTC time for isolated MinIO STS request has an unexpected format"
    );
    Ok(value)
}

fn aws_v4_signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let mut initial = b"AWS4".to_vec();
    initial.extend_from_slice(secret.as_bytes());
    let date_key = hmac_sha256(&initial, date.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, service.as_bytes());
    hmac_sha256(&service_key, b"aws4_request")
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts arbitrary key sizes");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

fn sha256_hex(value: &[u8]) -> String {
    hex_encode(&Sha256::digest(value))
}

fn hex_encode(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sts_xml_value(xml: &str, element: &str) -> Result<String> {
    let open = format!("<{element}>");
    let close = format!("</{element}>");
    let (_, after_open) = xml
        .split_once(&open)
        .with_context(|| format!("isolated MinIO STS response has no {element}"))?;
    let (value, _) = after_open
        .split_once(&close)
        .with_context(|| format!("isolated MinIO STS response has no closing {element}"))?;
    ensure!(
        !value.is_empty(),
        "isolated MinIO STS response has an empty {element}"
    );
    Ok(value.to_owned())
}

#[derive(Deserialize)]
struct Manifest {
    workspace_root: String,
    shared_docker: bool,
    compose_project: String,
    compose_file: String,
    compose_env: String,
    runtime_dir: String,
    minio: ManifestMinio,
    iceberg_rest: ManifestIcebergRest,
}

fn read_manifest(manifest_path: &Path) -> Result<Manifest> {
    let contents = fs::read_to_string(manifest_path).with_context(|| {
        format!(
            "read generated fixture manifest {}",
            manifest_path.display()
        )
    })?;
    serde_json::from_str(&contents).with_context(|| {
        format!(
            "decode generated fixture manifest {}",
            manifest_path.display()
        )
    })
}

#[derive(Deserialize)]
struct ManifestMinio {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
}

#[derive(Deserialize)]
struct ManifestIcebergRest {
    uri: String,
    warehouse: String,
}

fn repository_root() -> Result<PathBuf> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .nth(2)
        .context("resolve repository root from cluster-harness manifest directory")?
        .canonicalize()
        .context("canonicalize repository root")
}

fn ensure_absolute_directory(path: &Path) -> Result<PathBuf> {
    fs::create_dir_all(path)
        .with_context(|| format!("create scenario runtime root {}", path.display()))?;
    path.canonicalize()
        .with_context(|| format!("canonicalize scenario runtime root {}", path.display()))
}

fn unique_fixture_id() -> String {
    let sequence = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{FIXTURE_PREFIX}-{}-{nanos}-{sequence}", std::process::id())
}

fn fixture_credentials(fixture_id: &str) -> (String, String) {
    (
        access_key("cca", fixture_id),
        format!("cca1-root-secret-{fixture_id}"),
    )
}

fn access_key(prefix: &str, value: &str) -> String {
    debug_assert!(prefix.len() <= 4);
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{prefix}{hash:016x}")
}

fn secret_key(prefix: &str, value: &str) -> String {
    format!("s{}", access_key(prefix, value))
}

fn validate_sql_identifier(kind: &str, value: &str) -> Result<()> {
    let mut characters = value.bytes();
    let Some(first) = characters.next() else {
        bail!("isolated fixture {kind} must not be empty");
    };
    if !(first.is_ascii_lowercase() || first == b'_')
        || !characters.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == b'_'
        })
    {
        bail!("isolated fixture {kind} must be a lower-case SQL identifier, got {value:?}");
    }
    Ok(())
}

fn write_config(
    config_file: &Path,
    compose_project: &str,
    access_key_id: &str,
    secret_access_key: &str,
) -> Result<()> {
    let contents = format!(
        "# Generated by the isolated CCA-1 system-test fixture.\nNOVA_ENV_SHARED_DOCKER=false\nNOVA_ENV_COMPOSE_PROJECT={}\nMINIO_ROOT_USER={}\nMINIO_ROOT_PASSWORD={}\n",
        shell_literal(compose_project),
        shell_literal(access_key_id),
        shell_literal(secret_access_key),
    );
    fs::write(config_file, contents)
        .with_context(|| format!("write isolated fixture config {}", config_file.display()))
}

fn shell_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\\"'\\\"'"))
}

fn fixture_command(
    script_path: &Path,
    repo_root: &Path,
    workspace_root: &Path,
    config_file: &Path,
    compose_project: &str,
    args: &[&str],
) -> Command {
    let mut command = Command::new(script_path);
    // The desktop shell may have sourced the shared fixture's generated
    // `env.sh`.  Those volatile ports must not leak into an isolated project:
    // `up.sh` chooses an unused port only when these values are absent.
    for inherited in [
        "NOVA_ENV_MINIO_PORT",
        "NOVA_ENV_MINIO_CONSOLE_PORT",
        "NOVA_ENV_REST_PORT",
        "NOVA_ENV_SPARK_UI_PORT",
        "NOVA_ENV_MYSQL_PORT",
        "NOVA_ENV_FE_GRPC_PORT",
        "NOVA_ENV_BE_GRPC_PORT",
        "NOVA_ENV_FE_HTTP_PORT",
        "NOVA_ENV_BE_HTTP_PORT",
        "NOVA_ENV_SHARED_COMPOSE_PROJECT",
        "NOVA_ENV_SHARED_REST_WAREHOUSE_URI",
        "NOVA_ENV_REST_WAREHOUSE_URI",
    ] {
        command.env_remove(inherited);
    }
    command
        .current_dir(repo_root)
        .args(args)
        .env("NOVAROCKS_WORKSPACE_ROOT", workspace_root)
        .env("NOVA_ENV_CONFIG_FILE", config_file)
        .env("NOVA_ENV_SHARED_DOCKER", "false")
        .env("NOVA_ENV_COMPOSE_PROJECT", compose_project)
        // The fixture creates this exact non-shared project and no other.
        // `down.sh --docker --purge` requires both pieces of this proof before
        // it will remove the project's MinIO volume.
        .env("NOVA_ENV_ALLOW_VOLUME_DELETE", "true")
        .env("NOVA_ENV_EXPECTED_COMPOSE_PROJECT", compose_project)
        .env(
            "NOVA_ENV_EXPECTED_MINIO_VOLUME",
            format!("{compose_project}_minio-data"),
        );
    command
}

fn safe_diagnostics(output: &Output, secrets: &[&str]) -> String {
    let mut text = String::from_utf8_lossy(&output.stderr).into_owned();
    if !output.stdout.is_empty() {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(&String::from_utf8_lossy(&output.stdout));
    }
    let mut text = text.replace('\n', " ");
    for secret in secrets.iter().copied().filter(|secret| !secret.is_empty()) {
        text = text.replace(secret, "<redacted>");
    }
    if text.len() <= MAX_DIAGNOSTIC_BYTES {
        text
    } else {
        format!("{}...<truncated>", &text[..MAX_DIAGNOSTIC_BYTES])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn isolated_command_forces_unique_non_shared_environment() {
        let root = PathBuf::from("/tmp/cca1-scenario/cca1-vended-rest-1-2-3");
        let config = root.join("isolated-compose.env");
        let command = fixture_command(
            Path::new("/repo/docker/iceberg-rest/up.sh"),
            Path::new("/repo"),
            &root,
            &config,
            "nr-cca1-vended-rest-1-2-3",
            &[],
        );
        let environments = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            environments.get("NOVA_ENV_SHARED_DOCKER"),
            Some(&Some("false".to_string()))
        );
        assert_eq!(
            environments.get("NOVA_ENV_COMPOSE_PROJECT"),
            Some(&Some("nr-cca1-vended-rest-1-2-3".to_string()))
        );
        assert_eq!(
            environments.get("NOVAROCKS_WORKSPACE_ROOT"),
            Some(&Some(root.to_string_lossy().into_owned()))
        );
        assert_eq!(
            environments.get("NOVA_ENV_CONFIG_FILE"),
            Some(&Some(config.to_string_lossy().into_owned()))
        );
        assert_eq!(
            environments.get("NOVA_ENV_ALLOW_VOLUME_DELETE"),
            Some(&Some("true".to_string()))
        );
        assert_eq!(
            environments.get("NOVA_ENV_EXPECTED_COMPOSE_PROJECT"),
            Some(&Some("nr-cca1-vended-rest-1-2-3".to_string()))
        );
        assert_eq!(
            environments.get("NOVA_ENV_EXPECTED_MINIO_VOLUME"),
            Some(&Some("nr-cca1-vended-rest-1-2-3_minio-data".to_string()))
        );
    }

    #[test]
    fn generated_config_has_no_shared_docker_fallback() {
        let config = std::env::temp_dir().join(format!("{FIXTURE_PREFIX}-config-test"));
        let _ = fs::remove_dir_all(&config);
        fs::create_dir_all(&config).expect("create fixture config test directory");
        let config_file = config.join("isolated-compose.env");
        write_config(&config_file, "nr-cca1-vended-rest-test", "key", "secret")
            .expect("write config");
        let contents = fs::read_to_string(&config_file).expect("read config");
        assert!(contents.contains("NOVA_ENV_SHARED_DOCKER=false"));
        assert!(contents.contains("NOVA_ENV_COMPOSE_PROJECT='nr-cca1-vended-rest-test'"));
        assert!(!contents.contains("shared.env"));
        fs::remove_dir_all(config).expect("remove fixture config test directory");
    }

    #[test]
    fn generated_access_keys_fit_minio_s3_limits_and_debug_is_redacted() {
        let initial = IsolatedS3Identity {
            access_key_id: access_key("ccai", "fixture"),
            secret_access_key: "do-not-print-this".to_string(),
        };
        assert_eq!(initial.access_key_id.len(), 20);
        assert!(!format!("{initial:?}").contains("do-not-print-this"));
    }

    #[test]
    fn diagnostics_redact_fixture_secrets() {
        let output = Output {
            status: std::process::ExitStatus::from_raw(1),
            stdout: b"first-secret".to_vec(),
            stderr: b"second-secret".to_vec(),
        };
        let diagnostics = safe_diagnostics(&output, &["first-secret", "second-secret"]);
        assert!(!diagnostics.contains("first-secret"));
        assert!(!diagnostics.contains("second-secret"));
        assert!(diagnostics.contains("<redacted>"));
    }

    #[test]
    fn fixture_table_identifiers_are_strictly_bounded() {
        validate_sql_identifier("namespace", "vended_rest_db").expect("valid namespace");
        validate_sql_identifier("table", "vended_rest_data").expect("valid table");
        assert!(validate_sql_identifier("table", "vended-rest").is_err());
        assert!(validate_sql_identifier("table", "vended_rest; DROP TABLE t").is_err());
        assert!(validate_sql_identifier("table", "1vended").is_err());
    }
}
