#![allow(dead_code)]
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

use crate::types::RunnerConfig;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::NamedTempFile;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BenchmarkBootstrapOptions {
    pub enabled: bool,
    pub rebuild: bool,
    pub scales: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyBenchmarkFixture {
    pub dataset_key: BTreeMap<String, String>,
    pub exact_warehouse: String,
    pub manifest_uri: String,
    pub ready_uri: String,
    pub publication_identity: String,
    pub publication_etag: String,
    pub reused: bool,
    pub built: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BootstrapMode {
    Check,
    Ensure,
    Rebuild,
}

#[derive(Debug, Deserialize, Serialize)]
struct ResolvedBenchmarkDataset {
    schema_version: u32,
    dataset_key: BTreeMap<String, String>,
    dataset_root: String,
    ready_uri: String,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct EnsureResult {
    schema_version: u32,
    dataset_key: BTreeMap<String, String>,
    state: String,
    reused: bool,
    built: bool,
    exact_warehouse: String,
    manifest_uri: String,
    publication: Publication,
}

#[derive(Debug, Deserialize)]
struct Publication {
    ready_uri: String,
    etag: String,
    identity: String,
}

#[derive(Debug, Deserialize)]
struct FixtureError {
    schema_version: u32,
    error: String,
    dataset_key: BTreeMap<String, String>,
    message: String,
}

pub fn is_benchmark_suite(suite: &str) -> bool {
    matches!(suite, "ssb" | "tpc-h" | "tpc-ds")
}

pub fn is_auto_bootstrap_supported_suite(suite: &str) -> bool {
    matches!(suite, "ssb" | "tpc-h" | "tpc-ds")
}

pub fn parse_benchmark_scale_override(
    raw: &str,
    options: &mut BenchmarkBootstrapOptions,
) -> Result<()> {
    let (suite, scale) = raw
        .split_once('=')
        .with_context(|| format!("invalid benchmark scale override: {raw}"))?;
    let suite = suite.trim();
    let scale = scale.trim();

    if !is_benchmark_suite(suite) {
        bail!("unknown benchmark suite in scale override: {suite}");
    }
    if scale.is_empty() {
        bail!("benchmark scale override for {suite} must not be empty");
    }
    if scale.contains('=') {
        bail!("invalid benchmark scale override: {raw}");
    }

    options.scales.insert(suite.to_string(), scale.to_string());
    Ok(())
}

pub fn parse_scale_overrides(raw_overrides: &[String]) -> Result<BTreeMap<String, String>> {
    let mut options = BenchmarkBootstrapOptions::default();
    for raw in raw_overrides {
        parse_benchmark_scale_override(raw, &mut options)?;
    }
    Ok(options.scales)
}

pub fn benchmark_scale_for_suite(
    options: &BenchmarkBootstrapOptions,
    suite: &str,
) -> Result<String> {
    if !is_benchmark_suite(suite) {
        bail!("unknown benchmark suite: {suite}");
    }

    Ok(options
        .scales
        .get(suite)
        .cloned()
        .unwrap_or_else(|| default_benchmark_scale(suite).to_string()))
}

pub fn default_benchmark_scale(suite: &str) -> &'static str {
    match suite {
        "ssb" => "1",
        "tpc-h" => "1",
        "tpc-ds" => "1GB",
        _ => "",
    }
}

fn build_benchmark_bootstrap_command(
    script_path: &Path,
    suite: &str,
    scale: &str,
    resolved_dataset_path: &Path,
    mode: BootstrapMode,
) -> Command {
    let mut command = Command::new(script_path);
    command
        .arg("--suite")
        .arg(suite)
        .arg("--scale")
        .arg(scale)
        .arg("--resolved-dataset")
        .arg(resolved_dataset_path);
    command.arg(match mode {
        BootstrapMode::Check => "--check",
        BootstrapMode::Ensure => "--ensure",
        BootstrapMode::Rebuild => "--rebuild",
    });

    command
}

pub fn command_preview(command: &Command) -> String {
    let mut parts = vec![shell_quote(command.get_program())];
    let mut redact_next = false;

    for arg in command.get_args() {
        if redact_next {
            parts.push("<redacted>".to_string());
            redact_next = false;
            continue;
        }

        parts.push(shell_quote(arg));
        if arg == "--mysql-password" {
            redact_next = true;
        }
    }

    parts.join(" ")
}

fn run_benchmark_bootstrap_command(command: &mut Command) -> Result<std::process::Output> {
    let preview = command_preview(command);
    command
        .output()
        .with_context(|| format!("failed to run benchmark bootstrap command: {preview}"))
}

pub fn ensure_benchmark_data(
    options: &BenchmarkBootstrapOptions,
    runner_config: &RunnerConfig,
    base_dir: &Path,
    suite: &str,
) -> Result<Option<ReadyBenchmarkFixture>> {
    if !is_auto_bootstrap_supported_suite(suite) {
        return Ok(None);
    }

    let script_path = benchmark_bootstrap_script_path(runner_config, base_dir);
    let scale = benchmark_scale_for_suite(options, suite)?;
    let resolved = resolve_benchmark_dataset(runner_config, base_dir, suite, &scale)?;
    let mut resolved_file =
        NamedTempFile::new().context("create resolved benchmark dataset file")?;
    serde_json::to_writer(&mut resolved_file, &resolved)
        .context("write resolved benchmark dataset")?;
    resolved_file
        .as_file_mut()
        .sync_all()
        .context("sync resolved benchmark dataset")?;

    let mode = if options.rebuild {
        BootstrapMode::Rebuild
    } else if options.enabled {
        BootstrapMode::Ensure
    } else {
        BootstrapMode::Check
    };
    let mut command =
        build_benchmark_bootstrap_command(&script_path, suite, &scale, resolved_file.path(), mode);
    let preview = command_preview(&command);
    let output = run_benchmark_bootstrap_command(&mut command)?;
    validate_typed_bootstrap_output(runner_config, base_dir, suite, &scale, &output)?;
    parse_bootstrap_output(&output, &resolved, &preview)
        .with_context(|| format!("failed to prepare benchmark data for {suite}"))
        .map(Some)
}

fn resolve_benchmark_dataset(
    runner_config: &RunnerConfig,
    base_dir: &Path,
    suite: &str,
    scale: &str,
) -> Result<ResolvedBenchmarkDataset> {
    let resolver_path = benchmark_fixture_resolver_path(runner_config, base_dir);
    let mut command = Command::new("python3");
    command
        .arg(&resolver_path)
        .arg("--workspace-root")
        .arg(base_dir)
        .arg("--suite")
        .arg(suite)
        .arg("--scale")
        .arg(scale);
    if let Some(shared_root) = benchmark_fixture_shared_root(runner_config) {
        command.arg("--shared-root").arg(shared_root);
    }
    let preview = command_preview(&command);
    let output = command
        .output()
        .with_context(|| format!("failed to run benchmark fixture resolver: {preview}"))?;
    if !output.status.success() {
        bail!(
            "benchmark fixture resolver failed: {preview}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let resolved: ResolvedBenchmarkDataset =
        serde_json::from_slice(&output.stdout).with_context(|| {
            format!("benchmark fixture resolver returned malformed JSON: {preview}")
        })?;
    validate_resolved_dataset(&resolved, suite, scale)?;
    Ok(resolved)
}

fn benchmark_fixture_resolver_path(runner_config: &RunnerConfig, base_dir: &Path) -> PathBuf {
    runner_config
        .values
        .get("benchmark_fixture_resolver")
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                base_dir.join(path)
            }
        })
        .unwrap_or_else(|| {
            base_dir.join("tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py")
        })
}

fn benchmark_fixture_shared_root(runner_config: &RunnerConfig) -> Option<String> {
    runner_config
        .values
        .get("benchmark_shared_root")
        .or_else(|| runner_config.values.get("benchmark_fixture_shared_root"))
        .cloned()
        .or_else(|| std::env::var("NOVA_ENV_SHARED_BENCHMARK_ROOT").ok())
        .filter(|value| !value.trim().is_empty())
}

fn validate_typed_bootstrap_output(
    runner_config: &RunnerConfig,
    base_dir: &Path,
    suite: &str,
    scale: &str,
    output: &std::process::Output,
) -> Result<()> {
    let mut output_file = NamedTempFile::new().context("create bootstrap result file")?;
    use std::io::Write;
    output_file
        .write_all(&output.stdout)
        .context("write bootstrap result")?;
    output_file
        .as_file_mut()
        .sync_all()
        .context("sync bootstrap result")?;

    let mut command = Command::new("python3");
    command
        .arg(benchmark_fixture_resolver_path(runner_config, base_dir))
        .arg("--workspace-root")
        .arg(base_dir)
        .arg("--suite")
        .arg(suite)
        .arg("--scale")
        .arg(scale)
        .arg(if output.status.success() {
            "--validate-ensure-result"
        } else {
            "--validate-error"
        })
        .arg(output_file.path());
    if let Some(shared_root) = benchmark_fixture_shared_root(runner_config) {
        command.arg("--shared-root").arg(shared_root);
    }
    let preview = command_preview(&command);
    let validation = command
        .output()
        .with_context(|| format!("failed to validate benchmark fixture result: {preview}"))?;
    if validation.status.success() {
        return Ok(());
    }
    bail!(
        "benchmark bootstrap emitted an invalid typed result: {preview}: {}",
        String::from_utf8_lossy(&validation.stderr).trim()
    )
}

pub fn dry_run_benchmark_fixture(
    runner_config: &RunnerConfig,
    base_dir: &Path,
    suite: &str,
    options: &BenchmarkBootstrapOptions,
) -> Result<Option<ReadyBenchmarkFixture>> {
    if !is_auto_bootstrap_supported_suite(suite) {
        return Ok(None);
    }
    let scale = benchmark_scale_for_suite(options, suite)?;
    let resolved = resolve_benchmark_dataset(runner_config, base_dir, suite, &scale)?;
    Ok(Some(ReadyBenchmarkFixture {
        dataset_key: resolved.dataset_key,
        exact_warehouse: "unresolved://benchmark-fixture-ready-required".to_string(),
        manifest_uri: "unresolved://benchmark-fixture-ready-required".to_string(),
        ready_uri: resolved.ready_uri,
        publication_identity: "unresolved".to_string(),
        publication_etag: "unresolved".to_string(),
        reused: false,
        built: false,
    }))
}

fn validate_resolved_dataset(
    resolved: &ResolvedBenchmarkDataset,
    suite: &str,
    _requested_scale: &str,
) -> Result<()> {
    if resolved.schema_version != 1 {
        bail!("ResolvedBenchmarkDataset has an unknown schema_version");
    }
    if resolved.dataset_key.get("suite").map(String::as_str) != Some(suite)
        || resolved
            .dataset_key
            .get("scale")
            .is_none_or(|scale| scale.trim().is_empty())
        || resolved
            .dataset_key
            .get("fixture_contract_id")
            .is_none_or(String::is_empty)
    {
        bail!("ResolvedBenchmarkDataset has an invalid dataset_key");
    }
    if !resolved.dataset_root.starts_with("s3://") || !resolved.ready_uri.starts_with("s3://") {
        bail!("ResolvedBenchmarkDataset has a non-S3 dataset location");
    }
    Ok(())
}

fn parse_bootstrap_output(
    output: &std::process::Output,
    resolved: &ResolvedBenchmarkDataset,
    preview: &str,
) -> Result<ReadyBenchmarkFixture> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let value: serde_json::Value = serde_json::from_str(stdout.trim()).with_context(|| {
        format!("benchmark bootstrap did not emit a typed JSON result: {preview}")
    })?;
    if output.status.success() {
        let result: EnsureResult = serde_json::from_value(value)
            .context("benchmark bootstrap emitted malformed EnsureResult")?;
        return validate_ensure_result(result, resolved);
    }

    if let Ok(error) = serde_json::from_value::<FixtureError>(value) {
        validate_fixture_error(error, resolved)?;
    }
    bail!(
        "benchmark bootstrap failed: {preview}: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )
}

fn validate_ensure_result(
    result: EnsureResult,
    resolved: &ResolvedBenchmarkDataset,
) -> Result<ReadyBenchmarkFixture> {
    if result.schema_version != 1
        || result.state != "ReadyValid"
        || result.reused == result.built
        || result.dataset_key != resolved.dataset_key
        || result.publication.ready_uri != resolved.ready_uri
        || result.publication.etag.trim().is_empty()
        || result.publication.identity.trim().is_empty()
        || !result.exact_warehouse.starts_with("s3://")
        || !result.manifest_uri.starts_with("s3://")
    {
        bail!("benchmark bootstrap emitted an invalid EnsureResult");
    }
    Ok(ReadyBenchmarkFixture {
        dataset_key: result.dataset_key,
        exact_warehouse: result.exact_warehouse,
        manifest_uri: result.manifest_uri,
        ready_uri: result.publication.ready_uri,
        publication_identity: result.publication.identity,
        publication_etag: result.publication.etag,
        reused: result.reused,
        built: result.built,
    })
}

fn validate_fixture_error(error: FixtureError, resolved: &ResolvedBenchmarkDataset) -> Result<()> {
    const KNOWN_ERRORS: &[&str] = &[
        "ready_invalid",
        "wait_timeout",
        "lease_lost",
        "writer_failed",
        "publication_conflict",
        "publication_failed",
    ];
    if error.schema_version != 1
        || error.dataset_key != resolved.dataset_key
        || !KNOWN_ERRORS.contains(&error.error.as_str())
        || error.message.trim().is_empty()
    {
        bail!("benchmark bootstrap emitted an invalid FixtureError");
    }
    bail!("benchmark fixture {}: {}", error.error, error.message)
}

fn benchmark_bootstrap_script_path(runner_config: &RunnerConfig, base_dir: &Path) -> PathBuf {
    runner_config
        .values
        .get("benchmark_bootstrap_script")
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                base_dir.join(path)
            }
        })
        .unwrap_or_else(|| {
            base_dir
                .join("tests")
                .join("sql")
                .join("fixtures")
                .join("benchmarks")
                .join("bootstrap_benchmark_data.sh")
        })
}

fn shell_quote(value: &OsStr) -> String {
    let value = value.to_string_lossy();
    if value.is_empty() {
        return "''".to_string();
    }
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':' | '='))
    {
        return value.into_owned();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn recognizes_supported_benchmark_suites() {
        assert!(is_benchmark_suite("ssb"));
        assert!(is_benchmark_suite("tpc-h"));
        assert!(is_benchmark_suite("tpc-ds"));
        assert!(!is_benchmark_suite("join"));
    }

    #[test]
    fn auto_bootstrap_supports_standard_benchmark_suites() {
        assert!(is_auto_bootstrap_supported_suite("ssb"));
        assert!(is_auto_bootstrap_supported_suite("tpc-h"));
        assert!(is_auto_bootstrap_supported_suite("tpc-ds"));
        assert!(!is_auto_bootstrap_supported_suite("join"));
    }

    #[test]
    fn parses_scale_overrides_and_defaults() {
        let mut options = BenchmarkBootstrapOptions::default();
        parse_benchmark_scale_override("ssb=10", &mut options).unwrap();
        parse_benchmark_scale_override("tpc-h=100", &mut options).unwrap();

        assert_eq!(benchmark_scale_for_suite(&options, "ssb").unwrap(), "10");
        assert_eq!(benchmark_scale_for_suite(&options, "tpc-h").unwrap(), "100");
        assert_eq!(
            benchmark_scale_for_suite(&options, "tpc-ds").unwrap(),
            "1GB"
        );
    }

    #[test]
    fn parses_cli_scale_override_list() {
        let overrides = vec!["ssb=10".to_string(), "tpc-ds=100GB".to_string()];

        let scales = parse_scale_overrides(&overrides).unwrap();

        assert_eq!(scales.get("ssb").map(String::as_str), Some("10"));
        assert_eq!(scales.get("tpc-ds").map(String::as_str), Some("100GB"));
        assert_eq!(scales.get("tpc-h"), None);
    }

    #[test]
    fn rejects_bad_scale_overrides() {
        let mut options = BenchmarkBootstrapOptions::default();

        assert!(parse_benchmark_scale_override("ssb", &mut options).is_err());
        assert!(parse_benchmark_scale_override("ssb=", &mut options).is_err());
        assert!(parse_benchmark_scale_override("ssb=1=2", &mut options).is_err());
        assert!(parse_benchmark_scale_override("unknown=1", &mut options).is_err());
        assert!(parse_benchmark_scale_override("=1", &mut options).is_err());
    }

    #[test]
    fn resolver_normalizes_scale_without_runner_reimplementing_the_contract() {
        let mut resolved = resolved_fixture();
        resolved
            .dataset_key
            .insert("suite".to_string(), "tpc-ds".to_string());
        resolved.dataset_key.insert("scale".to_string(), "1GB".to_string());

        validate_resolved_dataset(&resolved, "tpc-ds", "1gb")
            .expect("the resolver owns scale normalization");
    }

    #[test]
    fn bootstrap_command_uses_resolved_dataset_without_mysql_or_catalog_arguments() {
        let command = build_benchmark_bootstrap_command(
            Path::new("tests/sql/fixtures/benchmarks/bootstrap_benchmark_data.sh"),
            "ssb",
            "1",
            Path::new("/tmp/resolved.json"),
            BootstrapMode::Rebuild,
        );

        let program = command.get_program().to_string_lossy();
        let args: Vec<_> = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert_eq!(
            program,
            "tests/sql/fixtures/benchmarks/bootstrap_benchmark_data.sh"
        );
        assert_eq!(
            args,
            vec![
                "--suite",
                "ssb",
                "--scale",
                "1",
                "--resolved-dataset",
                "/tmp/resolved.json",
                "--rebuild",
            ]
        );
        assert!(!args.iter().any(|arg| arg.contains("mysql")));
        assert!(!args.iter().any(|arg| arg.contains("catalog")));
    }

    #[test]
    fn command_preview_still_redacts_generic_password_arguments() {
        let mut command = Command::new("fixture-driver");
        command.arg("--mysql-password").arg("very-secret-password");

        let preview = command_preview(&command);

        assert!(preview.contains("--mysql-password <redacted>"));
        assert!(!preview.contains("very-secret-password"));
    }

    #[test]
    fn typed_ensure_result_requires_resolved_key_and_ready_uri() {
        let resolved = resolved_fixture();
        let stdout = r#"{
          "schema_version": 1,
          "dataset_key": {"suite":"ssb","scale":"1","fixture_contract_id":"abc"},
          "state":"ReadyValid",
          "reused":true,
          "built":false,
          "exact_warehouse":"s3://novarocks/shared/benchmarks/ssb/1/abc/warehouse",
          "manifest_uri":"s3://novarocks/shared/benchmarks/ssb/1/abc/manifest.json",
          "publication":{"ready_uri":"s3://novarocks/shared/benchmarks/ssb/1/abc/READY.json","etag":"etag","identity":"identity"}
        }"#;
        let output = Command::new("true").output().expect("run true");
        let output = std::process::Output {
            stdout: stdout.as_bytes().to_vec(),
            ..output
        };

        let fixture = parse_bootstrap_output(&output, &resolved, "fixture-driver").unwrap();

        assert_eq!(
            fixture.exact_warehouse,
            "s3://novarocks/shared/benchmarks/ssb/1/abc/warehouse"
        );
        assert!(fixture.reused);
        assert!(!fixture.built);
    }

    #[test]
    fn ready_invalid_error_fails_closed_without_an_ensure_retry() {
        let resolved = resolved_fixture();
        let stdout = r#"{
          "schema_version":1,
          "error":"ready_invalid",
          "dataset_key":{"suite":"ssb","scale":"1","fixture_contract_id":"abc"},
          "message":"READY manifest mismatch"
        }"#;
        let output = Command::new("false").output().expect("run false");
        let output = std::process::Output {
            stdout: stdout.as_bytes().to_vec(),
            ..output
        };

        let error = parse_bootstrap_output(&output, &resolved, "fixture-driver")
            .expect_err("ReadyInvalid must fail closed");

        assert!(error.to_string().contains("ready_invalid"));
    }

    #[test]
    fn ensure_passes_full_resolved_contract_to_driver_and_validates_result() {
        let temp = tempdir().expect("tempdir");
        let resolver = temp.path().join("resolver.py");
        let driver = temp.path().join("driver.sh");
        fs::write(&resolver, "import json\nimport sys\na=sys.argv[1:]\nif '--validate-ensure-result' in a:\n p=json.load(open(a[a.index('--validate-ensure-result')+1])); assert p['state']=='ReadyValid'; raise SystemExit(0)\nif '--validate-error' in a: raise SystemExit(0)\nprint(json.dumps({'schema_version':1,'dataset_key':{'suite':'ssb','scale':'1','fixture_contract_id':'abc'},'dataset_root':'s3://novarocks/shared/benchmarks/ssb/1/abc','ready_uri':'s3://novarocks/shared/benchmarks/ssb/1/abc/READY.json','contract':{'preserved_for_driver':True}}))\n").expect("write resolver");
        fs::write(&driver, "#!/usr/bin/env bash\nset -euo pipefail\ntest \"$1\" = --suite; test \"$2\" = ssb; test \"$3\" = --scale; test \"$4\" = 1; test \"$5\" = --resolved-dataset\npython3 - \"$6\" <<'PY'\nimport json,sys\nassert json.load(open(sys.argv[1]))['contract']['preserved_for_driver'] is True\nPY\ntest \"$7\" = --ensure\nprintf '%s\\n' '{\"schema_version\":1,\"dataset_key\":{\"suite\":\"ssb\",\"scale\":\"1\",\"fixture_contract_id\":\"abc\"},\"state\":\"ReadyValid\",\"reused\":false,\"built\":true,\"exact_warehouse\":\"s3://novarocks/shared/benchmarks/ssb/1/abc/warehouse\",\"manifest_uri\":\"s3://novarocks/shared/benchmarks/ssb/1/abc/manifest.json\",\"publication\":{\"ready_uri\":\"s3://novarocks/shared/benchmarks/ssb/1/abc/READY.json\",\"etag\":\"etag\",\"identity\":\"identity\"}}'\n").expect("write driver");
        fs::set_permissions(&driver, fs::Permissions::from_mode(0o755))
            .expect("make driver executable");

        let mut config = RunnerConfig::default();
        config.values.insert(
            "benchmark_fixture_resolver".to_string(),
            resolver.display().to_string(),
        );
        config.values.insert(
            "benchmark_bootstrap_script".to_string(),
            driver.display().to_string(),
        );
        let options = BenchmarkBootstrapOptions {
            enabled: true,
            rebuild: false,
            scales: BTreeMap::new(),
        };

        let fixture = ensure_benchmark_data(&options, &config, temp.path(), "ssb")
            .expect("ensure succeeds")
            .expect("benchmark suite returns fixture");

        assert!(fixture.built);
        assert_eq!(fixture.dataset_key["fixture_contract_id"], "abc");
    }

    fn resolved_fixture() -> ResolvedBenchmarkDataset {
        ResolvedBenchmarkDataset {
            schema_version: 1,
            dataset_key: BTreeMap::from([
                ("suite".to_string(), "ssb".to_string()),
                ("scale".to_string(), "1".to_string()),
                ("fixture_contract_id".to_string(), "abc".to_string()),
            ]),
            dataset_root: "s3://novarocks/shared/benchmarks/ssb/1/abc".to_string(),
            ready_uri: "s3://novarocks/shared/benchmarks/ssb/1/abc/READY.json".to_string(),
            extra: BTreeMap::new(),
        }
    }
}
