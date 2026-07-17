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
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const FORMAT: &str = "novarocks-compat-artifact-v1";
const PROBE_FORMAT: &str = "novarocks-compat-probe-v1";
const MANIFEST_KEYS: [&str; 6] = [
    "format", "binary", "sha256", "git_head", "profile", "features",
];
const PROBE_MANIFEST_KEYS: [&str; 6] = [
    "format", "path", "sha256", "git_head", "profile", "features",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompatArtifact {
    pub(crate) binary: PathBuf,
    pub(crate) sha256: String,
    pub(crate) git_head: String,
    pub(crate) profile: String,
    pub(crate) probe_binary: PathBuf,
    pub(crate) probe_sha256: String,
}

impl CompatArtifact {
    pub(crate) fn resolve(repo_root: &Path, profile: &str) -> Result<Self> {
        if std::env::var_os("NOVAROCKS_COMPAT_PROBE_BIN").is_some() {
            bail!(
                "NOVAROCKS_COMPAT_PROBE_BIN is unsupported; the probe must come from a validated probe manifest"
            );
        }
        let manifest_path = match std::env::var_os("NOVAROCKS_COMPAT_ARTIFACT_MANIFEST") {
            Some(path) => PathBuf::from(path),
            None => build_artifact(repo_root, profile)?,
        };
        let values = parse_manifest(&manifest_path)?;

        require_value(&values, "format", FORMAT)?;
        require_value(&values, "features", "compat")?;
        require_value(&values, "profile", profile)?;

        let binary_value = values.get("binary").expect("required key checked");
        let binary_path = PathBuf::from(binary_value);
        if !binary_path.is_absolute() {
            bail!("compat artifact binary path is not absolute: {binary_value}");
        }
        let binary = binary_path
            .canonicalize()
            .with_context(|| format!("canonicalize compat binary {}", binary_path.display()))?;
        if !binary.is_file() {
            bail!("compat artifact binary is not a file: {}", binary.display());
        }
        #[cfg(unix)]
        if fs::metadata(&binary)?.permissions().mode() & 0o111 == 0 {
            bail!(
                "compat artifact binary is not executable: {}",
                binary.display()
            );
        }

        let expected_sha = values.get("sha256").expect("required key checked");
        if !is_lower_hex(expected_sha, 64) {
            bail!("compat artifact sha256 must be 64 lowercase hex");
        }
        let actual_sha = sha256_file(&binary)?;
        if &actual_sha != expected_sha {
            bail!(
                "compat artifact SHA-256 mismatch: manifest={} actual={}",
                expected_sha,
                actual_sha
            );
        }

        let expected_head = values.get("git_head").expect("required key checked");
        if !is_lower_hex(expected_head, 40) {
            bail!("compat artifact git_head must be 40 lowercase hex");
        }
        let current_head = git_head(repo_root)?;
        if &current_head != expected_head {
            bail!(
                "compat artifact git head mismatch: manifest={} current={}",
                expected_head,
                current_head
            );
        }

        if let Some(default_binary) = std::env::var_os("NOVAROCKS_BIN") {
            let default_binary = PathBuf::from(default_binary)
                .canonicalize()
                .context("canonicalize NOVAROCKS_BIN")?;
            if default_binary == binary {
                bail!("default and compat artifacts are identical");
            }
        }

        let probe_manifest_path = manifest_path
            .parent()
            .context("compat artifact manifest has no parent directory")?
            .join("probe-manifest.txt");
        let probe_values = parse_probe_manifest(&probe_manifest_path)?;
        require_probe_value(&probe_values, "format", PROBE_FORMAT)?;
        require_probe_value(&probe_values, "features", "compat")?;
        require_probe_value(&probe_values, "profile", profile)?;

        let probe_path_value = probe_values.get("path").expect("required key checked");
        let probe_path = PathBuf::from(probe_path_value);
        if !probe_path.is_absolute() {
            bail!("compat probe path is not absolute: {probe_path_value}");
        }
        let probe_binary = probe_path
            .canonicalize()
            .with_context(|| format!("canonicalize compat probe {}", probe_path.display()))?;
        if !probe_binary.is_file() {
            bail!("compat probe is not a file: {}", probe_binary.display());
        }
        #[cfg(unix)]
        if fs::metadata(&probe_binary)?.permissions().mode() & 0o111 == 0 {
            bail!("compat probe is not executable: {}", probe_binary.display());
        }

        let expected_probe_sha = probe_values.get("sha256").expect("required key checked");
        if !is_lower_hex(expected_probe_sha, 64) {
            bail!("compat probe sha256 must be 64 lowercase hex");
        }
        let probe_sha256 = sha256_file(&probe_binary)?;
        if &probe_sha256 != expected_probe_sha {
            bail!(
                "compat probe SHA-256 mismatch: manifest={} actual={}",
                expected_probe_sha,
                probe_sha256
            );
        }

        let expected_probe_head = probe_values.get("git_head").expect("required key checked");
        if !is_lower_hex(expected_probe_head, 40) {
            bail!("compat probe git_head must be 40 lowercase hex");
        }
        if expected_probe_head != &current_head {
            bail!(
                "compat probe git head mismatch: manifest={} current={}",
                expected_probe_head,
                current_head
            );
        }

        Ok(Self {
            binary,
            sha256: actual_sha,
            git_head: current_head,
            profile: profile.to_string(),
            probe_binary,
            probe_sha256,
        })
    }
}

fn build_artifact(repo_root: &Path, profile: &str) -> Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let output_dir = repo_root.join(format!(
        ".sql-test-runner-runtime/compat-artifact-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("create compat artifact runtime {}", output_dir.display()))?;

    let builder = repo_root.join("tools/ci/build-compat-artifact.sh");
    let output = Command::new(&builder)
        .args(["--profile", profile, "--output-dir"])
        .arg(&output_dir)
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("run compat artifact builder {}", builder.display()))?;
    if !output.status.success() {
        bail!(
            "compat artifact builder failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output_dir.join("manifest.txt"))
}

fn parse_manifest(path: &Path) -> Result<BTreeMap<String, String>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read compat artifact manifest {}", path.display()))?;
    let mut values = BTreeMap::new();
    for (index, line) in contents.lines().enumerate() {
        let (key, value) = line.split_once('=').ok_or_else(|| {
            anyhow::anyhow!(
                "invalid compat artifact manifest line {}: {}",
                index + 1,
                line
            )
        })?;
        if !MANIFEST_KEYS.contains(&key) {
            bail!("unknown manifest key: {key}");
        }
        if values.insert(key.to_string(), value.to_string()).is_some() {
            bail!("duplicate manifest key: {key}");
        }
    }
    for key in MANIFEST_KEYS {
        if !values.contains_key(key) {
            bail!("missing manifest key: {key}");
        }
    }
    Ok(values)
}

fn parse_probe_manifest(path: &Path) -> Result<BTreeMap<String, String>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read compat probe manifest {}", path.display()))?;
    let mut values = BTreeMap::new();
    for (index, line) in contents.lines().enumerate() {
        let (key, value) = line.split_once('=').ok_or_else(|| {
            anyhow::anyhow!(
                "invalid compat probe manifest line {}: {}",
                index + 1,
                line
            )
        })?;
        if !PROBE_MANIFEST_KEYS.contains(&key) {
            bail!("unknown probe manifest key: {key}");
        }
        if values.insert(key.to_string(), value.to_string()).is_some() {
            bail!("duplicate probe manifest key: {key}");
        }
    }
    for key in PROBE_MANIFEST_KEYS {
        if !values.contains_key(key) {
            bail!("missing probe manifest key: {key}");
        }
    }
    Ok(values)
}

fn require_value(values: &BTreeMap<String, String>, key: &str, expected: &str) -> Result<()> {
    let actual = values.get(key).expect("required key checked");
    if actual != expected {
        bail!("invalid manifest {key}: expected {expected}, got {actual}");
    }
    Ok(())
}

fn require_probe_value(
    values: &BTreeMap<String, String>,
    key: &str,
    expected: &str,
) -> Result<()> {
    let actual = values.get(key).expect("required key checked");
    if actual != expected {
        bail!("invalid probe manifest {key}: expected {expected}, got {actual}");
    }
    Ok(())
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn git_head(repo_root: &Path) -> Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("read git head from {}", repo_root.display()))?;
    if !output.status.success() {
        bail!(
            "git rev-parse HEAD failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let head = String::from_utf8(output.stdout).context("git head is not UTF-8")?;
    let head = head.trim().to_string();
    if !is_lower_hex(&head, 40) {
        bail!("current git head must be 40 lowercase hex: {head}");
    }
    Ok(head)
}

#[cfg(test)]
mod tests {
    use super::CompatArtifact;
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct EnvGuard {
        keys: Vec<(&'static str, Option<std::ffi::OsString>)>,
    }

    impl EnvGuard {
        fn capture(keys: &[&'static str]) -> Self {
            Self {
                keys: keys
                    .iter()
                    .map(|key| (*key, std::env::var_os(key)))
                    .collect(),
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in &self.keys {
                match value {
                    Some(value) => unsafe { std::env::set_var(key, value) },
                    None => unsafe { std::env::remove_var(key) },
                }
            }
        }
    }

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("canonical repo root")
    }

    fn test_dir(repo_root: &Path) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = repo_root.join(format!(
            ".sql-test-runner-runtime/compat-artifact-test-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create compat artifact test dir");
        path
    }

    fn git_head(repo_root: &Path) -> String {
        let output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(repo_root)
            .output()
            .expect("run git rev-parse");
        assert!(output.status.success());
        String::from_utf8(output.stdout)
            .expect("git head utf8")
            .trim()
            .to_string()
    }

    fn sha256(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn write_executable(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).expect("write executable");
        let mut permissions = fs::metadata(path).expect("binary metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("make executable");
    }

    fn manifest_text(binary: &Path, sha256: &str, git_head: &str) -> String {
        format!(
            "format=novarocks-compat-artifact-v1\nbinary={}\nsha256={}\ngit_head={}\nprofile=dev-opt\nfeatures=compat\n",
            binary.display(),
            sha256,
            git_head
        )
    }

    fn probe_manifest_text(probe: &Path, sha256: &str, git_head: &str) -> String {
        format!(
            "format=novarocks-compat-probe-v1\npath={}\nsha256={}\ngit_head={}\nprofile=dev-opt\nfeatures=compat\n",
            probe.display(),
            sha256,
            git_head
        )
    }

    fn resolve_error(repo_root: &Path) -> String {
        CompatArtifact::resolve(repo_root, "dev-opt")
            .expect_err("compat artifact validation must fail")
            .to_string()
    }

    #[test]
    fn compat_artifact_resolve_enforces_build_and_integrity_contract() {
        let _env = EnvGuard::capture(&[
            "NOVAROCKS_COMPAT_ARTIFACT_MANIFEST",
            "NOVAROCKS_COMPAT_PROBE_BIN",
            "NOVAROCKS_BIN",
            "SCT_COMPAT_BUILD_HOOK",
        ]);
        unsafe {
            std::env::remove_var("NOVAROCKS_COMPAT_ARTIFACT_MANIFEST");
            std::env::remove_var("NOVAROCKS_COMPAT_PROBE_BIN");
            std::env::remove_var("NOVAROCKS_BIN");
            std::env::remove_var("SCT_COMPAT_BUILD_HOOK");
        }

        let repo_root = repo_root();
        let root = test_dir(&repo_root);
        let binary = root.join("novarocks-compat");
        let bytes = b"#!/usr/bin/env bash\nexit 0\n";
        write_executable(&binary, bytes);
        let probe = root.join("starrocks-compat-probe");
        let probe_bytes = b"#!/usr/bin/env bash\necho probe\n";
        write_executable(&probe, probe_bytes);
        let head = git_head(&repo_root);
        let manifest = root.join("manifest.txt");
        let probe_manifest = root.join("probe-manifest.txt");
        fs::write(&manifest, manifest_text(&binary, &sha256(bytes), &head))
            .expect("write valid manifest");
        fs::write(
            &probe_manifest,
            probe_manifest_text(&probe, &sha256(probe_bytes), &head),
        )
        .expect("write valid probe manifest");
        let must_not_build = root.join("must-not-build.sh");
        fs::write(
            &must_not_build,
            "#!/usr/bin/env bash\necho builder must not run >&2\nexit 99\n",
        )
        .expect("write rejecting builder hook");
        let mut permissions = fs::metadata(&must_not_build)
            .expect("rejecting hook metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&must_not_build, permissions).expect("make rejecting hook executable");
        unsafe {
            std::env::set_var("NOVAROCKS_COMPAT_ARTIFACT_MANIFEST", &manifest);
            std::env::set_var("SCT_COMPAT_BUILD_HOOK", &must_not_build);
        }

        let artifact = CompatArtifact::resolve(&repo_root, "dev-opt").expect("valid artifact");
        assert_eq!(
            artifact.binary,
            binary.canonicalize().expect("canonical binary")
        );
        assert_eq!(artifact.sha256, sha256(bytes));
        assert_eq!(artifact.git_head, head);
        assert_eq!(artifact.profile, "dev-opt");
        assert_eq!(
            artifact.probe_binary,
            probe.canonicalize().expect("canonical probe binary")
        );
        assert_eq!(artifact.probe_sha256, sha256(probe_bytes));

        fs::write(&probe, b"tampered probe\n").expect("tamper probe binary");
        assert!(resolve_error(&repo_root).contains("probe SHA-256 mismatch"));
        write_executable(&probe, probe_bytes);

        fs::remove_file(&probe_manifest).expect("remove probe manifest");
        assert!(resolve_error(&repo_root).contains("probe-manifest.txt"));
        fs::write(
            &probe_manifest,
            probe_manifest_text(&probe, &sha256(probe_bytes), &head),
        )
        .expect("restore probe manifest");

        fs::write(
            &probe_manifest,
            format!(
                "{}profile=release\n",
                probe_manifest_text(&probe, &sha256(probe_bytes), &head)
            ),
        )
        .expect("write duplicate probe manifest key");
        assert!(resolve_error(&repo_root).contains("duplicate probe manifest key: profile"));

        fs::write(
            &probe_manifest,
            format!(
                "{}unexpected=value\n",
                probe_manifest_text(&probe, &sha256(probe_bytes), &head)
            ),
        )
        .expect("write unknown probe manifest key");
        assert!(resolve_error(&repo_root).contains("unknown probe manifest key: unexpected"));

        fs::write(
            &probe_manifest,
            probe_manifest_text(
                &probe,
                &sha256(probe_bytes),
                "0000000000000000000000000000000000000000",
            ),
        )
        .expect("write stale probe manifest");
        assert!(resolve_error(&repo_root).contains("probe git head mismatch"));
        fs::write(
            &probe_manifest,
            probe_manifest_text(&probe, &sha256(probe_bytes), &head),
        )
        .expect("restore valid probe manifest");

        unsafe { std::env::set_var("NOVAROCKS_COMPAT_PROBE_BIN", &probe) };
        assert!(resolve_error(&repo_root).contains("NOVAROCKS_COMPAT_PROBE_BIN"));
        unsafe { std::env::remove_var("NOVAROCKS_COMPAT_PROBE_BIN") };

        let mut permissions = fs::metadata(&binary)
            .expect("binary metadata")
            .permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&binary, permissions).expect("make binary non-executable");
        assert!(resolve_error(&repo_root).contains("not executable"));
        write_executable(&binary, bytes);

        fs::write(&binary, b"tampered\n").expect("tamper binary");
        assert!(resolve_error(&repo_root).contains("SHA-256 mismatch"));
        write_executable(&binary, bytes);

        unsafe { std::env::set_var("NOVAROCKS_BIN", &binary) };
        assert_eq!(
            resolve_error(&repo_root),
            "default and compat artifacts are identical"
        );
        unsafe { std::env::remove_var("NOVAROCKS_BIN") };

        fs::write(
            &manifest,
            format!(
                "{}profile=release\n",
                manifest_text(&binary, &sha256(bytes), &head)
            ),
        )
        .expect("write duplicate-key manifest");
        assert!(resolve_error(&repo_root).contains("duplicate manifest key: profile"));

        fs::write(
            &manifest,
            manifest_text(&binary, &sha256(bytes), &head).replace("features=compat\n", ""),
        )
        .expect("write missing-key manifest");
        assert!(resolve_error(&repo_root).contains("missing manifest key: features"));

        fs::write(
            &manifest,
            format!(
                "{}unexpected=value\n",
                manifest_text(&binary, &sha256(bytes), &head)
            ),
        )
        .expect("write unknown-key manifest");
        assert!(resolve_error(&repo_root).contains("unknown manifest key: unexpected"));

        fs::write(
            &manifest,
            manifest_text(
                &binary,
                &sha256(bytes),
                "0000000000000000000000000000000000000000",
            ),
        )
        .expect("write stale-head manifest");
        assert!(resolve_error(&repo_root).contains("git head mismatch"));

        let hook = root.join("fake-builder.sh");
        fs::write(
            &hook,
            r##"#!/usr/bin/env bash
set -euo pipefail
test "$*" = "cargo build --profile dev-opt --features compat --bin novarocks --bin starrocks-compat-probe"
mkdir -p "$CARGO_TARGET_DIR/dev-opt"
printf '%s\n' '#!/usr/bin/env bash' 'exit 0' >"$CARGO_TARGET_DIR/dev-opt/novarocks"
chmod +x "$CARGO_TARGET_DIR/dev-opt/novarocks"
printf '%s\n' '#!/usr/bin/env bash' 'exit 0' >"$CARGO_TARGET_DIR/dev-opt/starrocks-compat-probe"
chmod +x "$CARGO_TARGET_DIR/dev-opt/starrocks-compat-probe"
"##,
        )
        .expect("write fake builder");
        let mut permissions = fs::metadata(&hook).expect("hook metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&hook, permissions).expect("make hook executable");
        unsafe {
            std::env::remove_var("NOVAROCKS_COMPAT_ARTIFACT_MANIFEST");
            std::env::set_var("SCT_COMPAT_BUILD_HOOK", &hook);
        }

        let built = CompatArtifact::resolve(&repo_root, "dev-opt").expect("auto-built artifact");
        assert!(built.binary.is_absolute());
        assert!(built.binary.is_file());
        assert!(
            built
                .binary
                .starts_with(repo_root.join(".sql-test-runner-runtime"))
        );
        assert_eq!(built.git_head, git_head(&repo_root));
        assert_eq!(built.profile, "dev-opt");
        assert!(built.probe_binary.is_file());
        assert_eq!(
            built.probe_binary.parent(),
            built.binary.parent(),
            "the independently proven probe remains in the same artifact bin directory"
        );

        fs::remove_dir_all(&root).expect("cleanup compat artifact test dir");
        if let Some(runtime_dir) = built.binary.parent().and_then(Path::parent) {
            fs::remove_dir_all(runtime_dir).expect("cleanup auto-build runtime dir");
        }
    }
}
